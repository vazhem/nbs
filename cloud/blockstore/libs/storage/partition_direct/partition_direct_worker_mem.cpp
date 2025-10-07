#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/storage/core/libs/opentelemetry/impl/helpers.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>
#include <contrib/ydb/library/wilson_ids/wilson.h>

#include "partition_direct_storage.h"
#include "partition_direct_worker.h"
#include "partition_direct_worker_mem.h"
#include "partition_direct_state.h"  // For TRegionChunkMapping

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NBlockStore;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////
// Memory Worker Storage - serves requests immediately from memory with zero-filled data

class TMemoryWorkerStorage final
{
private:
    const TActorId OwnerActorId;
    const TWorkerStorageConfig Config;

public:
    explicit TMemoryWorkerStorage(TActorId ownerActorId, const TWorkerStorageConfig& config)
        : OwnerActorId(ownerActorId)
        , Config(config)
    {
    }

    NCloud::NProto::TError ReadBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    NCloud::NProto::TError WriteBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    NCloud::NProto::TError ZeroBlocks(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        NWilson::TTraceId traceId);
};

////////////////////////////////////////////////////////////////////////////////
// TMemoryWorkerStorage implementation

NCloud::NProto::TError TMemoryWorkerStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(traceId);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TMemoryWorkerStorage::ReadBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // Fill the buffer with zeros
    auto guard = request->Sglist.Acquire();
    if (guard) {
        // Fill buffer with zeros
        memset(const_cast<char*>(guard.Get()[0].Data()), 0, size);
    } else {
        return MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    // Send response immediately
    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
    ctx.Send(requestInfo->Sender, response.release(), 0, requestInfo->Cookie);

    return {};  // No error
}

NCloud::NProto::TError TMemoryWorkerStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(traceId);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->BlocksCount;
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TMemoryWorkerStorage::WriteBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // For write requests, just send success response immediately
    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
    ctx.Send(requestInfo->Sender, response.release(), 0, requestInfo->Cookie);

    return {};  // No error
}

NCloud::NProto::TError TMemoryWorkerStorage::ZeroBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(ctx, traceId, request);

    // For zero blocks, just send success response immediately
    auto response = std::make_unique<TEvService::TEvZeroBlocksResponse>();
    ctx.Send(requestInfo->Sender, response.release(), 0, requestInfo->Cookie);

    return {};  // No error
}

////////////////////////////////////////////////////////////////////////////////
// Memory Worker Actor

class TPartitionDirectWorkerMemActor
    : public NActors::TActor<TPartitionDirectWorkerMemActor>
{
private:
    const ui32 WorkerId;
    TWorkerStorageConfig Config;
    std::unique_ptr<TMemoryWorkerStorage> Storage;

public:
    TPartitionDirectWorkerMemActor(
        ui32 workerId,
        const TWorkerStorageConfig& config)
        : TActor(&TThis::StateWork)
        , WorkerId(workerId)
        , Config(config)
    {
        // Create memory worker storage
        Storage = std::make_unique<TMemoryWorkerStorage>(SelfId(), Config);
    }

private:
    STFUNC(StateWork);

    void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Config update handler
    void HandleUpdateConfig(
        const TEvPartitionDirectWorker::TEvUpdateConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool ValidateBlockRange(ui64 blockIndex, ui32 blocksCount) const
    {
        return blockIndex + blocksCount <= Config.BlockCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionDirectWorkerMemActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocalRequest);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocalRequest);

        // Config update handler
        HFunc(TEvPartitionDirectWorker::TEvUpdateConfig, HandleUpdateConfig);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TPartitionDirectWorkerMemActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = msg->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[MemWorker%u] Received ReadBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
        WorkerId,
        requestId,
        record.GetStartIndex(),
        record.GetBlocksCount(),
        record.BlockSize);

    // Validate block range
    if (!ValidateBlockRange(record.GetStartIndex(), record.GetBlocksCount())) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeError(E_ARGUMENT, "Invalid block range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto guard = record.Sglist.Acquire();
    if (!guard) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeError(E_CANCELLED, "Failed to acquire sglist"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // Create request info from original event to preserve sender/cookie/context
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[MemWorker%u] ReadBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        ev->TraceId.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->SetBlocksCount(record.GetBlocksCount());
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request), ev->TraceId.Clone());

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerMemActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = msg->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[MemWorker%u] Received WriteBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
        WorkerId,
        requestId,
        record.GetStartIndex(),
        record.BlocksCount,
        record.BlockSize);

    // Validate block range
    if (!ValidateBlockRange(record.GetStartIndex(), record.BlocksCount)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            MakeError(E_ARGUMENT, "Invalid block range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto guard = record.Sglist.Acquire();
    if (!guard) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            MakeError(E_CANCELLED, "Failed to acquire sglist"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // Create request info from original event to preserve sender/cookie/context
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[MemWorker%u] WriteBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        ev->TraceId.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->BlocksCount = record.BlocksCount;
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request), ev->TraceId.Clone());

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerMemActor::HandleUpdateConfig(
    const TEvPartitionDirectWorker::TEvUpdateConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[MemWorker%u] Received config update: BlockSize=%u, BlockCount=%lu",
        WorkerId,
        msg->Config.BlockSize,
        msg->Config.BlockCount);

    // Update the config
    Config = msg->Config;
}

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionDirectWorkerMemActor(
    ui32 workerId,
    const TWorkerStorageConfig& config)
{
    return new TPartitionDirectWorkerMemActor(workerId, config);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
