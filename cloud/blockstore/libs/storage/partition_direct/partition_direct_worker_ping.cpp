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
#include <contrib/ydb/core/blobstorage/ddisk/ddisk_events.h>

#include "partition_direct_storage.h"
#include "partition_direct_worker.h"
#include "partition_direct_worker_ping.h"
#include "partition_direct_state.h"  // For TRegionChunkMapping

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NBlockStore;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////
// Ping Worker Storage - sends pings instead of actual read/write operations

class TPingWorkerStorage final
{
private:
    const TActorId OwnerActorId;
    const TWorkerStorageConfig Config;
    ui64 NextRequestId = 1;

    // Pending request context for ping mode
    struct TPendingPingRequest {
        TRequestInfoPtr OriginalRequest;
        ui64 RequestId;
        ui32 Size;  // Size of data to generate (all zeros)
        bool IsRead;  // True for read, false for write
        std::shared_ptr<NProto::TReadBlocksLocalRequest> OriginalReadRequest;
        NWilson::TSpan Span;

        TPendingPingRequest() = default;
        TPendingPingRequest(
            TRequestInfoPtr request,
            ui64 requestId,
            ui32 size,
            bool isRead,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> readRequest = nullptr,
            NWilson::TSpan&& span = NWilson::TSpan())
            : OriginalRequest(request)
            , RequestId(requestId)
            , Size(size)
            , IsRead(isRead)
            , OriginalReadRequest(std::move(readRequest))
            , Span(std::move(span))
        {}
    };

    THashMap<ui64, TPendingPingRequest> PendingRequests;

public:
    explicit TPingWorkerStorage(TActorId ownerActorId, const TWorkerStorageConfig& config)
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

    // Method for handling DDisk ping responses
    void HandleDDiskPingResponse(
        const NActors::TActorContext& ctx,
        const TEvBlobStorage::TEvDDiskPingResponse::TPtr& ev);

private:
    ui64 GenerateRequestId() {
        return NextRequestId++;
    }

    NCloud::NProto::TError SendPingToDDisk(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        ui64 offset,
        ui32 size,
        NWilson::TTraceId traceId);
};

////////////////////////////////////////////////////////////////////////////////
// TPingWorkerStorage implementation

NCloud::NProto::TError TPingWorkerStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TPingWorkerStorage::ReadBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create Wilson span for tracing async ping operations
    NWilson::TSpan asyncIOSpan(NKikimr::TWilson::BlobStorage, traceId.Clone(), "PartitionDirect.PingRead");

    // Create pending request context - don't move request yet
    TPendingPingRequest requestCtx(
        requestInfo,
        requestId,
        size,
        true,  // IsRead
        request,
        std::move(asyncIOSpan));

    // Get traceId from span before moving requestCtx
    NWilson::TTraceId spanTraceId = requestCtx.Span.GetTraceId();

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send ping request to DDisk with span's traceId
    return SendPingToDDisk(ctx, requestId, offset, size, std::move(spanTraceId));
}

NCloud::NProto::TError TPingWorkerStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->BlocksCount;
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TPingWorkerStorage::WriteBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create Wilson span for tracing async ping operations
    NWilson::TSpan asyncIOSpan(NKikimr::TWilson::BlobStorage, traceId.Clone(), "PartitionDirect.PingWrite");

    // Create pending request context
    TPendingPingRequest requestCtx(
        requestInfo,
        requestId,
        size,
        false,  // IsWrite
        nullptr,
        std::move(asyncIOSpan));

    // Get traceId from span before moving requestCtx
    NWilson::TTraceId spanTraceId = requestCtx.Span.GetTraceId();

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send ping request to DDisk with span's traceId
    return SendPingToDDisk(ctx, requestId, offset, size, std::move(spanTraceId));
}

NCloud::NProto::TError TPingWorkerStorage::ZeroBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(ctx, requestInfo, request, traceId);
    // Not implemented for now
    return MakeError(E_NOT_IMPLEMENTED, "ZeroBlocks not implemented in ping worker storage");
}

NCloud::NProto::TError TPingWorkerStorage::SendPingToDDisk(
    const NActors::TActorContext& ctx,
    ui64 requestId,
    ui64 offset,
    ui32 size,
    NWilson::TTraceId traceId)
{
    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TPingWorkerStorage::SendPingToDDisk: requestId=" << requestId
        << " traceId=" << traceId.GetHexTraceId()
        << " offset=" << offset << " size=" << size
        << " regionCacheSize=" << Config.RegionChunkCache.size());

    // Use FindChunkForOffset to select the DDisk, just like in regular worker
    ui32 chunkId;
    TString ddiskServiceId;
    ui32 chunkRelativeOffset;

    if (!Config.FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "TPingWorkerStorage::SendPingToDDisk: FindChunkForOffset failed for offset " << offset);
        return MakeError(E_FAIL, "Cannot find chunk for offset");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TPingWorkerStorage::SendPingToDDisk FOUND CHUNK: chunkId=" << chunkId
        << " ddiskServiceId=" << ddiskServiceId << " chunkRelativeOffset=" << chunkRelativeOffset);

    // Get DDisk actor ID
    NActors::TActorId ddiskActorId;
    for (const auto& actorId : Config.DDiskServiceIds) {
        if (actorId.ToString() == ddiskServiceId) {
            ddiskActorId = actorId;
            break;
        }
    }

    if (!ddiskActorId) {
        return MakeError(E_FAIL, "Cannot find DDisk actor for service ID");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "SendPingToDDisk: requestId=" << requestId
        << " ddiskActorId=" << ddiskActorId.ToString());

    // Create DDisk ping request
    auto request = std::make_unique<TEvBlobStorage::TEvDDiskPing>();

    PendingRequests[requestId].Span.Event("Send_TEvDDiskPing");
    // Send with TraceId for request tracing
    ctx.Send(new IEventHandle(ddiskActorId, ctx.SelfID, request.release(), 0,
        requestId, nullptr, std::move(traceId)));

    return {};  // No error
}

void TPingWorkerStorage::HandleDDiskPingResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskPingResponse::TPtr& ev)
{
    const ui64 requestId = ev->Cookie;
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "HandleDDiskPingResponse: requestId=" << requestId << " status=" << record.GetStatus());

    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "Unknown request ID in DDisk ping response: " << requestId);
        return;
    }

    // Get reference before erasing - we'll move data out of it
    auto& requestCtx = it->second;

    requestCtx.Span.Event("Received_TEvDDiskPingResponse");

    if (record.GetStatus() == NKikimrProto::OK) {
        if (requestCtx.IsRead) {
            // For read requests, fill the buffer with zeros
            // Access the request before it's moved
            auto originalReadRequest = requestCtx.OriginalReadRequest;
            if (originalReadRequest) {
                auto guard = originalReadRequest->Sglist.Acquire();
                if (guard) {
                    // Fill buffer with zeros
                    memset(const_cast<char*>(guard.Get()[0].Data()), 0, requestCtx.Size);
                }
            }

            // End Wilson span successfully before accessing members
            if (requestCtx.Span) {
                requestCtx.Span.EndOk();
            }

            // Send response - access members before moving
            auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
            ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0,
                     requestCtx.OriginalRequest->Cookie);
        } else {
            // End Wilson span successfully
            if (requestCtx.Span) {
                requestCtx.Span.EndOk();
            }

            // For write requests, just send success response
            auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
            ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0,
                     requestCtx.OriginalRequest->Cookie);
        }
    } else {
        // End Wilson span with error
        if (requestCtx.Span) {
            requestCtx.Span.EndError("DDisk ping failed");
        }

        // Handle error
        auto error = MakeError(E_IO, TStringBuilder() << "DDisk ping failed");
        if (requestCtx.IsRead) {
            auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
            ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0,
                     requestCtx.OriginalRequest->Cookie);
        } else {
            auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
            ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0,
                     requestCtx.OriginalRequest->Cookie);
        }
    }

    // Remove completed request
    PendingRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////
// Ping Worker Actor

class TPartitionDirectWorkerPingActor
    : public NActors::TActor<TPartitionDirectWorkerPingActor>
{
private:
    const ui32 WorkerId;
    TWorkerStorageConfig Config;
    std::unique_ptr<TPingWorkerStorage> Storage;

public:
    TPartitionDirectWorkerPingActor(
        ui32 workerId,
        const TWorkerStorageConfig& config)
        : TActor(&TThis::StateWork)
        , WorkerId(workerId)
        , Config(config)
    {
        // Create ping worker storage
        Storage = std::make_unique<TPingWorkerStorage>(SelfId(), Config);
    }

private:
    STFUNC(StateWork);

    void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    // DDisk ping response handler
    void HandleDDiskPingResponse(
        const TEvBlobStorage::TEvDDiskPingResponse::TPtr& ev,
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

STFUNC(TPartitionDirectWorkerPingActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocalRequest);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocalRequest);

        // DDisk ping response handler
        HFunc(TEvBlobStorage::TEvDDiskPingResponse, HandleDDiskPingResponse);

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

void TPartitionDirectWorkerPingActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] Received ReadBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
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
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] ReadBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        ev->TraceId.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->SetBlocksCount(record.GetBlocksCount());
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request), std::move(ev->TraceId.Clone()));

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerPingActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] Received WriteBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
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
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] WriteBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        ev->TraceId.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->BlocksCount = record.BlocksCount;
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request), std::move(ev->TraceId.Clone()));

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerPingActor::HandleDDiskPingResponse(
    const TEvBlobStorage::TEvDDiskPingResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] Received DDisk ping response from %s",
        WorkerId,
        ev->Sender.ToString().c_str());

    // Forward to storage for processing
    Storage->HandleDDiskPingResponse(ctx, ev);
}

void TPartitionDirectWorkerPingActor::HandleUpdateConfig(
    const TEvPartitionDirectWorker::TEvUpdateConfig::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[PingWorker%u] Received config update: DDiskCount=%lu, ChunkSize=%u, StorageType=%d",
        WorkerId,
        msg->Config.DDiskServiceIds.size(),
        msg->Config.ChunkSize,
        static_cast<int>(msg->Config.StorageType));

    // Update the config
    Config = msg->Config;
}

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionDirectWorkerPingActor(
    ui32 workerId,
    const TWorkerStorageConfig& config)
{
    return new TPartitionDirectWorkerPingActor(workerId, config);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
