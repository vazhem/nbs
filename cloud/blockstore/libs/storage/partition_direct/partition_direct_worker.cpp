#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

#include "partition_direct_storage.h"
#include "partition_direct_storage_proxy.h"
#include "partition_direct_worker.h"
#include "partition_direct_state.h"  // For TRegionChunkMapping

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////
// Worker-specific storage implementation

class TWorkerStorage final : public TPartitionStorage
{
private:
    const TActorId OwnerActorId;
    const TWorkerStorageConfig Config;
    ui64 NextRequestId = 1;
    THashMap<ui64, TDDiskRequestContext> PendingRequests;

public:
    explicit TWorkerStorage(TActorId ownerActorId, const TWorkerStorageConfig& config)
        : OwnerActorId(ownerActorId)
        , Config(config)
    {
    }

    NCloud::NProto::TError ReadBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId = {}) override;

    NCloud::NProto::TError WriteBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId = {}) override;

    NCloud::NProto::TError ZeroBlocks(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        const NWilson::TTraceId& traceId = {}) override;

    // Methods for handling YDB DDisk responses - called by worker actor
    void HandleDDiskReadResponse(
        const NActors::TActorContext& ctx,
        const TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev);

    void HandleDDiskWriteResponse(
        const NActors::TActorContext& ctx,
        const TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev);

private:
    ui64 GenerateRequestId() {
        return NextRequestId++;
    }

    NCloud::NProto::TError SendReadToDDisks(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        ui64 offset,
        ui32 size,
        const NWilson::TTraceId& traceId);

    NCloud::NProto::TError SendWriteToDDisks(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        ui64 offset,
        ui32 size,
        const TString& data,
        const NWilson::TTraceId& traceId);
};

////////////////////////////////////////////////////////////////////////////////
// TWorkerStorage implementation

NCloud::NProto::TError TWorkerStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId)
{
    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::ReadBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create request context
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Read,
        requestInfo,
        requestId,
        Config.DDiskServiceIds,  // Use all DDisk IDs for now
        offset,
        size);

    requestCtx.OriginalReadRequest = request;
    requestCtx.ReadData.ReserveAndResize(size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send read request to DDisk
    return SendReadToDDisks(ctx, requestId, offset, size, traceId);
}

NCloud::NProto::TError TWorkerStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    const NWilson::TTraceId& traceId)
{
    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->BlocksCount;
    const ui64 offset = startIndex * Config.BlockSize;
    const ui32 size = blocksCount * Config.BlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::WriteBlocksLocal: startIndex=" << startIndex
        << " blocksCount=" << blocksCount << " offset=" << offset << " size=" << size);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Extract data from sglist
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    TString data(guard.Get()[0].Data(), size);

    // Create request context
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Write,
        requestInfo,
        requestId,
        Config.DDiskServiceIds,  // Use all DDisk IDs for now
        offset,
        size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send write request to DDisk
    return SendWriteToDDisks(ctx, requestId, offset, size, data, traceId);
}

NCloud::NProto::TError TWorkerStorage::ZeroBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(ctx, requestInfo, request, traceId);
    // Not implemented for now
    return MakeError(E_NOT_IMPLEMENTED, "ZeroBlocks not implemented in worker storage");
}

NCloud::NProto::TError TWorkerStorage::SendReadToDDisks(
    const NActors::TActorContext& ctx,
    ui64 requestId,
    ui64 offset,
    ui32 size,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(traceId);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::SendReadToDDisks: requestId=" << requestId
        << " offset=" << offset << " size=" << size
        << " regionCacheSize=" << Config.RegionChunkCache.size());

    // Find chunk for this offset
    ui32 chunkId;
    TString ddiskServiceId;
    ui32 chunkRelativeOffset;

    if (!Config.FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "TWorkerStorage::SendReadToDDisks: FindChunkForOffset failed for offset " << offset);
        return MakeError(E_FAIL, "Cannot find chunk for offset");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::SendReadToDDisks FOUND CHUNK: chunkId=" << chunkId
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
        "SendReadToDDisks: requestId=" << requestId << " chunkId=" << chunkId
        << " offset=" << chunkRelativeOffset << " size=" << size);

    // Create DDisk read request
    auto request = std::make_unique<TEvBlobStorage::TEvDDiskReadRequest>();
    request->Record.SetChunkId(chunkId);
    request->Record.SetOffset(chunkRelativeOffset);
    request->Record.SetSize(size);

    ctx.Send(ddiskActorId, request.release(), 0, requestId);

    return {};  // No error
}

NCloud::NProto::TError TWorkerStorage::SendWriteToDDisks(
    const NActors::TActorContext& ctx,
    ui64 requestId,
    ui64 offset,
    ui32 size,
    const TString& data,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(traceId);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::SendWriteToDDisks: requestId=" << requestId
        << " offset=" << offset << " size=" << size << " dataSize=" << data.size()
        << " regionCacheSize=" << Config.RegionChunkCache.size());

    // Find chunk for this offset
    ui32 chunkId;
    TString ddiskServiceId;
    ui32 chunkRelativeOffset;

    // DEBUG: Log calculation details BEFORE calling FindChunkForOffset
    ui32 debugGroupIndex = Config.CalculateGroupIndex(offset);
    ui64 debugGroupOffset = Config.CalculateGroupOffset(offset);
    ui32 debugChunkIndexInGroup = static_cast<ui32>(debugGroupOffset / Config.ChunkSize);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::SendWriteToDDisks DEBUG CALC: offset=" << offset
        << " ChunkSize=" << Config.ChunkSize
        << " groupIndex=" << debugGroupIndex
        << " groupOffset=" << debugGroupOffset
        << " chunkIndexInGroup=" << debugChunkIndexInGroup
        << " expectedChunkRelativeOffset=" << (debugGroupOffset - (debugChunkIndexInGroup * Config.ChunkSize)));

    if (!Config.FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "TWorkerStorage::SendWriteToDDisks: FindChunkForOffset failed for offset " << offset);
        return MakeError(E_FAIL, "Cannot find chunk for offset");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "TWorkerStorage::SendWriteToDDisks FOUND CHUNK: chunkId=" << chunkId
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
        "SendWriteToDDisks: requestId=" << requestId << " chunkId=" << chunkId
        << " offset=" << chunkRelativeOffset << " size=" << size);

    // Create DDisk write request
    auto request = std::make_unique<TEvBlobStorage::TEvDDiskWriteRequest>();
    request->Record.SetChunkId(chunkId);
    request->Record.SetOffset(chunkRelativeOffset);
    request->Record.SetSize(size);  // CRITICAL FIX: Set size explicitly
    request->Record.SetData(data);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "SendWriteToDDisks: FIXED DDisk request chunkId=" << chunkId
        << " offset=" << chunkRelativeOffset << " size=" << size
        << " dataSize=" << data.size());

    ctx.Send(ddiskActorId, request.release(), 0, requestId);

    return {};  // No error
}

void TWorkerStorage::HandleDDiskReadResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev)
{
    const ui64 requestId = ev->Cookie;
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "HandleDDiskReadResponse: requestId=" << requestId << " status=" << record.GetStatus());

    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "Unknown request ID in DDisk read response: " << requestId);
        return;
    }

    auto& requestCtx = it->second;

    if (record.GetStatus() == NKikimrProto::OK) {
        // Copy data to read buffer
        requestCtx.ReadData = record.GetData();

        // Complete the read request
        auto guard = requestCtx.OriginalReadRequest->Sglist.Acquire();
        if (guard) {
            memcpy(const_cast<char*>(guard.Get()[0].Data()),
                   requestCtx.ReadData.data(),
                   requestCtx.ReadData.size());
        }

        // Send response
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
        ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0, requestCtx.OriginalRequest->Cookie);
    } else {
        // Handle error
        auto error = MakeError(E_IO, TStringBuilder() << "DDisk read failed: " << record.GetErrorReason());
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0, requestCtx.OriginalRequest->Cookie);
    }

    // Remove completed request
    PendingRequests.erase(it);
}

void TWorkerStorage::HandleDDiskWriteResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev)
{
    const ui64 requestId = ev->Cookie;
    const auto& record = ev->Get()->Record;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "HandleDDiskWriteResponse: requestId=" << requestId << " status=" << record.GetStatus());

    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "Unknown request ID in DDisk write response: " << requestId);
        return;
    }

    auto& requestCtx = it->second;

    if (record.GetStatus() == NKikimrProto::OK) {
        // Send success response
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
        ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0, requestCtx.OriginalRequest->Cookie);
    } else {
        // Handle error
        auto error = MakeError(E_IO, TStringBuilder() << "DDisk write failed: " << record.GetErrorReason());
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        ctx.Send(requestCtx.OriginalRequest->Sender, response.release(), 0, requestCtx.OriginalRequest->Cookie);
    }

    // Remove completed request
    PendingRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////
// TWorkerStorageConfig implementation

bool TWorkerStorageConfig::FindChunkForOffset(ui64 offset, ui32& chunkId, TString& ddiskServiceId, ui32& chunkRelativeOffset) const
{
    // Step 1: Use original striping logic to determine the group (exactly like TPartitionState)
    ui32 groupIndex = CalculateGroupIndex(offset);
    ui64 groupOffset = CalculateGroupOffset(offset);

    // Step 2: Calculate which region this group offset belongs to
    // CRITICAL: This calculation must match the allocation logic
    ui32 chunkIndexInGroup = static_cast<ui32>(groupOffset / ChunkSize);

    // Step 3: Determine which region should handle this chunk index in the group
    if (RegionChunkCache.empty()) {
        // CRITICAL ERROR: No regions allocated - this should not happen
        return false;
    }

    // CRITICAL: This must match CompletePreallocateVolumeChunks logic
    // Use RegionChunkCache.size() as totalRegions (should match GetTotalRegionsNeeded())
    ui64 totalRegions = RegionChunkCache.size();
    ui64 targetRegionIndex = chunkIndexInGroup % totalRegions;

    // Step 4: Get the chunk ID from the region mapping
    auto regionIt = RegionChunkCache.find(targetRegionIndex);
    if (regionIt == RegionChunkCache.end()) {
        // Region not found - this is now a CRITICAL ERROR
        return false;
    }

    const auto& regionMapping = regionIt->second;
    if (groupIndex >= regionMapping.ChunkIds.size() ||
        regionMapping.ChunkIds[groupIndex] == 0) {
        // Group not found or zero in region - CRITICAL ERROR
        return false;
    }

    chunkId = regionMapping.ChunkIds[groupIndex];
    ddiskServiceId = regionMapping.DDiskServiceIds[groupIndex];
    chunkRelativeOffset = static_cast<ui32>(groupOffset - (chunkIndexInGroup * ChunkSize));

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionDirectWorkerActor
    : public NActors::TActor<TPartitionDirectWorkerActor>
{
private:
    const ui32 WorkerId;
    const TWorkerStorageConfig Config;
    TPartitionStoragePtr Storage;  // Worker's own storage instance

public:
    TPartitionDirectWorkerActor(
        ui32 workerId,
        const TWorkerStorageConfig& config)
        : TActor(&TThis::StateWork)
        , WorkerId(workerId)
        , Config(config)
    {
        // Create worker's own storage instance using the config
        Storage = std::make_shared<TWorkerStorage>(SelfId(), Config);
    }


private:
    STFUNC(StateWork);

    void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    // DDisk response handlers (needed for storage operations)
    void HandleDDiskReadResponse(
        const NKikimr::TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDDiskWriteResponse(
        const NKikimr::TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool ValidateBlockRange(ui64 blockIndex, ui32 blocksCount) const
    {
        return blockIndex + blocksCount <= Config.BlockCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

STFUNC(TPartitionDirectWorkerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocalRequest);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocalRequest);

        // DDisk response handlers
        HFunc(NKikimr::TEvBlobStorage::TEvDDiskReadResponse, HandleDDiskReadResponse);
        HFunc(NKikimr::TEvBlobStorage::TEvDDiskWriteResponse, HandleDDiskWriteResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TPartitionDirectWorkerActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] Received ReadBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
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

    // Create new trace id for this worker
    NWilson::TTraceId trace = NWilson::TTraceId::NewTraceId(1, 4095);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] ReadBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        trace.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->SetBlocksCount(record.GetBlocksCount());
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request), trace);

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] Received WriteBlocksLocal request #%lu (offset: %lu, count: %lu, bs: %lu)",
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

    // Create new trace id for this worker
    NWilson::TTraceId trace = NWilson::TTraceId::NewTraceId(1, 4095);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] WriteBlocks: requestId=%lu -> traceId=%s",
        WorkerId,
        requestId,
        trace.GetHexTraceId().c_str());

    // Create the request object for storage
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->BlocksCount = record.BlocksCount;
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;

    auto error = Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request), trace);

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
}

void TPartitionDirectWorkerActor::HandleDDiskReadResponse(
    const NKikimr::TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] Received DDisk read response from %s",
        WorkerId,
        ev->Sender.ToString().c_str());

    // Forward DDisk responses to worker storage for processing
    auto* workerStorage = dynamic_cast<TWorkerStorage*>(Storage.get());
    if (workerStorage) {
        workerStorage->HandleDDiskReadResponse(ctx, ev);
    } else {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[Worker%u] Storage is not TWorkerStorage - cannot handle DDisk response",
            WorkerId);
    }
}

void TPartitionDirectWorkerActor::HandleDDiskWriteResponse(
    const NKikimr::TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION_WORKER,
        "[Worker%u] Received DDisk write response from %s",
        WorkerId,
        ev->Sender.ToString().c_str());

    // Forward DDisk responses to worker storage for processing
    auto* workerStorage = dynamic_cast<TWorkerStorage*>(Storage.get());
    if (workerStorage) {
        workerStorage->HandleDDiskWriteResponse(ctx, ev);
    } else {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION_WORKER,
            "[Worker%u] Storage is not TWorkerStorage - cannot handle DDisk response",
            WorkerId);
    }
}

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionDirectWorkerActor(
    ui32 workerId,
    const TWorkerStorageConfig& config)
{
    return new TPartitionDirectWorkerActor(workerId, config);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
