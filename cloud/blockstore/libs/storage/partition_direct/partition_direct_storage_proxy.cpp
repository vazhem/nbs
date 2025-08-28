#include "partition_direct_storage_proxy.h"
#include "partition_direct_state.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/services/blobstorage_service_id.h>
#include <contrib/ydb/core/blobstorage/ddisk/ddisk_events.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

// Use DDisk events from blobstorage.h

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError TProxyStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    const auto& ddiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (ddiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui32 blockSize = PartitionState->GetBlockSize();

    // Calculate offset and size in bytes
    const ui64 offset = startIndex * blockSize;
    const ui32 size = blocksCount * blockSize;

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create request context with original request info
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Read,
        requestInfo,  // Use the provided original request info
        requestId,
        ddiskServiceIds,
        offset,
        size);

    // Store the original request for accessing sglist during completion
    requestCtx.OriginalReadRequest = request;
    requestCtx.ReadData.ReserveAndResize(size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send read request to DDisk actors
    auto sendResult = SendReadToDDisks(ctx, requestId, offset, size);
    if (NCloud::HasError(sendResult)) {
        // Remove the pending request - error will be handled at higher level
        PendingRequests.erase(requestId);
        return sendResult;
    }

    // Return immediately - response will be sent asynchronously
    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TProxyStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    const auto& ddiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (ddiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return NCloud::MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    // Use the provided original request info for tracking

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->BlocksCount;
    const ui32 blockSize = PartitionState->GetBlockSize();

    // Calculate offset and size in bytes
    const ui64 offset = startIndex * blockSize;
    const ui32 size = blocksCount * blockSize;

    // Extract data from all segments of sglist
    TString data;
    data.reserve(size);

    const auto& sglist = guard.Get();
    for (const auto& segment : sglist) {
        data.append(segment.Data(), segment.Size());
    }

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create request context
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Write,
        requestInfo,  // Use the provided original request info
        requestId,
        ddiskServiceIds,
        offset,
        size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send write request to DDisk actors
    auto sendResult = SendWriteToDDisks(ctx, requestId, offset, size, data);
    if (NCloud::HasError(sendResult)) {
        // Remove the pending request - error will be handled at higher level
        PendingRequests.erase(requestId);
        return sendResult;
    }

    // Return immediately - response will be sent asynchronously
    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TProxyStorage::ZeroBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    const auto& ddiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (ddiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    // Use the provided original request info for tracking

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui32 blockSize = PartitionState->GetBlockSize();

    // Calculate offset and size in bytes
    const ui64 offset = startIndex * blockSize;
    const ui32 size = blocksCount * blockSize;

    // Create zero data
    TString zeroData(size, 0);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create request context with original request info
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Write,
        requestInfo,  // Use the provided original request info
        requestId,
        ddiskServiceIds,
        offset,
        size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Send write request to DDisk actors (zero data)
    auto sendResult = SendWriteToDDisks(ctx, requestId, offset, size, zeroData);
    if (NCloud::HasError(sendResult)) {
        // Remove the pending request - error will be handled at higher level
        PendingRequests.erase(requestId);
        return sendResult;
    }

    // Return immediately - response will be sent asynchronously
    return NCloud::MakeError(S_OK);
}

////////////////////////////////////////////////////////////////////////////////
// Helper methods

void TProxyStorage::SendZeroDataResponse(
    const TActorContext& ctx,
    ui64 requestId,
    ui32 size)
{
    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        return;
    }

    auto& requestCtx = it->second;
    // Fill the read data with zeros
    requestCtx.ReadData.assign(size, 0);

    // Complete the request successfully
    CompleteReadRequest(ctx, requestCtx);
    PendingRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////
// Private methods

NCloud::NProto::TError TProxyStorage::SendReadToDDisks(
    const TActorContext& ctx,
    ui64 requestId,
    ui64 offset,
    ui32 size)
{
    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        return NCloud::MakeError(E_FAIL, "Request not found");
    }

    auto& requestCtx = it->second;

    // For ErasureMirror3Direct, we can read from any replica
    // Choose first available DDisk for simplicity
    if (!requestCtx.DDiskActorIds.empty()) {
        const auto& targetActorId = requestCtx.DDiskActorIds[0];

        // Validate ActorId before sending
        if (!targetActorId) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << requestId << "] Invalid DDisk ActorId");
            return NCloud::MakeError(E_FAIL, "Invalid DDisk ActorId");
        }

        // Calculate which chunk this offset belongs to and get chunk info
        ui64 regionIndex = CalculateRegionIndex(offset);
        TChunkRegionInfo chunkInfo;
        if (!PartitionState->FindChunkForRegion(regionIndex, chunkInfo)) {
            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << requestId << "] No chunk allocated for offset " << offset
                << " (region " << regionIndex << ") - returning zero data");

            // Return zero data for unallocated regions
            SendZeroDataResponse(ctx, requestId, size);
            return NCloud::MakeError(S_OK);
        }

        // Calculate chunk-relative offset: offset - regionStartOffset
        ui64 regionStartOffset = chunkInfo.StartOffset;
        ui32 chunkRelativeOffset = static_cast<ui32>(offset - regionStartOffset);

        auto request = std::make_unique<TEvBlobStorage::TEvDDiskReadRequest>();
        request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
        request->Record.SetSize(size);
        request->Record.SetChunkId(chunkInfo.ChunkId);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] SendReadToDDisks: requestId=" << requestId
            << ", absoluteOffset=" << offset << ", chunkRelativeOffset=" << chunkRelativeOffset
            << ", size=" << size << ", chunkId=" << chunkInfo.ChunkId
            << ", target=" << targetActorId.ToString());

        // Set pending responses to 1 for reads (we only read from one DDisk)
        requestCtx.PendingResponses = 1;

        ctx.Send(targetActorId, request.release(), 0, requestId);
        return NCloud::MakeError(S_OK);
    }

    return NCloud::MakeError(E_FAIL, "No DDisk actors available");
}

NCloud::NProto::TError TProxyStorage::SendWriteToDDisks(
    const TActorContext& ctx,
    ui64 requestId,
    ui64 offset,
    ui32 size,
    const TString& data)
{
    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        return NCloud::MakeError(E_FAIL, "Request not found");
    }

    auto& requestCtx = it->second;

    // Calculate which chunk this offset belongs to and allocate if needed
    ui64 regionIndex = CalculateRegionIndex(offset);
    TChunkRegionInfo chunkInfo;

    if (!PartitionState->FindChunkForRegion(regionIndex, chunkInfo)) {
        // With pre-allocation, all chunks should already be allocated
        // If we reach here, it means pre-allocation didn't cover this region
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No chunk allocated for offset " << offset
            << " (region " << regionIndex << ") - volume may not be fully pre-allocated");
        return NCloud::MakeError(E_FAIL, "No chunk allocated for requested region");
    }

    // Calculate chunk-relative offset: offset - regionStartOffset
    ui64 regionStartOffset = chunkInfo.StartOffset;
    ui32 chunkRelativeOffset = static_cast<ui32>(offset - regionStartOffset);

    // For ErasureMirror3Direct, write to all replicas
    for (const auto& ddiskActorId : requestCtx.DDiskActorIds) {
        auto request = std::make_unique<TEvBlobStorage::TEvDDiskWriteRequest>();
        request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
        request->Record.SetSize(size);
        request->Record.SetData(data);
        request->Record.SetChunkId(chunkInfo.ChunkId);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] SendWriteToDDisks: requestId=" << requestId
            << ", absoluteOffset=" << offset << ", chunkRelativeOffset=" << chunkRelativeOffset
            << ", size=" << size << ", chunkId=" << chunkInfo.ChunkId
            << ", target=" << ddiskActorId.ToString());

        ctx.Send(ddiskActorId, request.release(), 0, requestId);
    }

    // Set pending responses to the number of DDisk actors we sent to
    requestCtx.PendingResponses = requestCtx.DDiskActorIds.size();

    return NCloud::MakeError(S_OK);
}

void TProxyStorage::HandleDDiskReadResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev)
{
    const auto* msg = ev->Get();
    const ui64 requestId = ev->Cookie;

    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        return; // Request already completed or timed out
    }

    auto& requestCtx = it->second;

    // Convert YDB status to NBS error
    NCloud::NProto::TError error;
    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        error = NCloud::MakeError(E_FAIL, msg->Record.GetErrorReason());
    } else {
        error = NCloud::MakeError(S_OK);
    }

    // Update context with response
    if (NCloud::HasError(error)) {
        requestCtx.AccumulatedError = error;
    } else {
        requestCtx.ReadData = msg->Record.GetData();
    }

    requestCtx.PendingResponses--;

    // Complete request when all responses received (for reads, we only need one successful response)
    if (requestCtx.PendingResponses == 0 || !NCloud::HasError(error)) {
        CompleteReadRequest(ctx, requestCtx);
        PendingRequests.erase(it);
    }
}

void TProxyStorage::HandleDDiskWriteResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev)
{
    const auto* msg = ev->Get();
    const ui64 requestId = ev->Cookie;

    auto it = PendingRequests.find(requestId);
    if (it == PendingRequests.end()) {
        return; // Request already completed or timed out
    }

    auto& requestCtx = it->second;

    // Convert YDB status to NBS error
    NCloud::NProto::TError error;
    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        error = NCloud::MakeError(E_FAIL, msg->Record.GetErrorReason());
    } else {
        error = NCloud::MakeError(S_OK);
    }

    // Update context with response
    if (NCloud::HasError(error)) {
        requestCtx.AccumulatedError = error;
    }

    requestCtx.PendingResponses--;

    // Complete request when all responses received
    if (requestCtx.PendingResponses == 0) {
        CompleteWriteRequest(ctx, requestCtx);
        PendingRequests.erase(it);
    }
}

void TProxyStorage::CompleteReadRequest(
    const NActors::TActorContext& ctx,
    const TDDiskRequestContext& requestCtx)
{
    // Create response
    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();

    if (NCloud::HasError(requestCtx.AccumulatedError)) {
        response->Record.MutableError()->CopyFrom(requestCtx.AccumulatedError);
    } else {
        // Success - copy read data to the original request's sglist
        if (requestCtx.OriginalReadRequest) {
            auto guard = requestCtx.OriginalReadRequest->Sglist.Acquire();
            if (guard) {
                const auto& sglist = guard.Get();

                // Copy data from ReadData to all segments of the sglist
                const char* src = requestCtx.ReadData.data();
                ui32 copiedBytes = 0;

                for (const auto& segment : sglist) {
                    ui32 bytesToCopy = Min(segment.Size(), requestCtx.ReadData.size() - copiedBytes);
                    if (bytesToCopy > 0) {
                        memcpy(const_cast<char*>(segment.Data()), src + copiedBytes, bytesToCopy);
                        copiedBytes += bytesToCopy;
                    }
                    if (copiedBytes >= requestCtx.ReadData.size()) {
                        break;
                    }
                }
            }
        }

        response->Record.MutableError()->CopyFrom(NCloud::MakeError(S_OK));
    }

    // Reply to original request
    NCloud::Reply(ctx, *requestCtx.OriginalRequest, std::move(response));
}

void TProxyStorage::CompleteWriteRequest(
    const NActors::TActorContext& ctx,
    const TDDiskRequestContext& requestCtx)
{
    // Create response
    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();

    if (NCloud::HasError(requestCtx.AccumulatedError)) {
        response->Record.MutableError()->CopyFrom(requestCtx.AccumulatedError);
    } else {
        response->Record.MutableError()->CopyFrom(NCloud::MakeError(S_OK));
    }

    // Reply to original request
    NCloud::Reply(ctx, *requestCtx.OriginalRequest, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////
// Chunk allocation methods

bool TProxyStorage::FindChunkForOffset(ui64 offset, ui32& chunkId)
{
    ui64 regionIndex = CalculateRegionIndex(offset);

    TChunkRegionInfo chunkInfo;
    if (PartitionState->FindChunkForRegion(regionIndex, chunkInfo)) {
        chunkId = chunkInfo.ChunkId;
        return true;
    }

    return false;  // Chunk not found for this region
}

ui64 TProxyStorage::CalculateRegionIndex(ui64 offset)
{
    return PartitionState->CalculateRegionIndex(offset);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
