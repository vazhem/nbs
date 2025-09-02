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
    // Add detailed logging to trace I/O patterns
    const ui64 readStartIndex = request->GetStartIndex();
    const ui32 readBlocksCount = request->GetBlocksCount();
    const ui32 readBlockSize = PartitionState->GetBlockSize();
    const ui64 readAbsoluteOffset = readStartIndex * readBlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "ReadBlocksLocal ENTRY: startIndex=" << readStartIndex
        << " blocksCount=" << readBlocksCount << " absoluteOffset=" << readAbsoluteOffset
        << " size=" << (readBlocksCount * readBlockSize) << " bytes"
        << " STRIPE=" << (readAbsoluteOffset / 524288)
        << " GROUP=" << PartitionState->CalculateGroupIndex(readAbsoluteOffset));

    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    // Calculate offset and size in bytes (use previously declared variables)
    const ui64 offset = readStartIndex * readBlockSize;
    const ui32 size = readBlocksCount * readBlockSize;

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = PartitionState->CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ CalculateGroupIndex: offset=" << offset << " startIndex=" << readStartIndex
        << " groupIndex=" << groupIndex << " block=" << (offset / PartitionState->GetBlockSize())
        << " stripe=" << ((offset / PartitionState->GetBlockSize()) / 128));

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ DETAILED TRACE: requestId=" << requestId << " original_offset=" << offset
        << " original_size=" << size);

    // Get DDisk actors for the specific group
    auto groupDDiskServiceIds = PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    if (groupDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, TStringBuilder() << "No DDisk actors available for group " << groupIndex);
    }

    // Create request context with original request info
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Read,
        requestInfo,  // Use the provided original request info
        requestId,
        groupDDiskServiceIds,  // Use only DDisk actors for the specific group
        offset,
        size);

    // Store the original request for accessing sglist during completion
    requestCtx.OriginalReadRequest = request;
    requestCtx.ReadData.ReserveAndResize(size);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Check if read spans multiple stripe groups OR chunk boundaries - if so, split it
    ui64 startBlock = offset / PartitionState->GetBlockSize();
    ui64 endBlock = startBlock + (size / PartitionState->GetBlockSize()) - 1;
    ui64 stripeSize = PartitionState->STRIPE_SIZE / PartitionState->GetBlockSize(); // 128 blocks per stripe
    ui64 chunkSize = PartitionState->GetChunkSize(); // bytes

    ui64 startStripe = startBlock / stripeSize;
    ui64 endStripe = endBlock / stripeSize;
    ui64 startChunk = offset / chunkSize;
    ui64 endChunk = (offset + size - 1) / chunkSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "BOUNDARY SPAN CHECK: startBlock=" << startBlock << " endBlock=" << endBlock
        << " startStripe=" << startStripe << " endStripe=" << endStripe
        << " startChunk=" << startChunk << " endChunk=" << endChunk
        << " chunkSize=" << chunkSize);

    if (startStripe != endStripe || startChunk != endChunk) {
        // Read spans multiple stripes or chunks - need to split
        bool spansStripes = (startStripe != endStripe);
        bool spansChunks = (startChunk != endChunk);
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "BOUNDARY SPAN DETECTED: requestId=" << requestId
            << " spansStripes=" << spansStripes << " (" << startStripe << "-" << endStripe << ")"
            << " spansChunks=" << spansChunks << " (" << startChunk << "-" << endChunk << ") - SPLITTING");

        // For multi-segment reads, create segment tracking
        PendingRequests[requestId].IsMultiSegmentRead = true;
        PendingRequests[requestId].ReadSegments.clear();

        ui64 currentOffset = offset;
        ui32 totalSent = 0;

        while (currentOffset < offset + size) {
            // Find the nearest boundary (stripe or chunk)
            ui64 currentStripe = (currentOffset / PartitionState->GetBlockSize()) / stripeSize;
            ui64 currentChunk = currentOffset / chunkSize;

            ui64 nextStripeBoundary = (currentStripe + 1) * stripeSize * PartitionState->GetBlockSize();
            ui64 nextChunkBoundary = (currentChunk + 1) * chunkSize;

            // Choose the nearest boundary
            ui64 nextBoundary = std::min(nextStripeBoundary, nextChunkBoundary);
            ui64 segmentEndOffset = std::min(offset + size, nextBoundary);
            ui32 segmentSize = segmentEndOffset - currentOffset;

            // Create segment tracking entry
            PendingRequests[requestId].ReadSegments.emplace_back(currentOffset, segmentSize);

            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "SPLIT SEGMENT: offset=" << currentOffset << "-" << segmentEndOffset
                << " size=" << segmentSize << " segmentIndex=" << totalSent
                << " nearestBoundary=" << nextBoundary);

            // Encode segment index in high bits of requestId for multi-segment reads
            ui64 segmentRequestId = requestId | (static_cast<ui64>(totalSent) << 32);

            auto sendResult = SendReadToDDisks(ctx, segmentRequestId, currentOffset, segmentSize);
            if (NCloud::HasError(sendResult)) {
                PendingRequests.erase(requestId);

                LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                    "BOUNDARY SPAN ERROR: offset=" << currentOffset << "-" << segmentEndOffset
                    << " size=" << segmentSize << " segmentIndex=" << totalSent
                    << " error=" << sendResult.GetMessage());
                return sendResult;
            }

            totalSent++;
            currentOffset = segmentEndOffset;
        }

        // Update expected responses for multi-segment read
        PendingRequests[requestId].PendingResponses = totalSent;
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "MULTI-STRIPE READ: sent " << totalSent << " segments");
                } else {
                // Single stripe - use original logic
                LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                    "READ SINGLE BOUNDARY: offset=" << offset << " size=" << size);
                auto sendResult = SendReadToDDisks(ctx, requestId, offset, size);
                if (NCloud::HasError(sendResult)) {
                    PendingRequests.erase(requestId);
                    return sendResult;
                }
            }

    // Return immediately - response will be sent asynchronously
    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TProxyStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    // Add detailed logging to trace I/O patterns (calculate size from sglist later)
    const ui64 writeStartIndex = request->GetStartIndex();
    const ui32 writeBlockSize = PartitionState->GetBlockSize();
    const ui64 writeAbsoluteOffset = writeStartIndex * writeBlockSize;

    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return NCloud::MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    // Use the provided original request info for tracking

    // Extract data from all segments of sglist to calculate actual size
    TString data;
    const auto& sglist = guard.Get();
    for (const auto& segment : sglist) {
        data.append(segment.Data(), segment.Size());
    }

    // Calculate offset and size in bytes from actual data
    const ui64 offset = writeStartIndex * writeBlockSize;
    const ui32 size = data.size();
    const ui32 writeBlocksCount = size / writeBlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WriteBlocksLocal ENTRY: startIndex=" << writeStartIndex
        << " blocksCount=" << writeBlocksCount << " absoluteOffset=" << writeAbsoluteOffset
        << " actualSize=" << size << " bytes"
        << " STRIPE=" << (writeAbsoluteOffset / 524288)
        << " GROUP=" << PartitionState->CalculateGroupIndex(writeAbsoluteOffset));

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Create request context FIRST (needed for multi-segment tracking)
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Write,
        requestInfo,  // Use the provided original request info
        requestId,
        {},  // DDisk actors will be set later based on striping
        offset,
        size);

    // Store context early for multi-segment tracking
    PendingRequests[requestId] = std::move(requestCtx);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE CalculateGroupIndex about to call: offset=" << offset << " startIndex=" << writeStartIndex);

    // Calculate data hash for verification
    TString dataHash = TString();
    if (data.size() >= 16) {
        dataHash = TStringBuilder() << "Hash:" << (ui32)(data[0]) << (ui32)(data[8]) << (ui32)(data[data.size()-8]) << (ui32)(data[data.size()-1]);
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE DETAILED TRACE: requestId=" << requestId << " original_offset=" << offset
        << " original_size=" << size << " " << dataHash);

    // Check if write spans multiple stripe groups OR chunk boundaries - if so, split it
    ui64 startBlock = offset / PartitionState->GetBlockSize();
    ui64 endBlock = startBlock + (size / PartitionState->GetBlockSize()) - 1;
    ui64 stripeSize = PartitionState->STRIPE_SIZE / PartitionState->GetBlockSize(); // 128 blocks per stripe
    ui64 chunkSize = PartitionState->GetChunkSize(); // bytes

    ui64 startStripe = startBlock / stripeSize;
    ui64 endStripe = endBlock / stripeSize;
    ui64 startChunk = offset / chunkSize;
    ui64 endChunk = (offset + size - 1) / chunkSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE BOUNDARY SPAN CHECK: startBlock=" << startBlock << " endBlock=" << endBlock
        << " startStripe=" << startStripe << " endStripe=" << endStripe
        << " startChunk=" << startChunk << " endChunk=" << endChunk
        << " chunkSize=" << chunkSize);

    if (startStripe != endStripe || startChunk != endChunk) {
        // Write spans multiple stripes or chunks - need to split
        bool spansStripes = (startStripe != endStripe);
        bool spansChunks = (startChunk != endChunk);
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE BOUNDARY SPAN DETECTED: requestId=" << requestId
            << " spansStripes=" << spansStripes << " (" << startStripe << "-" << endStripe << ")"
            << " spansChunks=" << spansChunks << " (" << startChunk << "-" << endChunk << ") - SPLITTING");

        // Set up multi-segment write tracking
        auto& storedRequestCtx = PendingRequests[requestId];
        storedRequestCtx.IsMultiSegmentWrite = true;
        storedRequestCtx.WriteSegments.clear();

        // Split write and send segments
        ui64 currentOffset = offset;
        ui32 totalSent = 0;
        ui32 dataOffset = 0; // Track position in data string

        while (currentOffset < offset + size) {
            // Find the nearest boundary (stripe or chunk)
            ui64 currentStripe = (currentOffset / PartitionState->GetBlockSize()) / stripeSize;
            ui64 currentChunk = currentOffset / chunkSize;

            ui64 nextStripeBoundary = (currentStripe + 1) * stripeSize * PartitionState->GetBlockSize();
            ui64 nextChunkBoundary = (currentChunk + 1) * chunkSize;

            // Choose the nearest boundary
            ui64 nextBoundary = std::min(nextStripeBoundary, nextChunkBoundary);
            ui64 segmentEndOffset = std::min(offset + size, nextBoundary);
            ui32 segmentSize = segmentEndOffset - currentOffset;

            // Extract segment data
            TString segmentData = data.substr(dataOffset, segmentSize);

            // Calculate segment data hash for verification
            TString segmentHash = TString();
            if (segmentData.size() >= 16) {
                segmentHash = TStringBuilder() << "SegHash:" << (ui32)(segmentData[0]) << (ui32)(segmentData[8]) << (ui32)(segmentData[segmentData.size()-8]) << (ui32)(segmentData[segmentData.size()-1]);
            }
            ui32 segmentGroupIndex = PartitionState->CalculateGroupIndex(currentOffset);

            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "🔧 WRITE SPLIT SEGMENT: offset=" << currentOffset << "-" << segmentEndOffset
                << " size=" << segmentSize << " segmentIndex=" << totalSent
                << " nearestBoundary=" << nextBoundary << " groupIndex=" << segmentGroupIndex << " " << segmentHash);

            // Encode segment index in high bits of requestId for multi-segment writes
            ui64 segmentRequestId = requestId | (static_cast<ui64>(totalSent) << 32);

            // Track write segment
            storedRequestCtx.WriteSegments.emplace_back(currentOffset, segmentSize);

            // Send segment to appropriate group
            auto sendResult = SendWriteToDDisks(ctx, segmentRequestId, currentOffset, segmentSize, segmentData);
            if (NCloud::HasError(sendResult)) {
                PendingRequests.erase(requestId);
                LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE BOUNDARY SPAN ERROR: offset=" << currentOffset << "-" << segmentEndOffset
                    << " size=" << segmentSize << " segmentIndex=" << totalSent
                    << " error=" << sendResult.GetMessage());
                return sendResult;
            }

            totalSent++;
            currentOffset = segmentEndOffset;
            dataOffset += segmentSize;
        }

        // For multi-segment writes, PendingResponses will be set by SendWriteToDDisks
        // based on the actual DDisk actors used for each segment

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "🔧 WRITE MULTI-BOUNDARY: sent " << totalSent << " segments, responses will be counted per segment");

        // DON'T return immediately - wait for all segments to complete
        // Response will be sent in HandleDDiskWriteResponse when all segments complete
        return NCloud::MakeError(S_OK);
    }

    // Single boundary - use original logic
    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = PartitionState->CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE SINGLE GROUP: offset=" << offset << " groupIndex=" << groupIndex
        << " block=" << (offset / PartitionState->GetBlockSize()) << " stripe=" << ((offset / PartitionState->GetBlockSize()) / 128));

    // Get DDisk actors for the specific group and update stored context
    auto groupDDiskServiceIds = PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    if (groupDDiskServiceIds.empty()) {
        // Add debug logging to understand the issue
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No DDisk actors available for group " << groupIndex
            << " (offset=" << offset << ", startIndex=" << request->GetStartIndex() << ")");

        PendingRequests.erase(requestId);
        return NCloud::MakeError(E_FAIL, TStringBuilder() << "No DDisk actors available for group " << groupIndex);
    }

    // Update stored context with DDisk actors for single-segment write
    auto& storedRequestCtx = PendingRequests[requestId];
    storedRequestCtx.DDiskActorIds = groupDDiskServiceIds;

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
    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = PartitionState->GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
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

    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = PartitionState->CalculateGroupIndex(offset);

    // Get DDisk actors for the specific group
    auto groupDDiskServiceIds = PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    if (groupDDiskServiceIds.empty()) {
        // Add debug logging to understand the issue
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No DDisk actors available for group " << groupIndex
            << " (offset=" << offset << ", startIndex=" << request->GetStartIndex() << ")");

        return NCloud::MakeError(E_FAIL, TStringBuilder() << "No DDisk actors available for group " << groupIndex);
    }

    // Create request context with original request info
    TDDiskRequestContext requestCtx(
        TDDiskRequestContext::ERequestType::Write,
        requestInfo,  // Use the provided original request info
        requestId,
        groupDDiskServiceIds,  // Use only DDisk actors for the specific group
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
    // For multi-segment reads, decode the base requestId for lookup
    ui64 baseRequestId = requestId & 0xFFFFFFFF;  // Lower 32 bits
    ui32 segmentIndex = static_cast<ui32>(requestId >> 32);  // Upper 32 bits

    // Use base requestId for lookup, but keep full requestId for DDisk cookie
    auto it = PendingRequests.find(baseRequestId);
    if (it == PendingRequests.end()) {
        return NCloud::MakeError(E_FAIL, "Request not found");
    }

    if (segmentIndex > 0) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "SendReadToDDisks MULTI-SEGMENT: baseId=" << baseRequestId
            << " segmentIndex=" << segmentIndex << " fullId=" << requestId);
    }

    auto& requestCtx = it->second;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "SendReadToDDisks ENTRY: requestId=" << requestId << " offset=" << offset
        << " size=" << size);

    // Calculate which group should handle this read offset (same logic as writes)
    ui32 groupIndex = PartitionState->CalculateGroupIndex(offset);
    ui64 regionIndex = offset / PartitionState->GetBlockSize();

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "SendReadToDDisks CALCULATED: groupIndex=" << groupIndex << " regionIndex=" << regionIndex);

    // Get DDisk actors for the specific group (same as write logic)
    auto groupDDiskServiceIds = PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    if (groupDDiskServiceIds.empty()) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No DDisk actors available for group " << groupIndex);
        return NCloud::MakeError(E_FAIL, "No DDisk actors available for group");
    }

    // For reads, choose the first DDisk actor from the correct group
    const auto& targetActorId = groupDDiskServiceIds[0];

    // Validate ActorId before sending
    if (!targetActorId) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Invalid DDisk ActorId");
        return NCloud::MakeError(E_FAIL, "Invalid DDisk ActorId");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] read Fixed striping: offset=" << offset
        << " blockSize=" << PartitionState->GetBlockSize()
        << " groupIndex=" << groupIndex << " regionIndex=" << regionIndex);

    TChunkRegionInfo chunkInfo;
    if (!PartitionState->FindChunkForRegion(regionIndex, chunkInfo)) {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No chunk allocated for offset " << offset
            << " (group " << groupIndex << ", region " << regionIndex
            << ") - returning zero data");

        // Return zero data for unallocated regions
        SendZeroDataResponse(ctx, requestId, size);
        return NCloud::MakeError(S_OK);
    }

    // Calculate chunk-relative offset using the same logic as FindChunkForRegion
    ui64 logicalStartOffset = regionIndex * PartitionState->GetBlockSize();
    ui64 chunkRegionIndex = logicalStartOffset / PartitionState->GetChunkSize();
    ui64 chunkStartOffset = chunkRegionIndex * PartitionState->GetChunkSize();
    ui64 chunkRelativeOffsetLong = logicalStartOffset - chunkStartOffset;

    ui32 chunkRelativeOffset = static_cast<ui32>(chunkRelativeOffsetLong);

    // Validate offset before sending to DDisk
    if (chunkRelativeOffsetLong > UINT32_MAX) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Chunk relative offset overflow: " << chunkRelativeOffsetLong
            << " > UINT32_MAX, logicalStartOffset=" << logicalStartOffset
            << ", chunkStartOffset=" << chunkStartOffset);
        return NCloud::MakeError(E_FAIL, "Chunk relative offset overflow");
    }

    // Additional validation: check if chunkRelativeOffset is reasonable for chunk size
    ui32 chunkSize = PartitionState->GetChunkSize();
    ui32 remainingChunkSpace = chunkSize - chunkRelativeOffset;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "CHUNK BOUNDARY CHECK: requestId=" << requestId << " absoluteOffset=" << offset
        << " size=" << size << " chunkId=" << chunkInfo.ChunkId
        << " chunkRelativeOffset=" << chunkRelativeOffset << " chunkSize=" << chunkSize
        << " remainingSpace=" << remainingChunkSpace << " requestFits=" << (size <= remainingChunkSpace));

    if (chunkRelativeOffset >= chunkSize) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Chunk relative offset " << chunkRelativeOffset
            << " >= chunkSize " << chunkSize << " for chunkId " << chunkInfo.ChunkId);
        return NCloud::MakeError(E_FAIL, "Invalid chunk relative offset");
    }

    // Check if request spans beyond current chunk boundary
    // Note: Boundary spanning is now handled at ReadBlocksLocal level
    if (size > remainingChunkSpace) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "READ CHUNK BOUNDARY SPAN: requestId=" << requestId
            << " size=" << size << " > remainingSpace=" << remainingChunkSpace
            << " Should have been split at ReadBlocksLocal level! offset=" << offset << " chunkId=" << chunkInfo.ChunkId);
        return NCloud::MakeError(E_FAIL, "Read request spans chunk boundary - should have been split");
    }

    auto request = std::make_unique<TEvBlobStorage::TEvDDiskReadRequest>();
    request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
    request->Record.SetSize(size);
    request->Record.SetChunkId(chunkInfo.ChunkId);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] SendReadToDDisks: requestId=" << requestId
        << ", absoluteOffset=" << offset << ", groupIndex=" << groupIndex
        << ", regionIndex=" << regionIndex
        << ", logicalStartOffset=" << logicalStartOffset << ", chunkStartOffset=" << chunkStartOffset
        << ", chunkRelativeOffset=" << chunkRelativeOffset
        << ", size=" << size << ", chunkId=" << chunkInfo.ChunkId
        << ", target=" << targetActorId.ToString());

    // Set pending responses to 1 for single-segment reads only
    // For multi-segment reads, PendingResponses is already set correctly
    if (segmentIndex == 0 && !requestCtx.IsMultiSegmentRead) {
        requestCtx.PendingResponses = 1;
    }

    ctx.Send(targetActorId, request.release(), 0, requestId);
    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TProxyStorage::SendWriteToDDisks(
    const TActorContext& ctx,
    ui64 requestId, // This requestId now includes segmentIndex in high bits
    ui64 offset,
    ui32 size,
    const TString& data)
{
    ui64 baseRequestId = requestId & 0xFFFFFFFF;  // Lower 32 bits
    ui32 segmentIndex = static_cast<ui32>(requestId >> 32);  // Upper 32 bits

    auto it = PendingRequests.find(baseRequestId); // Use baseRequestId for lookup
    if (it == PendingRequests.end()) {
        return NCloud::MakeError(E_FAIL, "Request not found");
    }

    auto& requestCtx = it->second;

    // Calculate region index properly for striping
    // Region indices should be consecutive, but groups are determined by striping logic
    ui32 groupIndex = PartitionState->CalculateGroupIndex(offset);
    ui64 regionIndex = offset / PartitionState->GetBlockSize();

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] write Fixed striping: offset=" << offset
        << " blockSize=" << PartitionState->GetBlockSize()
        << " groupIndex=" << groupIndex << " regionIndex=" << regionIndex);

    TChunkRegionInfo chunkInfo;
    if (!PartitionState->FindChunkForRegion(regionIndex, chunkInfo)) {
        // With pre-allocation, all chunks should already be allocated
        // If we reach here, it means pre-allocation didn't cover this region
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No chunk allocated for offset " << offset
            << " (group " << groupIndex << ", region " << regionIndex
            << ") - volume may not be fully pre-allocated");
        return NCloud::MakeError(E_FAIL, "No chunk allocated for requested region");
    }

    // Calculate chunk-relative offset using the same logic as FindChunkForRegion
    ui64 logicalStartOffset = regionIndex * PartitionState->GetBlockSize();
    ui64 chunkRegionIndex = logicalStartOffset / PartitionState->GetChunkSize();
    ui64 chunkStartOffset = chunkRegionIndex * PartitionState->GetChunkSize();
    ui64 chunkRelativeOffsetLong = logicalStartOffset - chunkStartOffset;

    ui32 chunkRelativeOffset = static_cast<ui32>(chunkRelativeOffsetLong);

    // Validate offset before sending to DDisk
    if (chunkRelativeOffsetLong > UINT32_MAX) {
                    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << requestId << "] Write chunk relative offset overflow: " << chunkRelativeOffsetLong
                << " > UINT32_MAX, logicalStartOffset=" << logicalStartOffset
                << ", chunkStartOffset=" << chunkStartOffset);
        return NCloud::MakeError(E_FAIL, "Chunk relative offset overflow");
    }

    // Additional validation: check if chunkRelativeOffset is reasonable for chunk size
    ui32 chunkSize = PartitionState->GetChunkSize();
    ui32 remainingChunkSpace = chunkSize - chunkRelativeOffset;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE CHUNK BOUNDARY CHECK: requestId=" << requestId << " absoluteOffset=" << offset
        << " size=" << size << " chunkId=" << chunkInfo.ChunkId
        << " chunkRelativeOffset=" << chunkRelativeOffset << " chunkSize=" << chunkSize
        << " remainingSpace=" << remainingChunkSpace << " requestFits=" << (size <= remainingChunkSpace));

    if (chunkRelativeOffset >= chunkSize) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Write chunk relative offset " << chunkRelativeOffset
            << " >= chunkSize " << chunkSize << " for chunkId " << chunkInfo.ChunkId);
        return NCloud::MakeError(E_FAIL, "Invalid chunk relative offset");
    }

    // Check if request spans beyond current chunk boundary
    // Note: Boundary spanning is now handled at WriteBlocksLocal level
    if (size > remainingChunkSpace) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE CHUNK BOUNDARY SPAN: requestId=" << requestId
            << " size=" << size << " > remainingSpace=" << remainingChunkSpace
            << " Should have been split at WriteBlocksLocal level! offset=" << offset << " chunkId=" << chunkInfo.ChunkId);
        return NCloud::MakeError(E_FAIL, "Write request spans chunk boundary - should have been split");
    }

    // Get DDisk actors for this segment's group (needed for multi-segment writes)
    auto segmentDDiskServiceIds = PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    if (segmentDDiskServiceIds.empty()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No DDisk actors available for segment group " << groupIndex);
        return NCloud::MakeError(E_FAIL, "No DDisk actors available for segment group");
    }

    // For multi-segment writes, update context DDisk actors for this segment
    if (requestCtx.IsMultiSegmentWrite) {
        requestCtx.DDiskActorIds = segmentDDiskServiceIds;
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE SEGMENT DDisk: segmentIndex=" << segmentIndex
            << " groupIndex=" << groupIndex << " ddiskCount=" << segmentDDiskServiceIds.size());
    }

    // For ErasureMirror3Direct, write to all replicas for this segment
    for (const auto& ddiskActorId : segmentDDiskServiceIds) {
        auto request = std::make_unique<TEvBlobStorage::TEvDDiskWriteRequest>();
        request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
        request->Record.SetSize(size);
        request->Record.SetData(data);
        request->Record.SetChunkId(chunkInfo.ChunkId);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] SendWriteToDDisks: requestId=" << requestId
            << ", absoluteOffset=" << offset << ", groupIndex=" << groupIndex
            << ", regionIndex=" << regionIndex
            << ", logicalStartOffset=" << logicalStartOffset << ", chunkStartOffset=" << chunkStartOffset
            << ", chunkRelativeOffset=" << chunkRelativeOffset
            << ", size=" << size << ", chunkId=" << chunkInfo.ChunkId
            << ", target=" << ddiskActorId.ToString());

        ctx.Send(ddiskActorId, request.release(), 0, requestId);
    }

    // For multi-segment writes, accumulate the response count for each segment
    if (requestCtx.IsMultiSegmentWrite) {
        requestCtx.PendingResponses += segmentDDiskServiceIds.size();
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE SEGMENT SENT: segmentIndex=" << segmentIndex
            << " ddiskCount=" << segmentDDiskServiceIds.size()
            << " totalPending=" << requestCtx.PendingResponses);
    } else {
        // Single-segment writes
        requestCtx.PendingResponses = segmentDDiskServiceIds.size();
    }

    return NCloud::MakeError(S_OK);
}

void TProxyStorage::HandleDDiskReadResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev)
{
    const auto* msg = ev->Get();
    const ui64 responseRequestId = ev->Cookie;

    // For multi-segment reads, decode the base requestId and segment index
    const ui64 baseRequestId = responseRequestId & 0xFFFFFFFF;  // Lower 32 bits
    const ui32 segmentIndex = static_cast<ui32>(responseRequestId >> 32);  // Upper 32 bits

    auto it = PendingRequests.find(baseRequestId);
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

    // Log response data details for verification
    const TString& responseData = msg->Record.GetData();
    TString responseHash = TString();
    if (responseData.size() >= 16) {
        responseHash = TStringBuilder() << "RespHash:" << (ui32)(responseData[0]) << (ui32)(responseData[8]) << (ui32)(responseData[responseData.size()-8]) << (ui32)(responseData[responseData.size()-1]);
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ RESPONSE: baseRequestId=" << baseRequestId << " segmentIndex=" << segmentIndex
        << " responseSize=" << responseData.size() << " " << responseHash);

    // Handle multi-segment reads
    if (requestCtx.IsMultiSegmentRead) {
        HandleMultiSegmentResponse(ctx, requestCtx, msg, error, segmentIndex);
        requestCtx.PendingResponses--;

        // Check if all segments are complete
        if (requestCtx.PendingResponses == 0) {
            if (AllSegmentsComplete(requestCtx)) {
                ReassembleAndCompleteRead(ctx, requestCtx);
            } else {
                // Some segments failed
                requestCtx.AccumulatedError = NCloud::MakeError(E_FAIL, "Failed to read all segments");
                CompleteReadRequest(ctx, requestCtx);
            }
            PendingRequests.erase(it);
        }
    } else {
        // Single segment read - original logic
        if (NCloud::HasError(error)) {
            requestCtx.AccumulatedError = error;
        } else {
            requestCtx.ReadData = msg->Record.GetData();
        }

        requestCtx.PendingResponses--;

        if (requestCtx.PendingResponses == 0 || !NCloud::HasError(error)) {
            CompleteReadRequest(ctx, requestCtx);
            PendingRequests.erase(it);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Multi-segment read support

void TProxyStorage::HandleMultiSegmentResponse(
    const NActors::TActorContext& ctx,
    TDDiskRequestContext& requestCtx,
    const TEvBlobStorage::TEvDDiskReadResponse* msg,
    const NCloud::NProto::TError& error,
    ui32 segmentIndex)
{
    if (NCloud::HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "SEGMENT ERROR: requestId=" << requestCtx.RequestId
            << " segmentIndex=" << segmentIndex << " error=" << error.GetMessage());
        return;
    }

    // Validate segment index
    if (segmentIndex >= requestCtx.ReadSegments.size()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "INVALID SEGMENT INDEX: segmentIndex=" << segmentIndex
            << " maxSegments=" << requestCtx.ReadSegments.size());
        return;
    }

    const TString& responseData = msg->Record.GetData();
    auto& segment = requestCtx.ReadSegments[segmentIndex];

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 SEGMENT RESPONSE: requestId=" << requestCtx.RequestId
        << " segmentIndex=" << segmentIndex << " offset=" << segment.Offset
        << " expectedSize=" << segment.Size << " receivedSize=" << responseData.size());

    // Store the response data
    segment.Data = responseData;
    segment.IsComplete = true;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 SEGMENT COMPLETE: segmentIndex=" << segmentIndex
        << " offset=" << segment.Offset << " size=" << segment.Data.size());
}

bool TProxyStorage::AllSegmentsComplete(const TDDiskRequestContext& requestCtx)
{
    for (const auto& segment : requestCtx.ReadSegments) {
        if (!segment.IsComplete) {
            return false;
        }
    }
    return true;
}

void TProxyStorage::ReassembleAndCompleteRead(
    const NActors::TActorContext& ctx,
    TDDiskRequestContext& requestCtx)
{
    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 REASSEMBLING: requestId=" << requestCtx.RequestId
        << " segments=" << requestCtx.ReadSegments.size());

    // Calculate total size
    ui32 totalSize = 0;
    for (const auto& segment : requestCtx.ReadSegments) {
        totalSize += segment.Data.size();
    }

    // Reassemble segments in order (they're already in order from the split logic)
    requestCtx.ReadData.clear();
    requestCtx.ReadData.reserve(totalSize);

    for (size_t i = 0; i < requestCtx.ReadSegments.size(); ++i) {
        const auto& segment = requestCtx.ReadSegments[i];

        // Calculate segment data hash for verification
        TString segmentReadHash = TString();
        if (segment.Data.size() >= 16) {
            segmentReadHash = TStringBuilder() << "ReadSegHash:" << (ui32)(segment.Data[0]) << (ui32)(segment.Data[8]) << (ui32)(segment.Data[segment.Data.size()-8]) << (ui32)(segment.Data[segment.Data.size()-1]);
        }

        requestCtx.ReadData.append(segment.Data);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "🔧 APPENDED SEGMENT: index=" << i << " offset=" << segment.Offset
            << " size=" << segment.Data.size() << " totalSize=" << requestCtx.ReadData.size()
            << " " << segmentReadHash);
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 REASSEMBLY COMPLETE: totalSize=" << requestCtx.ReadData.size()
        << " expectedSize=" << requestCtx.Size);

    // Complete the read request
    CompleteReadRequest(ctx, requestCtx);
}

void TProxyStorage::HandleDDiskWriteResponse(
    const NActors::TActorContext& ctx,
    const TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev)
{
    const auto* msg = ev->Get();
    const ui64 responseRequestId = ev->Cookie;

    // For multi-segment writes, decode the base requestId and segment index
    const ui64 baseRequestId = responseRequestId & 0xFFFFFFFF;  // Lower 32 bits
    const ui32 segmentIndex = static_cast<ui32>(responseRequestId >> 32);  // Upper 32 bits

    auto it = PendingRequests.find(baseRequestId);
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

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE RESPONSE: baseRequestId=" << baseRequestId << " segmentIndex=" << segmentIndex
        << " status=" << (NCloud::HasError(error) ? "ERROR" : "OK"));

    if (requestCtx.IsMultiSegmentWrite) {
        // Handle multi-segment write response
        if (segmentIndex < requestCtx.WriteSegments.size()) {
            requestCtx.WriteSegments[segmentIndex].IsComplete = true;
            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "WRITE SEGMENT COMPLETE: requestId=" << baseRequestId
                << " segmentIndex=" << segmentIndex << " offset=" << requestCtx.WriteSegments[segmentIndex].Offset);
        }

        if (NCloud::HasError(error)) {
            requestCtx.AccumulatedError = error;
        }

        requestCtx.PendingResponses--;

        if (requestCtx.PendingResponses == 0) {
            // Check if all segments completed successfully
            bool allComplete = true;
            for (const auto& segment : requestCtx.WriteSegments) {
                if (!segment.IsComplete) {
                    allComplete = false;
                    break;
                }
            }

            if (!allComplete && !NCloud::HasError(requestCtx.AccumulatedError)) {
                requestCtx.AccumulatedError = NCloud::MakeError(E_FAIL, "Not all write segments completed");
            }

            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "WRITE MULTI-SEGMENT COMPLETE: requestId=" << baseRequestId
                << " allComplete=" << allComplete);

            CompleteWriteRequest(ctx, requestCtx);
            PendingRequests.erase(it);
        }
    } else {
        // Single segment write - original logic
        if (NCloud::HasError(error)) {
            requestCtx.AccumulatedError = error;
        }

        requestCtx.PendingResponses--;

        if (requestCtx.PendingResponses == 0) {
            CompleteWriteRequest(ctx, requestCtx);
            PendingRequests.erase(it);
        }
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
    // Fixed striping logic: consecutive region indices
    ui64 regionIndex = offset / PartitionState->GetBlockSize();

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
