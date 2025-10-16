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
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>
#include <contrib/ydb/library/wilson_ids/wilson.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

// Use DDisk events from blobstorage.h

////////////////////////////////////////////////////////////////////////////////



NCloud::NProto::TError TProxyStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // Add detailed logging to trace I/O patterns
    const ui64 readStartIndex = request->GetStartIndex();
    const ui32 readBlocksCount = request->GetBlocksCount();
    const ui32 readBlockSize = GetBlockSize();
    const ui64 readAbsoluteOffset = readStartIndex * readBlockSize;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "ReadBlocksLocal ENTRY: startIndex=" << readStartIndex
        << " blocksCount=" << readBlocksCount << " absoluteOffset=" << readAbsoluteOffset
        << " size=" << (readBlocksCount * readBlockSize) << " bytes"
        << " STRIPE=" << (readAbsoluteOffset / 524288)
        << " GROUP=" << CalculateGroupIndex(readAbsoluteOffset));

    NWilson::TSpan span(NKikimr::TWilson::BlobStorage, std::move(traceId), "PartitionDirect.ReadBlocks");

    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    // Calculate offset and size in bytes (use previously declared variables)
    const ui64 offset = readStartIndex * readBlockSize;
    const ui32 size = readBlocksCount * readBlockSize;

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ CalculateGroupIndex: offset=" << offset << " startIndex=" << readStartIndex
        << " groupIndex=" << groupIndex << " block=" << (offset / GetBlockSize())
        << " stripe=" << ((offset / GetBlockSize()) / 128));

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ DETAILED TRACE: requestId=" << requestId << " original_offset=" << offset
        << " traceId=" << traceId.GetHexTraceId()
        << " original_size=" << size);

    // Get DDisk actors for the specific group
    auto groupDDiskServiceIds = GetDDiskServiceIdsForGroup(groupIndex);
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
    requestCtx.Span = std::move(span);

    // Store context
    PendingRequests[requestId] = std::move(requestCtx);

    // Check if read spans multiple chunks - need to check chunk boundaries properly
    ui32 startGroupIndex = CalculateGroupIndex(offset);
    ui32 endGroupIndex = CalculateGroupIndex(offset + size - 1);

    // Get chunk boundaries for start and end offsets
    ui32 startChunkId, endChunkId;
    TString startDDiskServiceId, endDDiskServiceId;
    ui32 startChunkRelativeOffset, endChunkRelativeOffset;

    bool startChunkFound = FindChunkForOffset(offset, startChunkId, startDDiskServiceId, startChunkRelativeOffset);
    bool endChunkFound = FindChunkForOffset(offset + size - 1, endChunkId, endDDiskServiceId, endChunkRelativeOffset);

    bool spansChunks = (startChunkFound && endChunkFound && startChunkId != endChunkId);
    bool spansGroups = (startGroupIndex != endGroupIndex);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ BOUNDARY CHECK: offset=" << offset << " size=" << size
        << " startGroup=" << startGroupIndex << " endGroup=" << endGroupIndex
        << " startChunk=" << (startChunkFound ? startChunkId : 0) << " endChunk=" << (endChunkFound ? endChunkId : 0)
        << " spansGroups=" << spansGroups
        << " spansChunks=" << spansChunks);

    if (spansGroups || spansChunks) {
        // Read spans multiple groups or chunks - need to split
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "READ BOUNDARY SPAN DETECTED: requestId=" << requestId
            << " traceId=" << traceId.GetHexTraceId()
            << " spansGroups=" << spansGroups << " (" << startGroupIndex << "-" << endGroupIndex << ")"
            << " spansChunks=" << spansChunks << " (" << startChunkId << "-" << endChunkId << ") - SPLITTING");

        // For multi-segment reads, create segment tracking
        PendingRequests[requestId].IsMultiSegmentRead = true;
        PendingRequests[requestId].ReadSegments.clear();

        ui64 currentOffset = offset;
        ui32 totalSent = 0;

        while (currentOffset < offset + size) {
            // Calculate group and chunk for current offset
            ui32 currentGroupIndex = CalculateGroupIndex(currentOffset);

            // Find the chunk boundary within this group
            ui32 currentChunkId;
            TString currentDDiskServiceId;
            ui32 currentChunkRelativeOffset;

            if (!FindChunkForOffset(currentOffset, currentChunkId, currentDDiskServiceId, currentChunkRelativeOffset)) {
                // Cannot find chunk - abort splitting
                LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                    "READ SPLIT ERROR: Cannot find chunk for offset " << currentOffset);
                PendingRequests.erase(requestId);
                return NCloud::MakeError(E_FAIL, "Cannot find chunk for offset during splitting");
            }

            // Calculate the end of current chunk
            ui64 chunkEndOffset = currentOffset - currentChunkRelativeOffset + GetChunkSize();

            // Calculate next group boundary (stripe boundary)
            ui32 currentStripe = currentOffset / Config.STRIPE_SIZE;
            ui64 nextStripeBoundary = (currentStripe + 1) * Config.STRIPE_SIZE;

            // Choose the nearest boundary (chunk or stripe)
            ui64 nextBoundary = std::min(chunkEndOffset, nextStripeBoundary);
            ui64 segmentEndOffset = std::min(offset + size, nextBoundary);
            ui32 segmentSize = segmentEndOffset - currentOffset;

            // Create segment tracking entry
            PendingRequests[requestId].ReadSegments.emplace_back(currentOffset, segmentSize);

            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "READ SPLIT SEGMENT: offset=" << currentOffset << "-" << segmentEndOffset
                << " size=" << segmentSize << " segmentIndex=" << totalSent
                << " groupIndex=" << currentGroupIndex << " chunkId=" << currentChunkId
                << " chunkBoundary=" << chunkEndOffset << " stripeBoundary=" << nextStripeBoundary
                << " nearestBoundary=" << nextBoundary);

            // Encode segment index in high bits of requestId for multi-segment reads
            ui64 segmentRequestId = requestId | (static_cast<ui64>(totalSent) << 32);

            auto sendResult = SendReadToDDisks(ctx, segmentRequestId,
                currentOffset, segmentSize, PendingRequests[requestId].Span.GetTraceId());
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
            "MULTI-SEGMENT READ: sent " << totalSent << " segments");
    } else {
        // Single chunk/group - use direct lookup
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "READ SINGLE CHUNK: offset=" << offset << " size=" << size
            << " groupIndex=" << startGroupIndex);
        auto sendResult = SendReadToDDisks(ctx, requestId, offset, size, PendingRequests[requestId].Span.GetTraceId());
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
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // Add detailed logging to trace I/O patterns (calculate size from sglist later)
    const ui64 writeStartIndex = request->GetStartIndex();
    const ui32 writeBlockSize = GetBlockSize();
    const ui64 writeAbsoluteOffset = writeStartIndex * writeBlockSize;

    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        return NCloud::MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    NWilson::TSpan span(NKikimr::TWilson::BlobStorage, std::move(traceId), "PartitionDirect.WriteBlocks");

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
        << " GROUP=" << CalculateGroupIndex(writeAbsoluteOffset));

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

    requestCtx.Span = std::move(span);

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
        "WRITE DETAILED TRACE: requestId=" << requestId
        << " traceId=" << traceId.GetHexTraceId()
        << " original_offset=" << offset
        << " original_size=" << size << " " << dataHash);

    // Check if write spans multiple chunks - need to check chunk boundaries properly
    ui32 startGroupIndex = CalculateGroupIndex(offset);
    ui32 endGroupIndex = CalculateGroupIndex(offset + size - 1);

    // Get chunk boundaries for start and end offsets
    ui32 startChunkId, endChunkId;
    TString startDDiskServiceId, endDDiskServiceId;
    ui32 startChunkRelativeOffset, endChunkRelativeOffset;

    bool startChunkFound = FindChunkForOffset(offset, startChunkId, startDDiskServiceId, startChunkRelativeOffset);
    bool endChunkFound = FindChunkForOffset(offset + size - 1, endChunkId, endDDiskServiceId, endChunkRelativeOffset);

    bool spansChunks = (startChunkFound && endChunkFound && startChunkId != endChunkId);
    bool spansGroups = (startGroupIndex != endGroupIndex);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE BOUNDARY CHECK: offset=" << offset << " size=" << size
        << " startGroup=" << startGroupIndex << " endGroup=" << endGroupIndex
        << " startChunk=" << (startChunkFound ? startChunkId : 0) << " endChunk=" << (endChunkFound ? endChunkId : 0)
        << " spansGroups=" << spansGroups
        << " spansChunks=" << spansChunks);

    if (spansGroups || spansChunks) {
        // Write spans multiple groups or chunks - need to split
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE BOUNDARY SPAN DETECTED: requestId=" << requestId
            << " traceId=" << traceId.GetHexTraceId()
            << " spansGroups=" << spansGroups << " (" << startGroupIndex << "-" << endGroupIndex << ")"
            << " spansChunks=" << spansChunks << " (" << startChunkId << "-" << endChunkId << ") - SPLITTING");

        // Set up multi-segment write tracking
        auto& storedRequestCtx = PendingRequests[requestId];
        storedRequestCtx.IsMultiSegmentWrite = true;
        storedRequestCtx.WriteSegments.clear();

        // Split write and send segments
        ui64 currentOffset = offset;
        ui32 totalSent = 0;
        ui32 dataOffset = 0; // Track position in data string

        while (currentOffset < offset + size) {
            // Calculate group and chunk for current offset
            ui32 currentGroupIndex = CalculateGroupIndex(currentOffset);

            // Find the chunk boundary within this group
            ui32 currentChunkId;
            TString currentDDiskServiceId;
            ui32 currentChunkRelativeOffset;

            if (!FindChunkForOffset(currentOffset, currentChunkId, currentDDiskServiceId, currentChunkRelativeOffset)) {
                // Cannot find chunk - abort splitting
                LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE SPLIT ERROR: Cannot find chunk for offset " << currentOffset);
                PendingRequests.erase(requestId);
                return NCloud::MakeError(E_FAIL, "Cannot find chunk for offset during splitting");
            }

            // Calculate the end of current chunk
            ui64 chunkEndOffset = currentOffset - currentChunkRelativeOffset + GetChunkSize();

            // Calculate next group boundary (stripe boundary)
            ui32 currentStripe = currentOffset / Config.STRIPE_SIZE;
            ui64 nextStripeBoundary = (currentStripe + 1) * Config.STRIPE_SIZE;

            // Choose the nearest boundary (chunk or stripe)
            ui64 nextBoundary = std::min(chunkEndOffset, nextStripeBoundary);
            ui64 segmentEndOffset = std::min(offset + size, nextBoundary);
            ui32 segmentSize = segmentEndOffset - currentOffset;

            // Extract segment data
            TString segmentData = data.substr(dataOffset, segmentSize);

            // Validation: Check for data extraction consistency
            if (segmentData.size() != segmentSize) {
                LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                    "SEGMENT DATA SIZE MISMATCH: expected=" << segmentSize
                    << " actual=" << segmentData.size() << " dataOffset=" << dataOffset
                    << " totalDataSize=" << data.size());
            }

            // Calculate segment data hash for verification
            TString segmentHash = TString();
            if (segmentData.size() >= 16) {
                segmentHash = TStringBuilder() << "SegHash:" << (ui32)(segmentData[0]) << (ui32)(segmentData[8]) << (ui32)(segmentData[segmentData.size()-8]) << (ui32)(segmentData[segmentData.size()-1]);
            }

            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "🔧 WRITE SPLIT SEGMENT: offset=" << currentOffset << "-" << segmentEndOffset
                << " size=" << segmentSize << " segmentIndex=" << totalSent
                << " groupIndex=" << currentGroupIndex << " chunkId=" << currentChunkId
                << " chunkBoundary=" << chunkEndOffset << " stripeBoundary=" << nextStripeBoundary
                << " " << segmentHash);

            // Encode segment index in high bits of requestId for multi-segment writes
            ui64 segmentRequestId = requestId | (static_cast<ui64>(totalSent) << 32);

            // Track write segment
            storedRequestCtx.WriteSegments.emplace_back(currentOffset, segmentSize);

            // Send segment to appropriate group
            auto sendResult = SendWriteToDDisks(ctx, segmentRequestId, currentOffset,
                segmentSize, segmentData, PendingRequests[requestId].Span.GetTraceId());
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

    // Single chunk/group - use original logic
    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE SINGLE CHUNK: offset=" << offset
        << " groupIndex=" << groupIndex << " block=" << (offset / GetBlockSize()));

    // Get DDisk actors for the specific group and update stored context
    auto groupDDiskServiceIds = GetDDiskServiceIdsForGroup(groupIndex);
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
    auto sendResult = SendWriteToDDisks(ctx, requestId, offset, size, data, PendingRequests[requestId].Span.GetTraceId());
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
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    NWilson::TTraceId traceId)
{
    // DDisk selection will be done after calculating the group for the offset
    // For now, just check that we have some DDisk actors available
    const auto& allDDiskServiceIds = GetDDiskServiceIds();
    if (allDDiskServiceIds.empty()) {
        return NCloud::MakeError(E_FAIL, "No DDisk actors available");
    }

    // Use the provided original request info for tracking

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blocksCount = request->GetBlocksCount();
    const ui32 blockSize = GetBlockSize();

    // Calculate offset and size in bytes
    const ui64 offset = startIndex * blockSize;
    const ui32 size = blocksCount * blockSize;

    // Create zero data
    TString zeroData(size, 0);

    // Generate unique request ID
    const ui64 requestId = GenerateRequestId();

    // Calculate which group should handle this offset (512KB striping)
    ui32 groupIndex = CalculateGroupIndex(offset);

    // Get DDisk actors for the specific group
    auto groupDDiskServiceIds = GetDDiskServiceIdsForGroup(groupIndex);
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
    auto sendResult = SendWriteToDDisks(ctx, requestId, offset, size, zeroData, std::move(traceId));
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
    ui32 size,
    NWilson::TTraceId traceId)
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
        "SendReadToDDisks ENTRY: requestId=" << requestId
        << " traceId=" << traceId.GetHexTraceId()
        << " offset=" << offset
        << " size=" << size);

    // Use FindChunkForOffset to get the actual chunk ID and DDisk service
    ui32 chunkId;
    TString ddiskServiceId;
    ui32 chunkRelativeOffset;

    if (!FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset)) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] FindChunkForOffset failed for offset " << offset);
        return NCloud::MakeError(E_FAIL, "Cannot find chunk for offset");
        }

    // Calculate group for logging purposes
    ui32 groupIndex = CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "SendReadToDDisks FOUND CHUNK: groupIndex=" << groupIndex << " chunkId=" << chunkId
        << " ddiskServiceId=" << ddiskServiceId << " chunkRelativeOffset=" << chunkRelativeOffset);

    // Get DDisk actors for the specific group based on the chunk allocation
    auto groupDDiskServiceIds = GetDDiskServiceIdsForGroup(groupIndex);
    if (groupDDiskServiceIds.empty()) {
            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No DDisk actors available for group " << groupIndex);
        return NCloud::MakeError(E_FAIL, "No DDisk actors available for group");
    }

    // Use the specific DDisk from FindChunkForOffset for consistency
    TActorId targetActorId;
    bool foundMatchingActor = false;
    for (const auto& actorId : groupDDiskServiceIds) {
        if (actorId.ToString() == ddiskServiceId) {
            targetActorId = actorId;
            foundMatchingActor = true;
            break;
        }
    }

    if (!foundMatchingActor) {
        return NCloud::MakeError(E_FAIL, "Cannot find ddisk");
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] read Using FindChunkForOffset: offset=" << offset
        << " groupIndex=" << groupIndex << " chunkId=" << chunkId
        << " chunkRelativeOffset=" << chunkRelativeOffset);

    // Validate chunk relative offset (already calculated correctly by FindChunkForOffset)
    if (chunkRelativeOffset >= GetChunkSize()) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Invalid chunk relative offset: " << chunkRelativeOffset
            << " >= chunkSize " << GetChunkSize());
        return NCloud::MakeError(E_FAIL, "Invalid chunk relative offset");
        }

        // Additional validation: check if chunkRelativeOffset is reasonable for chunk size
        ui32 chunkSize = GetChunkSize();
    ui32 remainingChunkSpace = chunkSize - chunkRelativeOffset;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "CHUNK BOUNDARY CHECK: requestId=" << requestId
        << " traceId=" << traceId.GetHexTraceId()
        << " absoluteOffset=" << offset
        << " size=" << size << " chunkId=" << chunkId
        << " chunkRelativeOffset=" << chunkRelativeOffset << " chunkSize=" << chunkSize
        << " remainingSpace=" << remainingChunkSpace << " requestFits=" << (size <= remainingChunkSpace));

        if (chunkRelativeOffset >= chunkSize) {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << requestId << "] Chunk relative offset " << chunkRelativeOffset
            << " >= chunkSize " << chunkSize << " for chunkId " << chunkId);
            return NCloud::MakeError(E_FAIL, "Invalid chunk relative offset");
        }

    // Check if request spans beyond current chunk boundary
    // Note: Boundary spanning is now handled at ReadBlocksLocal level
    if (size > remainingChunkSpace) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "READ CHUNK BOUNDARY SPAN: requestId=" << requestId
            << " traceId=" << traceId.GetHexTraceId()
            << " size=" << size << " > remainingSpace=" << remainingChunkSpace
            << " Should have been split at ReadBlocksLocal level! offset=" << offset << " chunkId=" << chunkId);
        return NCloud::MakeError(E_FAIL, "Read request spans chunk boundary - should have been split");
        }

        auto request = std::make_unique<TEvBlobStorage::TEvDDiskReadRequest>();

    request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
    request->Record.SetSize(size);
    request->Record.SetChunkId(chunkId);

    // Log the exact read request being sent to DDisk
    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "📖 DDISK READ REQUEST: requestId=" << requestId
        << " traceId=" << traceId.GetHexTraceId()
        << " DDiskService=" << ddiskServiceId
        << " DDiskActor=" << targetActorId.ToString()
        << " chunkId=" << chunkId << " offset=" << chunkRelativeOffset
        << " size=" << size);

    // Set pending responses to 1 for single-segment reads only
    // For multi-segment reads, PendingResponses is already set correctly
    if (segmentIndex == 0 && !requestCtx.IsMultiSegmentRead) {
        requestCtx.PendingResponses = 1;
    }

    // Create child span for this DDisk subrequest from parent span
    // Use the parent span's traceId to create child span
    NWilson::TSpan childSpan(NKikimr::TWilson::BlobStorage,
                              requestCtx.Span.GetTraceId(),
                              "PartitionDirect.ReadBlocks.DDiskRead");

    // Get child span's traceId before moving the span
    NWilson::TTraceId childTraceId = childSpan.GetTraceId();

    // Store child span in context (key is requestId which includes segment index in high bits)
    requestCtx.ChildSpans[requestId] = std::move(childSpan);

    requestCtx.ChildSpans[requestId].Event("Send_TEvDDiskReadRequest");

    // Send with child span's TraceId for request tracing
    ctx.Send(new IEventHandle(targetActorId, ctx.SelfID, request.release(), 0, requestId, nullptr, std::move(childTraceId)));
        return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TProxyStorage::SendWriteToDDisks(
    const TActorContext& ctx,
    ui64 requestId, // This requestId now includes segmentIndex in high bits
    ui64 offset,
    ui32 size,
    const TString& data,
    NWilson::TTraceId traceId)
{
    ui64 baseRequestId = requestId & 0xFFFFFFFF;  // Lower 32 bits
    ui32 segmentIndex = static_cast<ui32>(requestId >> 32);  // Upper 32 bits

    auto it = PendingRequests.find(baseRequestId); // Use baseRequestId for lookup
    if (it == PendingRequests.end()) {
        return NCloud::MakeError(E_FAIL, "Request not found");
    }

    auto& requestCtx = it->second;

    // Use FindChunkForOffset to get the actual chunk ID and DDisk service
    ui32 chunkId;
    TString ddiskServiceId;
    ui32 chunkRelativeOffset;

    if (!FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset)) {
        // With pre-allocation, all chunks should already be allocated
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] No chunk allocated for offset " << offset);
        return NCloud::MakeError(E_FAIL, "No chunk allocated for requested region");
    }

    // Calculate group for logging purposes
    ui32 groupIndex = CalculateGroupIndex(offset);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] write Using FindChunkForOffset: offset=" << offset
        << " groupIndex=" << groupIndex << " chunkId=" << chunkId
        << " ddiskServiceId=" << ddiskServiceId << " chunkRelativeOffset=" << chunkRelativeOffset);

    // Validate chunk relative offset (already calculated correctly by FindChunkForOffset)
    if (chunkRelativeOffset >= GetChunkSize()) {
                    LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Invalid write chunk relative offset: " << chunkRelativeOffset
            << " >= chunkSize " << GetChunkSize());
        return NCloud::MakeError(E_FAIL, "Invalid write chunk relative offset");
    }

    // Additional validation: check if chunkRelativeOffset is reasonable for chunk size
    ui32 chunkSize = GetChunkSize();
    ui32 remainingChunkSpace = chunkSize - chunkRelativeOffset;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE CHUNK BOUNDARY CHECK: requestId=" << requestId << " absoluteOffset=" << offset
        << " traceId=" << traceId.GetHexTraceId()
        << " size=" << size << " chunkId=" << chunkId
        << " chunkRelativeOffset=" << chunkRelativeOffset << " chunkSize=" << chunkSize
        << " remainingSpace=" << remainingChunkSpace << " requestFits=" << (size <= remainingChunkSpace));

    if (chunkRelativeOffset >= chunkSize) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] Write chunk relative offset " << chunkRelativeOffset
            << " >= chunkSize " << chunkSize << " for chunkId " << chunkId);
        return NCloud::MakeError(E_FAIL, "Invalid chunk relative offset");
    }

    // Check if request spans beyond current chunk boundary
    // Note: Boundary spanning is now handled at WriteBlocksLocal level
    if (size > remainingChunkSpace) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE CHUNK BOUNDARY SPAN: requestId=" << requestId
            << " traceId=" << traceId.GetHexTraceId()
            << " size=" << size << " > remainingSpace=" << remainingChunkSpace
            << " Should have been split at WriteBlocksLocal level! offset=" << offset << " chunkId=" << chunkId);
        return NCloud::MakeError(E_FAIL, "Write request spans chunk boundary - should have been split");
    }

    // Get DDisk actors for this segment's group
    auto segmentDDiskServiceIds = GetDDiskServiceIdsForGroup(groupIndex);
    if (segmentDDiskServiceIds.empty()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << requestId << "] traceId=" << traceId.GetHexTraceId()
            << " No DDisk actors available for segment group " << groupIndex);
        return NCloud::MakeError(E_FAIL, "No DDisk actors available for segment group");
    }

    // Use the specific DDisk from FindChunkForOffset for consistency
    TActorId targetActorId;
    bool foundMatchingActor = false;
    for (const auto& actorId : segmentDDiskServiceIds) {
        if (actorId.ToString() == ddiskServiceId) {
            targetActorId = actorId;
            foundMatchingActor = true;
            break;
        }
    }

    if (!foundMatchingActor) {
        return NCloud::MakeError(E_FAIL, "Cannot find ddisk");
    }

    // For multi-segment writes, track the specific DDisk actor for this segment
    if (requestCtx.IsMultiSegmentWrite) {
        requestCtx.DDiskActorIds = {targetActorId}; // Single specific DDisk for this chunk
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE SEGMENT DDisk: segmentIndex=" << segmentIndex
            << " groupIndex=" << groupIndex << " chunkId=" << chunkId
            << " targetDDisk=" << ddiskServiceId);
    }

    // Write to the specific DDisk that holds this chunk
    auto request = std::make_unique<TEvBlobStorage::TEvDDiskWriteRequest>();

    request->Record.SetOffset(chunkRelativeOffset);  // Chunk-relative offset
    request->Record.SetSize(size);
    request->Record.SetData(data);
    request->Record.SetChunkId(chunkId);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << requestId << "] traceId=" << traceId.GetHexTraceId()
        << " traceId=" << traceId.GetHexTraceId()
        << " 📤 WRITE REQUEST: offset=" << offset
        << " group=" << groupIndex << " chunkId=" << chunkId
        << " chunkRelOffset=" << chunkRelativeOffset << " size=" << size
        << " target=" << targetActorId.ToString() << " ddiskService=" << ddiskServiceId
        << " segmentIndex=" << segmentIndex
        << " encodedRequestId=" << requestId << " (baseId=" << (requestId & 0xFFFFFFFF)
        << " segIdx=" << (requestId >> 32) << ")");

    // Create child span for this DDisk subrequest from parent span
    // Use the parent span's traceId to create child span
    NWilson::TSpan childSpan(NKikimr::TWilson::BlobStorage,
                              requestCtx.Span.GetTraceId(),
                              "PartitionDirect.WriteBlocks.DDiskWrite");

    // Get child span's traceId before moving the span
    NWilson::TTraceId childTraceId = childSpan.GetTraceId();

    // Store child span in context (key is requestId which includes segment index in high bits)
    requestCtx.ChildSpans[requestId] = std::move(childSpan);

    requestCtx.ChildSpans[requestId].Event("Send_TEvDDiskWriteRequest");

    // Send with child span's TraceId for request tracing
    ctx.Send(new IEventHandle(targetActorId, ctx.SelfID, request.release(), 0, requestId, nullptr, std::move(childTraceId)));

    // For multi-segment writes, accumulate the response count for each segment
    if (requestCtx.IsMultiSegmentWrite) {
        requestCtx.PendingResponses += 1; // Single DDisk per chunk
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE SEGMENT SENT: segmentIndex=" << segmentIndex
            << " chunkId=" << chunkId
            << " totalPending=" << requestCtx.PendingResponses);
    } else {
        // Single-segment writes
        requestCtx.PendingResponses = 1; // Single DDisk per chunk
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

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << baseRequestId << "] 📥 READ RESPONSE RECEIVED: responseRequestId=" << responseRequestId
        << " traceId=" << ev->TraceId.GetHexTraceId()
        << " (baseId=" << baseRequestId << " segIdx=" << segmentIndex << ")"
        << " status=" << msg->Record.GetStatus());

    auto it = PendingRequests.find(baseRequestId);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "🚨 READ RESPONSE ORPHAN: baseRequestId=" << baseRequestId
            << " traceId=" << ev->TraceId.GetHexTraceId()
            << " segmentIndex=" << segmentIndex << " responseRequestId=" << responseRequestId
            << " - REQUEST NOT FOUND IN PENDING REQUESTS!");
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

    // End child span for this DDisk subrequest
    auto childSpanIt = requestCtx.ChildSpans.find(responseRequestId);
    if (childSpanIt != requestCtx.ChildSpans.end()) {
        childSpanIt->second.Event("Received_TEvDDiskReadResponse");

        if (NCloud::HasError(error)) {
            childSpanIt->second.EndError(msg->Record.GetErrorReason());
        } else {
            childSpanIt->second.EndOk();
        }
        // Remove child span after ending it (don't access after this)
        requestCtx.ChildSpans.erase(childSpanIt);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "READ RESPONSE: ended child span for requestId=" << responseRequestId);
    } else {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "READ RESPONSE: child span not found for requestId=" << responseRequestId);
    }

    // Log response data details for verification
    const TString& responseData = msg->Record.GetData();
    TString responseHash = TString();
    if (responseData.size() >= 16) {
        responseHash = TStringBuilder() << "RespHash:" << (ui32)(responseData[0]) << (ui32)(responseData[8]) << (ui32)(responseData[responseData.size()-8]) << (ui32)(responseData[responseData.size()-1]);
    }

    // // DETAILED DATA LOGGING: Log actual data received from DDisk
    // if (responseData.size() >= 32) {
    //     TString dataLogPrefix = TString();
    //     TString dataLogSuffix = TString();
    //     for (int i = 0; i < std::min<int>(16, responseData.size()); i++) {
    //         dataLogPrefix += TStringBuilder() << " " << (ui32)(ui8)responseData[i];
    //     }
    //     if (responseData.size() > 16) {
    //         for (int i = std::max<int>(16, static_cast<int>(responseData.size()-16)); i < static_cast<int>(responseData.size()); i++) {
    //             dataLogSuffix += TStringBuilder() << " " << (ui32)(ui8)responseData[i];
    //         }
    //     }
    //     LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
    //         "📖 DDISK READ DATA: requestId=" << baseRequestId
    //         << " traceId=" << ev->TraceId.GetHexTraceId()
    //         << " segmentIndex=" << segmentIndex << " offset=" << requestCtx.Offset
    //         << " size=" << responseData.size() << " dataPrefix[" << dataLogPrefix << " ]"
    //         << " dataSuffix[" << dataLogSuffix << " ]");
    // }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "READ RESPONSE: baseRequestId=" << baseRequestId << " segmentIndex=" << segmentIndex
        << " responseSize=" << responseData.size() << " " << responseHash);

    // Handle multi-segment reads
    if (requestCtx.IsMultiSegmentRead) {
        HandleMultiSegmentResponse(ctx, requestCtx, msg, error, segmentIndex, ev->TraceId);
        requestCtx.PendingResponses--;

        // Check if all segments are complete
        if (requestCtx.PendingResponses == 0) {
            if (AllSegmentsComplete(requestCtx)) {
                ReassembleAndCompleteRead(ctx, requestCtx, ev->TraceId);

                // End parent span with success
                if (requestCtx.Span) {
                    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                        "READ: ending parent span with OK status for requestId=" << baseRequestId);
                    requestCtx.Span.EndOk();
                }
            } else {
                // Some segments failed
                requestCtx.AccumulatedError = NCloud::MakeError(E_FAIL, "Failed to read all segments");
                CompleteReadRequest(ctx, requestCtx);

                // End parent span with error
                if (requestCtx.Span) {
                    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                        "READ: ending parent span with ERROR status for requestId=" << baseRequestId);
                    requestCtx.Span.EndError("Failed to read all segments");
                }
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

        // End parent span with appropriate status
        if (requestCtx.Span) {
            if (NCloud::HasError(requestCtx.AccumulatedError)) {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "READ: ending parent span with ERROR status for requestId=" << baseRequestId
                    << " error=" << requestCtx.AccumulatedError.GetMessage());
                requestCtx.Span.EndError(requestCtx.AccumulatedError.GetMessage());
            } else {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "READ: ending parent span with OK status for requestId=" << baseRequestId);
                requestCtx.Span.EndOk();
            }
        }

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
    ui32 segmentIndex,
    const NWilson::TTraceId& traceId)
{
    if (NCloud::HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "SEGMENT ERROR: requestId=" << requestCtx.RequestId
            << " traceId=" << traceId.GetHexTraceId()
            << " segmentIndex=" << segmentIndex << " error=" << error.GetMessage());
        return;
    }

    // Validate segment index
    if (segmentIndex >= requestCtx.ReadSegments.size()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "INVALID SEGMENT INDEX: segmentIndex=" << segmentIndex
            << " traceId=" << traceId.GetHexTraceId()
            << " maxSegments=" << requestCtx.ReadSegments.size());
        return;
    }

    const TString& responseData = msg->Record.GetData();
    auto& segment = requestCtx.ReadSegments[segmentIndex];

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 SEGMENT RESPONSE: requestId=" << requestCtx.RequestId
        << " traceId=" << traceId.GetHexTraceId()
        << " segmentIndex=" << segmentIndex << " offset=" << segment.Offset
        << " expectedSize=" << segment.Size << " receivedSize=" << responseData.size());

    // Store the response data
    segment.Data = responseData;
    segment.IsComplete = true;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 SEGMENT COMPLETE: segmentIndex=" << segmentIndex
        << " traceId=" << traceId.GetHexTraceId()
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
    TDDiskRequestContext& requestCtx,
    const NWilson::TTraceId& traceId)
{
    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "🔧 REASSEMBLING: requestId=" << requestCtx.RequestId
        << " traceId=" << traceId.GetHexTraceId()
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

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << baseRequestId << "] 📥 WRITE RESPONSE RECEIVED: responseRequestId=" << responseRequestId
        << " traceId=" << ev->TraceId.GetHexTraceId()
        << " (baseId=" << baseRequestId << " segIdx=" << segmentIndex << ")"
        << " status=" << msg->Record.GetStatus());

    auto it = PendingRequests.find(baseRequestId);
    if (it == PendingRequests.end()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "🚨 WRITE RESPONSE ORPHAN: baseRequestId=" << baseRequestId
            << " traceId=" << ev->TraceId.GetHexTraceId()
            << " segmentIndex=" << segmentIndex << " responseRequestId=" << responseRequestId
            << " - REQUEST NOT FOUND IN PENDING REQUESTS!");
        return; // Request already completed or timed out
    }

    auto& requestCtx = it->second;

    // Convert YDB status to NBS error
    NCloud::NProto::TError error;
    if (msg->Record.GetStatus() != NKikimrProto::OK) {
        error = NCloud::MakeError(E_FAIL, msg->Record.GetErrorReason());
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "📝 DDISK WRITE FAILED: requestId=" << baseRequestId
            << " traceId=" << ev->TraceId.GetHexTraceId()
            << " segmentIndex=" << segmentIndex
            << " status=" << msg->Record.GetStatus()
            << " error=" << msg->Record.GetErrorReason());
    } else {
        error = NCloud::MakeError(S_OK);
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "📝 DDISK WRITE SUCCESS: requestId=" << baseRequestId
            << " traceId=" << ev->TraceId.GetHexTraceId()
            << " segmentIndex=" << segmentIndex
            << " confirmedBytes=" << (msg->Record.HasSize() ? msg->Record.GetSize() : 0)
            << " chunkId=" << (msg->Record.HasChunkId() ? msg->Record.GetChunkId() : 0)
            << " status=OK");
    }

    // End child span for this DDisk subrequest
    auto childSpanIt = requestCtx.ChildSpans.find(responseRequestId);
    if (childSpanIt != requestCtx.ChildSpans.end()) {
        childSpanIt->second.Event("Received_TEvDDiskWriteResponse");
        if (NCloud::HasError(error)) {
            childSpanIt->second.EndError(msg->Record.GetErrorReason());
        } else {
            childSpanIt->second.EndOk();
        }
        // Remove child span after ending it (don't access after this)
        requestCtx.ChildSpans.erase(childSpanIt);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE RESPONSE: ended child span for requestId=" << responseRequestId);
    } else {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "WRITE RESPONSE: child span not found for requestId=" << responseRequestId);
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "WRITE RESPONSE: baseRequestId=" << baseRequestId
        << " traceId=" << ev->TraceId.GetHexTraceId() << " segmentIndex=" << segmentIndex
        << " status=" << (NCloud::HasError(error) ? "ERROR" : "OK"));

    if (requestCtx.IsMultiSegmentWrite) {
        // Handle multi-segment write response
        if (segmentIndex < requestCtx.WriteSegments.size()) {
            requestCtx.WriteSegments[segmentIndex].IsComplete = true;
            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "WRITE SEGMENT COMPLETE: requestId=" << baseRequestId
                << " traceId=" << ev->TraceId.GetHexTraceId()
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
                << " traceId=" << ev->TraceId.GetHexTraceId()
                << " allComplete=" << allComplete);

        CompleteWriteRequest(ctx, requestCtx);

        // End parent span with appropriate status
        if (requestCtx.Span) {
            if (NCloud::HasError(requestCtx.AccumulatedError)) {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE: ending parent span with ERROR status for requestId=" << baseRequestId
                    << " error=" << requestCtx.AccumulatedError.GetMessage());
                requestCtx.Span.EndError(requestCtx.AccumulatedError.GetMessage());
            } else {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE: ending parent span with OK status for requestId=" << baseRequestId);
                requestCtx.Span.EndOk();
            }
        }

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

        // End parent span with appropriate status
        if (requestCtx.Span) {
            if (NCloud::HasError(requestCtx.AccumulatedError)) {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE: ending parent span with ERROR status for requestId=" << baseRequestId
                    << " error=" << requestCtx.AccumulatedError.GetMessage());
                requestCtx.Span.EndError(requestCtx.AccumulatedError.GetMessage());
            } else {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "WRITE: ending parent span with OK status for requestId=" << baseRequestId);
                requestCtx.Span.EndOk();
            }
        }

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
// Config update for workers

void TProxyStorage::UpdateConfig(const TWorkerStorageConfig& newConfig)
{
    if (UsePartitionState) {
        // This method should only be called for workers, not for main actor
        return;
    }
    Config = newConfig;
}

////////////////////////////////////////////////////////////////////////////////
// Helper methods to abstract data access (supports both Config and PartitionState modes)

ui32 TProxyStorage::GetBlockSize() const
{
    if (UsePartitionState) {
        return PartitionState->GetBlockSize();
    } else {
        return Config.BlockSize;
    }
}

ui32 TProxyStorage::GetChunkSize() const
{
    if (UsePartitionState) {
        return PartitionState->GetChunkSize();
    } else {
        return Config.ChunkSize;
    }
}

const TVector<TActorId>& TProxyStorage::GetDDiskServiceIds() const
{
    if (UsePartitionState) {
        return PartitionState->GetDDiskServiceIds();
    } else {
        return Config.DDiskServiceIds;
    }
}

TVector<TActorId> TProxyStorage::GetDDiskServiceIdsForGroup(ui32 groupIndex) const
{
    if (UsePartitionState) {
        return PartitionState->GetDDiskServiceIdsForGroup(groupIndex);
    } else {
        return Config.GetDDiskServiceIdsForGroup(groupIndex);
    }
}

ui32 TProxyStorage::CalculateGroupIndex(ui64 offset) const
{
    if (UsePartitionState) {
        return PartitionState->CalculateGroupIndex(offset);
    } else {
        return Config.CalculateGroupIndex(offset);
    }
}

bool TProxyStorage::FindChunkForOffset(ui64 offset, ui32& chunkId, TString& ddiskServiceId, ui32& chunkRelativeOffset) const
{
    if (UsePartitionState) {
        return PartitionState->FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset);
    } else {
        return Config.FindChunkForOffset(offset, chunkId, ddiskServiceId, chunkRelativeOffset);
    }
}


} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
