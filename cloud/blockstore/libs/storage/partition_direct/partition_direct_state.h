#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/services/blobstorage_service_id.h>
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>


#include "partition_direct_storage.h"
#include "partition_direct_storage_mem.h"
#include "partition_direct_storage_proxy.h"
#include "public.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TDDiskInfo {
    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    ui32 VSlotId = 0;
    ui32 OrderInGroup = 0;
    ui32 GroupIndex = 0;    // Which group this DDisk belongs to (0-2 for striping)
    NActors::TActorId ServiceId;  // Constructed DDisk service ID

    TDDiskInfo() = default;

    TDDiskInfo(ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui32 orderInGroup, ui32 groupIndex = 0)
        : NodeId(nodeId)
        , PDiskId(pdiskId)
        , VSlotId(vslotId)
        , OrderInGroup(orderInGroup)
        , GroupIndex(groupIndex)
        , ServiceId(NKikimr::MakeBlobStorageVDiskID(nodeId, pdiskId, vslotId))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

enum class EChunkStatus : ui32 {
    Available = 0,
    Allocated = 1,
    Error = 2
};

struct TChunkRegionInfo {
    ui64 StartOffset;        // 4MB aligned offset (fits within PDisk chunk size of ~4.25MB)
    ui32 ChunkId;            // PDisk chunk ID
    TString DDiskServiceId;  // DDisk service ID for sending requests

    TChunkRegionInfo() = default;

    TChunkRegionInfo(ui64 startOffset, ui32 chunkId, const TString& ddiskServiceId)
        : StartOffset(startOffset)
        , ChunkId(chunkId)
        , DDiskServiceId(ddiskServiceId)
    {}

    ui64 GetRegionIndex() const {
        static constexpr ui64 CHUNK_SIZE = 128 * 1024 * 1024;
        return StartOffset / CHUNK_SIZE;
    }
};

struct TGroupChunkInfo {
    ui32 ChunkId;
    TString DDiskServiceId;
    ui32 ChunkIndexInGroup;  // 0, 1, 2, ... within this group

    TGroupChunkInfo() = default;

    TGroupChunkInfo(ui32 chunkId, const TString& ddiskServiceId, ui32 chunkIndexInGroup)
        : ChunkId(chunkId)
        , DDiskServiceId(ddiskServiceId)
        , ChunkIndexInGroup(chunkIndexInGroup)
    {}
};

// New region-based striping approach
struct TRegionChunkMapping {
    ui64 RegionIndex;
    ui64 StartOffset;                              // Start offset of this region in volume
    ui64 EndOffset;                                // End offset of this region in volume
    TVector<ui32> ChunkIds;                        // One chunk ID per group (16 chunks total)
    TVector<TString> DDiskServiceIds;              // Corresponding DDisk service IDs

    TRegionChunkMapping() = default;

    TRegionChunkMapping(ui64 regionIndex, ui64 startOffset, ui64 endOffset)
        : RegionIndex(regionIndex)
        , StartOffset(startOffset)
        , EndOffset(endOffset)
    {
        ChunkIds.resize(16);  // GROUPS_COUNT
        DDiskServiceIds.resize(16);
    }
};

struct TAvailableChunkInfo {
    ui32 ChunkId;
    TString DDiskServiceId;  // DDisk service ID for sending requests
    EChunkStatus Status;
    ui64 RegionIndex;        // Which region this chunk serves (0 if not allocated)

    TAvailableChunkInfo() = default;

    TAvailableChunkInfo(ui32 chunkId, const TString& ddiskServiceId, EChunkStatus status, ui64 regionIndex)
        : ChunkId(chunkId)
        , DDiskServiceId(ddiskServiceId)
        , Status(status)
        , RegionIndex(regionIndex)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionState
{
private:
    const NProto::TPartitionConfig& Config;
    NKikimr::TTabletStorageInfoPtr StorageInfo;
    TPartitionStoragePtr Storage;

    EStorageType storageType = EStorageType::Memory;
    ui32 VirtualGroupId = 0;  // ID of the virtual group with mirror-3-direct erasure
    ui64 StoragePoolId = 0;   // ID of the storage pool containing our group
    TVector<TDDiskInfo> DDiskInfos;  // Information about DDisk actors in the group
    TVector<NActors::TActorId> DDiskServiceIds;  // Pre-cached service IDs for efficiency
    ui32 ChunkSize;  // Dynamic chunk size from DDisk (in bytes)

    // Chunk management in-memory cache (single-threaded actor, no mutex needed)
    THashMap<ui64, TChunkRegionInfo> ChunkRegionCache;  // startOffset -> chunk info (legacy)
    THashMap<ui32, TAvailableChunkInfo> AvailableChunksCache;  // chunkId -> chunk info

    // Group-aware chunk cache: groupIndex -> vector of chunks in order (legacy)
    THashMap<ui32, TVector<TGroupChunkInfo>> GroupChunkCache;

    // New region-based chunk mapping: regionIndex -> chunk mapping
    THashMap<ui64, TRegionChunkMapping> RegionChunkCache;

public:
    // Number of DDisk actors in the group from config
    static constexpr ui32 DDISK_COUNT = 2;

    // Constructor
    TPartitionState(
        NKikimr::TTabletStorageInfoPtr storageInfo,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        const NProto::TPartitionConfig& partitionConfig,
        EStorageAccessMode& storageAccessMode,
        ui32& siblingCount,
        const TActorId& volumeActorId,
        ui64& volumeTabletId)
        : Config(partitionConfig)
        , StorageInfo(storageInfo)
        , Storage(nullptr)  // Will be initialized later by actor
        , ChunkSize(0)  // Will be set later when we get DDisk response
    {
        Y_UNUSED(config);
        Y_UNUSED(diagnosticsConfig);
        Y_UNUSED(profileLog);
        Y_UNUSED(blockDigestGenerator);
        Y_UNUSED(storageAccessMode);
        Y_UNUSED(siblingCount);
        Y_UNUSED(volumeActorId);
        Y_UNUSED(volumeTabletId);
    }

    // Basic getters
    EStorageType GetStorageType() const
    {
        return storageType;
    }

    void SetStorageType(EStorageType newStorageType)
    {
        storageType = newStorageType;
    }

    ui32 GetVirtualGroupId() const
    {
        return VirtualGroupId;
    }

    void SetVirtualGroupId(ui32 groupId)
    {
        VirtualGroupId = groupId;
    }

    ui32 GetBlockSize() const
    {
        return Config.GetBlockSize();
    }

    ui64 GetBlockCount() const
    {
        return Config.GetBlocksCount();
    }

    TPartitionStoragePtr GetStorage() const
    {
        return Storage;
    }

    void SetStorage(TPartitionStoragePtr storage)
    {
        Storage = storage;
    }

    bool ValidateBlockRange(ui64 blockIndex, ui32 blocksCount) const
    {
        return blockIndex + blocksCount <= GetBlockCount();
    }

    TBuffer* AllocateBuffer(ui32 size)
    {
        return new TBuffer(size);
    }

    NProto::TError ReadBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 startIndex,
        ui32 blocksCount,
        const TBlockDataRef& buffer,
        const NWilson::TTraceId& traceId = {});

    NProto::TError WriteBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 startIndex,
        ui32 blocksCount,
        const TBlockDataRef& buffer,
        const NWilson::TTraceId& traceId = {});

    NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 startIndex,
        ui32 blocksCount);

    void CopyToSgList(const TBlockDataRef& buffer, const TSgList& sglist);
    void CopyFromSgList(const TSgList& sglist, TBlockDataRef& buffer);

    bool CheckBlockRange(const TBlockRange64& range) const
    {
        return range.Start < GetBlockCount() && range.End < GetBlockCount();
    }

    ui64 GetStoragePoolId() const
    {
        return StoragePoolId;
    }

    void SetStoragePoolId(ui64 poolId)
    {
        StoragePoolId = poolId;
    }

    const TVector<TDDiskInfo>& GetDDiskInfos() const
    {
        return DDiskInfos;
    }

    void SetDDiskInfos(const TVector<TDDiskInfo>& ddiskInfos)
    {
        DDiskInfos = ddiskInfos;

        // Pre-cache service IDs for efficiency
        DDiskServiceIds.clear();
        DDiskServiceIds.reserve(ddiskInfos.size());
        for (const auto& ddiskInfo : ddiskInfos) {
            DDiskServiceIds.push_back(ddiskInfo.ServiceId);
        }
    }

    bool HasDDiskInfos() const
    {
        return !DDiskInfos.empty();
    }

    NActors::TActorId GetDDiskServiceId(ui32 ddiskIndex) const
    {
        if (ddiskIndex >= DDiskInfos.size()) {
            return NActors::TActorId();
        }
        return DDiskInfos[ddiskIndex].ServiceId;
    }

    const TVector<NActors::TActorId>& GetDDiskServiceIds() const
    {
        return DDiskServiceIds;
    }

    // Get DDisk service IDs for a specific group (for striping)
    TVector<NActors::TActorId> GetDDiskServiceIdsForGroup(ui32 groupIndex) const
    {
        TVector<NActors::TActorId> groupDDisks;
        for (const auto& ddiskInfo : DDiskInfos) {
            if (ddiskInfo.GroupIndex == groupIndex) {
                groupDDisks.push_back(ddiskInfo.ServiceId);
            }
        }
        return groupDDisks;
    }

    //
    // Chunk management methods
    //

    // Striping constants for multi-group operations
    static constexpr ui64 STRIPE_SIZE = 512 * 1024;  // 512KB stripe size (legacy)
    static constexpr ui32 GROUPS_COUNT = 16;             // number of groups

    // Region-based methods
    ui64 GetRegionSize() const {
        return static_cast<ui64>(GROUPS_COUNT) * ChunkSize;
    }

    ui64 CalculateRegionIndex(ui64 offset) const {
        return offset / GetRegionSize();
    }

    ui64 CalculateOffsetWithinRegion(ui64 offset) const {
        return offset % GetRegionSize();
    }

    ui32 CalculateGroupWithinRegion(ui64 offsetWithinRegion) const {
        return static_cast<ui32>(offsetWithinRegion / ChunkSize);
    }

    ui32 CalculateOffsetWithinChunk(ui64 offsetWithinRegion) const {
        return static_cast<ui32>(offsetWithinRegion % ChunkSize);
    }

    // Striping methods for multi-group operations
    ui32 CalculateGroupIndex(ui64 offset) const
    {
        // Calculate which group should handle this offset based on 512KB striping
        ui64 stripeIndex = offset / STRIPE_SIZE;
        ui32 groupIndex = static_cast<ui32>(stripeIndex % GROUPS_COUNT);

        return groupIndex;
    }

    ui64 CalculateGroupOffset(ui64 offset) const
    {
        // Calculate the offset within the group by removing striping
        ui64 stripeIndex = offset / STRIPE_SIZE;
        ui64 offsetWithinStripe = offset % STRIPE_SIZE;
        ui64 groupStripeIndex = stripeIndex / GROUPS_COUNT;
        return groupStripeIndex * STRIPE_SIZE + offsetWithinStripe;
    }

    ui64 CalculateGlobalOffset(ui32 groupIndex, ui64 groupOffset) const
    {
        // Reverse calculation: convert group offset back to global offset
        ui64 groupStripeIndex = groupOffset / STRIPE_SIZE;
        ui64 offsetWithinStripe = groupOffset % STRIPE_SIZE;
        ui64 globalStripeIndex = groupStripeIndex * GROUPS_COUNT + groupIndex;
        return globalStripeIndex * STRIPE_SIZE + offsetWithinStripe;
    }

    // Getter for chunk size
    ui32 GetChunkSize() const
    {
        return ChunkSize;
    }

    // Setter for chunk size (called when receiving DDisk response)
    void SetChunkSize(ui32 chunkSize)
    {
        if (ChunkSize == 0) {
            ChunkSize = chunkSize;
            // LOG_INFO can't be used here without context, will be logged from caller
        }
    }

    // Region-based chunk lookup (new approach)
    bool FindChunkForOffset(ui64 offset, ui32& chunkId, TString& ddiskServiceId, ui32& chunkRelativeOffset) const
    {
        // Step 1: Use original striping logic to determine the group
        ui32 groupIndex = CalculateGroupIndex(offset);
        ui64 groupOffset = CalculateGroupOffset(offset);

        // Step 2: Calculate which region this group offset belongs to
        // CRITICAL: This calculation must match the allocation logic
        ui32 chunkIndexInGroup = static_cast<ui32>(groupOffset / ChunkSize);

        // Step 3: Determine which region should handle this chunk index in the group
        // FIXED: Use consistent region selection logic
        ui64 totalRegions = GetTotalRegionsNeeded();
        if (RegionChunkCache.empty()) {
            // CRITICAL ERROR: No regions allocated - this should not happen
            return false;
        }

        // CRITICAL: This must match CompletePreallocateVolumeChunks logic:
        // ui64 targetRegionIndex = logicalChunkIndexInGroup % totalRegionsNeeded;
        ui64 targetRegionIndex = chunkIndexInGroup % totalRegions;

        // Step 4: Get the chunk ID from the region mapping
        auto regionIt = RegionChunkCache.find(targetRegionIndex);
        if (regionIt == RegionChunkCache.end()) {
            // Region not found - this is now a CRITICAL ERROR (no fallback to legacy)
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

public:

    void AddChunkRegionToCache(const TChunkRegionInfo& chunkInfo)
    {
        ChunkRegionCache[chunkInfo.StartOffset] = chunkInfo;
    }

    void AddAvailableChunkToCache(const TAvailableChunkInfo& chunkInfo)
    {
        AvailableChunksCache[chunkInfo.ChunkId] = chunkInfo;
    }

    // Region-based chunk management methods
    void AddRegionMapping(ui64 regionIndex, ui32 groupIndex, ui32 chunkId, const TString& ddiskServiceId)
    {
        auto& regionMapping = RegionChunkCache[regionIndex];
        if (regionMapping.ChunkIds.empty()) {
            // Initialize new region
            ui64 startOffset = regionIndex * GetRegionSize();
            ui64 endOffset = startOffset + GetRegionSize() - 1;
            regionMapping = TRegionChunkMapping(regionIndex, startOffset, endOffset);
        }

        regionMapping.ChunkIds[groupIndex] = chunkId;
        regionMapping.DDiskServiceIds[groupIndex] = ddiskServiceId;
    }

    bool GetRegionMapping(ui64 regionIndex, TRegionChunkMapping& mapping) const
    {
        auto it = RegionChunkCache.find(regionIndex);
        if (it != RegionChunkCache.end()) {
            mapping = it->second;
            return true;
        }
        return false;
    }

    // Group-aware chunk management
    void AddChunkToGroup(ui32 groupIndex, ui32 chunkId, const TString& ddiskServiceId)
    {
        auto& groupChunks = GroupChunkCache[groupIndex];
        ui32 chunkIndexInGroup = static_cast<ui32>(groupChunks.size());

        groupChunks.emplace_back(chunkId, ddiskServiceId, chunkIndexInGroup);

        // DEBUG: This should be logged in CompletePreallocateVolumeChunks

        // Also maintain legacy cache for compatibility
        ui64 groupOffset = chunkIndexInGroup * ChunkSize;
        ui64 globalOffset = CalculateGlobalOffset(groupIndex, groupOffset);

        // Create legacy chunk region info
        TChunkRegionInfo legacyInfo(globalOffset, chunkId, ddiskServiceId);

        AddChunkRegionToCache(legacyInfo);
    }

    // Loader for chunk caches from persisted data
    void LoadChunkCaches(const TVector<TChunkRegionInfo>& regions)
    {
        // Rebuild both GroupChunkCache (for backward compatibility) and RegionChunkCache
        GroupChunkCache.clear();
        RegionChunkCache.clear();

        // Sort regions by GlobalOffset to process in allocation order
        TVector<TChunkRegionInfo> sortedRegions(regions.begin(), regions.end());
        std::sort(sortedRegions.begin(), sortedRegions.end(),
            [](const TChunkRegionInfo& a, const TChunkRegionInfo& b) {
                return a.StartOffset < b.StartOffset;
            });

        for (const auto& region : sortedRegions) {
            // Legacy GroupChunkCache rebuild
            for (ui32 testGroupIndex = 0; testGroupIndex < GROUPS_COUNT; ++testGroupIndex) {
                auto& groupChunks = GroupChunkCache[testGroupIndex];
                ui64 testGroupOffset = groupChunks.size() * ChunkSize;
                ui64 expectedGlobalOffset = CalculateGlobalOffset(testGroupIndex, testGroupOffset);

                if (expectedGlobalOffset == region.StartOffset) {
                    // This region belongs to testGroupIndex
                    groupChunks.emplace_back(region.ChunkId, region.DDiskServiceId, groupChunks.size());

                    // Map to new region-based system
                    ui64 regionIndex = CalculateRegionIndex(region.StartOffset);
                    ui64 offsetWithinRegion = CalculateOffsetWithinRegion(region.StartOffset);
                    ui32 groupWithinRegion = CalculateGroupWithinRegion(offsetWithinRegion);

                    // Add to RegionChunkCache
                    auto& regionMapping = RegionChunkCache[regionIndex];
                    if (regionMapping.ChunkIds.empty()) {
                        ui64 startOffset = regionIndex * GetRegionSize();
                        ui64 endOffset = startOffset + GetRegionSize() - 1;
                        regionMapping = TRegionChunkMapping(regionIndex, startOffset, endOffset);
                    }
                    regionMapping.ChunkIds[groupWithinRegion] = region.ChunkId;
                    regionMapping.DDiskServiceIds[groupWithinRegion] = region.DDiskServiceId;
                    break;
                }
            }
        }
    }

    ui32 GetAvailableChunksCount() const
    {
        ui32 count = 0;
        for (const auto& [id, chunk] : AvailableChunksCache) {
            if (chunk.Status == EChunkStatus::Available) {
                count++;
            }
        }
        return count;
    }

    ui32 GetGroupChunkCount(ui32 groupIndex) const
    {
        auto it = GroupChunkCache.find(groupIndex);
        if (it == GroupChunkCache.end()) {
            return 0;
        }
        return static_cast<ui32>(it->second.size());
    }

    ui32 GetChunkRegionCacheSize() const
    {
        return static_cast<ui32>(ChunkRegionCache.size());
    }

    ui32 GetRegionCount() const
    {
        return static_cast<ui32>(RegionChunkCache.size());
    }

    ui64 GetTotalRegionsNeeded() const
    {
        ui64 volumeSize = static_cast<ui64>(GetBlockSize()) * GetBlockCount();
        ui64 regionSize = GetRegionSize();
        ui64 result = (volumeSize + regionSize - 1) / regionSize;  // Round up

        // Validation: Log if parameters seem inconsistent
        static ui64 lastVolumeSize = 0;
        static ui64 lastResult = 0;
        if (lastVolumeSize != 0 && (lastVolumeSize != volumeSize || lastResult != result)) {
            // Volume parameters changed during operation - potential issue
        }
        lastVolumeSize = volumeSize;
        lastResult = result;

        return result;
    }

    void UpdateChunkStatus(ui32 chunkId, EChunkStatus status, ui64 regionIndex)
    {
        auto it = AvailableChunksCache.find(chunkId);
        if (it != AvailableChunksCache.end()) {
            it->second.Status = status;
            it->second.RegionIndex = regionIndex;
        }
    }

    // Find the DDisk ActorId that corresponds to a service ID (using ActorId string representation)
    NActors::TActorId GetDDiskActorIdByServiceId(const TString& serviceId) const
    {
        for (const auto& actorId : DDiskServiceIds) {
            if (actorId.ToString() == serviceId) {
                return actorId;
            }
        }
        return NActors::TActorId();  // Not found
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
