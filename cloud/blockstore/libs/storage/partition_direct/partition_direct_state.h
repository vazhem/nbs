#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/services/blobstorage_service_id.h>

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
    NActors::TActorId ServiceId;  // Constructed DDisk service ID

    TDDiskInfo() = default;

    TDDiskInfo(ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui32 orderInGroup)
        : NodeId(nodeId)
        , PDiskId(pdiskId)
        , VSlotId(vslotId)
        , OrderInGroup(orderInGroup)
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

    EStorageType storageType = EStorageType::Proxy;
    ui32 VirtualGroupId = 0;  // ID of the virtual group with mirror-3-direct erasure
    ui64 StoragePoolId = 0;   // ID of the storage pool containing our group
    TVector<TDDiskInfo> DDiskInfos;  // Information about DDisk actors in the group
    TVector<NActors::TActorId> DDiskServiceIds;  // Pre-cached service IDs for efficiency
    ui32 ChunkSize;  // Dynamic chunk size from DDisk (in bytes)

    // Chunk management in-memory cache (single-threaded actor, no mutex needed)
    THashMap<ui64, TChunkRegionInfo> ChunkRegionCache;  // startOffset -> chunk info
    THashMap<ui32, TAvailableChunkInfo> AvailableChunksCache;  // chunkId -> chunk info

public:
    explicit TPartitionState(
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        const NProto::TPartitionConfig& partitionConfig,
        EStorageAccessMode storageAccessMode,
        ui32 siblingCount,
        const NActors::TActorId& owner,
        ui64 initialCommitId)
        : Config(partitionConfig)
        , StorageInfo(std::move(storage))
        , ChunkSize(0)  // Will be set when first DDisk reports chunk size
    {
        if (storageType == EStorageType::Memory) {
            Storage = std::make_shared<TInMemoryStorage>(Config.GetBlockSize());
        } else {
            Storage = std::make_shared<TProxyStorage>(
                Config.GetBlockSize(), owner, this);
        }

        Y_UNUSED(config);
        Y_UNUSED(diagnosticsConfig);
        Y_UNUSED(profileLog);
        Y_UNUSED(digestGenerator);
        Y_UNUSED(storageAccessMode);
        Y_UNUSED(siblingCount);
        Y_UNUSED(owner);
        Y_UNUSED(initialCommitId);
    }

    const NProto::TPartitionConfig& GetConfig() const
    {
        return Config;
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
        const TBlockDataRef& buffer);

    NProto::TError WriteBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        ui64 startIndex,
        ui32 blocksCount,
        const TBlockDataRef& buffer);

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

    ui32 GetVirtualGroupId() const
    {
        return VirtualGroupId;
    }

    void SetVirtualGroupId(ui32 groupId)
    {
        VirtualGroupId = groupId;
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

    //
    // Chunk management methods
    //

    // Calculate region index from offset using dynamic chunk size
    ui64 CalculateRegionIndex(ui64 offset) const
    {
        Y_ABORT_UNLESS(ChunkSize > 0, "ChunkSize must be set before calculating regions");
        return offset / ChunkSize;
    }

    ui64 CalculateRegionStartOffset(ui64 regionIndex) const
    {
        Y_ABORT_UNLESS(ChunkSize > 0, "ChunkSize must be set before calculating regions");
        return regionIndex * ChunkSize;
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
        } else if (ChunkSize != chunkSize) {
            // LOG_WARN can't be used here without context, will be logged from caller
            Y_ABORT_UNLESS(false, "Chunk size mismatch! Expected: %u, received: %u",
                          ChunkSize, chunkSize);
        }
    }

    // Cache operations (single-threaded actor, no locking needed)
    bool FindChunkForRegion(ui64 regionIndex, TChunkRegionInfo& chunkInfo) const
    {
        ui64 startOffset = CalculateRegionStartOffset(regionIndex);
        auto it = ChunkRegionCache.find(startOffset);
        if (it != ChunkRegionCache.end()) {
            chunkInfo = it->second;
            return true;
        }
        return false;
    }

    void AddChunkRegionToCache(const TChunkRegionInfo& chunkInfo)
    {
        ChunkRegionCache[chunkInfo.StartOffset] = chunkInfo;
    }

    void AddAvailableChunkToCache(const TAvailableChunkInfo& chunkInfo)
    {
        AvailableChunksCache[chunkInfo.ChunkId] = chunkInfo;
    }

    bool FindAvailableChunkForDDisk(const TString& ddiskServiceId, ui32& chunkId)
    {
        for (const auto& [id, chunk] : AvailableChunksCache) {
            if (chunk.DDiskServiceId == ddiskServiceId && chunk.Status == EChunkStatus::Available) {
                chunkId = id;
                return true;
            }
        }
        return false;
    }

    void UpdateChunkStatus(ui32 chunkId, EChunkStatus newStatus, ui64 regionIndex)
    {
        auto it = AvailableChunksCache.find(chunkId);
        if (it != AvailableChunksCache.end()) {
            it->second.Status = newStatus;
            it->second.RegionIndex = regionIndex;
        }
    }

    void LoadChunkCaches(const TVector<TChunkRegionInfo>& regions, const TVector<TAvailableChunkInfo>& chunks)
    {
        ChunkRegionCache.clear();
        for (const auto& region : regions) {
            ChunkRegionCache[region.StartOffset] = region;
        }

        AvailableChunksCache.clear();
        for (const auto& chunk : chunks) {
            AvailableChunksCache[chunk.ChunkId] = chunk;
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

    ui32 GetChunkRegionCacheSize() const
    {
        return static_cast<ui32>(ChunkRegionCache.size());
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
