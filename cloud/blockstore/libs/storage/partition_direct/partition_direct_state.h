#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/services/blobstorage_service_id.h>

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
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
