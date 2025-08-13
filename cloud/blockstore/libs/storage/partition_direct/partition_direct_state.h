#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/storage/core/libs/actors/public.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include "partition_direct_storage.h"
#include "partition_direct_storage_mem.h"
#include "partition_direct_storage_proxy.h"
#include "public.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionState
{
private:
    const NProto::TPartitionConfig& Config;
    NKikimr::TTabletStorageInfoPtr StorageInfo;
    TPartitionStoragePtr Storage;

    EStorageType storageType = EStorageType::Memory;

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
                Config.GetBlockSize());
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
        ui64 startIndex,
        ui32 blocksCount,
        const TBlockDataRef& buffer);

    NProto::TError WriteBlocks(
        const NActors::TActorContext& ctx,
        ui64 startIndex,
        ui32 blocksCount,
        const TBlockDataRef& buffer);

    NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        ui64 startIndex,
        ui32 blocksCount);

    void CopyToSgList(const TBlockDataRef& buffer, const TSgList& sglist);
    void CopyFromSgList(const TSgList& sglist, TBlockDataRef& buffer);

    bool CheckBlockRange(const TBlockRange64& range) const
    {
        return range.Start < GetBlockCount() && range.End < GetBlockCount();
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
