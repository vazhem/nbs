#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/generic/hash.h>

#include "partition_direct_storage.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryStorage final
    : public TPartitionStorage
{
private:
    THashMap<ui64, TString> Blocks;
    ui32 BlockSize;

public:
    TInMemoryStorage(ui32 blockSize)
        : BlockSize(blockSize)
    {}

    NProto::TError ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    NProto::TError WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    NProto::TError ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    // Остальные методы IStorage можно оставить с заглушками
    // или реализовать по аналогии
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
