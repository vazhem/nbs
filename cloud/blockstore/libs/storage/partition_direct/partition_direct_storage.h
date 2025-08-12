#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionStorage
{
public:
    virtual ~TPartitionStorage() = default;

    virtual NProto::TError ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) = 0;

    virtual NProto::TError WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) = 0;

    virtual NProto::TError ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) = 0;
};

using TPartitionStoragePtr = std::shared_ptr<TPartitionStorage>;

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
