#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EStorageType {
    Memory,
    Proxy
};

class TPartitionStorage
{
public:
    virtual ~TPartitionStorage() = default;

    virtual NProto::TError ReadBlocksLocal(
        const NActors::TActorContext& ctx,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) = 0;

    virtual NProto::TError WriteBlocksLocal(
        const NActors::TActorContext& ctx,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) = 0;

    virtual NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) = 0;
};

using TPartitionStoragePtr = std::shared_ptr<TPartitionStorage>;

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
