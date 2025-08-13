#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include "partition_direct_storage.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

class TProxyStorage final
    : public TPartitionStorage
{
public:
    TProxyStorage(ui32 blockSize)
    {
        Y_UNUSED(blockSize);
    }

    NProto::TError ReadBlocksLocal(
        const TActorContext& ctx,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    NProto::TError WriteBlocksLocal(
        const TActorContext& ctx,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    NProto::TError ZeroBlocks(
        const TActorContext& ctx,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
