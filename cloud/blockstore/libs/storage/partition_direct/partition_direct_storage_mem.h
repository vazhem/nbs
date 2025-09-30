#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

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

    NCloud::NProto::TError ReadBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NCloud::NProto::TError WriteBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NCloud::NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        NWilson::TTraceId traceId) override;
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
