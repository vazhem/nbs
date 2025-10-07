#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

namespace NCloud::NBlockStore::NProto {

// Output operators for proto enums (defined in partition_direct.cpp)
IOutputStream& operator<<(IOutputStream& out, EPartitionDirectMode mode);
IOutputStream& operator<<(IOutputStream& out, EPartitionDirectWorkerMode mode);

} // namespace NCloud::NBlockStore::NProto

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Use proto enums directly (similar to EResyncPolicy pattern)

class TPartitionStorage
{
public:
    virtual ~TPartitionStorage() = default;

    virtual NCloud::NProto::TError ReadBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual NCloud::NProto::TError WriteBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) = 0;

    virtual NCloud::NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        NWilson::TTraceId traceId ) = 0;
};

using TPartitionStoragePtr = std::shared_ptr<TPartitionStorage>;

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
