#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

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

    virtual NCloud::NProto::TError ReadBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId = {}) = 0;

    virtual NCloud::NProto::TError WriteBlocksLocal(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId = {}) = 0;

    virtual NCloud::NProto::TError ZeroBlocks(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        const NWilson::TTraceId& traceId = {}) = 0;
};

using TPartitionStoragePtr = std::shared_ptr<TPartitionStorage>;

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
