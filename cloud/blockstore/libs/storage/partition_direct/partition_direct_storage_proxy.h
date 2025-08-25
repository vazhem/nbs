#pragma once

#include "partition_direct_storage.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/blobstorage/ddisk/ddisk_events.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

// DDisk events are defined in blobstorage.h

class TPartitionState;

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

//
// Async request context for tracking DDisk operations
//
struct TDDiskRequestContext {
    enum class ERequestType {
        Read,
        Write
    };

    ERequestType RequestType;
    TRequestInfoPtr OriginalRequest;
    ui64 RequestId;
    TVector<TActorId> DDiskActorIds;
    ui32 PendingResponses;
    NCloud::NProto::TError AccumulatedError;
    TString ReadData;  // For read requests
    ui64 Offset;
    ui32 Size;
    std::shared_ptr<NProto::TReadBlocksLocalRequest> OriginalReadRequest;  // For accessing sglist in reads

    TDDiskRequestContext() = default;

    TDDiskRequestContext(
        ERequestType requestType,
        TRequestInfoPtr originalRequest,
        ui64 requestId,
        TVector<TActorId> ddiskActorIds,
        ui64 offset,
        ui32 size)
        : RequestType(requestType)
        , OriginalRequest(std::move(originalRequest))
        , RequestId(requestId)
        , DDiskActorIds(std::move(ddiskActorIds))
        , PendingResponses(DDiskActorIds.size())
        , Offset(offset)
        , Size(size)
    {}
};

class TProxyStorage final
    : public TPartitionStorage
{
private:
    TActorId OwnerActorId;
    TPartitionState* PartitionState;
    ui64 NextRequestId = 1;
    THashMap<ui64, TDDiskRequestContext> PendingRequests;

public:
    explicit TProxyStorage(ui32 blockSize, TActorId ownerActorId, TPartitionState* partitionState)
        : OwnerActorId(ownerActorId)
        , PartitionState(partitionState)
    {
        Y_UNUSED(blockSize);
    }

    NCloud::NProto::TError ReadBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    NCloud::NProto::TError WriteBlocksLocal(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    NCloud::NProto::TError ZeroBlocks(
        const TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    // Methods for handling YDB DDisk responses - called by partition actor
    void HandleDDiskReadResponse(
        const NActors::TActorContext& ctx,
        const TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev);

    void HandleDDiskWriteResponse(
        const NActors::TActorContext& ctx,
        const TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev);

private:
    ui64 GenerateRequestId() {
        return NextRequestId++;
    }

    void SendReadToDDisks(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        ui64 offset,
        ui32 size);

    void SendWriteToDDisks(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        ui64 offset,
        ui32 size,
        const TString& data);

    void CompleteReadRequest(
        const NActors::TActorContext& ctx,
        const TDDiskRequestContext& requestCtx);

    void CompleteWriteRequest(
        const NActors::TActorContext& ctx,
        const TDDiskRequestContext& requestCtx);
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
