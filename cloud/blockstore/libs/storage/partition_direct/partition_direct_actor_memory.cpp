#include "partition_direct_actor_memory.h"
#include "partition_direct_storage_mem.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/storage/core/libs/actors/helpers.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

//////////////////////////////////////////////////////////////////////////////

TPartitionMemoryActor::TPartitionMemoryActor(
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const NActors::TActorId& volumeActorId,
    ui64 volumeTabletId)
    : TPartitionActor(
        owner,
        std::move(storage),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(partitionConfig),
        storageAccessMode,
        siblingCount,
        volumeActorId,
        volumeTabletId)
{
    // Create in-memory storage for direct access
    InMemoryStorage = std::make_shared<TInMemoryStorage>(PartitionConfig.GetBlockSize());

    // Set the storage type to memory
    StorageType = EStorageType::Memory;

    // Set in-memory storage in state
    State->SetStorage(InMemoryStorage);

    LOG_INFO_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] TPartitionMemoryActor created with in-memory storage");
}

//////////////////////////////////////////////////////////////////////////////

void TPartitionMemoryActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Handling ReadBlocksLocal request #%lu directly in memory (offset: %lu, count: %lu, bs: %lu)",
        TabletID(),
        requestId,
        record.GetStartIndex(),
        record.GetBlocksCount(),
        record.BlockSize);

    // Create request info for in-memory storage
    auto requestInfo = CreateRequestInfo(ev);
    if (!requestInfo) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeError(E_ARGUMENT, "Failed to create request info"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // Create shared request object
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->SetBlocksCount(record.GetBlocksCount());
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;  // Copy sglist directly from original request

    // Handle request directly with in-memory storage
    auto error = InMemoryStorage->ReadBlocksLocal(
        ctx,
        requestInfo,
        request,
        std::move(ev->TraceId.Clone()));

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] In-memory read failed: %s",
            TabletID(),
            error.GetMessage().c_str());

        // Send error response
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    }
    // Note: TInMemoryStorage::ReadBlocksLocal sends the response directly
}

//////////////////////////////////////////////////////////////////////////////

void TPartitionMemoryActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    // Extract request ID from CallContext for logging
    ui64 requestId = ev->Get()->CallContext->RequestId;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Handling WriteBlocksLocal request #%lu directly in memory (offset: %lu, count: %lu, bs: %lu)",
        TabletID(),
        requestId,
        record.GetStartIndex(),
        record.BlocksCount,
        record.BlockSize);

    // Create request info for in-memory storage
    auto requestInfo = CreateRequestInfo(ev);
    if (!requestInfo) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            MakeError(E_ARGUMENT, "Failed to create request info"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // Create shared request object
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(record.GetStartIndex());
    request->BlocksCount = record.BlocksCount;
    request->BlockSize = record.BlockSize;
    request->Sglist = record.Sglist;  // Copy sglist directly from original request

    // Handle request directly with in-memory storage
    auto error = InMemoryStorage->WriteBlocksLocal(
        ctx,
        requestInfo,
        request,
        std::move(ev->TraceId.Clone()));

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] In-memory write failed: %s",
            TabletID(),
            error.GetMessage().c_str());

        // Send error response
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    }
    // Note: TInMemoryStorage::WriteBlocksLocal sends the response directly
}

//////////////////////////////////////////////////////////////////////////////

TRequestInfoPtr TPartitionMemoryActor::CreateRequestInfo(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev)
{
    return MakeIntrusive<TRequestInfo>(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);
}

//////////////////////////////////////////////////////////////////////////////

TRequestInfoPtr TPartitionMemoryActor::CreateRequestInfo(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev)
{
    return MakeIntrusive<TRequestInfo>(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
