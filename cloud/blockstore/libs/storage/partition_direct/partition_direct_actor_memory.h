#pragma once

#include "partition_direct_actor.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

//////////////////////////////////////////////////////////////////////////////
//
// In-memory partition actor implementation
//
class TPartitionMemoryActor final : public TPartitionActor
{
private:
    // In-memory storage for direct access
    std::shared_ptr<TInMemoryStorage> InMemoryStorage;

public:
    TPartitionMemoryActor(
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
        ui64 volumeTabletId);

protected:
    // Override base class handlers to handle requests directly in memory
    void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx) override;

private:
    // Helper method to create request info for in-memory storage
    TRequestInfoPtr CreateRequestInfo(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev);

    TRequestInfoPtr CreateRequestInfo(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
