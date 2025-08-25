#pragma once

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/partition_direct.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/base/tablet_pipecache.h>
#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>

#include "public.h"
#include "partition_direct_state.h"
#include "partition_direct_database.h"
#include "partition_direct_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

using namespace NKikimr;

class TPartitionActor final
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
{
public:
    static constexpr auto LogComponent = TBlockStoreComponents::PARTITION;

    // Transaction counters (minimal implementation)
    struct TTransactionCounters
    {
        enum ETransactionType
        {
            TX_InitSchema,
            TX_LoadState,
            TX_SaveVirtualGroupId,
            TX_SaveGroupInfo
        };
    };
    using TCounters = TTransactionCounters;

private:
    const TStorageConfigPtr Config;
    const NProto::TPartitionConfig PartitionConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    std::unique_ptr<TPartitionState> State;

public:
    TPartitionActor(
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

    static TString GetStateName(ui32 state);

private:
    void Enqueue(STFUNC_SIG) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;
    void DoActivateExecutor(const NActors::TActorContext& ctx);
    void OnDetach(const NActors::TActorContext& ctx) override;
    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    STFUNC(StateWork);

    void HandleWaitReady(
        const TEvPartition::TEvWaitReadyRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Обработчики запросов vhost
    void HandleReadBlocksLocalRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksLocalRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleControllerConfigResponse(
        const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCreateStoragePoolResponse(
        const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleQueryBaseConfigResponse(
        const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool ProcessGroupConfiguration(
        const NKikimrBlobStorage::TBaseConfig& baseConfig,
        const NActors::TActorContext& ctx);

    // YDB DDisk response handlers
    void HandleDDiskReadResponse(
        const NKikimr::TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDDiskWriteResponse(
        const NKikimr::TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // PipeCache event handlers
    void HandleDeliveryProblem(
        const TEvPipeCache::TEvDeliveryProblem::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Storage pool management methods
    void CheckAndCreateVirtualGroup(const NActors::TActorContext& ctx);
    void RequestStoragePoolGroups(const NActors::TActorContext& ctx);
    TString GetStoragePoolName();

    BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxPartitionDirect)
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
