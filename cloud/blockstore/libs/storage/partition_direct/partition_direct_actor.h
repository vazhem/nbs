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
#include "partition_direct_worker.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

using namespace NKikimr;

class TPartitionActor final
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
{
public:
    static constexpr auto LogComponent = TBlockStoreComponents::PARTITION;

    // Initialization state machine
    enum class EPartitionState
    {
        Boot = 0,           // Initial state, schema initialization
        CreateDDiskGroups,  // Creating storage pool and DDisk groups
        GetChunkSize,       // First chunk request to learn PDisk configuration
        PreAllocateChunks,  // Pre-allocate all chunks for volume
        Ready              // Fully initialized, ready for IO
    };

    // Transaction counters (minimal implementation)
    struct TTransactionCounters
    {
        enum ETransactionType
        {
            TX_InitSchema,
            TX_LoadState,
            TX_SaveVirtualGroupId,
            TX_SaveChunkSize,
            TX_SaveGroupInfo,
            TX_SaveReservedChunks,
            TX_PreallocateVolumeChunks,
            TX_AllocateChunkForRegion
        };
    };
    using TCounters = TTransactionCounters;

private:
    const TStorageConfigPtr Config;
    const NProto::TPartitionConfig PartitionConfig;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    std::unique_ptr<TPartitionState> State;

    // Storage type flag
    EStorageType StorageType = EStorageType::Proxy;

    // State machine
    EPartitionState CurrentState = EPartitionState::Boot;
    ui32 RetryCount = 0;
    static constexpr ui32 MAX_RETRIES = 10;
    static constexpr ui32 RETRY_DELAY_MS = 1000;  // 1 second

    // Worker pool
    TVector<NActors::TActorId> WorkerActors;
    ui32 NextWorkerIndex = 0;
    static constexpr ui32 DEFAULT_WORKER_COUNT = 32;

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

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
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

    void HandleDDiskReserveChunksResponse(
        const NKikimr::TEvBlobStorage::TEvDDiskReserveChunksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    // PipeCache event handlers
    void HandleDeliveryProblem(
        const TEvPipeCache::TEvDeliveryProblem::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Storage pool management methods
    void CheckAndCreateVirtualGroup(const NActors::TActorContext& ctx);
    void RequestStoragePoolGroups(const NActors::TActorContext& ctx);
    TString GetStoragePoolName();

    // State machine methods
    void TransitionToState(EPartitionState newState, const NActors::TActorContext& ctx);
    void RetryCurrentState(const NActors::TActorContext& ctx, const TString& reason);
    void ScheduleRetry(const NActors::TActorContext& ctx, const TString& reason);
    void HandleRetryTimer(const NActors::TActorContext& ctx);
    TString StateToString(EPartitionState state) const;

    // Additional methods for chunk management
    void RequestChunksFromDDisks(const NActors::TActorContext& ctx);
    void RequestInitialChunksForChunkSize(const NActors::TActorContext& ctx);
    void RequestAllVolumeChunks(const NActors::TActorContext& ctx);
    ui64 CalculateRegionIndex(ui64 offset);
    ui64 CalculateRegionStartOffset(ui64 regionIndex);

    // Worker pool management
    void CreateWorkerPool(const NActors::TActorContext& ctx);
    NActors::TActorId SelectNextWorker();

private:
    BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxPartitionDirect)
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
