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

#include "public.h"
#include "partition_direct_state.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

using namespace NKikimr;

class TPartitionActor final
    : public NActors::TActor<TPartitionActor>
    , public TTabletBase<TPartitionActor>
{
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
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
