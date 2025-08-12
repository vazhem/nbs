#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>

#include "partition_direct_actor.h"
#include "partition_direct_state.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NBlockStore;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

TPartitionActor::TPartitionActor(
    const TActorId& owner,
    NKikimr::TTabletStorageInfoPtr storage,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::TPartitionConfig partitionConfig,
    EStorageAccessMode storageAccessMode,
    ui32 siblingCount,
    const TActorId& volumeActorId,
    ui64 volumeTabletId)
    : TActor<TPartitionActor>(&TThis::StateWork)
    , TTabletBase(owner, std::move(storage), nullptr)
    , Config(std::move(config))
    , PartitionConfig(std::move(partitionConfig))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , State(std::make_unique<TPartitionState>(
        std::move(storage),
        Config,
        DiagnosticsConfig,
        std::move(profileLog),
        std::move(blockDigestGenerator),
        PartitionConfig,
        storageAccessMode,
        siblingCount,
        volumeActorId,
        volumeTabletId))
{
    Y_UNUSED(owner);
}

TString TPartitionActor::GetStateName(ui32 state)
{
    Y_UNUSED(state);
    return "Work";
}

////////////////////////////////////////////////////////////////////////////////
STFUNC(TPartitionActor::StateWork)
{
    LOG_DEBUG(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPartition::TEvWaitReadyRequest, HandleWaitReady);

        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlocksLocalRequest);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteBlocksLocalRequest);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::PARTITION,
                    __PRETTY_FUNCTION__);
            }
            break;
    }
}

void TPartitionActor::HandleWaitReady(
    const TEvPartition::TEvWaitReadyRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "Received WaitReady request");

    auto response = std::make_unique<TEvPartition::TEvWaitReadyResponse>();
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPartitionActor::HandleReadBlocksLocalRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "Received ReadBlocksLocal request (offset: %lu, count: %lu, bs: %lu)",
        record.GetStartIndex(),
        record.GetBlocksCount(),
        record.BlockSize);

    auto guard = record.Sglist.Acquire();
    if (!guard) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
            MakeError(E_CANCELLED, "Failed to acquire sglist"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
    auto error = State->ReadBlocks(
        record.GetStartIndex(),
        record.blockscount(),
        {const_cast<char*>(guard.Get()[0].Data()),
         record.blockscount() * State->GetBlockSize()});

    response->Record.MutableError()->CopyFrom(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPartitionActor::HandleWriteBlocksLocalRequest(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "Received WriteBlocksLocal request (offset: %lu, count: %lu, bs: %lu)",
        record.GetStartIndex(),
        record.BlocksCount,
        record.BlockSize);

    auto guard = record.Sglist.Acquire();
    if (!guard) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            MakeError(E_CANCELLED, "Failed to acquire sglist"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
    auto error = State->WriteBlocks(
        record.GetStartIndex(),
        record.BlocksCount,
        {const_cast<char*>(guard.Get()[0].Data()),
         record.BlocksCount * State->GetBlockSize()});

    response->Record.MutableError()->CopyFrom(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::Enqueue(STFUNC_SIG)
{
    ALOG_ERROR(TBlockStoreComponents::VOLUME,
        "[" << TabletID() << "]"
        << " IGNORING message type# " << ev->GetTypeRewrite()
        << " from Sender# " << ToString(ev->Sender));
}

void TPartitionActor::DefaultSignalTabletActive(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx); // postpone until LoadState transaction completes
}

void TPartitionActor::OnActivateExecutor(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Activated executor");
}

void TPartitionActor::OnDetach(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " OnDetach");

    Die(ctx);
}

void TPartitionActor::OnTabletDead(
    NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " OnTabletDead");

    Die(ctx);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
