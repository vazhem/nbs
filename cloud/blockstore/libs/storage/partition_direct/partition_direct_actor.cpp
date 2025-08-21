#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/core/tablet_flat/tablet_flat_executed.h>
#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>
#include <contrib/ydb/core/protos/blobstorage_config.pb.h>
#include <contrib/ydb/core/base/tablet_pipecache.h>

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
        HFunc(TEvBlobStorage::TEvControllerConfigResponse, HandleControllerConfigResponse);

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
        ctx,
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
        ctx,
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
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " DefaultSignalTabletActive - starting InitSchema");

    if (!Executor()->GetStats().IsFollower) {
        ExecuteTx<TInitSchema>(ctx);
    } else {
        ExecuteTx<TLoadState>(ctx);
    }
}

void TPartitionActor::OnActivateExecutor(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Activated executor");

    // First ensure schema is initialized - this is called by the tablet framework
    // and bypasses DefaultSignalTabletActive, so we need to handle initialization here
    if (!Executor()->GetStats().IsFollower) {
        // Start with schema initialization for leaders
        ExecuteTx<TInitSchema>(ctx);
        return;
    }

    // For followers, start with LoadState
    ExecuteTx<TLoadState>(ctx);
}

void TPartitionActor::DoActivateExecutor(const NActors::TActorContext& ctx)
{
    // Check if virtual group is already created
    ui32 existingGroupId = State->GetVirtualGroupId();
    if (existingGroupId != 0) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Virtual group already exists with GroupID: " << existingGroupId);
        return;
    }

    // Создаем storage pool с группами для DDisk
    auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* command = request->Record.MutableRequest()->AddCommand();
    auto* defineStoragePool = command->MutableDefineStoragePool();

    // Настраиваем storage pool для DDisk акторов
    defineStoragePool->SetBoxId(1);
    defineStoragePool->SetName(Sprintf("partition_direct_pool_%lu", TabletID()));
    defineStoragePool->SetErasureSpecies("mirror-3-direct");  // Или "mirror-3-dc"
    defineStoragePool->SetVDiskKind("Default");
    defineStoragePool->SetNumGroups(1);

    // Настраиваем фильтр дисков
    auto* pdiskFilter = defineStoragePool->AddPDiskFilter();
    auto* property = pdiskFilter->AddProperty();
    property->SetType(NKikimrBlobStorage::SSD);

    // Добавляем размер слота (в байтах)
    // Например, 1 ГБ = 1073741824 байт
    defineStoragePool->AddExpectedGroupSlotSize(1073741824);

    // FIX: Настраиваем правильную геометрию для mirror-3-direct
    auto* groupGeometry = defineStoragePool->MutableGeometry();
    groupGeometry->SetNumFailRealms(1);           // 3 realm для mirror-3-direct
    groupGeometry->SetNumFailDomainsPerFailRealm(1);  // 1 domain на realm
    groupGeometry->SetNumVDisksPerFailDomain(1);     // 1 vdisk на domain

    // Отправляем запрос
    auto bsControllerTabletId = MakeBSControllerID(StateStorageGroupFromTabletID(TabletID()));
    ctx.Send(MakePipePeNodeCacheID(false),
             new TEvPipeCache::TEvForward(request.Release(), bsControllerTabletId, true),
             IEventHandle::FlagTrackDelivery);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Sent request to create virtual group mirror-3-direct");
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

void TPartitionActor::HandleControllerConfigResponse(
    const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& response = ev->Get()->Record;

    const auto& configResponse = response.GetResponse();
    if (configResponse.GetSuccess()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Successfully created virtual group mirror-3-direct");

        // Check if we have group ID in response
        if (configResponse.StatusSize() > 0) {
            const auto& status = configResponse.GetStatus(0);
            if (status.GetSuccess()) {
                // Check if we have GroupId in the response
                for (const auto& groupId : status.GetGroupId()) {
                    ui32 virtualGroupId = groupId;

                    // Save the GroupId in the partition state
                    State->SetVirtualGroupId(virtualGroupId);

                    // Create transaction to save VirtualGroupId to database
                    TTxPartitionDirect::TSaveVirtualGroupId args;
                    args.GroupId = virtualGroupId;
                    ExecuteTx<TSaveVirtualGroupId>(ctx, args);

                    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                        "[" << TabletID() << "]"
                        << " Virtual group created with GroupID: " << virtualGroupId);
                }
            }
        }
    } else {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Failed to create virtual group mirror-3-direct"
            << " Error: " << configResponse.GetErrorDescription());
    }
}

bool TPartitionActor::PrepareLoadState(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TLoadState& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Reading state from local db");

    bool ready = db.ReadMeta(args.Meta);

    if (!ready) {
        return false;
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " State read");

    return true;
}

void TPartitionActor::ExecuteLoadState(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TLoadState& args)
{
    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] State data loaded",
        TabletID());

    TPartitionDirectDatabase db(tx.DB);

    if (!args.Meta) {
        // initialize with empty meta
        args.Meta.ConstructInPlace();
    }

    db.WriteMeta(*args.Meta);
}

void TPartitionActor::CompleteLoadState(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TLoadState& args)
{
    if (args.Meta) {
        // Load VirtualGroupId from saved state
        ui32 savedGroupId = args.Meta->GetVirtualGroupId();
        if (savedGroupId != 0) {
            State->SetVirtualGroupId(savedGroupId);
            LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << " Loaded VirtualGroupId from state: " << savedGroupId);
        }
    }

    // Now proceed with virtual group creation if needed
    DoActivateExecutor(ctx);
}

bool TPartitionActor::PrepareInitSchema(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteInitSchema(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TInitSchema& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);

    TPartitionDirectDatabase db(tx.DB);
    db.InitSchema();
}

void TPartitionActor::CompleteInitSchema(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TInitSchema& args)
{
    Y_UNUSED(args);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Schema initialized, starting LoadState");

    ExecuteTx<TLoadState>(ctx);
}

bool TPartitionActor::PrepareSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Reading state from local db for SaveVirtualGroupId");

    bool ready = db.ReadMeta(args.Meta);

    if (!ready) {
        return false;
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " State read for SaveVirtualGroupId");

    return true;
}

void TPartitionActor::ExecuteSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    if (!args.Meta) {
        // Initialize with empty meta if not present
        args.Meta.ConstructInPlace();
    }

    // Set the VirtualGroupId in the metadata
    args.Meta->SetVirtualGroupId(args.GroupId);

    // Write the updated metadata (no reads here, only writes)
    db.WriteMeta(*args.Meta);
}

void TPartitionActor::CompleteSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " VirtualGroupId saved to database: " << args.GroupId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
