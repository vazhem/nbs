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
// Constants for request cookies

enum ERequestCookie {
    COOKIE_CREATE_STORAGE_POOL = 1,
    COOKIE_QUERY_BASE_CONFIG = 2
};

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

        // YDB DDisk responses
        HFunc(NKikimr::TEvBlobStorage::TEvDDiskReadResponse, HandleDDiskReadResponse);
        HFunc(NKikimr::TEvBlobStorage::TEvDDiskWriteResponse, HandleDDiskWriteResponse);

        // PipeCache events
        HFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);

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

    // Create request info from original event to preserve sender/cookie/context
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    auto error = State->ReadBlocks(
        ctx,
        requestInfo,
        record.GetStartIndex(),
        record.blockscount(),
        {const_cast<char*>(guard.Get()[0].Data()),
         record.blockscount() * State->GetBlockSize()});
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

    // Create request info from original event to preserve sender/cookie/context
    auto requestInfo = CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext);

    auto error = State->WriteBlocks(
        ctx,
        requestInfo,
        record.GetStartIndex(),
        record.BlocksCount,
        {const_cast<char*>(guard.Get()[0].Data()),
         record.BlocksCount * State->GetBlockSize()});
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
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Tablet activated, will check virtual group status after loading state");
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
    // Differentiate by cookie
    switch (ev->Cookie) {
        case COOKIE_CREATE_STORAGE_POOL:
            HandleCreateStoragePoolResponse(ev, ctx);
            break;
        case COOKIE_QUERY_BASE_CONFIG:
            HandleQueryBaseConfigResponse(ev, ctx);
            break;
        default:
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << " Unknown cookie in ControllerConfigResponse: " << ev->Cookie);
            break;
    }
}

void TPartitionActor::HandleCreateStoragePoolResponse(
    const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& response = ev->Get()->Record;
    const auto& configResponse = response.GetResponse();

    if (configResponse.GetSuccess()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Successfully created storage pool mirror-3-direct");

        // Обрабатываем статусы операций
        if (configResponse.StatusSize() > 0) {
            const auto& status = configResponse.GetStatus(0);
            if (status.GetSuccess()) {
                LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                    "[" << TabletID() << "]"
                    << " Storage pool creation status success");

                // Сохраняем назначенный StoragePoolId если он есть (не равен 0)
                ui64 assignedStoragePoolId = status.GetAssignedStoragePoolId();
                if (assignedStoragePoolId != 0) {
                    State->SetStoragePoolId(assignedStoragePoolId);
                    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                        "[" << TabletID() << "]"
                        << " Assigned StoragePoolId: " << assignedStoragePoolId);
                }

                // После успешного создания отправляем запрос на чтение groups
                RequestStoragePoolGroups(ctx);
            } else {
                LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                    "[" << TabletID() << "]"
                    << " Storage pool creation status failed"
                    << " Error: " << status.GetErrorDescription());
            }
        } else {
            LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << " No status in storage pool create response");
        }
    } else {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Failed to create storage pool mirror-3-direct"
            << " Error: " << configResponse.GetErrorDescription());
    }
}

void TPartitionActor::HandleQueryBaseConfigResponse(
    const TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& response = ev->Get()->Record;
    const auto& configResponse = response.GetResponse();

    if (!configResponse.GetSuccess()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Failed to query base configuration: "
            << configResponse.GetErrorDescription());
        return;
    }

    if (configResponse.StatusSize() == 0) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No status in base config query response");
        return;
    }

    const auto& status = configResponse.GetStatus(0);
    if (!status.GetSuccess() || !status.HasBaseConfig()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Base config query status failed: "
            << status.GetErrorDescription());
        return;
    }

    const auto& baseConfig = status.GetBaseConfig();
    if (!ProcessGroupConfiguration(baseConfig, ctx)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Failed to process group configuration");
    }
}

bool TPartitionActor::ProcessGroupConfiguration(
    const NKikimrBlobStorage::TBaseConfig& baseConfig,
    const NActors::TActorContext& ctx)
{
    const ui64 boxId = 1;  // Our storage pool uses BoxId=1
    const ui64 storagePoolId = State->GetStoragePoolId();

    // Find our group
    const NKikimrBlobStorage::TBaseConfig::TGroup* targetGroup = nullptr;
    for (const auto& group : baseConfig.GetGroup()) {
        if (group.GetBoxId() == boxId &&
            (storagePoolId == 0 || group.GetStoragePoolId() == storagePoolId)) {
            targetGroup = &group;
            break;
        }
    }

    if (!targetGroup) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No groups found for BoxId: " << boxId
            << ", StoragePoolId: " << storagePoolId);
        return false;
    }

    // Verify this is a Mirror3Direct group
    const auto& erasureSpecies = targetGroup->GetErasureSpecies();
    if (erasureSpecies != "mirror-3-direct") {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Group " << targetGroup->GetGroupId()
            << " is not mirror-3-direct, species: " << erasureSpecies);
        return false;
    }

    ui32 virtualGroupId = targetGroup->GetGroupId();

    // Extract DDisk information from VSlots
    TVector<TPartitionDirectDatabase::TDDiskInfo> ddiskInfos;
    for (ui32 i = 0; i < targetGroup->VSlotIdSize(); ++i) {
        const auto& vslot = targetGroup->GetVSlotId(i);

        TPartitionDirectDatabase::TDDiskInfo info;
        info.NodeId = vslot.GetNodeId();
        info.PDiskId = vslot.GetPDiskId();
        info.VSlotId = vslot.GetVSlotId();
        info.OrderInGroup = i;

        ddiskInfos.push_back(info);

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] DDisk[" << i << "] Node:" << info.NodeId
            << " PDisk:" << info.PDiskId << " VSlot:" << info.VSlotId
            << " ServiceId:" << MakeBlobStorageVDiskID(info.NodeId, info.PDiskId, info.VSlotId).ToString());
    }

    // Create transaction to save everything
    TTxPartitionDirect::TSaveGroupInfo args;
    args.GroupId = virtualGroupId;
    args.DDiskInfos = ddiskInfos;
    ExecuteTx<TSaveGroupInfo>(ctx, args);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Successfully configured group " << virtualGroupId
        << " with " << ddiskInfos.size() << " DDisk actors");

    return true;
}

TString TPartitionActor::GetStoragePoolName() {
    return Sprintf("partition_direct_pool_%lu", TabletID());
}

void TPartitionActor::RequestStoragePoolGroups(
    const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Requesting base configuration to find groups for StoragePool: " << GetStoragePoolName());

    // Создаем запрос для получения базовой конфигурации (включая информацию о группах)
    auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* command = request->Record.MutableRequest()->AddCommand();
    auto* queryBaseConfig = command->MutableQueryBaseConfig();

    // Указываем, что нам не нужна информация о устройствах (devices)
    queryBaseConfig->SetRetrieveDevices(false);

    // Отправляем запрос к BSController с cookie для идентификации
    auto bsControllerTabletId = MakeBSControllerID(StateStorageGroupFromTabletID(TabletID()));
    ctx.Send(MakePipePeNodeCacheID(false),
             new TEvPipeCache::TEvForward(request.Release(), bsControllerTabletId, true),
             IEventHandle::FlagTrackDelivery,
             COOKIE_QUERY_BASE_CONFIG);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Base configuration query request sent");
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

    // Read VirtualGroupId directly using dedicated method
    bool hasGroupId = db.ReadVirtualGroupId(args.VirtualGroupId);

    if (hasGroupId) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Loaded VirtualGroupId from database: " << args.VirtualGroupId);
    } else {
        args.VirtualGroupId = 0;
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " No VirtualGroupId found in database");
    }

    // Read DDisk information
    bool hasDDiskInfos = db.ReadDDiskInfos(args.DDiskInfos);

    if (hasDDiskInfos && !args.DDiskInfos.empty()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Loaded " << args.DDiskInfos.size()
            << " DDisk actors from database");
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No DDisk information found in database");
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
    Y_UNUSED(tx);  // No database operations needed in execute phase
    Y_UNUSED(args); // Data was already loaded in prepare phase

    LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] State data loaded",
        TabletID());
}

void TPartitionActor::CompleteLoadState(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TLoadState& args)
{
    // Load VirtualGroupId from database
    if (args.VirtualGroupId != 0) {
        State->SetVirtualGroupId(args.VirtualGroupId);
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Restored VirtualGroupId from database: " << args.VirtualGroupId);
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " No existing VirtualGroupId in database");
    }

    // Load DDisk information and set in state
    if (!args.DDiskInfos.empty()) {
        TVector<TDDiskInfo> ddiskInfos;
        ddiskInfos.reserve(args.DDiskInfos.size());

        for (const auto& dbInfo : args.DDiskInfos) {
            ddiskInfos.emplace_back(
                dbInfo.NodeId,
                dbInfo.PDiskId,
                dbInfo.VSlotId,
                dbInfo.OrderInGroup
            );
        }

        State->SetDDiskInfos(ddiskInfos);

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Restored " << ddiskInfos.size()
            << " DDisk actors from database");

        for (ui32 i = 0; i < ddiskInfos.size(); ++i) {
            const auto& info = ddiskInfos[i];
            LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "] DDisk[" << i << "] Node:" << info.NodeId
                << " PDisk:" << info.PDiskId << " VSlot:" << info.VSlotId
                << " ServiceId:" << info.ServiceId.ToString());
        }
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No DDisk information to restore");
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " State data loaded");

    // После загрузки состояния проверяем нужно ли создавать virtual group
    CheckAndCreateVirtualGroup(ctx);
}

void TPartitionActor::CheckAndCreateVirtualGroup(const NActors::TActorContext& ctx)
{
    // Check if virtual group is already created (from loaded state)
    ui32 existingGroupId = State->GetVirtualGroupId();
    if (existingGroupId != 0) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Virtual group already exists with GroupID: " << existingGroupId
            << " (loaded from database)");
        return;
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " No existing virtual group found, creating storage pool");

    // Создаем storage pool с группами для DDisk
    auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto* command = request->Record.MutableRequest()->AddCommand();
    auto* defineStoragePool = command->MutableDefineStoragePool();

    // Настраиваем storage pool для DDisk акторов
    defineStoragePool->SetBoxId(1);
    defineStoragePool->SetName(GetStoragePoolName());
    defineStoragePool->SetErasureSpecies("mirror-3-direct");
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
    groupGeometry->SetNumFailRealms(1);           // 1 realm для mirror-3-direct
    groupGeometry->SetNumFailDomainsPerFailRealm(1);  // 1 domain на realm
    groupGeometry->SetNumVDisksPerFailDomain(1);     // 1 vdisk на domain

    // Отправляем запрос с cookie для идентификации
    auto bsControllerTabletId = MakeBSControllerID(StateStorageGroupFromTabletID(TabletID()));
    ctx.Send(MakePipePeNodeCacheID(false),
             new TEvPipeCache::TEvForward(request.Release(), bsControllerTabletId, true),
             IEventHandle::FlagTrackDelivery,
             COOKIE_CREATE_STORAGE_POOL);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Sent request to create virtual group mirror-3-direct");
}

////////////////////////////////////////////////////////////////////////////////
// TInitSchema transaction

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
        "[" << TabletID() << "] Schema initialized, starting LoadState");

    ExecuteTx<TLoadState>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);  // No preparation needed for simple write
    Y_UNUSED(args);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Preparing to save VirtualGroupId: " << args.GroupId);

    return true;  // Always ready for simple write operation
}

void TPartitionActor::ExecuteSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Writing VirtualGroupId to database: " << args.GroupId);

    // Use dedicated method to save VirtualGroupId
    db.WriteVirtualGroupId(args.GroupId);
}

void TPartitionActor::CompleteSaveVirtualGroupId(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TSaveVirtualGroupId& args)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " VirtualGroupId saved to database: " << args.GroupId);
}

////////////////////////////////////////////////////////////////////////////////
// YDB DDisk response handlers

void TPartitionActor::HandleDDiskReadResponse(
    const NKikimr::TEvBlobStorage::TEvDDiskReadResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] HandleDDiskReadResponse", TabletID());

    // Forward to storage proxy for processing
    auto* proxyStorage = dynamic_cast<TProxyStorage*>(State->GetStorage().get());
    if (proxyStorage) {
        proxyStorage->HandleDDiskReadResponse(ctx, ev);
    } else {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Storage is not TProxyStorage", TabletID());
    }
}

void TPartitionActor::HandleDDiskWriteResponse(
    const NKikimr::TEvBlobStorage::TEvDDiskWriteResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] HandleDDiskWriteResponse", TabletID());

    // Forward to storage proxy for processing
    auto* proxyStorage = dynamic_cast<TProxyStorage*>(State->GetStorage().get());
    if (proxyStorage) {
        proxyStorage->HandleDDiskWriteResponse(ctx, ev);
    } else {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Storage is not TProxyStorage", TabletID());
    }
}

void TPartitionActor::HandleDeliveryProblem(
    const TEvPipeCache::TEvDeliveryProblem::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] HandleDeliveryProblem from sender: %lu",
        TabletID(), ev->Sender.LocalId());
}

bool TPartitionActor::PrepareSaveGroupInfo(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveGroupInfo& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteSaveGroupInfo(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveGroupInfo& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    // Write virtual group ID
    db.WriteVirtualGroupId(args.GroupId);

    // Write DDisk information
    db.WriteDDiskInfos(args.DDiskInfos);
}

void TPartitionActor::CompleteSaveGroupInfo(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TSaveGroupInfo& args)
{
    // Update in-memory state with DDisk information
    TVector<TDDiskInfo> ddiskInfos;
    ddiskInfos.reserve(args.DDiskInfos.size());

    for (const auto& dbInfo : args.DDiskInfos) {
        ddiskInfos.emplace_back(
            dbInfo.NodeId,
            dbInfo.PDiskId,
            dbInfo.VSlotId,
            dbInfo.OrderInGroup
        );
    }

    State->SetDDiskInfos(ddiskInfos);
    State->SetVirtualGroupId(args.GroupId);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " SaveGroupInfo transaction completed for GroupId: " << args.GroupId
        << " with " << ddiskInfos.size() << " DDisk actors");
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
