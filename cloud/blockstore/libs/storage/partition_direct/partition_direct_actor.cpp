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
        HFunc(NKikimr::TEvBlobStorage::TEvDDiskReserveChunksResponse, HandleDDiskReserveChunksResponse);

        // PipeCache events
        HFunc(TEvPipeCache::TEvDeliveryProblem, HandleDeliveryProblem);

        // Timer events for state machine retries
        HFunc(TEvents::TEvWakeup, HandleWakeup);

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

void TPartitionActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleRetryTimer(ctx);
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

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
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

    // Handle errors from storage operations
    if (NCloud::HasError(error)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
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
                RetryCurrentState(ctx, TStringBuilder() << "Storage pool status failure: "
                                  << status.GetErrorDescription());
            }
        } else {
            LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << " No status in storage pool create response");
            RetryCurrentState(ctx, "No status in storage pool create response");
        }
    } else {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Failed to create storage pool mirror-3-direct"
            << " Error: " << configResponse.GetErrorDescription());
        RetryCurrentState(ctx, TStringBuilder() << "Storage pool creation failed: "
                          << configResponse.GetErrorDescription());
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
        RetryCurrentState(ctx, TStringBuilder() << "Base config query failed: "
                          << configResponse.GetErrorDescription());
        return;
    }

    if (configResponse.StatusSize() == 0) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No status in base config query response");
        RetryCurrentState(ctx, "No status in base config query response");
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
    bool hasChunkSize = db.ReadChunkSize(args.ChunkSize);

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

    if (hasChunkSize) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Loaded ChunkSize from database: " << args.ChunkSize);
    } else {
        args.ChunkSize = 0;
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " No ChunkSize found in database - will be set when DDisk responds");
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

    // Read chunk regions
    bool hasChunkRegions = db.ReadAllChunkRegions(args.ChunkRegions);
    if (hasChunkRegions && !args.ChunkRegions.empty()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Loaded " << args.ChunkRegions.size()
            << " chunk regions from database");
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No chunk regions found in database");
    }

    // Read available chunks
    bool hasAvailableChunks = db.ReadAvailableChunks(args.AvailableChunks);
    if (hasAvailableChunks && !args.AvailableChunks.empty()) {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Loaded " << args.AvailableChunks.size()
            << " available chunks from database");
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No available chunks found in database");
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

    // Load ChunkSize from database
    if (args.ChunkSize != 0) {
        State->SetChunkSize(args.ChunkSize);
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " Restored ChunkSize from database: " << args.ChunkSize);
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "]"
            << " No ChunkSize found in database - will be set when DDisk responds");
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

    // Load chunk cache from database
    TVector<TChunkRegionInfo> regionInfos;
    TVector<TAvailableChunkInfo> chunkInfos;

    regionInfos.reserve(args.ChunkRegions.size());
    for (const auto& dbRegion : args.ChunkRegions) {
        regionInfos.emplace_back(
            dbRegion.StartOffset,
            dbRegion.ChunkId,
            dbRegion.DDiskServiceId
        );
    }

    chunkInfos.reserve(args.AvailableChunks.size());
    for (const auto& dbChunk : args.AvailableChunks) {
        chunkInfos.emplace_back(
            dbChunk.ChunkId,
            dbChunk.DDiskServiceId,
            static_cast<EChunkStatus>(dbChunk.Status),
            dbChunk.RegionIndex
        );
    }

    State->LoadChunkCaches(regionInfos, chunkInfos);

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Loaded chunk cache: " << regionInfos.size()
        << " regions, " << chunkInfos.size() << " available chunks");

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " State data loaded");

    // State machine: determine next state based on loaded data
    if (args.VirtualGroupId == 0) {
        // No virtual group - need to create one
        TransitionToState(EPartitionState::CreateDDiskGroups, ctx);
    } else if (args.ChunkSize == 0) {
        // Have group but no chunk size - need to learn it
        TransitionToState(EPartitionState::GetChunkSize, ctx);
    } else {
        // Check if we need more chunks for full volume
        ui64 volumeSize = static_cast<ui64>(State->GetBlockSize()) * State->GetBlockCount();
        ui64 totalRegionsNeeded = (volumeSize + args.ChunkSize - 1) / args.ChunkSize;
        ui32 currentRegionCount = State->GetChunkRegionCacheSize();

        if (currentRegionCount < totalRegionsNeeded) {
            TransitionToState(EPartitionState::PreAllocateChunks, ctx);
        } else {
            TransitionToState(EPartitionState::Ready, ctx);
        }
    }
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
// SaveChunkSize transaction

bool TPartitionActor::PrepareSaveChunkSize(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveChunkSize& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);  // No preparation needed for simple write
    Y_UNUSED(args);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Preparing to save ChunkSize: " << args.ChunkSize);

    return true;
}

void TPartitionActor::ExecuteSaveChunkSize(
    const NActors::TActorContext& ctx,
    NKikimr::NTabletFlatExecutor::TTransactionContext& tx,
    TTxPartitionDirect::TSaveChunkSize& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " Saving ChunkSize to database: " << args.ChunkSize);

    db.WriteChunkSize(args.ChunkSize);
}

void TPartitionActor::CompleteSaveChunkSize(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TSaveChunkSize& args)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "]"
        << " ChunkSize saved to database: " << args.ChunkSize);
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

    // State machine: after group creation, move to GetChunkSize
    if (CurrentState == EPartitionState::CreateDDiskGroups) {
        TransitionToState(EPartitionState::GetChunkSize, ctx);
    } else {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Unexpected state during SaveGroupInfo completion: "
            << StateToString(CurrentState) << " - retrying CreateDDiskGroups");
        RetryCurrentState(ctx, "Unexpected state during SaveGroupInfo completion");
    }
}

////////////////////////////////////////////////////////////////////////////////
// State machine implementation

void TPartitionActor::TransitionToState(EPartitionState newState, const NActors::TActorContext& ctx)
{
    EPartitionState oldState = CurrentState;
    CurrentState = newState;

    if (oldState != newState) {
        RetryCount = 0;  // Reset retry count on state transition

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] State transition: "
            << StateToString(oldState) << " -> " << StateToString(newState));
    } else {
        // Retry
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Retry: "
            << StateToString(newState));
    }

    switch (newState) {
        case EPartitionState::CreateDDiskGroups: {
            // Create a storage pool for this partition
            auto poolName = GetStoragePoolName();
            CheckAndCreateVirtualGroup(ctx);
            break;
        }
        case EPartitionState::GetChunkSize:
            RequestInitialChunksForChunkSize(ctx);
            break;

        case EPartitionState::PreAllocateChunks:
            RequestAllVolumeChunks(ctx);
            break;

        case EPartitionState::Ready:
            LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "] Partition fully initialized and ready for IO");
            break;

        case EPartitionState::Boot:
            // No action needed - handled by tablet framework
            break;
    }
}

void TPartitionActor::RetryCurrentState(const NActors::TActorContext& ctx, const TString& reason)
{
    if (RetryCount >= MAX_RETRIES) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Max retries (" << MAX_RETRIES
            << ") exceeded for state " << StateToString(CurrentState)
            << ", reason: " << reason);
        // For now, continue retrying indefinitely as requested
        RetryCount = 0;
    }

    RetryCount++;

    LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Retrying state " << StateToString(CurrentState)
        << " (attempt " << RetryCount << "/" << MAX_RETRIES << "), reason: " << reason);

    ScheduleRetry(ctx, reason);
}

void TPartitionActor::ScheduleRetry(const NActors::TActorContext& ctx, const TString& reason)
{
    // Schedule retry with exponential backoff
    ui32 delayMs = RETRY_DELAY_MS * (1 << std::min(RetryCount - 1, 5u));  // Cap at 32 seconds

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Scheduling retry in " << delayMs << "ms, reason: " << reason);

    ctx.Schedule(TDuration::MilliSeconds(delayMs), new TEvents::TEvWakeup());
}

void TPartitionActor::HandleRetryTimer(const NActors::TActorContext& ctx)
{
    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Retry timer fired for state " << StateToString(CurrentState));

    // Re-execute current state logic
    TransitionToState(CurrentState, ctx);
}

TString TPartitionActor::StateToString(EPartitionState state) const
{
    switch (state) {
        case EPartitionState::Boot:
            return "Boot";
        case EPartitionState::CreateDDiskGroups:
            return "CreateDDiskGroups";
        case EPartitionState::GetChunkSize:
            return "GetChunkSize";
        case EPartitionState::PreAllocateChunks:
            return "PreAllocateChunks";
        case EPartitionState::Ready:
            return "Ready";
        default:
            return "Unknown";
    }
}

////////////////////////////////////////////////////////////////////////////////
// Chunk management methods

void TPartitionActor::RequestChunksFromDDisks(const NActors::TActorContext& ctx)
{
    // This method now delegates to the appropriate state-specific method
    switch (CurrentState) {
        case EPartitionState::GetChunkSize:
            RequestInitialChunksForChunkSize(ctx);
            break;
        case EPartitionState::PreAllocateChunks:
            RequestAllVolumeChunks(ctx);
            break;
        default:
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "] RequestChunksFromDDisks called in invalid state: "
                << StateToString(CurrentState));
    }
}

void TPartitionActor::RequestInitialChunksForChunkSize(const NActors::TActorContext& ctx)
{
    const auto& ddiskServiceIds = State->GetDDiskServiceIds();
    if (ddiskServiceIds.empty()) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No DDisk actors available for initial chunk reservation");
        RetryCurrentState(ctx, "No DDisk actors available for initial chunk reservation");
        return;
    }

    // Request small number of chunks to learn ChunkSize
    const ui32 initialChunksPerDDisk = 10;

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Requesting initial " << initialChunksPerDDisk
        << " chunks from " << ddiskServiceIds.size() << " DDisk actors to learn ChunkSize");

    for (size_t i = 0; i < ddiskServiceIds.size(); ++i) {
        const auto& ddiskActorId = ddiskServiceIds[i];
        auto request = std::make_unique<TEvBlobStorage::TEvDDiskReserveChunksRequest>();
        request->Record.SetChunkCount(initialChunksPerDDisk);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Sending initial chunk reservation request to DDisk: "
            << ddiskActorId.ToString());

        // Use cookie to encode the DDisk index, so we can identify which DDisk responded
        ui64 ddiskIndex = static_cast<ui64>(i);
        ctx.Send(ddiskActorId, request.release(), 0, ddiskIndex);
    }
}

void TPartitionActor::RequestAllVolumeChunks(const NActors::TActorContext& ctx)
{
    const auto& ddiskServiceIds = State->GetDDiskServiceIds();
    if (ddiskServiceIds.empty()) {
        LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] No DDisk actors available for volume chunk allocation");
        RetryCurrentState(ctx, "No DDisk actors available for volume chunk allocation");
        return;
    }

    ui32 chunkSize = State->GetChunkSize();
    if (chunkSize == 0) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Cannot allocate all volume chunks: ChunkSize unknown");
        RetryCurrentState(ctx, "ChunkSize unknown for volume chunk allocation");
        return;
    }

    // Calculate total volume size and required chunks
    ui64 volumeSize = static_cast<ui64>(State->GetBlockSize()) * State->GetBlockCount();
    ui64 totalChunksNeeded = (volumeSize + chunkSize - 1) / chunkSize;  // Round up
    ui32 ddiskCount = static_cast<ui32>(ddiskServiceIds.size());
    ui32 chunksPerDDisk = static_cast<ui32>((totalChunksNeeded + ddiskCount - 1) / ddiskCount);  // Round up

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Pre-allocating ALL chunks for volume: "
        << " VolumeSize=" << volumeSize << " bytes"
        << " ChunkSize=" << chunkSize << " bytes"
        << " TotalChunks=" << totalChunksNeeded
        << " DDisks=" << ddiskCount
        << " ChunksPerDDisk=" << chunksPerDDisk);

    for (size_t i = 0; i < ddiskServiceIds.size(); ++i) {
        const auto& ddiskActorId = ddiskServiceIds[i];
        auto request = std::make_unique<TEvBlobStorage::TEvDDiskReserveChunksRequest>();
        request->Record.SetChunkCount(chunksPerDDisk);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Sending volume chunk reservation request to DDisk: "
            << ddiskActorId.ToString() << " for " << chunksPerDDisk << " chunks");

        // Use cookie to encode the DDisk index, so we can identify which DDisk responded
        ui64 ddiskIndex = static_cast<ui64>(i);
        ctx.Send(ddiskActorId, request.release(), 0, ddiskIndex);
    }
}

void TPartitionActor::HandleDDiskReserveChunksResponse(
    const TEvBlobStorage::TEvDDiskReserveChunksResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Received chunk reservation response from DDisk: "
        << ev->Sender.ToString() << " status: " << record.GetStatus()
        << " chunks: " << record.ChunkIdsSize());

    if (record.GetStatus() != NKikimrProto::OK) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Chunk reservation failed from DDisk: "
            << ev->Sender.ToString() << " error: " << record.GetErrorReason());
        return;
    }

    // Set chunk size from DDisk response (if available)
    if (record.HasChunkSize() && record.GetChunkSize() > 0) {
        ui32 receivedChunkSize = record.GetChunkSize();

        // Only save ChunkSize if it's not already set (first time learning it)
        if (State->GetChunkSize() == 0) {
            State->SetChunkSize(receivedChunkSize);

            LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "] Set chunk size to " << receivedChunkSize
                << " bytes from DDisk: " << ev->Sender.ToString()
                << " - saving to database");

            // Save ChunkSize to database for persistence across restarts
            auto args = TTxPartitionDirect::TSaveChunkSize{};
            args.ChunkSize = receivedChunkSize;
            ExecuteTx<TSaveChunkSize>(ctx, std::move(args));
        } else {
            // Verify chunk size consistency across DDisks
            ui32 currentChunkSize = State->GetChunkSize();
            if (currentChunkSize != receivedChunkSize) {
                LOG_WARN_S(ctx, TBlockStoreComponents::PARTITION,
                    "[" << TabletID() << "] ChunkSize mismatch! Current: "
                    << currentChunkSize << ", received: " << receivedChunkSize
                    << " from DDisk: " << ev->Sender.ToString());
            }
        }
    }

    // Extract DDisk index from cookie to identify which DDisk sent the response
    ui64 ddiskIndex = ev->Cookie;
    const auto& ddiskServiceIds = State->GetDDiskServiceIds();

    if (ddiskIndex >= ddiskServiceIds.size()) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Invalid DDisk index in cookie: " << ddiskIndex
            << " (max: " << ddiskServiceIds.size() << ")");
        return;
    }

    // Use the DDisk ActorId as service ID for storage
    TString ddiskServiceId = ddiskServiceIds[ddiskIndex].ToString();

    // Pre-allocate chunks and assign them to regions
    TTxPartitionDirect::TPreallocateVolumeChunks args;
    args.DDiskServiceId = ddiskServiceId;
    args.ChunkIds.reserve(record.ChunkIdsSize());

    for (ui32 chunkId : record.GetChunkIds()) {
        args.ChunkIds.push_back(chunkId);
    }

    // Calculate starting region index for this DDisk
    // Distribute regions evenly across DDisks
    ui32 chunksPerDDisk = static_cast<ui32>(args.ChunkIds.size());
    args.StartRegionIndex = ddiskIndex * chunksPerDDisk;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Pre-allocating " << args.ChunkIds.size()
        << " chunks from DDisk[" << ddiskIndex << "] " << ddiskServiceId
        << " starting from region " << args.StartRegionIndex);

    // Determine which transaction to use based on current state
    if (CurrentState == EPartitionState::GetChunkSize) {
        // Save initial chunks without pre-assignment
        TTxPartitionDirect::TSaveReservedChunks saveArgs;
        saveArgs.DDiskServiceId = ddiskServiceId;
        saveArgs.ChunkIds = args.ChunkIds;
        ExecuteTx<TSaveReservedChunks>(ctx, saveArgs);
    } else if (CurrentState == EPartitionState::PreAllocateChunks) {
        // Pre-allocate and assign chunks to regions
        ExecuteTx<TPreallocateVolumeChunks>(ctx, args);
    } else {
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Received chunk reservation response in unexpected state: "
            << StateToString(CurrentState));
    }
}

// This method is no longer needed as we use DDisk service ID directly

ui64 TPartitionActor::CalculateRegionIndex(ui64 offset)
{
    return State->CalculateRegionIndex(offset);
}

ui64 TPartitionActor::CalculateRegionStartOffset(ui64 regionIndex)
{
    return State->CalculateRegionStartOffset(regionIndex);
}

////////////////////////////////////////////////////////////////////////////////
// SaveReservedChunks transaction

bool TPartitionActor::PrepareSaveReservedChunks(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TSaveReservedChunks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // Nothing to prepare - all data should be in memory
    return true;
}

void TPartitionActor::ExecuteSaveReservedChunks(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TSaveReservedChunks& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    // Save each reserved chunk to the database
    for (ui32 chunkId : args.ChunkIds) {
        db.WriteAvailableChunk(
            chunkId,
            args.DDiskServiceId,
            static_cast<ui32>(EChunkStatus::Available),
            0  // regionIndex, unused for available chunks
        );
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Saved " << args.ChunkIds.size()
        << " chunks from DDisk " << args.DDiskServiceId);
}

void TPartitionActor::CompleteSaveReservedChunks(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TSaveReservedChunks& args)
{
    // Update in-memory cache with the new chunks
    for (ui32 chunkId : args.ChunkIds) {
        TAvailableChunkInfo chunkInfo{
            chunkId,
            args.DDiskServiceId,
            EChunkStatus::Available,
            0  // regionIndex, unused for available chunks
        };
        State->AddAvailableChunkToCache(chunkInfo);
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Reserved " << args.ChunkIds.size()
        << " chunks from DDisk " << args.DDiskServiceId);

    // State machine: after saving initial chunks, move to PreAllocateChunks
    if (CurrentState == EPartitionState::GetChunkSize) {
        TransitionToState(EPartitionState::PreAllocateChunks, ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////
// PreallocateVolumeChunks transaction

bool TPartitionActor::PreparePreallocateVolumeChunks(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TPreallocateVolumeChunks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // Nothing to prepare - all data should be in memory
    return true;
}

void TPartitionActor::ExecutePreallocateVolumeChunks(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TPreallocateVolumeChunks& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    // Save chunks as available first
    for (ui32 chunkId : args.ChunkIds) {
        db.WriteAvailableChunk(
            chunkId,
            args.DDiskServiceId,
            static_cast<ui32>(EChunkStatus::Available),
            0  // regionIndex, unused for available chunks
        );
    }

    // Pre-assign chunks to regions
    ui32 chunkIndex = 0;
    for (ui32 chunkId : args.ChunkIds) {
        ui64 regionIndex = args.StartRegionIndex + chunkIndex;
        ui64 startOffset = State->CalculateRegionStartOffset(regionIndex);

        // Write region mapping
        db.WriteChunkRegion(startOffset, chunkId, args.DDiskServiceId);

        // Update chunk status to allocated
        db.UpdateChunkStatus(chunkId, static_cast<ui32>(EChunkStatus::Allocated), regionIndex);

        chunkIndex++;
    }

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Pre-allocated " << args.ChunkIds.size()
        << " chunks from DDisk " << args.DDiskServiceId
        << " starting from region " << args.StartRegionIndex);
}

void TPartitionActor::CompletePreallocateVolumeChunks(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TPreallocateVolumeChunks& args)
{
    // Update in-memory caches
    ui32 chunkIndex = 0;
    for (ui32 chunkId : args.ChunkIds) {
        // Add to available chunks cache (marked as allocated)
        TAvailableChunkInfo chunkInfo{
            chunkId,
            args.DDiskServiceId,
            EChunkStatus::Allocated,  // Mark as allocated
            0  // regionIndex, unused for available chunks
        };
        State->AddAvailableChunkToCache(chunkInfo);

        // Add region mapping to cache
        ui64 regionIndex = args.StartRegionIndex + chunkIndex;
        ui64 startOffset = State->CalculateRegionStartOffset(regionIndex);

        TChunkRegionInfo regionInfo{
            startOffset,
            chunkId,
            args.DDiskServiceId
        };
        State->AddChunkRegionToCache(regionInfo);

        chunkIndex++;
    }

    LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
        "[" << TabletID() << "] Pre-allocated and assigned " << args.ChunkIds.size()
        << " chunks from DDisk " << args.DDiskServiceId
        << " to regions starting from " << args.StartRegionIndex);

    // Check if all DDisks have completed their pre-allocation
    ui32 chunkSize = State->GetChunkSize();
    ui64 volumeSize = static_cast<ui64>(State->GetBlockSize()) * State->GetBlockCount();
    ui64 totalRegionsNeeded = (volumeSize + chunkSize - 1) / chunkSize;  // Round up
    ui32 currentRegionCount = State->GetChunkRegionCacheSize();

    if (currentRegionCount >= totalRegionsNeeded) {
        // All regions allocated - transition to Ready state
        TransitionToState(EPartitionState::Ready, ctx);
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Pre-allocation progress: "
            << currentRegionCount << "/" << totalRegionsNeeded << " regions allocated");
    }
}

////////////////////////////////////////////////////////////////////////////////
// AllocateChunkForRegion transaction

bool TPartitionActor::PrepareAllocateChunkForRegion(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TAllocateChunkForRegion& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    // Read current chunk region to see if it already exists
    TPartitionDirectDatabase::TChunkRegion existingRegion;
    if (db.ReadChunkRegion(args.StartOffset, existingRegion)) {
        args.ChunkRegion = existingRegion;
    }

    return true;
}

void TPartitionActor::ExecuteAllocateChunkForRegion(
    const NActors::TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartitionDirect::TAllocateChunkForRegion& args)
{
    Y_UNUSED(ctx);

    TPartitionDirectDatabase db(tx.DB);

    // If region doesn't already exist, create it
    if (!args.ChunkRegion) {
        db.WriteChunkRegion(args.StartOffset, args.ChunkId, args.DDiskServiceId);

        // Mark chunk as allocated
        db.UpdateChunkStatus(args.ChunkId, static_cast<ui32>(EChunkStatus::Allocated), 0);

        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Allocated chunk " << args.ChunkId
            << " for region starting at offset " << args.StartOffset);
    } else {
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Region at offset " << args.StartOffset
            << " already exists with chunk " << args.ChunkRegion->ChunkId);
    }
}

void TPartitionActor::CompleteAllocateChunkForRegion(
    const NActors::TActorContext& ctx,
    TTxPartitionDirect::TAllocateChunkForRegion& args)
{
    // Update in-memory cache if new region was created
    if (!args.ChunkRegion) {
        TChunkRegionInfo regionInfo{
            args.StartOffset,
            args.ChunkId,
            args.DDiskServiceId
        };
        State->AddChunkRegionToCache(regionInfo);

        // Update chunk status in cache
        State->UpdateChunkStatus(args.ChunkId, EChunkStatus::Allocated, 0);

        LOG_INFO_S(ctx, TBlockStoreComponents::PARTITION,
            "[" << TabletID() << "] Successfully allocated chunk " << args.ChunkId
            << " for region starting at offset " << args.StartOffset);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
