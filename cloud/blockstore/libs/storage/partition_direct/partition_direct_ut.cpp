#include "partition_direct.h"
#include "partition_direct_state.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <library/cpp/testing/unittest/registar.h>

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NCloud::NStorage;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestPartitionInfo
{
    TString DiskId = "test";
    ui64 TabletId = TestTabletId;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
};

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final
    : public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

const TActorId VolumeActorId(0, "VVV");

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageServiceConfig DefaultConfig()
{
    NProto::TStorageServiceConfig config;
    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

////////////////////////////////////////////////////////////////////////////////

void InitTestActorRuntime(
    TTestActorRuntime& runtime,
    const NProto::TStorageServiceConfig& config,
    ui32 blocksCount,
    std::unique_ptr<TTabletStorageInfo> tabletInfo,
    TTestPartitionInfo partitionInfo = {})
{
    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );

    NProto::TPartitionConfig partConfig;
    partConfig.SetDiskId(partitionInfo.DiskId);
    partConfig.SetStorageMediaKind(partitionInfo.MediaKind);
    partConfig.SetBlockSize(DefaultBlockSize);
    partConfig.SetBlocksCount(blocksCount);

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            auto tablet = CreatePartitionTablet(
                owner,
                info,
                storageConfig,
                diagConfig,
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                partConfig,
                EStorageAccessMode::Default,
                1,  // siblingCount
                VolumeActorId,
                0   // volumeTabletId
            );
            return tablet.release();
        };

    auto bootstrapper =
        CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(bootstrapper);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    const NProto::TStorageServiceConfig& config = DefaultConfig(),
    ui32 blocksCount = 1024,
    const TTestPartitionInfo& testPartitionInfo = {})
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    runtime->AddLocalService(
        VolumeActorId,
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0)
    );

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    runtime->SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_TRACE);

    SetupTabletServices(*runtime);

    std::unique_ptr<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(
        testPartitionInfo.TabletId,
        TTabletTypes::BlockStorePartition));

    InitTestActorRuntime(
        *runtime,
        config,
        blocksCount,
        std::move(tabletInfo),
        testPartitionInfo);

    return runtime;
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionClient
{
private:
    TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    ui64 TabletId;

    const TActorId Sender;
    TActorId PipeClient;

public:
    TPartitionClient(
            TTestActorRuntime& runtime,
            ui32 nodeIdx = 0,
            ui64 tabletId = TestTabletId)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , TabletId(tabletId)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }

    template <typename TRequest>
    void SendToPipe(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime.SendToPipe(
            PipeClient,
            Sender,
            request.release(),
            NodeIdx,
            cookie);
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT_C(handle, TypeName<TResponse>() << " is expected");
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest(
        ui32 blockIndex,
        char fill = 0)
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        // request->Record.SetBlocksCount(1);
        *request->Record.MutableBlocks()->MutableBuffers()->Add() =
            TString(DefaultBlockSize, fill);
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest> CreateWriteBlocksLocalRequest(
        ui32 blockIndex,
        char fill = 0)
    {
        auto blockContent = TString(DefaultBlockSize, fill);
        TSgList sglist = {{blockContent.data(), blockContent.size()}};

        auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
        request->Record.SetStartIndex(blockIndex);
        // request->Record.SetBlocksCount(1);
        request->Record.Sglist = TGuardedSgList(std::move(sglist));
        request->Record.BlockSize = DefaultBlockSize;
        return request;
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest(
        ui32 blockIndex)
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest(
        ui32 blockIndex)
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksLocalRequest> CreateReadBlocksLocalRequest(
        ui32 blockIndex)
    {
        TString block(DefaultBlockSize, 0);
        TSgList sglist = {{block.data(), block.size()}};

        auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
        request->Record.SetStartIndex(blockIndex);
        request->Record.SetBlocksCount(1);
        request->Record.Sglist = TGuardedSgList(std::move(sglist));
        request->Record.BlockSize = DefaultBlockSize;
        return request;
    }

    void WriteBlocks(ui32 blockIndex, char fill = 0)
    {
        SendToPipe(CreateWriteBlocksRequest(blockIndex, fill));
        auto response = RecvResponse<TEvService::TEvWriteBlocksResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    }

    void WriteBlocksLocal(ui32 blockIndex, char fill = 0)
    {
        SendToPipe(CreateWriteBlocksLocalRequest(blockIndex, fill));
        auto response = RecvResponse<TEvService::TEvWriteBlocksLocalResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    }

    void ZeroBlocks(ui32 blockIndex)
    {
        SendToPipe(CreateZeroBlocksRequest(blockIndex));
        auto response = RecvResponse<TEvService::TEvZeroBlocksResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
    }

    TString ReadBlocks(ui32 blockIndex)
    {
        SendToPipe(CreateReadBlocksRequest(blockIndex));
        auto response = RecvResponse<TEvService::TEvReadBlocksResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
        return response->Record.GetBlocks().GetBuffers(0);
    }

    TString ReadBlocksLocal(ui32 blockIndex)
    {
        auto request = CreateReadBlocksLocalRequest(blockIndex);
        auto guard = request->Record.Sglist.Acquire();
        UNIT_ASSERT(guard);

        SendToPipe(std::move(request));
        auto response = RecvResponse<TEvService::TEvReadBlocksLocalResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());

        return TString(guard.Get()[0].AsStringBuf());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDirectTest)
{
    // Y_UNIT_TEST(ShouldReadWriteBlocks)
    // {
    //     auto runtime = PrepareTestActorRuntime();

    //     TPartitionClient partition(*runtime);

    //     partition.WriteBlocks(1, 1);
    //     partition.WriteBlocks(2, 2);
    //     partition.WriteBlocks(3, 3);

    //     UNIT_ASSERT_VALUES_EQUAL(
    //         TString(DefaultBlockSize, 1),
    //         partition.ReadBlocks(1));
    //     UNIT_ASSERT_VALUES_EQUAL(
    //         TString(DefaultBlockSize, 2),
    //         partition.ReadBlocks(2));
    //     UNIT_ASSERT_VALUES_EQUAL(
    //         TString(DefaultBlockSize, 3),
    //         partition.ReadBlocks(3));
    // }

    Y_UNIT_TEST(ShouldReadWriteBlocksLocal)
    {
        auto runtime = PrepareTestActorRuntime();

        // Enable verbose logging
        runtime->SetVerbose(true);

        TPartitionClient partition(*runtime);
        // partition.WaitReady();

        partition.WriteBlocksLocal(1, 1);
        partition.WriteBlocksLocal(2, 2);
        partition.WriteBlocksLocal(3, 3);

        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 1),
            partition.ReadBlocksLocal(1));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 2),
            partition.ReadBlocksLocal(2));
        UNIT_ASSERT_VALUES_EQUAL(
            TString(DefaultBlockSize, 3),
            partition.ReadBlocksLocal(3));
    }

    // Y_UNIT_TEST(ShouldZeroBlocks)
    // {
    //     auto runtime = PrepareTestActorRuntime();

    //     TPartitionClient partition(*runtime);

    //     partition.WriteBlocks(1, 1);
    //     partition.ZeroBlocks(1);

    //     UNIT_ASSERT_VALUES_EQUAL(
    //         TString(),
    //         partition.ReadBlocks(1));
    // }

    // Y_UNIT_TEST(ShouldValidateBlockRange)
    // {
    //     auto runtime = PrepareTestActorRuntime(DefaultConfig(), 10);

    //     TPartitionClient partition(*runtime);

    //     // Valid request
    //     partition.WriteBlocks(5, 1);

    //     // Invalid request - block index out of range
    //     {
    //         auto request = partition.CreateWriteBlocksRequest(10);
    //         partition.SendToPipe(std::move(request));
    //         auto response = partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
    //         UNIT_ASSERT_VALUES_EQUAL(
    //             E_ARGUMENT,
    //             response->GetStatus());
    //     }

    //     // Invalid request - blocks count out of range
    //     {
    //         auto request = partition.CreateWriteBlocksRequest(5);
    //         // request->Record.SetBlocksCount(10);
    //         partition.SendToPipe(std::move(request));
    //         auto response = partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
    //         UNIT_ASSERT_VALUES_EQUAL(
    //             E_ARGUMENT,
    //             response->GetStatus());
    //     }
    // }

    // Y_UNIT_TEST(ShouldOverwriteBlocks)
    // {
    //     auto runtime = PrepareTestActorRuntime();

    //     TPartitionClient partition(*runtime);

    //     partition.WriteBlocks(1, 1);
    //     partition.WriteBlocks(1, 2);
    //     partition.WriteBlocks(1, 3);

    //     UNIT_ASSERT_VALUES_EQUAL(
    //         TString(DefaultBlockSize, 3),
    //         partition.ReadBlocks(1));
    // }

    // Y_UNIT_TEST(ShouldHandleMultipleBlocksInOneRequest)
    // {
    //     auto runtime = PrepareTestActorRuntime();

    //     TPartitionClient partition(*runtime);

    //     // Write multiple blocks
    //     {
    //         auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    //         request->Record.SetStartIndex(0);
    //         // request->Record.SetBlocksCount(3);
    //         *request->Record.MutableBlocks()->MutableBuffers()->Add() =
    //             TString(DefaultBlockSize, 1);
    //         *request->Record.MutableBlocks()->MutableBuffers()->Add() =
    //             TString(DefaultBlockSize, 2);
    //         *request->Record.MutableBlocks()->MutableBuffers()->Add() =
    //             TString(DefaultBlockSize, 3);
    //         partition.SendToPipe(std::move(request));
    //         auto response = partition.RecvResponse<TEvService::TEvWriteBlocksResponse>();
    //         UNIT_ASSERT_C(
    //             SUCCEEDED(response->GetStatus()),
    //             response->GetErrorReason());
    //     }

    //     // Read multiple blocks
    //     {
    //         auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    //         request->Record.SetStartIndex(0);
    //         request->Record.SetBlocksCount(3);
    //         partition.SendToPipe(std::move(request));
    //         auto response = partition.RecvResponse<TEvService::TEvReadBlocksResponse>();
    //         UNIT_ASSERT_C(
    //             SUCCEEDED(response->GetStatus()),
    //             response->GetErrorReason());

    //         UNIT_ASSERT_VALUES_EQUAL(3, response->Record.GetBlocks().BuffersSize());
    //         UNIT_ASSERT_VALUES_EQUAL(
    //             TString(DefaultBlockSize, 1),
    //             response->Record.GetBlocks().GetBuffers(0));
    //         UNIT_ASSERT_VALUES_EQUAL(
    //             TString(DefaultBlockSize, 2),
    //             response->Record.GetBlocks().GetBuffers(1));
    //         UNIT_ASSERT_VALUES_EQUAL(
    //             TString(DefaultBlockSize, 3),
    //             response->Record.GetBlocks().GetBuffers(2));
    //     }
    // }

    // Y_UNIT_TEST(ShouldReturnErrorForInvalidSgList)
    // {
    //     auto runtime = PrepareTestActorRuntime();

    //     TPartitionClient partition(*runtime);

    //     // Invalid SgList - wrong block size
    //     {
    //         TString block(DefaultBlockSize / 2, 0);
    //         TSgList sglist = {{block.data(), block.size()}};

    //         auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    //         request->Record.SetStartIndex(0);
    //         // request->Record.SetBlocksCount(1);
    //         request->Record.Sglist = TGuardedSgList(std::move(sglist));
    //         request->Record.BlockSize = DefaultBlockSize;
    //         partition.SendToPipe(std::move(request));
    //         auto response = partition.RecvResponse<TEvService::TEvWriteBlocksLocalResponse>();
    //         UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    //     }
    // }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
