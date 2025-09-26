#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/kikimr/public.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <util/system/env.h>

#include "partition_direct_actor.h"
#include "partition_direct_actor_memory.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////


NActors::IActorPtr CreatePartitionTablet(
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
{
    // Set storage type based on environment variable
    EStorageType storageType;
    TString mode = GetEnv("NBS_PARTITION_DIRECT_MODE");

    if (mode == "MEMORY") {
        storageType = EStorageType::Memory;
    } else if (mode == "PROXY") {
        storageType = EStorageType::Proxy;
    } else {
        // Default to Proxy if not specified or invalid
        storageType = EStorageType::Proxy;
    }

    // Log the selected mode
    LOG_INFO_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "Using PartitionDirect mode: " << storageType);

    // Create appropriate actor based on storage type
    if (storageType == EStorageType::Memory) {
        return std::make_unique<TPartitionMemoryActor>(
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
            volumeTabletId);
    } else {
        return std::make_unique<TPartitionActor>(
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
            volumeTabletId);
    }
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
