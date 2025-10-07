#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/kikimr/public.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <util/system/env.h>

#include "partition_direct_actor.h"
#include "partition_direct_actor_memory.h"

namespace NCloud::NBlockStore::NProto {

// Output operators for proto enums (needed for logging)
IOutputStream& operator<<(IOutputStream& out, EPartitionDirectMode mode) {
    return out << EPartitionDirectMode_Name(mode);
}

IOutputStream& operator<<(IOutputStream& out, EPartitionDirectWorkerMode mode) {
    return out << EPartitionDirectWorkerMode_Name(mode);
}

} // namespace NCloud::NBlockStore::NProto

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
    const auto mode = config->GetPartitionDirectMode();

    // Log the selected mode
    LOG_INFO_S(TActivationContext::AsActorContext(), TBlockStoreComponents::PARTITION,
        "Using PartitionDirect mode: " << mode);

    // Create appropriate actor based on storage type
    if (mode == NProto::PARTITION_DIRECT_MODE_MEMORY) {
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
