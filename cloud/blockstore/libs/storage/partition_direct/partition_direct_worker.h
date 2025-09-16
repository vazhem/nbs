#pragma once

#include "partition_direct_storage.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

// Forward declarations
namespace NCloud::NBlockStore::NStorage::NPartitionDirect {
    struct TRegionChunkMapping;
}

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Thread-safe configuration data for worker storage
struct TWorkerStorageConfig {
    ui32 BlockSize;
    ui64 BlockCount;
    ui32 ChunkSize;

    // DDisk service IDs - copies for thread safety
    TVector<NActors::TActorId> DDiskServiceIds;
    THashMap<ui32, TVector<NActors::TActorId>> GroupToDDiskIds;  // groupIndex -> DDisk IDs

    // Region-based chunk mapping (read-only copies)
    THashMap<ui64, TRegionChunkMapping> RegionChunkCache;

    // Striping constants
    static constexpr ui64 STRIPE_SIZE = 512 * 1024;
    static constexpr ui32 GROUPS_COUNT = 32;

    TWorkerStorageConfig() = default;

    TWorkerStorageConfig(const TWorkerStorageConfig&) = default;
    TWorkerStorageConfig& operator=(const TWorkerStorageConfig&) = default;

    // Helper methods for workers (thread-safe)
    ui32 CalculateGroupIndex(ui64 offset) const {
        ui64 stripeIndex = offset / STRIPE_SIZE;
        return static_cast<ui32>(stripeIndex % GROUPS_COUNT);
    }

    ui64 CalculateGroupOffset(ui64 offset) const {
        ui64 stripeIndex = offset / STRIPE_SIZE;
        ui64 offsetWithinStripe = offset % STRIPE_SIZE;
        ui64 groupStripeIndex = stripeIndex / GROUPS_COUNT;
        return groupStripeIndex * STRIPE_SIZE + offsetWithinStripe;
    }

    bool FindChunkForOffset(ui64 offset, ui32& chunkId, TString& ddiskServiceId, ui32& chunkRelativeOffset) const;
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionDirectWorkerActor(
    ui32 workerId,
    const TWorkerStorageConfig& config);

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
