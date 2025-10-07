#pragma once

#include "partition_direct_storage.h"
#include "partition_direct_worker.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreatePartitionDirectWorkerMemActor(
    ui32 workerId,
    const TWorkerStorageConfig& config);

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
