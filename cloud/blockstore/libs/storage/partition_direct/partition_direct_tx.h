#pragma once

#include "partition_direct_database.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS(xxx, ...)                     \
    xxx(InitSchema,             __VA_ARGS__)                                   \
    xxx(LoadState,              __VA_ARGS__)                                   \
    xxx(SaveVirtualGroupId,     __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxPartitionDirect
{
    struct TInitSchema
    {
        using TArgs = TInitSchema;

        void Clear()
        {
        }
    };

    struct TLoadState
    {
        using TArgs = TLoadState;

        TMaybe<NProto::TPartitionDirectMeta> Meta;

        void Clear()
        {
            Meta.Clear();
        }
    };

    struct TSaveVirtualGroupId
    {
        using TArgs = TSaveVirtualGroupId;

        ui32 GroupId;
        TMaybe<NProto::TPartitionDirectMeta> Meta;

        void Clear()
        {
            GroupId = 0;
            Meta.Clear();
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
