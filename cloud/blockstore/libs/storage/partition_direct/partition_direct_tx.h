#pragma once

#include "partition_direct_database.h"

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS(xxx, ...)                     \
    xxx(InitSchema,             __VA_ARGS__)                                   \
    xxx(LoadState,              __VA_ARGS__)                                   \
    xxx(SaveVirtualGroupId,     __VA_ARGS__)                                   \
    xxx(SaveGroupInfo,          __VA_ARGS__)                                   \
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
        ui32 VirtualGroupId = 0;
        TVector<TPartitionDirectDatabase::TDDiskInfo> DDiskInfos;

        void Clear()
        {
            Meta.Clear();
            VirtualGroupId = 0;
            DDiskInfos.clear();
        }
    };

    struct TSaveVirtualGroupId
    {
        using TArgs = TSaveVirtualGroupId;

        ui32 GroupId;

        void Clear()
        {
            GroupId = 0;
        }
    };

    struct TSaveGroupInfo
    {
        using TArgs = TSaveGroupInfo;

        ui32 GroupId;
        TVector<TPartitionDirectDatabase::TDDiskInfo> DDiskInfos;

        void Clear()
        {
            GroupId = 0;
            DDiskInfos.clear();
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
