#pragma once

#include "partition_direct_database.h"

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_DIRECT_TRANSACTIONS(xxx, ...)                     \
    xxx(InitSchema,             __VA_ARGS__)                                   \
    xxx(LoadState,              __VA_ARGS__)                                   \
    xxx(SaveVirtualGroupId,     __VA_ARGS__)                                   \
    xxx(SaveChunkSize,          __VA_ARGS__)                                   \
    xxx(SaveGroupInfo,          __VA_ARGS__)                                   \
    xxx(SaveReservedChunks,     __VA_ARGS__)                                   \
    xxx(PreallocateVolumeChunks, __VA_ARGS__)                                  \
    xxx(AllocateChunkForRegion, __VA_ARGS__)                                   \
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
        ui32 ChunkSize = 0;  // PDisk chunk size
        TVector<TPartitionDirectDatabase::TDDiskInfo> DDiskInfos;
        TVector<TPartitionDirectDatabase::TChunkRegion> ChunkRegions;
        TVector<TPartitionDirectDatabase::TAvailableChunk> AvailableChunks;

        void Clear()
        {
            Meta.Clear();
            VirtualGroupId = 0;
            ChunkSize = 0;
            DDiskInfos.clear();
            ChunkRegions.clear();
            AvailableChunks.clear();
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

    struct TSaveChunkSize
    {
        using TArgs = TSaveChunkSize;

        ui32 ChunkSize;

        void Clear()
        {
            ChunkSize = 0;
        }
    };

    struct TSaveGroupInfo
    {
        using TArgs = TSaveGroupInfo;

        TVector<TPartitionDirectDatabase::TGroupInfo> GroupInfos;  // Multiple groups for striping
        TVector<TPartitionDirectDatabase::TGroupInfo> ExistingGroupsToDelete;  // For prepare phase

        void Clear()
        {
            GroupInfos.clear();
            ExistingGroupsToDelete.clear();
        }
    };

    struct TSaveReservedChunks
    {
        using TArgs = TSaveReservedChunks;

        TString DDiskServiceId;
        TVector<ui32> ChunkIds;

        void Clear()
        {
            DDiskServiceId.clear();
            ChunkIds.clear();
        }
    };

    struct TPreallocateVolumeChunks
    {
        using TArgs = TPreallocateVolumeChunks;

        TString DDiskServiceId;
        TVector<ui32> ChunkIds;
        ui64 StartRegionIndex;  // Starting region index for this DDisk's chunks (legacy)
        ui32 GroupIndex;        // Group index for group-aware allocation

        void Clear()
        {
            DDiskServiceId.clear();
            ChunkIds.clear();
            StartRegionIndex = 0;
        }
    };

    struct TAllocateChunkForRegion
    {
        using TArgs = TAllocateChunkForRegion;

        ui64 StartOffset;
        ui32 ChunkId;
        TString DDiskServiceId;

        // Results read during prepare phase
        TMaybe<TPartitionDirectDatabase::TChunkRegion> ChunkRegion;

        void Clear()
        {
            StartOffset = 0;
            ChunkId = 0;
            DDiskServiceId.clear();
            ChunkRegion = Nothing();
        }
    };
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
