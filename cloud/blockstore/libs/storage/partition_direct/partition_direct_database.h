#pragma once

#include "public.h"
#include "partition_direct_schema.h"

#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDirectDatabase
    : public NKikimr::NIceDb::TNiceDb
{
public:
    TPartitionDirectDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    //
    // Meta
    //

    void WriteMeta(const NProto::TPartitionDirectMeta& meta);
    bool ReadMeta(TMaybe<NProto::TPartitionDirectMeta>& meta);

    //
    // VirtualGroupId and ChunkSize
    //

    void WriteVirtualGroupId(ui32 groupId);
    bool ReadVirtualGroupId(ui32& groupId);

    void WriteChunkSize(ui32 chunkSize);
    bool ReadChunkSize(ui32& chunkSize);

    //
    // Group and DDisk Information (Multi-Group Support)
    //

    struct TDDiskInfo {
        ui32 NodeId = 0;
        ui32 PDiskId = 0;
        ui32 VSlotId = 0;
        ui32 OrderInGroup = 0;
        ui32 GroupIndex = 0;    // Which group this DDisk belongs to (0-2 for striping)
    };

    struct TGroupInfo {
        ui32 GroupId;       // Virtual Group ID from BSController
        ui32 GroupIndex;    // Striping order
        TVector<TDDiskInfo> DDiskInfos;  // DDisks in this group
    };

    void WriteGroupInfos(const TVector<TGroupInfo>& groupInfos, const TVector<TGroupInfo>& existingGroupsToDelete = {});
    bool ReadGroupInfos(TVector<TGroupInfo>& groupInfos);
    void ClearGroupInfos();

    // Legacy methods for backward compatibility (will use first group)
    void WriteDDiskInfos(const TVector<TDDiskInfo>& ddiskInfos);
    bool ReadDDiskInfos(TVector<TDDiskInfo>& ddiskInfos);
    void ClearDDiskInfos();

    //
    // Chunk Management
    //

    struct TAvailableChunk {
        ui32 ChunkId;
        TString DDiskServiceId;  // DDisk service ID for sending requests
        ui32 Status;             // EChunkStatus enum value
        ui64 RegionIndex;        // Which region this chunk serves (0 if not allocated)
    };

    struct TChunkRegion {
        ui64 StartOffset;        // 128MB aligned offset (primary key)
        ui32 ChunkId;            // PDisk chunk ID
        TString DDiskServiceId;  // DDisk service ID for sending requests
    };

    // Available chunks methods
    void WriteAvailableChunk(ui32 chunkId, const TString& ddiskServiceId, ui32 status, ui64 regionIndex);
    bool ReadAvailableChunks(TVector<TAvailableChunk>& availableChunks);
    void UpdateChunkStatus(ui32 chunkId, ui32 newStatus, ui64 regionIndex);
    bool FindAvailableChunkForDDisk(const TString& ddiskServiceId, ui32& chunkId);

    // Chunk regions methods
    void WriteChunkRegion(ui64 startOffset, ui32 chunkId, const TString& ddiskServiceId);
    bool ReadChunkRegion(ui64 startOffset, TChunkRegion& chunkRegion);
    bool ReadAllChunkRegions(TVector<TChunkRegion>& chunkRegions);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
