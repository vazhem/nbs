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
    // DDisk Information
    //

    struct TDDiskInfo {
        ui32 NodeId;
        ui32 PDiskId;
        ui32 VSlotId;
        ui32 OrderInGroup;
    };

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
