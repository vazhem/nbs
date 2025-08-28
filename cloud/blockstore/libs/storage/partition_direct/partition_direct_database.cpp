#include "partition_direct_database.h"
#include "partition_direct_schema.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TPartitionDirectDatabase::InitSchema()
{
    Materialize<TPartitionDirectSchema>();

    TSchemaInitializer<TPartitionDirectSchema::TTables>::InitStorage(Database.Alter());

    // No default records needed - tables will be populated as needed
}

void TPartitionDirectDatabase::WriteMeta(const NProto::TPartitionDirectMeta& meta)
{
    using TTable = TPartitionDirectSchema::Meta;

    Table<TTable>()
        .Key(1)
        .Update(NIceDb::TUpdate<TTable::PartitionDirectMeta>(meta));
}

bool TPartitionDirectDatabase::ReadMeta(TMaybe<NProto::TPartitionDirectMeta>& meta)
{
    using TTable = TPartitionDirectSchema::Meta;

    auto it = Table<TTable>()
        .Key(1)
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    if (it.IsValid()) {
        meta = it.GetValue<TTable::PartitionDirectMeta>();
    }

    return true;
}

void TPartitionDirectDatabase::WriteVirtualGroupId(ui32 groupId)
{
    using TTable = TPartitionDirectSchema::VirtualGroup;

    Table<TTable>()
        .Key(1)
        .Update(
            NIceDb::TUpdate<TTable::GroupId>(groupId)
        );
}

bool TPartitionDirectDatabase::ReadVirtualGroupId(ui32& groupId)
{
    using TTable = TPartitionDirectSchema::VirtualGroup;

    auto it = Table<TTable>()
        .Key(1)
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    if (!it.IsValid()) {
        return false;  // No record found
    }

    groupId = it.GetValue<TTable::GroupId>();
    return true;
}

void TPartitionDirectDatabase::WriteChunkSize(ui32 chunkSize)
{
    using TTable = TPartitionDirectSchema::VirtualGroup;

    Table<TTable>()
        .Key(1)
        .Update(
            NIceDb::TUpdate<TTable::ChunkSize>(chunkSize)
        );
}

bool TPartitionDirectDatabase::ReadChunkSize(ui32& chunkSize)
{
    using TTable = TPartitionDirectSchema::VirtualGroup;

    auto it = Table<TTable>()
        .Key(1)
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    if (!it.IsValid()) {
        return false;  // No record found
    }

    chunkSize = it.GetValue<TTable::ChunkSize>();
    return true;
}

void TPartitionDirectDatabase::WriteDDiskInfos(const TVector<TDDiskInfo>& ddiskInfos)
{
    using TTable = TPartitionDirectSchema::DDiskInfo;

    // Write new DDisk information
    for (ui32 i = 0; i < ddiskInfos.size(); ++i) {
        const auto& ddiskInfo = ddiskInfos[i];

        Table<TTable>()
            .Key(i + 1)  // Use 1-based keys
            .Update(
                NIceDb::TUpdate<TTable::NodeId>(ddiskInfo.NodeId),
                NIceDb::TUpdate<TTable::PDiskId>(ddiskInfo.PDiskId),
                NIceDb::TUpdate<TTable::VSlotId>(ddiskInfo.VSlotId),
                NIceDb::TUpdate<TTable::OrderInGroup>(ddiskInfo.OrderInGroup)
            );
    }
}

bool TPartitionDirectDatabase::ReadDDiskInfos(TVector<TDDiskInfo>& ddiskInfos)
{
    using TTable = TPartitionDirectSchema::DDiskInfo;

    ddiskInfos.clear();

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        TDDiskInfo info;
        info.NodeId = it.GetValue<TTable::NodeId>();
        info.PDiskId = it.GetValue<TTable::PDiskId>();
        info.VSlotId = it.GetValue<TTable::VSlotId>();
        info.OrderInGroup = it.GetValue<TTable::OrderInGroup>();

        ddiskInfos.push_back(info);

        if (!it.Next()) {
            break;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Chunk management methods

void TPartitionDirectDatabase::WriteAvailableChunk(ui32 chunkId, const TString& ddiskServiceId, ui32 status, ui64 regionIndex)
{
    using TTable = TPartitionDirectSchema::AvailableChunks;

    Table<TTable>()
        .Key(chunkId)
        .Update(
            NIceDb::TUpdate<TTable::DDiskServiceId>(ddiskServiceId),
            NIceDb::TUpdate<TTable::Status>(status),
            NIceDb::TUpdate<TTable::RegionIndex>(regionIndex)
        );
}

bool TPartitionDirectDatabase::ReadAvailableChunks(TVector<TAvailableChunk>& availableChunks)
{
    using TTable = TPartitionDirectSchema::AvailableChunks;

    availableChunks.clear();

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        TAvailableChunk chunk;
        chunk.ChunkId = it.GetValue<TTable::ChunkId>();
        chunk.DDiskServiceId = it.GetValue<TTable::DDiskServiceId>();
        chunk.Status = it.GetValue<TTable::Status>();
        chunk.RegionIndex = it.GetValue<TTable::RegionIndex>();

        availableChunks.push_back(chunk);

        if (!it.Next()) {
            break;
        }
    }

    return true;
}

void TPartitionDirectDatabase::WriteChunkRegion(ui64 startOffset, ui32 chunkId, const TString& ddiskServiceId)
{
    using TTable = TPartitionDirectSchema::ChunkRegions;

    Table<TTable>()
        .Key(startOffset)
        .Update(
            NIceDb::TUpdate<TTable::ChunkId>(chunkId),
            NIceDb::TUpdate<TTable::DDiskServiceId>(ddiskServiceId)
        );
}

bool TPartitionDirectDatabase::ReadChunkRegion(ui64 startOffset, TChunkRegion& chunkRegion)
{
    using TTable = TPartitionDirectSchema::ChunkRegions;

    auto it = Table<TTable>()
        .Key(startOffset)
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    if (!it.IsValid()) {
        return false;  // No record found
    }

    chunkRegion.StartOffset = startOffset;
    chunkRegion.ChunkId = it.GetValue<TTable::ChunkId>();
    chunkRegion.DDiskServiceId = it.GetValue<TTable::DDiskServiceId>();

    return true;
}

bool TPartitionDirectDatabase::ReadAllChunkRegions(TVector<TChunkRegion>& chunkRegions)
{
    using TTable = TPartitionDirectSchema::ChunkRegions;

    chunkRegions.clear();

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        TChunkRegion region;
        region.StartOffset = it.GetValue<TTable::StartOffset>();
        region.ChunkId = it.GetValue<TTable::ChunkId>();
        region.DDiskServiceId = it.GetValue<TTable::DDiskServiceId>();

        chunkRegions.push_back(region);

        if (!it.Next()) {
            break;
        }
    }

    return true;
}

void TPartitionDirectDatabase::UpdateChunkStatus(ui32 chunkId, ui32 newStatus, ui64 regionIndex)
{
    using TTable = TPartitionDirectSchema::AvailableChunks;

    Table<TTable>()
        .Key(chunkId)
        .Update(
            NIceDb::TUpdate<TTable::Status>(newStatus),
            NIceDb::TUpdate<TTable::RegionIndex>(regionIndex)
        );
}

bool TPartitionDirectDatabase::FindAvailableChunkForDDisk(const TString& ddiskServiceId, ui32& chunkId)
{
    using TTable = TPartitionDirectSchema::AvailableChunks;

    auto it = Table<TTable>()
        .Range()
        .Select();

    if (!it.IsReady()) {
        return false;
    }

    while (it.IsValid()) {
        if (it.GetValue<TTable::DDiskServiceId>() == ddiskServiceId &&
            it.GetValue<TTable::Status>() == 0) {  // EChunkStatus::Available = 0
            chunkId = it.GetValue<TTable::ChunkId>();
            return true;
        }

        if (!it.Next()) {
            break;
        }
    }

    return false;  // No available chunk found for this DDisk
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
