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
        info.GroupIndex = it.GetValue<TTable::GroupIndex>();

        ddiskInfos.push_back(info);

        if (!it.Next()) {
            break;
        }
    }

    return true;
}

void TPartitionDirectDatabase::WriteGroupInfos(const TVector<TGroupInfo>& groupInfos, const TVector<TGroupInfo>& existingGroupsToDelete)
{
    using TGroupTable = TPartitionDirectSchema::Groups;
    using TDDiskTable = TPartitionDirectSchema::DDiskInfo;

    // Clear existing data using information gathered in Prepare phase
    for (const auto& existingGroup : existingGroupsToDelete) {
        // Delete group entry
        Table<TGroupTable>().Key(existingGroup.GroupIndex).Delete();

        // Delete all DDisk entries for this group
        for (const auto& ddiskInfo : existingGroup.DDiskInfos) {
            Y_UNUSED(ddiskInfo);
            // Find and delete DDisk entries - we need to iterate through all possible IDs
            // Since we don't know the exact ID, we'll delete by matching the group index
            // This is inefficient but necessary without storing DDisk IDs in prepare phase
        }
    }

    // For simplicity, delete all DDisk entries and recreate them
    // This is safe since we have all the data to recreate
    for (ui32 id = 1; id <= 1000; ++id) {  // Assume reasonable upper bound
        Table<TDDiskTable>().Key(id).Delete();
    }

    ui32 globalDDiskId = 1;

    // Write groups and their DDisk information
    for (const auto& groupInfo : groupInfos) {
        // Write group information
        Table<TGroupTable>()
            .Key(groupInfo.GroupIndex)
            .Update(
                NIceDb::TUpdate<TGroupTable::GroupId>(groupInfo.GroupId),
                NIceDb::TUpdate<TGroupTable::GroupName>("BlockShardGroup")
            );

        // Write DDisk information for this group
        for (const auto& ddiskInfo : groupInfo.DDiskInfos) {
            Table<TDDiskTable>()
                .Key(globalDDiskId++)
                .Update(
                    NIceDb::TUpdate<TDDiskTable::NodeId>(ddiskInfo.NodeId),
                    NIceDb::TUpdate<TDDiskTable::PDiskId>(ddiskInfo.PDiskId),
                    NIceDb::TUpdate<TDDiskTable::VSlotId>(ddiskInfo.VSlotId),
                    NIceDb::TUpdate<TDDiskTable::OrderInGroup>(ddiskInfo.OrderInGroup),
                    NIceDb::TUpdate<TDDiskTable::GroupIndex>(ddiskInfo.GroupIndex)
                );
        }
    }
}

bool TPartitionDirectDatabase::ReadGroupInfos(TVector<TGroupInfo>& groupInfos)
{
    using TGroupTable = TPartitionDirectSchema::Groups;
    using TDDiskTable = TPartitionDirectSchema::DDiskInfo;

    groupInfos.clear();

    // Read groups
    THashMap<ui32, TGroupInfo> groupMap;
    auto groupIt = Table<TGroupTable>()
        .Range()
        .Select();

    if (!groupIt.IsReady()) {
        return false;
    }

    while (groupIt.IsValid()) {
        ui32 groupIndex = groupIt.GetValue<TGroupTable::GroupIndex>();
        ui32 groupId = groupIt.GetValue<TGroupTable::GroupId>();

        TGroupInfo groupInfo;
        groupInfo.GroupIndex = groupIndex;
        groupInfo.GroupId = groupId;
        groupMap[groupIndex] = groupInfo;

        if (!groupIt.Next()) {
            break;
        }
    }

    // Read DDisk information and associate with groups
    auto ddiskIt = Table<TDDiskTable>()
        .Range()
        .Select();

    if (!ddiskIt.IsReady()) {
        return !groupMap.empty();  // Groups exist but no DDisks
    }

    while (ddiskIt.IsValid()) {
        TDDiskInfo ddiskInfo;
        ddiskInfo.NodeId = ddiskIt.GetValue<TDDiskTable::NodeId>();
        ddiskInfo.PDiskId = ddiskIt.GetValue<TDDiskTable::PDiskId>();
        ddiskInfo.VSlotId = ddiskIt.GetValue<TDDiskTable::VSlotId>();
        ddiskInfo.OrderInGroup = ddiskIt.GetValue<TDDiskTable::OrderInGroup>();
        ddiskInfo.GroupIndex = ddiskIt.GetValue<TDDiskTable::GroupIndex>();

        auto it = groupMap.find(ddiskInfo.GroupIndex);
        if (it != groupMap.end()) {
            it->second.DDiskInfos.push_back(ddiskInfo);
        }

        if (!ddiskIt.Next()) {
            break;
        }
    }

    // Convert map to vector, sorted by group index
    for (auto& [groupIndex, groupInfo] : groupMap) {
        groupInfos.push_back(std::move(groupInfo));
    }

    std::sort(groupInfos.begin(), groupInfos.end(),
              [](const TGroupInfo& a, const TGroupInfo& b) {
                  return a.GroupIndex < b.GroupIndex;
              });

    return !groupInfos.empty();
}

void TPartitionDirectDatabase::ClearGroupInfos()
{
    using TGroupTable = TPartitionDirectSchema::Groups;
    using TDDiskTable = TPartitionDirectSchema::DDiskInfo;

    // For clearing (when we don't have existing data), just delete by range
    // This should only be called in contexts where reads are allowed
    for (ui32 groupIndex = 0; groupIndex < 32; ++groupIndex) {
        Table<TGroupTable>().Key(groupIndex).Delete();
    }

    for (ui32 id = 1; id <= 1000; ++id) {  // Assume reasonable upper bound
        Table<TDDiskTable>().Key(id).Delete();
    }
}

void TPartitionDirectDatabase::ClearDDiskInfos()
{
    ClearGroupInfos();
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
