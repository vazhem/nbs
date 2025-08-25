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

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
