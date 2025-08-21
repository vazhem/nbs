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

    // Initialize with default meta record
    using TTable = TPartitionDirectSchema::Meta;
    NProto::TPartitionDirectMeta defaultMeta;
    Table<TTable>()
        .Key(1)
        .Update(NIceDb::TUpdate<TTable::PartitionDirectMeta>(defaultMeta));
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
    TMaybe<NProto::TPartitionDirectMeta> meta;
    ReadMeta(meta);

    if (meta) {
        meta->SetVirtualGroupId(groupId);
        WriteMeta(*meta);
    } else {
        NProto::TPartitionDirectMeta newMeta;
        newMeta.SetVirtualGroupId(groupId);
        WriteMeta(newMeta);
    }
}

bool TPartitionDirectDatabase::ReadVirtualGroupId(ui32& groupId)
{
    TMaybe<NProto::TPartitionDirectMeta> meta;
    if (!ReadMeta(meta) || !meta) {
        return false;
    }

    groupId = meta->GetVirtualGroupId();
    return true;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
