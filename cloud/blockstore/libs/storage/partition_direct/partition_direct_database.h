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
    // VirtualGroupId
    //

    void WriteVirtualGroupId(ui32 groupId);
    bool ReadVirtualGroupId(ui32& groupId);
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
