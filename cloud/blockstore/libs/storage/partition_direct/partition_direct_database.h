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
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
