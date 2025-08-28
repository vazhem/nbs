#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <contrib/ydb/core/scheme/scheme_types_defs.h>
#include <contrib/ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionDirectSchema
    : public NKikimr::NIceDb::Schema
{
    struct Meta
        : public TTableSchema<1>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct PartitionDirectMeta
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = NProto::TPartitionDirectMeta;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            PartitionDirectMeta>;

        using StoragePolicy = TStoragePolicy<0>;  // System channel
    };

    struct VirtualGroup
        : public TTableSchema<2>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct GroupId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct ChunkSize
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            GroupId,
            ChunkSize>;

        using StoragePolicy = TStoragePolicy<0>;  // System channel
    };

    struct DDiskInfo
        : public TTableSchema<3>
    {
        struct Id
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct NodeId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct PDiskId
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct VSlotId
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct OrderInGroup
            : public Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            NodeId,
            PDiskId,
            VSlotId,
            OrderInGroup>;

        using StoragePolicy = TStoragePolicy<0>;  // System channel
    };

    struct AvailableChunks
        : public TTableSchema<4>
    {
        struct ChunkId
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct DDiskServiceId
            : public Column<2, NKikimr::NScheme::NTypeIds::Utf8>
        {
        };

        struct Status
            : public Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct RegionIndex
            : public Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<ChunkId>;
        using TColumns = TableColumns<
            ChunkId,
            DDiskServiceId,
            Status,
            RegionIndex>;

        using StoragePolicy = TStoragePolicy<0>;  // System channel
    };

    struct ChunkRegions
        : public TTableSchema<5>
    {
        struct StartOffset
            : public Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct ChunkId
            : public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct DDiskServiceId
            : public Column<3, NKikimr::NScheme::NTypeIds::Utf8>
        {
        };

        using TKey = TableKey<StartOffset>;
        using TColumns = TableColumns<
            StartOffset,
            ChunkId,
            DDiskServiceId>;

        using StoragePolicy = TStoragePolicy<0>;  // System channel
    };

    using TTables = SchemaTables<
        Meta,
        VirtualGroup,
        DDiskInfo,
        AvailableChunks,
        ChunkRegions
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<0>
    >;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
