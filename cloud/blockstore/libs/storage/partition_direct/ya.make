LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
    partition_direct_actor_memory.cpp
    partition_direct_database.cpp
    partition_direct_state.cpp
    partition_direct_storage_mem.cpp
    partition_direct_storage_proxy.cpp
    partition_direct_worker.cpp
    partition_direct_worker_ping.cpp
    partition_direct_worker_mem.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet

    library/cpp/cgiparam
    library/cpp/containers/dense_hash
    library/cpp/containers/intrusive_rb_tree
    library/cpp/containers/stack_vector
    library/cpp/lwtrace
    library/cpp/monlib/service/pages

    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
