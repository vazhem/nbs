LIBRARY()

SRCS(
    ddisk_actor.cpp
    ddisk_actor_impl.cpp
    ddisk_actor_mode_memory.cpp
    ddisk_actor_mode_pdisk.cpp
    ddisk_actor_mode_direct.cpp
    ddisk_actor_mode_direct_completion.cpp
    ddisk_worker_actor.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk
)

END()
