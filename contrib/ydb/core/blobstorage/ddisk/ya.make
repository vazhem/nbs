LIBRARY()

SRCS(
    ddisk_actor.cpp
    ddisk_actor_impl.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
)

END()
