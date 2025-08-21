LIBRARY()

SRCS(
    ddisk_actor.cpp
    skeleton/ddisk_skeletonfront.cpp
)

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
)

END()
