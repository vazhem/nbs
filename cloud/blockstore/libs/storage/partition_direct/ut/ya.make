UNITTEST_FOR(cloud/blockstore/libs/storage/partition_direct)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    partition_direct_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
    cloud/storage/core/libs/tablet
)

YQL_LAST_ABI_VERSION()

END()
