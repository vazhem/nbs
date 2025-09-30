#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include "partition_direct_storage_mem.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError TInMemoryStorage::ReadBlocksLocal(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(traceId);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->GetBlocksCount();
    const auto& sglist = request->Sglist;

    if (sglist.Empty()) {
        return NCloud::MakeError(E_ARGUMENT, "Empty sglist in ReadBlocksLocal");
    }

    auto guard = sglist.Acquire();
    if (!guard) {
        return NCloud::MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    char* dst = const_cast<char*>(guard.Get()[0].Data());
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        auto it = Blocks.find(blockIndex);
        if (it != Blocks.end()) {
            memcpy(dst, it->second.data(), request->BlockSize);
        } else {
            memset(dst, 0, request->BlockSize);
        }
        dst += request->BlockSize;
    }

    // Send success response - data is already in the request's sglist
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "InMemoryStorage: ReadBlocksLocal completed - startIndex: %lu, blockCount: %u",
        startIndex, blockCount);

    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
    response->Record.MutableError()->CopyFrom(NCloud::MakeError(S_OK));
    NCloud::Reply(ctx, *requestInfo, std::move(response));

    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TInMemoryStorage::WriteBlocksLocal(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(traceId);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->BlocksCount;
    const auto& sglist = request->Sglist;

    if (sglist.Empty()) {
        return NCloud::MakeError(E_ARGUMENT, "Empty sglist in WriteBlocksLocal");
    }

    auto guard = sglist.Acquire();
    if (!guard) {
        return NCloud::MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    const char* src = guard.Get()[0].Data();
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        TString blockData(src, request->BlockSize);
        Blocks[blockIndex] = std::move(blockData);
        src += request->BlockSize;
    }

    // Send success response
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "InMemoryStorage: WriteBlocksLocal completed - startIndex: %lu, blockCount: %u",
        startIndex, blockCount);

    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
    response->Record.MutableError()->CopyFrom(NCloud::MakeError(S_OK));
    NCloud::Reply(ctx, *requestInfo, std::move(response));

    return NCloud::MakeError(S_OK);
}

NCloud::NProto::TError TInMemoryStorage::ZeroBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(traceId);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->GetBlocksCount();

    TString zeroBlock(BlockSize, 0);
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        Blocks[blockIndex] = zeroBlock;
    }

    // Send success response - ZeroBlocks uses WriteBlocksLocalResponse
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "InMemoryStorage: ZeroBlocks completed - startIndex: %lu, blockCount: %u",
        startIndex, blockCount);

    auto response = std::make_unique<TEvService::TEvWriteBlocksLocalResponse>();
    response->Record.MutableError()->CopyFrom(NCloud::MakeError(S_OK));
    NCloud::Reply(ctx, *requestInfo, std::move(response));

    return NCloud::MakeError(S_OK);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
