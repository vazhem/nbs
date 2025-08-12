#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include "partition_direct_storage_mem.h"

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

NProto::TError TInMemoryStorage::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->GetBlocksCount();
    const auto& sglist = request->Sglist;

    if (sglist.Empty()) {
        return MakeError(E_ARGUMENT, "Empty sglist in ReadBlocksLocal");
    }

    auto guard = sglist.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    char* dst = const_cast<char*>(guard.Get()[0].Data());
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        auto it = Blocks.find(blockIndex);
        if (it != Blocks.end()) {
            memcpy(dst, it->second.data(), BlockSize);
        } else {
            memset(dst, 0, BlockSize);
        }
        dst += BlockSize;
    }

    return MakeError(S_OK);
}

NProto::TError TInMemoryStorage::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->BlocksCount;
    const auto& sglist = request->Sglist;

    if (sglist.Empty()) {
        return MakeError(E_ARGUMENT, "Empty sglist in WriteBlocksLocal");
    }

    auto guard = sglist.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    const char* src = guard.Get()[0].Data();
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        TString blockData(src, BlockSize);
        Blocks[blockIndex] = std::move(blockData);
        src += BlockSize;
    }

    return MakeError(S_OK);
}

NProto::TError TInMemoryStorage::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(callContext);

    const ui64 startIndex = request->GetStartIndex();
    const ui32 blockCount = request->GetBlocksCount();

    TString zeroBlock(BlockSize, 0);
    for (ui32 i = 0; i < blockCount; ++i) {
        ui64 blockIndex = startIndex + i;
        Blocks[blockIndex] = zeroBlock;
    }

    return MakeError(S_OK);
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
