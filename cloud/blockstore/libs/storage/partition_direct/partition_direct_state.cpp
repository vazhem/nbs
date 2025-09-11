#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include "partition_direct_state.h"
#include "partition_direct_storage_mem.h"

#include <cloud/blockstore/libs/storage/core/public.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

NProto::TError TPartitionState::ReadBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 startIndex,
    ui32 blockCount,
    const TBlockDataRef& buffer,
    const NWilson::TTraceId& traceId)
{
    if (!ValidateBlockRange(startIndex, blockCount)) {
        return MakeError(E_ARGUMENT, "Invalid block range");
    }

    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);
    request->BlockSize = GetBlockSize();
    request->Sglist = TGuardedSgList(TSgList{{
        buffer.Data(),
        static_cast<ui32>(blockCount * GetBlockSize())
    }});

    // Fallback to storage - use provided request info
    if (!Storage) {
        return MakeError(E_FAIL, "Storage not initialized");
    }
    return Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request), traceId);
}

NProto::TError TPartitionState::WriteBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 startIndex,
    ui32 blockCount,
    const TBlockDataRef& buffer,
    const NWilson::TTraceId& traceId)
{
    if (!ValidateBlockRange(startIndex, blockCount)) {
        return MakeError(E_ARGUMENT, "Invalid block range");
    }

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlocksCount = blockCount;
    request->BlockSize = GetBlockSize();
    request->Sglist = TGuardedSgList(TSgList{{
        buffer.Data(),
        static_cast<ui32>(blockCount * GetBlockSize())
    }});

    // Fallback to storage - use provided request info
    if (!Storage) {
        return MakeError(E_FAIL, "Storage not initialized");
    }
    return Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request), traceId);
}

NProto::TError TPartitionState::ZeroBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 startIndex,
    ui32 blockCount)
{
    if (!ValidateBlockRange(startIndex, blockCount)) {
        return MakeError(E_ARGUMENT, "Invalid block range");
    }

    auto request = std::make_shared<NProto::TZeroBlocksRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);

    if (!Storage) {
        return MakeError(E_FAIL, "Storage not initialized");
    }
    return Storage->ZeroBlocks(
        ctx,
        requestInfo,
        std::move(request));
}

void TPartitionState::CopyToSgList(const TBlockDataRef& buffer, const TSgList& sglist)
{
    auto* src = const_cast<char*>(buffer.Data());
    for (const auto& dst: sglist) {
        memcpy(const_cast<char*>(dst.Data()), src, dst.Size());
        src += dst.Size();
    }
}

void TPartitionState::CopyFromSgList(const TSgList& sglist, TBlockDataRef& buffer)
{
    auto* dst = const_cast<char*>(buffer.Data());
    for (const auto& src: sglist) {
        memcpy(dst, src.Data(), src.Size());
        dst += src.Size();
    }
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
