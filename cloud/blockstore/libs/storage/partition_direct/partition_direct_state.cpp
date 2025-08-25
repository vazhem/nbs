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
    const TBlockDataRef& buffer)
{
    if (!ValidateBlockRange(startIndex, blockCount)) {
        return MakeError(E_ARGUMENT, "Invalid block range");
    }

    // Check if DDisk actors are configured
    if (!HasDDiskInfos()) {
        // Fallback to storage layer if DDisk not configured yet
        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blockCount);
        request->BlockSize = GetBlockSize();
        request->Sglist = TGuardedSgList(TSgList{{
            buffer.Data(),
            static_cast<ui32>(blockCount * GetBlockSize())
        }});

        // Use provided request info for fallback storage
        return Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request));
    }

    // Use DDisk actors for direct access
    // For Mirror3Direct, we can read from any DDisk (choose first one for simplicity)
    ui32 selectedDDisk = 0;
    NActors::TActorId ddiskServiceId = GetDDiskServiceId(selectedDDisk);

    if (!ddiskServiceId) {
        return MakeError(E_FAIL, "Invalid DDisk service ID");
    }

    ui64 offset = startIndex * GetBlockSize();
    ui32 size = blockCount * GetBlockSize();

    // Note: This is a simplified implementation. In practice, you would:
    // 1. Send async DDisk request and handle response in actor
    // 2. For now, we'll still use storage layer but log DDisk availability

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "DDisk read available: offset=" << offset << " size=" << size
        << " ddiskServiceId=" << ddiskServiceId.ToString());

    // Fallback to storage for now (full DDisk integration requires async handling)
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->SetBlocksCount(blockCount);
    request->BlockSize = GetBlockSize();
    request->Sglist = TGuardedSgList(TSgList{{
        buffer.Data(),
        static_cast<ui32>(blockCount * GetBlockSize())
    }});

    // Fallback to storage - use provided request info
    return Storage->ReadBlocksLocal(ctx, requestInfo, std::move(request));
}

NProto::TError TPartitionState::WriteBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 startIndex,
    ui32 blockCount,
    const TBlockDataRef& buffer)
{
    if (!ValidateBlockRange(startIndex, blockCount)) {
        return MakeError(E_ARGUMENT, "Invalid block range");
    }

    // Check if DDisk actors are configured
    if (!HasDDiskInfos()) {
        // Fallback to storage layer if DDisk not configured yet
        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->BlocksCount = blockCount;
        request->BlockSize = GetBlockSize();
        request->Sglist = TGuardedSgList(TSgList{{
            buffer.Data(),
            static_cast<ui32>(blockCount * GetBlockSize())
        }});

        // Use provided request info for fallback storage
        return Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request));
    }

    // Use DDisk actors for direct access
    // For Mirror3Direct, we need to write to ALL DDisk actors for redundancy
    const auto& ddiskInfos = GetDDiskInfos();
    ui64 offset = startIndex * GetBlockSize();
    ui32 size = blockCount * GetBlockSize();

    // Note: This is a simplified implementation. In practice, you would:
    // 1. Send async DDisk write requests to all DDisk actors
    // 2. Wait for all responses before confirming success
    // 3. Handle partial failures and retry logic

    LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
        "DDisk write available to " << ddiskInfos.size() << " DDisk actors: offset="
        << offset << " size=" << size);

    for (ui32 i = 0; i < ddiskInfos.size(); ++i) {
        NActors::TActorId ddiskServiceId = GetDDiskServiceId(i);
        LOG_DEBUG_S(ctx, TBlockStoreComponents::PARTITION,
            "DDisk[" << i << "] serviceId=" << ddiskServiceId.ToString());
    }

    // Fallback to storage for now (full DDisk integration requires async handling)
    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->SetStartIndex(startIndex);
    request->BlocksCount = blockCount;
    request->BlockSize = GetBlockSize();
    request->Sglist = TGuardedSgList(TSgList{{
        buffer.Data(),
        static_cast<ui32>(blockCount * GetBlockSize())
    }});

    // Fallback to storage - use provided request info
    return Storage->WriteBlocksLocal(ctx, requestInfo, std::move(request));
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
