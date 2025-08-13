#include "partition_direct_storage_proxy.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

NProto::TError TProxyStorage::ReadBlocksLocal(
    const TActorContext& ctx,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
//     Y_UNUSED(ctx);
//     Y_UNUSED(request);

//     auto bsRequest = std::make_unique<TEvBlobStorage::TEvGet>(
//             0,
//             request->GetStartIndex(),  // shift
//             request->GetBlocksCount() * request->BlockSize,  // size
//             TInstant::Max(),
//             NKikimrBlobStorage::FastRead
//         );

//     int ch = 0;
//     int gen = 0;
//     const auto proxy = Info()->BSProxyIDForChannel(ch, gen);
//     SendToBSProxy(
//         ctx,
//         proxy,
//         bsRequest.release());
//     return MakeError(S_OK);

    Y_UNUSED(ctx);
    Y_UNUSED(request);
    // TODO: Реализовать обнуление блоков через прокси на основе канала
    return MakeError(E_NOT_IMPLEMENTED, "ReadBlocksLocal through proxy not implemented");
}

NProto::TError TProxyStorage::WriteBlocksLocal(
    const TActorContext& ctx,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    // TString blobContent;
    // const auto& sgList = request->SgList;
    // blobContent.ReserveAndResize(SgListGetSize(sgList));
    // SgListCopy(sgList, { blobContent.data(), blobContent.size() });

    // auto bsRequest = std::make_unique<TEvBlobStorage::TEvPut>(
    //     MakeBlobId(0, 0),  // временный blobId
    //     std::move(blobContent),
    //     TInstant::Max(),
    //     NKikimrBlobStorage::AsyncBlob);

    // int ch = 0;
    // int gen = 0;
    // const auto proxy = Info()->BSProxyIDForChannel(ch, gen);
    // SendToBSProxy(
    //     ctx,
    //     proxy,
    //     bsRequest.release());

    // return MakeError(S_OK);

    Y_UNUSED(ctx);
    Y_UNUSED(request);
    return MakeError(E_NOT_IMPLEMENTED, "WriteBlocksLocal through proxy not implemented");
}

NProto::TError TProxyStorage::ZeroBlocks(
    const TActorContext& ctx,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);
    // TODO: Реализовать обнуление блоков через прокси на основе канала
    return MakeError(E_NOT_IMPLEMENTED, "Zero blocks through proxy not implemented");
}

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
