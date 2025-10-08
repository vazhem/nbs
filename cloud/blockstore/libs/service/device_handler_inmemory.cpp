#include "device_handler_inmemory.h"

#include "storage.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryDeviceHandler final
    : public IDeviceHandler
{
private:
    const IStoragePtr Storage;
    const ui32 BlockSize;
    TLog Log;

public:
    TInMemoryDeviceHandler(IStoragePtr storage, ui32 blockSize, ILoggingServicePtr logging)
        : Storage(std::move(storage))
        , BlockSize(blockSize)
    {
        if (logging) {
            Log = logging->CreateLog("BLOCKSTORE_SERVER");
        }
        STORAGE_INFO("InMemoryDeviceHandler created: BlockSize=" << BlockSize);
    }

    TFuture<NProto::TReadBlocksLocalResponse> Read(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(checkpointId);

        STORAGE_DEBUG("InMemoryDeviceHandler: Read request - from=" << from
            << " length=" << length << " bytes=" << (BlockSize * length));

        NProto::TReadBlocksLocalResponse response;

        auto guard = sgList.Acquire();
        if (!guard) {
            *response.MutableError() = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in InMemoryDeviceHandler");
            STORAGE_ERROR("InMemoryDeviceHandler: Read failed to acquire sglist");
            return MakeFuture(response);
        }

        // Fill with zeros
        size_t responseSize = BlockSize * length;
        for (const auto& buf: guard.Get()) {
            if (responseSize == 0) {
                break;
            }

            // Validate buffer before accessing
            if (!buf.Data() || buf.Size() == 0) {
                continue;
            }

            auto size = std::min(buf.Size(), responseSize);
            memset((void*)buf.Data(), 0, size);
            responseSize -= size;
        }

        STORAGE_DEBUG("InMemoryDeviceHandler: Read completed successfully - "
            << "returned " << (BlockSize * length) << " bytes of zeros");

        return MakeFuture(response);
    }

    TFuture<NProto::TWriteBlocksLocalResponse> Write(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override
    {
        Y_UNUSED(callContext);

        STORAGE_DEBUG("InMemoryDeviceHandler: Write request - from=" << from
            << " length=" << length << " bytes=" << (BlockSize * length));

        // Acquire sgList to keep it alive during the operation
        auto guard = sgList.Acquire();
        if (!guard) {
            NProto::TWriteBlocksLocalResponse response;
            *response.MutableError() = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in InMemoryDeviceHandler");
            STORAGE_ERROR("InMemoryDeviceHandler: Write failed to acquire sglist");
            return MakeFuture(response);
        }

        // Simply accept the write without storing or validating anything
        STORAGE_DEBUG("InMemoryDeviceHandler: Write completed successfully - "
            << "discarded " << (BlockSize * length) << " bytes");

        NProto::TWriteBlocksLocalResponse response;
        return MakeFuture(response);
    }

    TFuture<NProto::TZeroBlocksResponse> Zero(
        TCallContextPtr callContext,
        ui64 from,
        ui64 length) override
    {
        Y_UNUSED(callContext);

        STORAGE_DEBUG("InMemoryDeviceHandler: Zero request - from=" << from
            << " length=" << length << " bytes=" << (BlockSize * length));

        // Simply accept the zero request
        NProto::TZeroBlocksResponse response;

        STORAGE_DEBUG("InMemoryDeviceHandler: Zero completed successfully");

        return MakeFuture(response);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        // Allocate real memory buffer for NBD server handler to use
        if (bytesCount == 0) {
            return nullptr;
        }

        // Use shared_ptr with custom deleter to manage the buffer
        return TStorageBuffer(
            new char[bytesCount],
            [](void* ptr) { delete[] static_cast<char*>(ptr); });
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInMemoryDeviceHandlerFactory final
    : public IDeviceHandlerFactory
{
private:
    const ILoggingServicePtr Logging;

public:
    explicit TInMemoryDeviceHandlerFactory(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {}

    IDeviceHandlerPtr CreateDeviceHandler(
        IStoragePtr storage,
        TString diskId,
        TString clientId,
        ui32 blockSize,
        bool unalignedRequestsDisabled,
        bool checkBufferModificationDuringWriting,
        NProto::EStorageMediaKind storageMediaKind,
        ui32 maxZeroBlocksSubRequestSize) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
        Y_UNUSED(unalignedRequestsDisabled);
        Y_UNUSED(checkBufferModificationDuringWriting);
        Y_UNUSED(storageMediaKind);
        Y_UNUSED(maxZeroBlocksSubRequestSize);

        return std::make_shared<TInMemoryDeviceHandler>(
            std::move(storage),
            blockSize,
            Logging);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateInMemoryDeviceHandlerFactory(ILoggingServicePtr logging)
{
    return std::make_shared<TInMemoryDeviceHandlerFactory>(std::move(logging));
}

}   // namespace NCloud::NBlockStore
