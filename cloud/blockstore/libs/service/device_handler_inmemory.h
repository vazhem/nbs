#pragma once

#include "public.h"

#include "device_handler.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IDeviceHandlerFactoryPtr CreateInMemoryDeviceHandlerFactory(
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore
