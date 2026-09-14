#pragma once

#include <string>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <async_simple/coro/Lazy.h>
#include <ylt/util/tl/expected.hpp>
#include "storage/distributed/object_storage_namespace.h"
#include "types.h"

namespace mooncake {
struct DurableDeleteCommand {
    UUID provider_id;
    UUID operation_id;
    UUID assignment_id;
    std::string tenant_id;
    std::string key;
    DurableObjectStorageNamespace storage_namespace;
    std::string provider_endpoint;
};

struct DurableDeleteReceipt {
    UUID provider_id;
    UUID operation_id;
    UUID assignment_id;
    std::string namespace_identity;
    ErrorCode result;
};

struct DurableDeleteRoute {
    std::string endpoint;
    DurableDeleteCommand command;
};

struct DurableDeletePlan {
    UUID operation_id;
    bool completed = false;
    std::vector<DurableDeleteRoute> providers;
};

struct DurableReadCommand {
    UUID provider_id;
    std::string tenant_id;
    std::string key;
    DurableObjectStorageNamespace storage_namespace;
    std::string provider_endpoint;
};

class DurableDeleteRpcHandler {
   public:
    using Executor =
        std::function<tl::expected<DurableDeleteReceipt, ErrorCode>(
            const DurableDeleteCommand&)>;
    void SetExecutor(Executor executor);
    using FenceExecutor = std::function<tl::expected<void, ErrorCode>(
        const DurableDeleteCommand&)>;
    void SetFenceExecutor(FenceExecutor executor);
    using ReadExecutor =
        std::function<tl::expected<void, ErrorCode>(const DurableReadCommand&)>;
    void SetReadExecutor(ReadExecutor executor);
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> CheckRead(
        DurableReadCommand command);
    void StopAndDrain();
    async_simple::coro::Lazy<tl::expected<DurableDeleteReceipt, ErrorCode>>
    Execute(DurableDeleteCommand command);

   private:
    std::mutex mutex_;
    Executor executor_;
    FenceExecutor fence_executor_;
    ReadExecutor read_executor_;
    std::condition_variable drained_;
    size_t in_flight_ = 0;
    bool stopping_ = false;
};

async_simple::coro::Lazy<tl::expected<DurableDeleteReceipt, ErrorCode>>
RequestDurableProviderDelete(std::string endpoint,
                             DurableDeleteCommand command);
async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
RequestDurableProviderRead(DurableReadCommand command);
}  // namespace mooncake
