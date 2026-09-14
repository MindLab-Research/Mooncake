#include "durable_delete_rpc.h"

#include <memory>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake {
void DurableDeleteRpcHandler::SetExecutor(Executor executor) {
    std::lock_guard lock(mutex_);
    if (!stopping_) executor_ = std::move(executor);
}

void DurableDeleteRpcHandler::SetFenceExecutor(FenceExecutor executor) {
    std::lock_guard lock(mutex_);
    if (!stopping_) fence_executor_ = std::move(executor);
}

void DurableDeleteRpcHandler::StopAndDrain() {
    std::unique_lock lock(mutex_);
    stopping_ = true;
    drained_.wait(lock, [this] { return in_flight_ == 0; });
    executor_ = {};
    fence_executor_ = {};
    read_executor_ = {};
}

void DurableDeleteRpcHandler::SetReadExecutor(ReadExecutor executor) {
    std::lock_guard lock(mutex_);
    if (!stopping_) read_executor_ = std::move(executor);
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
DurableDeleteRpcHandler::CheckRead(DurableReadCommand command) {
    struct CallState {
        ReadExecutor executor;
        DurableReadCommand command;
        std::function<void()> complete;
        ~CallState() {
            if (complete) complete();
        }
    };
    auto state = std::make_unique<CallState>();
    state->command = std::move(command);
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || !read_executor_)
            co_return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
        state->executor = read_executor_;
        state->complete = [this] {
            std::lock_guard lock(mutex_);
            --in_flight_;
            drained_.notify_all();
        };
        ++in_flight_;
    }
    auto* call = state.get();
    auto result = co_await coro_io::post(
        [call] { return call->executor(call->command); });
    co_return result.value();
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
RequestDurableProviderRead(DurableReadCommand command) {
    coro_rpc::coro_rpc_client client;
    auto connected = co_await client.connect(command.provider_endpoint);
    if (connected) co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    auto response =
        co_await client.call<&DurableDeleteRpcHandler::CheckRead>(command);
    if (!response) co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    co_return std::move(*response);
}

async_simple::coro::Lazy<tl::expected<DurableDeleteReceipt, ErrorCode>>
DurableDeleteRpcHandler::Execute(DurableDeleteCommand command) {
    struct CallState {
        Executor executor;
        FenceExecutor fence;
        DurableDeleteCommand command;
        std::function<void()> complete;
        ~CallState() {
            if (complete) complete();
        }
    };
    auto state = std::make_unique<CallState>();
    state->command = std::move(command);
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || !executor_)
            co_return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
        if (state->command.storage_namespace.protocol_version ==
                kDurableReadFenceProtocolVersion &&
            !fence_executor_)
            co_return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
        state->executor = executor_;
        state->fence = fence_executor_;
        state->complete = [this] {
            std::lock_guard lock(mutex_);
            --in_flight_;
            drained_.notify_all();
        };
        ++in_flight_;
    }
    auto* call = state.get();
    if (call->command.storage_namespace.protocol_version ==
        kDurableReadFenceProtocolVersion) {
        auto fenced = co_await coro_io::post(
            [call] { return call->fence(call->command); });
        if (!fenced.value())
            co_return tl::make_unexpected(fenced.value().error());
        // A negative cloud check precedes a lease's deadline. Waiting the
        // protocol bound after the permanent marker drains even absent peers.
        // No object lock or blocking worker is retained across this timer.
        if (!(co_await coro_io::sleep_for(
                std::chrono::milliseconds(kMaxDurableReadLeaseMs))))
            co_return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // Do not block the provider's single offload RPC thread on OSS or Master.
    auto result = co_await coro_io::post(
        [call] { return call->executor(call->command); });
    co_return result.value();
}

async_simple::coro::Lazy<tl::expected<DurableDeleteReceipt, ErrorCode>>
RequestDurableProviderDelete(std::string endpoint,
                             DurableDeleteCommand command) {
    // A static pool starts idle collectors on the calling server's executor.
    // They outlive the request and prevent that server from stopping. Deletes
    // are control-plane operations: own one connection for the whole request
    // and close it before returning, with no background reconnect/idle work.
    coro_rpc::coro_rpc_client client;
    auto connected = co_await client.connect(endpoint);
    if (connected) co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    auto response =
        co_await client.call<&DurableDeleteRpcHandler::Execute>(command);
    if (!response) {
        co_return tl::make_unexpected(response.error().code ==
                                              coro_rpc::errc::timed_out
                                          ? ErrorCode::RPC_TIMEOUT
                                          : ErrorCode::RPC_FAIL);
    }
    co_return std::move(*response);
}
}  // namespace mooncake
