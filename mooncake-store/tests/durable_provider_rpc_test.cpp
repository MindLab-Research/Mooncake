#include <gtest/gtest.h>
#include <atomic>
#include <algorithm>
#include <future>
#include <filesystem>
#include <unistd.h>
#include <async_simple/coro/SyncAwait.h>
#include <thread>
#include <vector>

#include "master_client.h"
#include "rpc_service.h"

namespace mooncake::test {
namespace {
DurableObjectStorageNamespace Scope() {
    return {3,
            "s3",
            "https://oss.example.test",
            "isolated-bucket",
            "region",
            "isolated-prefix",
            128};
}
}  // namespace

TEST(DurableProviderRpcTest, DiscoveryKeepsBothOwnersAndUnmountRemovesOnlyOne) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;
    MasterService master(config);
    const auto first = generate_uuid();
    const auto second = generate_uuid();
    ASSERT_TRUE(master.MountLocalDiskSegment(first, true));
    ASSERT_TRUE(master.MountLocalDiskSegment(second, true));
    const OffloadTaskItem task{.tenant_id = "default", .key = "regional-key", .size = 1024};
    StorageObjectMetadata a, b;
    a.data_size = b.data_size = 1024;
    a.transport_endpoint = "10.254.254.1:40000";
    b.transport_endpoint = "10.254.254.2:40000";
    ASSERT_TRUE(master.NotifyOffloadSuccess(first, {task}, {a}));
    ASSERT_TRUE(master.NotifyOffloadSuccess(second, {task}, {b}));
    ASSERT_TRUE(master.NotifyOffloadSuccess(second, {task}, {b}));
    auto both = master.GetReplicaListForAdmin("regional-key", TenantId::Default());
    ASSERT_TRUE(both);
    ASSERT_EQ(both->replicas.size(), 2);
    ASSERT_TRUE(master.UnmountLocalDiskSegment(first));
    auto remaining = master.GetReplicaListForAdmin("regional-key", TenantId::Default());
    ASSERT_TRUE(remaining);
    ASSERT_EQ(remaining->replicas.size(), 1);
    EXPECT_EQ(remaining->replicas[0].get_local_disk_descriptor().client_id, second);
    EXPECT_EQ(remaining->replicas[0].get_local_disk_descriptor().transport_endpoint,
              b.transport_endpoint);
    EXPECT_FALSE(master.NotifyOffloadSuccess(first, {task}, {a}));
}

TEST(DurableProviderRpcTest, ScopedRegistrationRoundtripAndUnmount) {
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    WrappedMasterService service(config);
    coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
    RegisterRpcService(server, service);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    const auto id = generate_uuid();
    MasterClient client(id);
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server.port())),
              ErrorCode::OK);
    auto before_mount =
        client.RegisterDurableDeleteProvider(id, Scope(), "127.0.0.1:12345");
    ASSERT_FALSE(before_mount);
    EXPECT_EQ(before_mount.error(), ErrorCode::SEGMENT_NOT_FOUND);
    ASSERT_TRUE(client.MountLocalDiskSegment(id, true));
    auto unsupported_v3 =
        client.RegisterDurableDeleteProvider(id, Scope(), "127.0.0.1:12345");
    ASSERT_FALSE(unsupported_v3);
    EXPECT_EQ(unsupported_v3.error(), ErrorCode::NOT_SUPPORTED);
    auto legacy_scope = Scope();
    legacy_scope.protocol_version = 2;
    std::atomic<int> successes{0};
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            if (client.RegisterDurableDeleteProvider(id, legacy_scope,
                                                     "127.0.0.1:12345"))
                ++successes;
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(successes, 4);
    auto changed = legacy_scope;
    changed.bucket = "other-bucket";
    auto conflict =
        client.RegisterDurableDeleteProvider(id, changed, "127.0.0.1:12345");
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(client.RegisterDurableDeleteProvider(id, legacy_scope,
                                                      "127.0.0.1:12346"));
    ASSERT_TRUE(client.UnmountLocalDiskSegment(id));
    EXPECT_FALSE(client.RegisterDurableDeleteProvider(id, legacy_scope,
                                                      "127.0.0.1:12345"));
    ASSERT_TRUE(client.MountLocalDiskSegment(id, true));
    // Unmount really removed the old volatile registration.
    EXPECT_TRUE(
        client.RegisterDurableDeleteProvider(id, changed, "127.0.0.1:12345"));
}

TEST(DurableProviderRpcTest, MissingCapabilityRpcDoesNotBreakOrdinaryMount) {
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    WrappedMasterService service(config);
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    // Model a Master predating capability advertisement.
    server.register_handler<&WrappedMasterService::ServiceReady>(&service);
    server.register_handler<&WrappedMasterService::MountLocalDiskSegment>(
        &service);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    const auto id = generate_uuid();
    MasterClient client(id);
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server.port())),
              ErrorCode::OK);
    EXPECT_FALSE(
        client.RegisterDurableDeleteProvider(id, Scope(), "127.0.0.1:12345"));
    EXPECT_TRUE(client.MountLocalDiskSegment(id, true));
}
TEST(DurableProviderRpcTest,
     CoordinatorOverlapsProviderGraceAndDrainsFailures) {
    char directory[] = "/tmp/mooncake-delete-fanout-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{directory};
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.client_live_ttl_sec = 120;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    config.durable_delete_journal_path = std::string(directory) + "/journal";
    WrappedMasterService master(config);
    std::atomic<int> fenced{0}, executed{0};
    std::atomic<bool> fail{true};
    std::vector<std::unique_ptr<DurableDeleteRpcHandler>> handlers;
    std::vector<std::unique_ptr<coro_rpc::coro_rpc_server>> providers;
    for (int i = 0; i < 3; ++i) {
        auto handler = std::make_unique<DurableDeleteRpcHandler>();
        handler->SetFenceExecutor([&](const DurableDeleteCommand& command) {
            auto valid = master.ValidateDurableDeleteAssignment(command);
            if (valid) ++fenced;
            return valid;
        });
        handler->SetExecutor(
            [&, i](const DurableDeleteCommand& command)
                -> tl::expected<DurableDeleteReceipt, ErrorCode> {
                ++executed;
                // Sequential fanout reaches its first executor with only one
                // fence installed; concurrent fanout installs all three first.
                const auto result =
                    fenced.load() != 3
                        ? ErrorCode::INTERNAL_ERROR
                        : (fail && i == 0 ? ErrorCode::DFS_PERMISSION_DENIED
                                          : ErrorCode::OK);
                return DurableDeleteReceipt{
                    command.provider_id, command.operation_id,
                    command.assignment_id, command.storage_namespace.Identity(),
                    result};
            });
        auto provider =
            std::make_unique<coro_rpc::coro_rpc_server>(1, 0, "127.0.0.1");
        provider->register_handler<&DurableDeleteRpcHandler::Execute>(
            handler.get());
        provider->async_start();
        ASSERT_FALSE(provider->get_errc());
        const auto endpoint = "127.0.0.1:" + std::to_string(provider->port());
        const auto id = generate_uuid();
        ASSERT_TRUE(master.MountLocalDiskSegment(id, true));
        ASSERT_TRUE(
            master.RegisterDurableDeleteProvider(id, Scope(), endpoint));
        StorageObjectMetadata metadata;
        metadata.data_size = 1024;
        metadata.transport_endpoint = endpoint;
        OffloadTaskItem task{
            .tenant_id = "default", .key = "fanout-key", .size = 1024};
        ASSERT_TRUE(master.NotifyOffloadSuccess(id, {task}, {metadata}));
        handlers.push_back(std::move(handler));
        providers.push_back(std::move(provider));
    }
    coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
    RegisterRpcService(server, master);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server.port())),
              ErrorCode::OK);
    auto failed = client.RemoveDurable("fanout-key");
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::DFS_PERMISSION_DENIED);
    EXPECT_EQ(executed.load(), 3);
    EXPECT_TRUE(master.GetReplicaListForAdmin("fanout-key"));
    fenced = 0;
    executed = 0;
    fail = false;
    EXPECT_TRUE(client.RemoveDurable("fanout-key"));
    EXPECT_EQ(executed.load(), 3);
    EXPECT_FALSE(master.GetReplicaListForAdmin("fanout-key"));
    for (auto& handler : handlers) handler->StopAndDrain();
}
}  // namespace mooncake::test

namespace mooncake::test {
TEST(DurableProviderRpcTest, ReadFencePropagatesCloudFailureAndDeletion) {
    char directory[] = "/tmp/mooncake-read-fence-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{directory};
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 60000;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    config.durable_delete_journal_path = std::string(directory) + "/journal";
    WrappedMasterService master(config);
    DurableDeleteRpcHandler handler;
    std::atomic<ErrorCode> outcome{ErrorCode::OK};
    const auto id = generate_uuid();
    handler.SetReadExecutor([&](const DurableReadCommand& command)
                                -> tl::expected<void, ErrorCode> {
        EXPECT_EQ(command.provider_id, id);
        EXPECT_EQ(command.tenant_id, "default");
        EXPECT_EQ(command.key, "warm-key");
        EXPECT_EQ(command.storage_namespace, Scope());
        if (outcome != ErrorCode::OK)
            return tl::make_unexpected(outcome.load());
        return {};
    });
    coro_rpc::coro_rpc_server provider(1, 0, "127.0.0.1");
    provider.register_handler<&DurableDeleteRpcHandler::CheckRead>(&handler);
    provider.async_start();
    ASSERT_FALSE(provider.get_errc());
    const auto endpoint = "127.0.0.1:" + std::to_string(provider.port());
    ASSERT_TRUE(master.MountLocalDiskSegment(id, true));
    ASSERT_TRUE(master.RegisterDurableDeleteProvider(id, Scope(), endpoint));
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = endpoint;
    ASSERT_TRUE(master.NotifyOffloadSuccess(
        id,
        {OffloadTaskItem{
            .tenant_id = "default", .key = "warm-key", .size = 1024}},
        {metadata}));
    coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
    RegisterRpcService(server, master);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server.port())),
              ErrorCode::OK);
    auto initial = client.GetReplicaList("warm-key");
    ASSERT_TRUE(initial);
    EXPECT_EQ(initial->lease_ttl_ms, kMaxDurableReadLeaseMs);
    auto initial_batch = client.BatchGetReplicaList({"warm-key"});
    ASSERT_EQ(initial_batch.size(), 1);
    ASSERT_TRUE(initial_batch[0]);
    EXPECT_EQ(initial_batch[0]->lease_ttl_ms, kMaxDurableReadLeaseMs);
    for (const auto error :
         {ErrorCode::DFS_PERMISSION_DENIED, ErrorCode::OBJECT_NOT_FOUND}) {
        outcome = error;
        auto single = client.GetReplicaList("warm-key");
        ASSERT_FALSE(single);
        EXPECT_EQ(single.error(), error);
        auto batch = client.BatchGetReplicaList({"warm-key"});
        ASSERT_EQ(batch.size(), 1);
        ASSERT_FALSE(batch[0]);
        EXPECT_EQ(batch[0].error(), error);
        EXPECT_TRUE(master.GetReplicaListForAdmin("warm-key"));
    }
    handler.StopAndDrain();
    EXPECT_FALSE(client.GetReplicaList("warm-key"));
}

TEST(DurableProviderRpcTest, ProviderShutdownDrainsExecutorAndClosesAdmission) {
    DurableDeleteRpcHandler handler;
    std::promise<void> entered, release;
    auto permit = release.get_future().share();
    handler.SetExecutor([&](const DurableDeleteCommand& command)
                            -> tl::expected<DurableDeleteReceipt, ErrorCode> {
        entered.set_value();
        permit.wait();
        return DurableDeleteReceipt{
            command.provider_id, command.operation_id, command.assignment_id,
            command.storage_namespace.Identity(), ErrorCode::OK};
    });
    auto execution = std::async(std::launch::async, [&] {
        return async_simple::coro::syncAwait(handler.Execute({}));
    });
    entered.get_future().wait();
    auto shutdown =
        std::async(std::launch::async, [&] { handler.StopAndDrain(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    release.set_value();
    EXPECT_TRUE(execution.get());
    shutdown.get();
    EXPECT_FALSE(async_simple::coro::syncAwait(handler.Execute({})));
}

TEST(DurableProviderRpcTest, VersionThreeRequiresSuccessfulFence) {
    DurableDeleteRpcHandler handler;
    std::atomic<int> deletes{0};
    handler.SetExecutor([&](const DurableDeleteCommand& command)
                            -> tl::expected<DurableDeleteReceipt, ErrorCode> {
        ++deletes;
        return DurableDeleteReceipt{
            command.provider_id, command.operation_id, command.assignment_id,
            command.storage_namespace.Identity(), ErrorCode::OK};
    });
    DurableDeleteCommand command{};
    command.storage_namespace = Scope();
    auto missing = async_simple::coro::syncAwait(handler.Execute(command));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::NOT_SUPPORTED);
    handler.SetFenceExecutor(
        [](const DurableDeleteCommand&) -> tl::expected<void, ErrorCode> {
            return tl::make_unexpected(ErrorCode::DFS_PERMISSION_DENIED);
        });
    auto failed = async_simple::coro::syncAwait(handler.Execute(command));
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::DFS_PERMISSION_DENIED);
    EXPECT_EQ(deletes.load(), 0);
    handler.StopAndDrain();
}

TEST(DurableProviderRpcTest, VersionThreeTimerAllowsReadsAndDrainsOnShutdown) {
    DurableDeleteRpcHandler handler;
    std::promise<std::chrono::steady_clock::time_point> fenced;
    std::atomic<int> deletes{0};
    handler.SetFenceExecutor(
        [&](const DurableDeleteCommand&) -> tl::expected<void, ErrorCode> {
            fenced.set_value(std::chrono::steady_clock::now());
            return {};
        });
    handler.SetReadExecutor(
        [](const DurableReadCommand&) -> tl::expected<void, ErrorCode> {
            return {};
        });
    handler.SetExecutor([&](const DurableDeleteCommand& command)
                            -> tl::expected<DurableDeleteReceipt, ErrorCode> {
        ++deletes;
        return DurableDeleteReceipt{
            command.provider_id, command.operation_id, command.assignment_id,
            command.storage_namespace.Identity(), ErrorCode::OK};
    });
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    server.register_handler<&DurableDeleteRpcHandler::Execute,
                            &DurableDeleteRpcHandler::CheckRead>(&handler);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    const auto endpoint = "127.0.0.1:" + std::to_string(server.port());
    DurableDeleteCommand command{};
    command.storage_namespace = Scope();
    auto execution = std::async(std::launch::async, [&] {
        return async_simple::coro::syncAwait(
            RequestDurableProviderDelete(endpoint, command));
    });
    auto fence_time = fenced.get_future();
    ASSERT_EQ(fence_time.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    const auto started = fence_time.get();
    DurableReadCommand read{};
    read.provider_endpoint = endpoint;
    EXPECT_TRUE(
        async_simple::coro::syncAwait(RequestDurableProviderRead(read)));
    EXPECT_EQ(deletes.load(), 0);
    auto shutdown =
        std::async(std::launch::async, [&] { handler.StopAndDrain(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    EXPECT_TRUE(execution.get());
    EXPECT_GE(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(kMaxDurableReadLeaseMs));
    shutdown.get();
    EXPECT_EQ(deletes.load(), 1);
    EXPECT_FALSE(async_simple::coro::syncAwait(handler.Execute(command)));
}

TEST(DurableProviderRpcTest,
     CoordinatorRejectsWrongReceiptAndRetainsFailedMetadata) {
    char directory[] = "/tmp/mooncake-delete-receipt-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{directory};
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    // This receipt-only provider has no heartbeat loop; three delete attempts
    // each wait out the protocol's read lease.
    config.client_live_ttl_sec = 60;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    config.durable_delete_journal_path = std::string(directory) + "/journal";
    WrappedMasterService master(config);
    DurableDeleteRpcHandler handler;
    std::atomic<int> mode{0};
    handler.SetExecutor([&](const DurableDeleteCommand& command)
                            -> tl::expected<DurableDeleteReceipt, ErrorCode> {
        auto valid = master.ValidateDurableDeleteAssignment(command);
        if (!valid) return tl::make_unexpected(valid.error());
        return DurableDeleteReceipt{
            command.provider_id, command.operation_id,
            mode == 0 ? generate_uuid() : command.assignment_id,
            command.storage_namespace.Identity(),
            mode == 1 ? ErrorCode::DFS_PERMISSION_DENIED : ErrorCode::OK};
    });
    handler.SetFenceExecutor([&](const DurableDeleteCommand& command) {
        return master.ValidateDurableDeleteAssignment(command);
    });
    coro_rpc::coro_rpc_server provider_server(1, 0, "127.0.0.1");
    provider_server.register_handler<&DurableDeleteRpcHandler::Execute>(
        &handler);
    provider_server.async_start();
    ASSERT_FALSE(provider_server.get_errc());
    const auto endpoint = "127.0.0.1:" + std::to_string(provider_server.port());
    const auto id = generate_uuid();
    ASSERT_TRUE(master.MountLocalDiskSegment(id, true));
    ASSERT_TRUE(master.RegisterDurableDeleteProvider(id, Scope(), endpoint));
    StorageObjectMetadata metadata;
    metadata.data_size = 1024;
    metadata.transport_endpoint = endpoint;
    OffloadTaskItem task{
        .tenant_id = "default", .key = "receipt-key", .size = 1024};
    ASSERT_TRUE(master.NotifyOffloadSuccess(id, {task}, {metadata}));
    coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
    RegisterRpcService(server, master);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect("127.0.0.1:" + std::to_string(server.port())),
              ErrorCode::OK);
    auto wrong = client.RemoveDurable("receipt-key");
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(master.GetReplicaListForAdmin("receipt-key"));
    mode = 1;
    auto failed = client.RemoveDurable("receipt-key");
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::DFS_PERMISSION_DENIED);
    EXPECT_TRUE(master.GetReplicaListForAdmin("receipt-key"));
    mode = 2;
    EXPECT_TRUE(client.RemoveDurable("receipt-key"));
    EXPECT_FALSE(master.GetReplicaListForAdmin("receipt-key"));
    EXPECT_TRUE(client.RemoveDurable("receipt-key"));
    handler.StopAndDrain();
}
}  // namespace mooncake::test

namespace mooncake::test {
TEST(DurableProviderRpcTest,
     ReadFenceFailsOverOnlyWithinScopeAndOnAvailability) {
    char directory[] = "/tmp/mooncake-read-failover-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{directory};
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.enable_metric_reporting = false;
    config.durable_delete_journal_path = std::string(directory) + "/journal";
    WrappedMasterService master(config);
    std::vector<UUID> ids{generate_uuid(), generate_uuid()};
    std::sort(ids.begin(), ids.end());
    std::atomic<ErrorCode> first_result{ErrorCode::DFS_PERMISSION_DENIED};
    std::atomic<int> peer_checks{0};
    DurableDeleteRpcHandler first, peer;
    first.SetReadExecutor(
        [&](const DurableReadCommand&) -> tl::expected<void, ErrorCode> {
            return tl::make_unexpected(first_result.load());
        });
    peer.SetReadExecutor([&](const DurableReadCommand& command)
                             -> tl::expected<void, ErrorCode> {
        ++peer_checks;
        EXPECT_EQ(command.storage_namespace, Scope());
        return {};
    });
    coro_rpc::coro_rpc_server a(1, 0, "127.0.0.1"), b(1, 0, "127.0.0.1");
    a.register_handler<&DurableDeleteRpcHandler::CheckRead>(&first);
    b.register_handler<&DurableDeleteRpcHandler::CheckRead>(&peer);
    a.async_start();
    b.async_start();
    ASSERT_FALSE(a.get_errc());
    ASSERT_FALSE(b.get_errc());
    for (size_t i = 0; i < ids.size(); ++i) {
        auto endpoint =
            "127.0.0.1:" + std::to_string(i == 0 ? a.port() : b.port());
        ASSERT_TRUE(master.MountLocalDiskSegment(ids[i], true));
        ASSERT_TRUE(
            master.RegisterDurableDeleteProvider(ids[i], Scope(), endpoint));
        StorageObjectMetadata metadata;
        metadata.data_size = 1024;
        metadata.transport_endpoint = endpoint;
        ASSERT_TRUE(master.NotifyOffloadSuccess(
            ids[i],
            {OffloadTaskItem{
                .tenant_id = "default", .key = "failover-key", .size = 1024}},
            {metadata}));
    }
    // An unrelated v2 namespace must not receive the v3 CheckRead RPC.
    auto legacy_id = generate_uuid();
    auto legacy = Scope();
    legacy.protocol_version = 2;
    legacy.bucket = "legacy-bucket";
    ASSERT_TRUE(master.MountLocalDiskSegment(legacy_id, true));
    ASSERT_TRUE(
        master.RegisterDurableDeleteProvider(legacy_id, legacy, "127.0.0.1:1"));
    auto denied =
        async_simple::coro::syncAwait(master.GetReplicaList("failover-key"));
    ASSERT_FALSE(denied);
    EXPECT_EQ(denied.error(), ErrorCode::DFS_PERMISSION_DENIED);
    EXPECT_EQ(peer_checks, 0);
    first_result = ErrorCode::OBJECT_NOT_FOUND;
    EXPECT_FALSE(
        async_simple::coro::syncAwait(master.GetReplicaList("failover-key")));
    EXPECT_EQ(peer_checks, 0);
    first_result = ErrorCode::DFS_SERVICE_UNAVAILABLE;
    EXPECT_TRUE(
        async_simple::coro::syncAwait(master.GetReplicaList("failover-key")));
    EXPECT_EQ(peer_checks, 1);
    a.stop();
    EXPECT_TRUE(
        async_simple::coro::syncAwait(master.GetReplicaList("failover-key")));
    EXPECT_EQ(peer_checks, 2);
    first.StopAndDrain();
    peer.StopAndDrain();
}
}  // namespace mooncake::test
