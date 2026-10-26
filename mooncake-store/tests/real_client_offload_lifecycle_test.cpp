#include <glog/logging.h>
#include <gtest/gtest.h>

// csignal must precede coro_http_client.hpp: the bundled ylt's coro_io.hpp
// calls std::signal without including <csignal> itself.
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <ylt/coro_http/coro_http_client.hpp>

#include "real_client.h"
#include "test_server_helpers.h"
#include "utils.h"

// Lifecycle of an offload-enabled provider against the Master's co-located
// HTTP metadata server (mint#1776).
namespace mooncake::test {
namespace {

class ScopedEnv {
   public:
    ScopedEnv(const char* name, const std::string& value) : name_(name) {
        if (const char* old = std::getenv(name)) old_value_ = old;
        setenv(name, value.c_str(), 1);
    }
    ~ScopedEnv() {
        if (old_value_) {
            setenv(name_, old_value_->c_str(), 1);
        } else {
            unsetenv(name_);
        }
    }

   private:
    const char* name_;
    std::optional<std::string> old_value_;
};

int RpcMetaStatus(const std::string& metadata_url, const std::string& host) {
    coro_http::coro_http_client client;
    // Same percent-encoding the transfer engine's HTTP plugin uses.
    std::string encoded;
    for (char c : "mooncake/rpc_meta/" + host) {
        if (c == '/') {
            encoded += "%2F";
        } else if (c == ':') {
            encoded += "%3A";
        } else {
            encoded += c;
        }
    }
    return client.get(metadata_url + "?key=" + encoded).status;
}

class RealClientOffloadLifecycleTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("RealClientOffloadLifecycleTest");
        FLAGS_logtostderr = true;
        offload_dir_ = std::filesystem::temp_directory_path() /
                       ("mc-offload-lifecycle-" + std::to_string(getpid()));
        std::filesystem::create_directories(offload_dir_);
        path_.emplace("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
                      offload_dir_.string());
        ASSERT_TRUE(master_.Start(
            InProcMasterConfigBuilder().set_enable_offload(true).build()));
        ASSERT_FALSE(master_.metadata_url().empty());
    }

    void TearDown() override {
        master_.Stop();
        std::filesystem::remove_all(offload_dir_);
        google::ShutdownGoogleLogging();
    }

    tl::expected<void, ErrorCode> SetupOffloadProvider(
        const std::shared_ptr<RealClient>& client, const std::string& host) {
        return client->setup_internal(
            host, master_.metadata_url(), /*global_segment_size=*/16 << 20,
            /*local_buffer_size=*/16 << 20, "tcp", "", master_.master_address(),
            nullptr, "",
            /*local_rpc_port=*/50052, /*enable_ssd_offload=*/true,
            /*start_offload_rpc_server=*/true, /*ssd_offload_path=*/"",
            /*tenant_id=*/"default", /*enable_client_http_server=*/false,
            /*client_http_port=*/0);
    }

    std::string Host() const {
        return "127.0.0.1:" + std::to_string(getFreeTcpPort());
    }

    testing::InProcMaster master_;
    std::filesystem::path offload_dir_;
    ScopedEnv backend_{"MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
                       "file_per_key_storage_backend"};
    std::optional<ScopedEnv> path_;
};

// FileStorage holds its own shared_ptr<Client>. Teardown must release it so
// ~TransferEngine runs and removes rpc_meta; otherwise a restart under the
// same host is rejected as a duplicate.
TEST_F(RealClientOffloadLifecycleTest, TearDownRemovesRpcMetaAndAllowsRestart) {
    const std::string host = Host();

    auto first = RealClient::create();
    auto setup = SetupOffloadProvider(first, host);
    ASSERT_TRUE(setup.has_value()) << toString(setup.error());
    EXPECT_EQ(RpcMetaStatus(master_.metadata_url(), host), 200);

    EXPECT_EQ(first->tearDownAll(), 0);
    EXPECT_EQ(RpcMetaStatus(master_.metadata_url(), host), 404);

    auto restarted = RealClient::create();
    setup = SetupOffloadProvider(restarted, host);
    ASSERT_TRUE(setup.has_value()) << toString(setup.error());
    EXPECT_EQ(RpcMetaStatus(master_.metadata_url(), host), 200);
    EXPECT_EQ(restarted->tearDownAll(), 0);
}

// A start rejected as a duplicate never registered rpc_meta, so its rollback
// must not delete the entry that belongs to the live owner.
TEST_F(RealClientOffloadLifecycleTest, FailedDuplicateKeepsOwnersRpcMeta) {
    const std::string host = Host();

    auto owner = RealClient::create();
    auto setup = SetupOffloadProvider(owner, host);
    ASSERT_TRUE(setup.has_value()) << toString(setup.error());

    {
        auto duplicate = RealClient::create();
        EXPECT_FALSE(SetupOffloadProvider(duplicate, host).has_value());
        duplicate->tearDownAll();
    }
    EXPECT_EQ(RpcMetaStatus(master_.metadata_url(), host), 200);
    EXPECT_EQ(owner->tearDownAll(), 0);
    EXPECT_EQ(RpcMetaStatus(master_.metadata_url(), host), 404);
}

}  // namespace
}  // namespace mooncake::test
