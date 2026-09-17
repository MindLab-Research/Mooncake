#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <filesystem>
#include <unistd.h>
#include <dlfcn.h>
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include "utils/s3_helper.h"
#include "file_storage.h"
#include "test_server_helpers.h"
#include <openssl/sha.h>
#include "real_client.h"
#include "store_c.h"
#include "storage/distributed/s3_object_storage_adapter.h"
#include "ha/snapshot/object/backends/s3/s3_snapshot_object_store.h"

using namespace mooncake;
extern char** environ;

static void Require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}

static std::string Hash(const std::vector<char>& data) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           digest);
    std::ostringstream result;
    for (unsigned char c : digest)
        result << std::hex << std::setw(2) << std::setfill('0')
               << static_cast<int>(c);
    return result.str();
}

static bool IsWriterRaceFixture(const S3ObjectStorageConfig& config) {
    return config.endpoint.starts_with("http://127.0.0.1:") ||
           (config.endpoint.starts_with("https://") &&
            config.endpoint.ends_with(".aliyuncs.com") &&
            config.key_prefix.starts_with(
                "ctgg-native-oss-delete-20260914/writer-race/"));
}

static void NativeDeleteTest(const std::string& phase = "roundtrip") {
    auto config = S3ObjectStorageConfig::FromEnvironment();
    const bool regional = phase != "roundtrip" && phase != "warm-peer";
    const bool persistence_fixture = getenv("CTGG_MINT_PERSISTENCE_TEST");
    const bool crash_phase =
        phase == "crash-lease" || phase == "crash-physical";
    const bool resume_phase =
        phase == "resume-lease" || phase == "resume-physical";
    const bool writer_phase = phase == "writer-busy" || phase == "writer-ready";
    Require(phase == "roundtrip" || phase == "delete" || phase == "cold" ||
                phase == "observe" || crash_phase || resume_phase ||
                writer_phase || phase == "warm-peer",
            "native delete phase");
    Require(
        config.endpoint.starts_with("http://127.0.0.1:") ||
            (regional && config.endpoint.starts_with("https://") &&
             config.endpoint.ends_with(".aliyuncs.com") &&
             config.key_prefix.starts_with("ctgg-native-oss-delete-20260914/")),
        "native deletion needs loopback or an isolated OSS fixture prefix");
    config.key_prefix += "/native-delete";
    setenv("MOONCAKE_S3_KEY_PREFIX", config.key_prefix.c_str(), 1);
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "s3_object_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1", 1);
    setenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "4194304", 1);
    char temporary[] = "/tmp/mooncake-native-delete-XXXXXX";
    std::string directory;
    if (regional) {
        const char* configured = getenv("CTGG_NATIVE_DELETE_STATE_DIR");
        Require(configured != nullptr, "regional state directory is required");
        directory = configured;
        Require(directory.starts_with("/opt/mindlab/ctgg-native-oss-delete-") &&
                    directory.find("..") == std::string::npos,
                "isolated regional state directory");
        std::filesystem::create_directories(directory);
    } else {
        Require(mkdtemp(temporary) != nullptr,
                "isolated native deletion directory");
        directory = temporary;
    }
    struct Cleanup {
        std::filesystem::path path;
        bool remove;
        ~Cleanup() {
            if (remove) std::filesystem::remove_all(path);
        }
    } cleanup{directory, !regional};
    const auto store_path = directory + "/store";
    std::filesystem::create_directories(store_path);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", store_path.c_str(), 1);
    S3ObjectStorageAdapter inspector(config);
    Require(inspector.Init().has_value(), "independent cloud inspector");
    // This inspector bypasses logical tombstone filtering. It verifies actual
    // physical cloud bytes while the provider uses the normal Store backend.
    S3Helper physical(config.endpoint, config.bucket, config.region);
    auto physical_key = [&](const std::string& key) {
        std::ostringstream hex;
        for (unsigned char c : TenantId::Default().MakeScopedKey(key))
            hex << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(c);
        return config.key_prefix + "/objects/" + hex.str();
    };
    auto fault_hits = reinterpret_cast<unsigned (*)()>(
        dlsym(RTLD_DEFAULT, "ctgg_delete_fault_hits"));
    auto fault_enable = reinterpret_cast<void (*)(int)>(
        dlsym(RTLD_DEFAULT, "ctgg_delete_fault_enable"));
    if (regional && phase == "delete")
        Require(fault_hits != nullptr && fault_enable != nullptr,
                "transport fault fixture must be loaded");
    const int first = phase == "cold" || phase == "observe" ? 1 : 0;
    const int end = phase == "delete" || crash_phase || resume_phase ||
                            writer_phase || phase == "warm-peer"
                        ? 1
                        : 2;
    for (int cold = first; cold < end; ++cold) {
        std::optional<uint64_t> expected_grace;
        if (resume_phase) {
            DurableDeleteJournal probe(directory + "/journal");
            const auto records = probe.Records();
            Require(records.size() == 1 && !records[0].completed &&
                        records[0].reader_grace_ms.has_value(),
                    "pending v3 journal must survive process crash");
            expected_grace = records[0].reader_grace_ms;
        }
        WrappedMasterServiceConfig master_config;
        master_config.default_kv_lease_ttl =
            regional || phase == "warm-peer" || persistence_fixture ? 5000 : 0;
        master_config.enable_offload = true;
        master_config.enable_metric_reporting = false;
        master_config.root_fs_dir = directory;
        master_config.durable_delete_journal_path =
            std::string(directory) + "/journal";
        const auto recovery_start = std::chrono::steady_clock::now();
        WrappedMasterService master(master_config);
        coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
        RegisterRpcService(server, master);
        server.async_start();
        Require(!server.get_errc(), "native Master RPC start");
        const auto address = "127.0.0.1:" + std::to_string(server.port());
        auto provider = RealClient::create();
        auto setup = provider->setup_internal(
            "127.0.0.1:" + std::to_string(getFreeTcpPort()), "P2PHANDSHAKE",
            64UL << 20, 16UL << 20, "tcp", "", address, nullptr, "",
            getFreeTcpPort(), true, true, store_path, "default");
        Require(setup.has_value(), "actual native provider setup");
        MasterClient client(generate_uuid());
        Require(client.Connect(address) == ErrorCode::OK,
                "native delete client");
        struct CClient {
            mooncake_store_t handle = mooncake_store_create();
            ~CClient() { mooncake_store_destroy(handle); }
        } c_client;
        const auto c_endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        Require(c_client.handle &&
                    mooncake_store_setup(c_client.handle, c_endpoint.c_str(),
                                         "P2PHANDSHAKE", 0, 16UL << 20, "tcp",
                                         "", address.c_str()) == 0,
                "durable deletion C ABI setup");
        auto durable_remove =
            [&](const std::string& key) -> tl::expected<void, ErrorCode> {
            int rc;
            if (const char* probe = getenv("CTGG_DURABLE_RUST_PROBE")) {
                int channel[2];
                Require(pipe2(channel, O_CLOEXEC) == 0,
                        "Rust probe output pipe");
                posix_spawn_file_actions_t actions;
                posix_spawn_file_actions_init(&actions);
                posix_spawn_file_actions_adddup2(&actions, channel[1],
                                                 STDOUT_FILENO);
                posix_spawn_file_actions_addclose(&actions, channel[0]);
                posix_spawn_file_actions_addclose(&actions, channel[1]);
                const auto endpoint =
                    "127.0.0.1:" + std::to_string(getFreeTcpPort());
                char* arguments[] = {const_cast<char*>(probe),
                                     const_cast<char*>(address.c_str()),
                                     const_cast<char*>(endpoint.c_str()),
                                     const_cast<char*>(key.c_str()), nullptr};
                pid_t process;
                const int spawned = posix_spawn(&process, probe, &actions,
                                                nullptr, arguments, environ);
                posix_spawn_file_actions_destroy(&actions);
                close(channel[1]);
                if (spawned != 0) {
                    close(channel[0]);
                    throw std::runtime_error("Rust probe spawn failed");
                }
                std::string output;
                char buffer[4096];
                ssize_t bytes;
                while ((bytes = read(channel[0], buffer, sizeof(buffer))) > 0)
                    output.append(buffer, bytes);
                close(channel[0]);
                int status;
                Require(waitpid(process, &status, 0) == process &&
                            WIFEXITED(status) && WEXITSTATUS(status) == 0,
                        "Rust probe exits normally");
                const auto marker = output.rfind("DURABLE_DELETE_RC=");
                Require(marker != std::string::npos,
                        "Rust probe result present");
                rc = std::stoi(output.substr(marker + 18));
                std::cout << "MINT_RUST_DURABLE_DELETE_RESULT rc=" << rc
                          << std::endl;
            } else {
                rc =
                    mooncake_store_remove_durable(c_client.handle, key.c_str());
            }
            if (rc != 0) return tl::make_unexpected(static_cast<ErrorCode>(rc));
            return {};
        };
        if (phase == "warm-peer") {
            for (const auto& key : {"race", "independent"}) {
                Require(provider->put(key, std::vector<char>(4096, 'w')) == 0,
                        "warm fixture native Put");
                bool durable = false;
                for (int attempt = 0; attempt < 600; ++attempt) {
                    auto query = provider->batch_query({key});
                    if (query.size() == 1 && query[0])
                        for (const auto& replica : query[0]->replicas)
                            durable |=
                                replica.is_local_disk_replica() &&
                                replica.status == ReplicaStatus::COMPLETE;
                    if (durable) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                Require(durable, "warm fixture reaches OSS");
                auto metadata = master.GetReplicaListForAdmin(key);
                bool memory = false;
                if (metadata)
                    for (const auto& replica : metadata->replicas)
                        memory |= replica.is_memory_replica();
                Require(memory, "fixture has a real MEMORY replica");
                auto read = provider->get_buffer(key);
                Require(read && read->size() == 4096,
                        "warm read succeeds before cloud fence");
            }
            const auto scoped = TenantId::Default().MakeScopedKey("race");
            Require(inspector.MarkDeletion(scoped).has_value() &&
                        inspector.Delete(scoped).has_value(),
                    "independent cloud adapter publishes delete intent and "
                    "removes bytes");
            Require(
                master.GetReplicaListForAdmin("race").has_value(),
                "peer Master is still warm and did not receive local deletion");
            Require(!provider->get_buffer("race"),
                    "warm MEMORY read must honor cloud intent");
            auto batch = provider->batch_query({"race", "independent"});
            Require(batch.size() == 2 && !batch[0] &&
                        batch[0].error() == ErrorCode::OBJECT_NOT_FOUND &&
                        batch[1],
                    "batch read rejects deleted key and preserves independent "
                    "lifecycle");
            Require(provider->get_buffer("independent") != nullptr,
                    "independent key remains natively readable");
            std::cout << "NATIVE_WARM_READ_FENCE_PASS" << std::endl;
            provider.reset();
            continue;
        }
        if (writer_phase) {
            const std::string key = "race";
            if (phase == "writer-busy") {
                bool discovered = false;
                for (int attempt = 0; attempt < 600; ++attempt) {
                    discovered = master.GetReplicaListForAdmin(key).has_value();
                    if (discovered) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                Require(discovered,
                        "native provider discovers old remote object");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5100));
            auto deleted = durable_remove(key);
            auto raw = physical.ObjectExists(physical_key(key));
            if (phase == "writer-busy") {
                Require(
                    !deleted && deleted.error() ==
                                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                    "native provider refuses deletion for remote admission");
                Require(master.GetReplicaListForAdmin(key).has_value() && raw &&
                            *raw,
                        "blocked native delete retains metadata and physical "
                        "bytes");
            } else {
                Require(
                    deleted.has_value(),
                    "replayed native deletion completes after writer drain");
                Require(!master.GetReplicaListForAdmin(key) && raw && !*raw,
                        "completed native delete has no metadata or physical "
                        "bytes");
                Require(durable_remove(key).has_value(),
                        "completed native journal supports idempotent retry");
            }
            std::cout << "NATIVE_WRITER_DRAIN_PASS phase=" << phase
                      << std::endl;
            provider.reset();
            continue;
        }
        if (crash_phase || resume_phase) {
            const std::string key = "pending-crash";
            if (crash_phase) {
                std::vector<char> bytes(4096, 'p');
                Require(provider->put(key, bytes) == 0, "crash fixture Put");
                bool durable = false;
                for (int attempt = 0; attempt < 600; ++attempt) {
                    auto query = provider->batch_query({key});
                    if (query.size() == 1 && query[0])
                        for (const auto& replica : query[0]->replicas)
                            durable |=
                                replica.is_local_disk_replica() &&
                                replica.status == ReplicaStatus::COMPLETE;
                    if (durable) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                Require(durable, "crash fixture durable offload");
                if (phase == "crash-lease") {
                    auto fenced = durable_remove(key);
                    Require(
                        !fenced &&
                            fenced.error() == ErrorCode::OBJECT_HAS_LEASE,
                        "crash occurs after durable fence before lease expiry");
                    std::cout << "NATIVE_PENDING_LEASE_CRASH_ARMED"
                              << std::endl;
                    _exit(73);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5100));
                auto crash = reinterpret_cast<void (*)(int)>(
                    dlsym(RTLD_DEFAULT, "ctgg_crash_after_delete_enable"));
                Require(crash != nullptr, "physical-delete crash hook loaded");
                crash(1);
                std::cout << "NATIVE_PENDING_PHYSICAL_CRASH_ARMED" << std::endl;
                (void)durable_remove(key);
                Require(false,
                        "physical-delete crash hook did not terminate process");
            }
            Require(!master.GetReplicaListForAdmin(key),
                    "pending recovery has no metadata snapshot");
            bool finished = false;
            for (int attempt = 0; attempt < 600; ++attempt) {
                auto deleted = durable_remove(key);
                if (deleted) {
                    finished = true;
                    break;
                }
                Require(deleted.error() == ErrorCode::OBJECT_HAS_LEASE,
                        "pending retry must wait for recorded reader grace");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            const auto waited =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - recovery_start)
                    .count();
            Require(finished && waited >= static_cast<int64_t>(*expected_grace),
                    "recovery cannot acknowledge before recorded grace");
            auto raw = physical.ObjectExists(physical_key(key));
            Require(raw && !*raw && !master.GetReplicaListForAdmin(key),
                    "resumed deletion removes actual cloud bytes and metadata");
            Require(durable_remove(key).has_value(),
                    "resumed delete is idempotent");
            std::cout << "NATIVE_PENDING_RECOVERY_PASS phase=" << phase
                      << " grace_ms=" << *expected_grace
                      << " waited_ms=" << waited << std::endl;
            provider.reset();
            continue;
        }
        std::vector<std::string> keys{"single", "concurrent"};
        if (regional) {
            keys.push_back("delete-failure");
            keys.push_back("control");
        }
        for (const auto& key : keys) {
            if (!cold) {
                std::vector<char> bytes(4096, 'd');
                Require(provider->put(key, bytes) == 0, "native fixture Put");
                bool durable = false;
                for (int attempt = 0; attempt < (regional ? 600 : 100);
                     ++attempt) {
                    auto query = provider->batch_query({key});
                    if (query.size() == 1 && query[0]) {
                        for (const auto& replica : query[0]->replicas)
                            durable |=
                                replica.is_local_disk_replica() &&
                                replica.status == ReplicaStatus::COMPLETE;
                    }
                    if (durable) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                Require(durable, "native offload was acknowledged");
                auto before =
                    inspector.Exists(TenantId::Default().MakeScopedKey(key));
                Require(before && *before, "cloud fixture is present");
                if ((regional || persistence_fixture) && key != "control")
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(5100));
                if (key == "delete-failure") {
                    const auto before_hits = fault_hits();
                    fault_enable(1);
                    auto failed = durable_remove(key);
                    fault_enable(0);
                    Require(!failed && fault_hits() > before_hits,
                            "actual provider Delete reached failing transport");
                    Require(failed.error() != ErrorCode::RPC_TIMEOUT &&
                                failed.error() != ErrorCode::RPC_FAIL,
                            "provider must finish failed cloud deletion before "
                            "fault removal; outer RPC failure is inconclusive");
                    std::cout << "NATIVE_DELETE_TERMINAL_FAILURE_PASS error="
                              << toString(failed.error())
                              << " fault_hits=" << (fault_hits() - before_hits)
                              << std::endl;
                    Require(master.GetReplicaListForAdmin(key).has_value(),
                            "failed cloud delete retains Master metadata");
                    auto retained = physical.ObjectExists(physical_key(key));
                    Require(retained && *retained,
                            "failed cloud delete retains physical OSS object");
                    std::cout << "NATIVE_DELETE_FAILURE_RETAINS_BOTH_PASS"
                              << std::endl;
                }
                if (key == "control") {
                    // Retain a live object as a positive control for discovery
                    // and native reads in the fresh cold/peer process.
                } else if (key != "concurrent") {
                    auto deleted = durable_remove(key);
                    Require(deleted.has_value(),
                            deleted ? ""
                                    : "native deletion: " +
                                          toString(deleted.error()));
                } else {
                    std::atomic<int> successes{0};
                    std::vector<std::thread> workers;
                    for (int i = 0; i < 8; ++i)
                        workers.emplace_back([&] {
                            if (durable_remove(key)) ++successes;
                        });
                    for (auto& worker : workers) worker.join();
                    Require(successes == 8,
                            "all concurrent native deletes are idempotent");
                }
            }
            if (key == "control") {
                auto buffer = provider->get_buffer(key);
                Require(
                    buffer && buffer->size() == 4096 &&
                        std::string(static_cast<char*>(buffer->ptr()),
                                    buffer->size()) == std::string(4096, 'd'),
                    "fresh provider discovers and natively reads control");
                std::cout << "NATIVE_DELETE_CONTROL_READ_PASS phase=" << phase
                          << std::endl;
                continue;
            }
            auto metadata = client.ExistKey(key);
            Require(metadata && !*metadata, "Master metadata remains absent");
            auto cloud =
                inspector.Exists(TenantId::Default().MakeScopedKey(key));
            Require(cloud && !*cloud, "deleted object cannot be discovered");
            auto raw = physical.ObjectExists(physical_key(key));
            Require(raw && !*raw, "physical cloud object is absent");
            if (phase != "observe")
                Require(durable_remove(key).has_value(),
                        "completed deletion retries after restart");
            std::cout << "NATIVE_DELETE_KEY_PASS phase=" << phase
                      << " cold=" << cold << " key=" << key << std::endl;
        }
        provider.reset();
    }
    std::cout << "NATIVE_DELETE_ROUNDTRIP_PASS phase=" << phase << std::endl;
}

static void ProviderRegistrationTest() {
    auto object_config = S3ObjectStorageConfig::FromEnvironment();
    Require(object_config.endpoint.starts_with("http://127.0.0.1:"),
            "provider registration fixture is loopback-only");
    char path[] = "/tmp/mooncake-provider-registration-XXXXXX";
    Require(mkdtemp(path) != nullptr, "isolated provider directory");
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{path};
    const auto master_root = std::string(path) + "/master";
    std::filesystem::create_directories(master_root);
    WrappedMasterServiceConfig master_config;
    master_config.default_kv_lease_ttl = 5000;
    master_config.enable_offload = true;
    master_config.enable_metric_reporting = false;
    master_config.root_fs_dir = master_root;
    master_config.durable_delete_journal_path = master_root + "/journal";
    WrappedMasterService master(master_config);
    coro_rpc::coro_rpc_server server(2, 0, "127.0.0.1");
    RegisterRpcService(server, master);
    server.async_start();
    Require(!server.get_errc(), "isolated Master start");
    const auto address = "127.0.0.1:" + std::to_string(server.port());
    const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client =
        Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt, address);
    Require(client.has_value(), "native provider client");
    auto config = FileStorageConfig::FromEnvironment();
    config.storage_backend_type = StorageBackendType::kS3ObjectStorage;
    config.storage_filepath = std::string(path) + "/store";
    config.local_buffer_size = 4 * 1024 * 1024;
    config.heartbeat_interval_seconds = 1;
    std::filesystem::create_directories(config.storage_filepath);
    FileStorage storage(config, *client, endpoint);
    Require(storage.Init().has_value(), "actual S3 FileStorage init");
    // Obtain the expected tuple independently from an initialized S3 adapter.
    S3ObjectStorageAdapter inspector(object_config);
    Require(inspector.Init().has_value(), "independent adapter init");
    const auto scope = inspector.GetDurableDeleteNamespace();
    Require(scope.has_value(), "actual namespace capability");
    MasterClient probe((*client)->getClientId());
    Require(probe.Connect(address) == ErrorCode::OK,
            "independent native Master RPC probe");
    auto changed = *scope;
    changed.bucket += "-wrong";
    auto mismatch = probe.RegisterDurableDeleteProvider(
        (*client)->getClientId(), changed, endpoint);
    Require(!mismatch && mismatch.error() == ErrorCode::INVALID_PARAMS,
            "FileStorage registered its actual bucket before the probe");
    Require(probe
                .RegisterDurableDeleteProvider((*client)->getClientId(), *scope,
                                               endpoint)
                .has_value(),
            "actual initialized namespace matches over RPC");
    std::cout << "PROVIDER_NAMESPACE_REGISTRATION_PASS" << std::endl;
}

static void AdapterTest() {
    auto config = S3ObjectStorageConfig::FromEnvironment();
    Require(config.endpoint.starts_with("http://127.0.0.1:"),
            "adapter mutation suite is loopback-only");
    config.key_prefix += "/adapter-test";
    S3ObjectStorageAdapter adapter(config);
    Require(!adapter.GetDurableDeleteNamespace(),
            "uninitialized adapter must not advertise deletion capability");
    Require(adapter.Init().has_value(), "adapter init");
    const auto capability = adapter.GetDurableDeleteNamespace();
    Require(capability && capability->protocol_version == 3 &&
                capability->backend == "s3" &&
                capability->endpoint == config.endpoint &&
                capability->bucket == config.bucket &&
                capability->region == config.region &&
                capability->key_prefix == config.key_prefix,
            "deletion namespace must reflect the initialized adapter");
    // MinIO also limits each path component to 255 bytes. Exercise the S3
    // total-key boundary with a segmented prefix so that this fixture does not
    // accidentally exercise the filesystem component limit instead.
    auto boundary_config = config;
    while (boundary_config.key_prefix.size() < 780) {
        boundary_config.key_prefix += "/" + std::string(100, 'p');
    }
    S3ObjectStorageAdapter boundary_adapter(boundary_config);
    Require(boundary_adapter.Init().has_value(), "boundary adapter init");
    const auto boundary_capability =
        boundary_adapter.GetDurableDeleteNamespace();
    Require(boundary_capability.has_value(), "boundary namespace capability");
    const std::string longest_key(boundary_capability->max_scoped_key_bytes,
                                  'x');
    Require(boundary_adapter.MarkDeletion(longest_key).has_value(),
            "advertised deletion key boundary is usable");
    auto boundary_intent = boundary_adapter.HasDeletionIntent(longest_key);
    Require(boundary_intent && *boundary_intent,
            "boundary intent persisted in the actual namespace");
    Require(!boundary_adapter.MarkDeletion(longest_key + "x"),
            "oversize deletion intent is refused");
    const std::string key("tenant/../opaque\0key", 20);
    std::vector<char> bytes(4096, 'a');
    Require(adapter.Put(key, bytes).has_value(), "opaque Put");
    // Releasing one SDK user must not shut down another live adapter.
    {
        S3SnapshotObjectStore snapshot;
        const auto snapshot_key = config.key_prefix + "/snapshot-lifecycle";
        Require(
            snapshot.UploadString(snapshot_key, "snapshot-data").has_value(),
            "snapshot upload alongside adapter");
        {
            S3ObjectStorageAdapter peer(config);
            Require(peer.Init().has_value(), "overlapping adapter init");
            Require(peer.Init().has_value(), "idempotent adapter init");
            auto visible = peer.Exists(key);
            Require(visible && *visible, "overlapping adapter sees object");
        }
        std::string restored;
        Require(snapshot.DownloadString(snapshot_key, restored).has_value() &&
                    restored == "snapshot-data",
                "snapshot survives peer shutdown");
        Require(snapshot.DeleteObjectsWithPrefix(snapshot_key).has_value(),
                "remove isolated snapshot fixture");
    }
    std::vector<char> output(bytes.size());
    auto get = adapter.Get(key, output.data(), output.size());
    Require(get && *get == bytes.size() && output == bytes, "opaque Get");
    Require(!adapter.Get(key, output.data(), 1), "short buffer must fail");
    auto exists = adapter.Exists(key);
    Require(exists && *exists, "Head existing");
    auto missing = adapter.Exists("never-written");
    Require(missing && !*missing, "Head missing");
    Require(!adapter.Get("never-written", output.data(), output.size()),
            "Get missing");
    Require(!adapter.Put(std::string(1024, 'x'), bytes), "long key rejected");
    Require(!adapter.PutV(key, nullptr, 1), "bad iovec rejected");
    char a[] = "abc", b[] = "def";
    iovec iov[] = {{a, 3}, {b, 3}};
    Require(adapter.PutV("iov", iov, 2).has_value(), "PutV");
    get = adapter.Get("iov", output.data(), output.size());
    Require(get && *get == 6 && std::string(output.data(), 6) == "abcdef",
            "GetV");
    Require(adapter.Put("zero", {}).has_value(), "empty Put");
    get = adapter.Get("zero", nullptr, 0);
    Require(get && *get == 0, "empty Get");
    std::vector<char> large((97UL << 20) + 13);
    for (size_t i = 0; i < large.size(); ++i)
        large[i] = static_cast<char>((i * 131 + 17) % 251);
    // Unaligned iovec boundaries exercise part assembly and the short tail.
    iovec parts[] = {
        {large.data(), 7},
        {nullptr, 0},
        {large.data() + 7, (35UL << 20) - 7},
        {large.data() + (35UL << 20), large.size() - (35UL << 20)}};
    Require(adapter.PutV("multipart", parts, 4).has_value(), "multipart PutV");
    std::vector<char> large_read(large.size() + 1, '!');
    get = adapter.Get("multipart", large_read.data(), large.size());
    Require(get && *get == large.size() &&
                std::equal(large.begin(), large.end(), large_read.begin()) &&
                large_read.back() == '!',
            "parallel range Get with boundary canary");
    Require(adapter.Delete("multipart").has_value(),
            "remove isolated multipart fixture");
    for (int i = 0; i < 1005; ++i)
        Require(adapter.Put("page/" + std::to_string(i), {}).has_value(),
                "pagination Put");
    auto list = adapter.ListKeys();
    Require(list && list->size() == 1008,
            "pagination and opaque-key roundtrip");
    bool found = false;
    for (const auto& item : *list) {
        if (item.logical_key == key) found = item.size == bytes.size();
        Require(adapter.Delete(item.logical_key).has_value(), "Delete");
    }
    Require(found, "opaque key preserved by List");
    Require(adapter.ListKeys()->empty(), "empty namespace after delete");
    std::cout << "{\"mode\":\"adapter\",\"result\":\"PASS\",\"pagination_"
                 "objects\":1008,\"durable_namespace_verified\":true,"
                 "\"max_scoped_key_bytes\":"
              << capability->max_scoped_key_bytes << "}" << std::endl;
}

static void MultipartAdmissionTest(bool abort_denied = false) {
    auto config = S3ObjectStorageConfig::FromEnvironment();
    Require(config.endpoint.starts_with("http://127.0.0.1:") ||
                (config.endpoint.starts_with("https://") &&
                 config.endpoint.ends_with(".aliyuncs.com") &&
                 config.key_prefix.starts_with("ctgg-review-5221305272/")),
            "multipart admission requires an isolated fixture namespace");
    S3ObjectStorageAdapter adapter(config);
    Require(adapter.Init().has_value(), "adapter init");
    S3Helper physical(config.endpoint, config.bucket, config.region);
    const std::vector<char> payload(1024, 'p');
    for (const bool completed : {false, true}) {
        const std::string key = completed ? "complete-crash" : "admitted-crash";
        std::string encoded;
        for (unsigned char c : key) {
            constexpr char hex[] = "0123456789abcdef";
            encoded += hex[c >> 4];
            encoded += hex[c & 15];
        }
        const auto object = config.key_prefix + "/objects/" + encoded;
        const auto marker = config.key_prefix + "/writers/" +
                            Hash(std::vector<char>(key.begin(), key.end())) +
                            "/test-crash";
        Require(adapter.Put(key, payload).has_value(), "initial object");
        auto id = physical.CreateUpload(object);
        Require(id.has_value(), "create upload capability");
        const auto body =
            std::string("mooncake-multipart-admission-v2\n") + *id;
        Require(physical.UploadBytes(marker, std::span<const char>(body))
                    .has_value(),
                "persist admission before any payload");
        if (abort_denied) {
            Require(!adapter.MarkDeletion(key), "abort denial propagates");
            auto object_exists = physical.ObjectExists(object);
            auto marker_exists = physical.ObjectExists(marker);
            Require(object_exists && *object_exists,
                    "denied abort retains data");
            Require(marker_exists && *marker_exists,
                    "denied abort retains admission");
            std::cout << "MULTIPART_ABORT_DENIED_PASS" << std::endl;
            return;
        }
        if (completed)
            Require(physical.UploadAdmittedSlices(object, *id, {payload})
                        .has_value(),
                    "complete before simulated lost reply/crash");
        // Recreate the adapter: no in-memory writer ownership or timer remains.
        S3ObjectStorageAdapter recovered(config);
        Require(recovered.Init().has_value(), "restart adapter");
        std::atomic<int> successes{0};
        std::vector<std::thread> deleters;
        for (int n = 0; n < 8; ++n)
            deleters.emplace_back([&] {
                if (recovered.MarkDeletion(key) && recovered.Delete(key))
                    ++successes;
            });
        for (auto& thread : deleters) thread.join();
        Require(successes == 8,
                "all concurrent orphan recovery deletes succeed");
        auto exists = physical.ObjectExists(object);
        Require(exists && !*exists, "physical object removed");
        auto admission = physical.ObjectExists(marker);
        Require(admission && !*admission, "orphan admission removed");
        Require(!physical.UploadAdmittedSlices(object, *id, {payload}),
                "late writer cannot publish with revoked upload ID");
        exists = physical.ObjectExists(object);
        Require(exists && !*exists, "late upload does not resurrect bytes");
        Require(!recovered.Put(key, payload),
                "new admission observes permanent fence");
    }
    // Forced multipart must preserve zero-length objects too.
    Require(adapter.Put("empty", {}).has_value(), "empty multipart object");
    auto empty = adapter.Exists("empty");
    Require(empty && *empty, "empty object remains present");
    std::cout << "MULTIPART_ADMISSION_PASS" << std::endl;
}

int main(int argc, char** argv) {
    ResourceTracker::getInstance();
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Require(argc >= 2, "mode required");
        std::string mode = argv[1];
        if (mode == "writer-drain") {
            Require(argc == 3, "writer-drain busy|ready");
            auto config = S3ObjectStorageConfig::FromEnvironment();
            Require(IsWriterRaceFixture(config),
                    "writer drain needs an isolated fixture prefix");
            S3ObjectStorageAdapter adapter(config);
            Require(adapter.Init().has_value(), "writer drain adapter init");
            const auto result = adapter.MarkDeletion("race");
            if (std::string(argv[2]) == "busy")
                Require(!result && result.error() ==
                                       ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                        "registered remote writer must block deletion");
            else {
                Require(std::string(argv[2]) == "ready" && result.has_value(),
                        "drained writer permits deletion");
                Require(adapter.Delete("race").has_value(),
                        "drained physical delete");
            }
            std::cout << "WRITER_DRAIN_PASS " << argv[2] << std::endl;
            return 0;
        }
        if (mode == "native-delete-oss") {
            Require(argc == 3, "native-delete-oss delete|cold|observe");
            NativeDeleteTest(argv[2]);
            return 0;
        }
        if (mode == "native-delete") {
            NativeDeleteTest();
            return 0;
        }
        if (mode == "native-warm-peer") {
            NativeDeleteTest("warm-peer");
            return 0;
        }
        if (mode == "provider-registration") {
            ProviderRegistrationTest();
            return 0;
        }
        if (mode == "deletion-race" || mode == "deletion-race-native") {
            Require(argc == 4, "deletion-race put|putv deleted|denied");
            auto config = S3ObjectStorageConfig::FromEnvironment();
            Require(IsWriterRaceFixture(config),
                    "deletion race needs an isolated fixture prefix");
            S3ObjectStorageAdapter adapter(config);
            Require(adapter.Init().has_value(), "race adapter init");
            std::cout << "DELETION_RACE_INIT" << std::endl;
            const std::vector<char> bytes(4096, 'r');
            const std::string method = argv[2];
            Require(method == "put" || method == "putv", "write method");
            iovec parts[] = {
                {const_cast<char*>(bytes.data()), 17},
                {const_cast<char*>(bytes.data()) + 17, bytes.size() - 17}};
            const auto key = mode == "deletion-race-native"
                                 ? TenantId::Default().MakeScopedKey("race")
                                 : std::string("race");
            auto result = method == "put" ? adapter.Put(key, bytes)
                                          : adapter.PutV(key, parts, 2);
            std::cout << "DELETION_RACE_WRITE_RETURNED" << std::endl;
            const std::string outcome = argv[3];
            Require(outcome == "deleted" || outcome == "denied",
                    "expected write outcome");
            Require(!result, "late write must not succeed");
            Require(result.error() == (outcome == "deleted"
                                           ? ErrorCode::FILE_NOT_FOUND
                                           : ErrorCode::DFS_PERMISSION_DENIED),
                    "late writer propagates cleanup outcome");
            std::cout << "DELETION_RACE_PASS" << std::endl;
            return 0;
        }
        if (mode == "multipart-abort-denied") {
            MultipartAdmissionTest(true);
            return 0;
        }
        if (mode == "multipart-retry") {
            auto config = S3ObjectStorageConfig::FromEnvironment();
            Require(config.endpoint.starts_with("http://127.0.0.1:"),
                    "loopback retry fixture");
            S3ObjectStorageAdapter adapter(config);
            Require(adapter.Init().has_value(), "retry adapter init");
            Require(adapter.MarkDeletion("admitted-crash").has_value(),
                    "retry revokes writer");
            Require(adapter.Delete("admitted-crash").has_value(),
                    "retry removes bytes");
            std::cout << "MULTIPART_RETRY_PASS" << std::endl;
            return 0;
        }
        if (mode == "multipart-admission") {
            MultipartAdmissionTest();
            return 0;
        }
        if (mode == "deletion-intent") {
            auto config = S3ObjectStorageConfig::FromEnvironment();
            Require(config.endpoint.starts_with("http://127.0.0.1:"),
                    "deletion intent suite is loopback-only");
            const std::string key("tenant\0intent", 13);
            {
                S3ObjectStorageAdapter adapter(config);
                Require(adapter.Init().has_value(), "intent adapter init");
                auto before = adapter.HasDeletionIntent(key);
                Require(before && !*before, "fresh fixture required");
                const std::vector<char> payload(1024, 't');
                Require(adapter.Put(key, payload).has_value(),
                        "write before intent");
                std::vector<std::thread> workers;
                std::atomic<int> successes{0};
                for (int i = 0; i < 4; ++i) {
                    workers.emplace_back([&] {
                        if (adapter.MarkDeletion(key)) ++successes;
                    });
                }
                for (auto& worker : workers) worker.join();
                Require(successes == 4, "all concurrent intents succeed");
            }
            S3ObjectStorageAdapter restarted(config);
            Require(restarted.Init().has_value(), "recreated adapter init");
            auto after = restarted.HasDeletionIntent(key);
            Require(after && *after,
                    "intent persists across adapter recreation");
            auto listed = restarted.ListKeys();
            Require(listed && listed->empty(), "intent is not an object");
            auto exists = restarted.Exists(key);
            Require(exists && !*exists, "tombstoned bytes are not visible");
            char buffer[1024];
            Require(!restarted.Get(key, buffer, sizeof(buffer)),
                    "tombstoned read rejected");
            const std::vector<char> replacement(1024, 'r');
            Require(!restarted.Put(key, replacement), "key reuse rejected");
            iovec part{const_cast<char*>(replacement.data()),
                       replacement.size()};
            Require(!restarted.PutV(key, &part, 1), "late offload rejected");
            Require(restarted.Delete(key).has_value(), "remove physical bytes");
            Require(restarted.Delete(key).has_value(),
                    "repeat physical removal");
            Require(restarted.MarkDeletion(key).has_value(), "retry intent");
            std::cout << "DELETION_INTENT_PASS" << std::endl;
            return 0;
        }
        if (mode == "adapter") {
            AdapterTest();
            return 0;
        }
        if (mode == "init-fails") {
            S3ObjectStorageAdapter adapter(
                S3ObjectStorageConfig::FromEnvironment());
            auto result = adapter.Init();
            Require(!result, "expected initialization failure");
            std::cout << "{\"mode\":\"init-fails\",\"error\":"
                      << toInt(result.error()) << "}" << std::endl;
            return 0;
        }
        Require(argc == 8, "mode key bytes hostname master rpc-port tenant");
        const std::string key = argv[2];
        const size_t size = std::stoull(argv[3]);
        const std::string host = argv[4], master = argv[5], tenant = argv[7];
        using Clock = std::chrono::steady_clock;
        const auto setup_start = Clock::now();
        auto client = RealClient::create();
        auto setup = client->setup_internal(
            host, "P2PHANDSHAKE", 64UL << 20, 16UL << 20, "tcp", "", master,
            nullptr, "", std::stoi(argv[6]), true, true, "", tenant);
        Require(setup.has_value(),
                setup ? "" : "setup: " + toString(setup.error()));
        const double setup_ms = std::chrono::duration<double, std::milli>(
                                    Clock::now() - setup_start)
                                    .count();
        std::vector<char> bytes(size);
        for (size_t i = 0; i < size; ++i)
            bytes[i] = static_cast<char>((i * 131 + 17) % 251);
        auto expected_hash = Hash(bytes);
        double api_ms = 0, ready_ms = 0;
        if (mode == "put") {
            const auto write_start = Clock::now();
            Require(client->put(key, bytes) == 0, "native Put");
            api_ms = std::chrono::duration<double, std::milli>(Clock::now() -
                                                               write_start)
                         .count();
            bool persisted = false;
            for (int i = 0; i < 120; ++i) {
                auto results = client->batch_query({key});
                if (results.size() == 1 && results[0]) {
                    for (const auto& replica : results[0]->replicas)
                        persisted |= replica.is_local_disk_replica() &&
                                     replica.status == ReplicaStatus::COMPLETE;
                }
                if (persisted) break;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            Require(persisted, "S3 offload not acknowledged within 120s");
            ready_ms = std::chrono::duration<double, std::milli>(Clock::now() -
                                                                 write_start)
                           .count();
        } else {
            Require(mode == "get", "unknown mode");
            auto results = client->batch_query({key});
            Require(results.size() == 1 && results[0], "cold key discovery");
            size_t disks = 0;
            for (const auto& replica : results[0]->replicas) {
                Require(!replica.is_memory_replica(),
                        "cold read has memory replica");
                disks += replica.is_local_disk_replica();
            }
            Require(disks > 0, "cold read has no cloud-backed replica");
            std::fill(bytes.begin(), bytes.end(), 0);
            Require(client->register_buffer(bytes.data(), size) == 0,
                    "register buffer");
            const auto read_start = Clock::now();
            auto read = client->get_into(key, bytes.data(), size);
            api_ms = std::chrono::duration<double, std::milli>(Clock::now() -
                                                               read_start)
                         .count();
            client->unregister_buffer(bytes.data());
            Require(read == static_cast<int64_t>(size),
                    "native Get size: " + std::to_string(read));
            Require(Hash(bytes) == expected_hash, "native Get SHA256");
        }
        std::cout << "{\"mode\":\"" << mode << "\",\"key\":\"" << key
                  << "\",\"bytes\":" << size << ",\"sha256\":\""
                  << expected_hash
                  << "\",\"result\":\"PASS\",\"setup_ms\":" << setup_ms
                  << ",\"api_ms\":" << api_ms;
        if (mode == "put") std::cout << ",\"ready_ms\":" << ready_ms;
        std::cout << "}" << std::endl;
        client.reset();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << std::endl;
        return 1;
    }
}
