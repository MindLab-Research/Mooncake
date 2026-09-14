#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>
#include <unistd.h>
#include <openssl/sha.h>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#ifndef MOONCAKE_TEST_C_ABI_SHARED
#include "real_client.h"
#endif
#include "store_c.h"

#ifndef MOONCAKE_TEST_C_ABI_SHARED
using namespace mooncake;
#endif

struct Request {
    std::string op;
    std::string key;
    std::string path;
};
YLT_REFL(Request, op, key, path);
struct Response {
    std::string op;
    std::string key;
    int error = 0;
    bool found = false;
    int memory = 0;
    int disk = 0;
    int64_t bytes = 0;
    int64_t pid = 0;
    int64_t client_memory_bytes = 0;
    double api_ms = 0;
    std::string sha256;
};
YLT_REFL(Response, op, key, error, found, memory, disk, bytes, pid,
         client_memory_bytes, api_ms, sha256);

static std::string Hash(const std::vector<char>& bytes) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(),
           digest);
    std::ostringstream result;
    for (auto c : digest)
        result << std::hex << std::setw(2) << std::setfill('0')
               << static_cast<int>(c);
    return result.str();
}

int main() {
#ifndef MOONCAKE_TEST_C_ABI_SHARED
    ResourceTracker::getInstance();
    google::InitGoogleLogging("s3_business_client");
    FLAGS_logtostderr = true;
#endif
    size_t memory_mib = 1024;
    if (const char* value = std::getenv("MOONCAKE_TEST_MEMORY_MIB")) {
        const std::string input(value);
        size_t consumed = 0;
        memory_mib = std::stoul(input, &consumed);
        if (consumed != input.size() || memory_mib < 64 || memory_mib > 4096)
            throw std::invalid_argument("invalid test memory pool");
    }
    size_t local_mib = 256;
    if (const char* value = std::getenv("MOONCAKE_TEST_LOCAL_BUFFER_MIB")) {
        size_t consumed = 0;
        const std::string input(value);
        local_mib = std::stoul(input, &consumed);
        if (consumed != input.size() || local_mib < 64 || local_mib > 4096)
            throw std::invalid_argument("invalid test local buffer");
    }
    auto client = mooncake_store_create();
    if (!client) {
        std::cerr << "mooncake_store_create returned null" << std::endl;
        return 2;
    }
    const char* host_id = std::getenv("MOONCAKE_TEST_HOST");
    const char* metadata = std::getenv("MOONCAKE_TEST_METADATA");
    const char* master = std::getenv("MOONCAKE_TEST_MASTER");
    const int setup = mooncake_store_setup_with_offload(
        client, host_id ? host_id : "127.0.0.1:17300",
        metadata ? metadata : "P2PHANDSHAKE", memory_mib << 20,
        local_mib << 20, "tcp", "", master ? master : "127.0.0.1:17400", "");
    Response ready;
    ready.op = "started";
    ready.pid = getpid();
    ready.client_memory_bytes = memory_mib << 20;
    ready.error = setup;
    std::string ready_json;
    struct_json::to_json(ready, ready_json);
    std::cout << ready_json << std::endl;
    if (setup) {
        mooncake_store_destroy(client);
        return 1;
    }
    for (std::string line; std::getline(std::cin, line);) {
        Response out;
        out.pid = getpid();
        try {
            Request req;
            struct_json::from_json(req, line);
            out.op = req.op;
            out.key = req.key;
            if (req.op == "quit") break;
            if (req.key.empty()) throw std::runtime_error("empty key");
            if (req.op == "remove") {
                out.error = mooncake_store_remove(client, req.key.c_str(), 1);
            } else if (req.op == "put") {
                std::ifstream file(req.path, std::ios::binary | std::ios::ate);
                if (!file || file.tellg() <= 0 || file.tellg() > (1025LL << 20))
                    throw std::runtime_error("invalid source size");
                std::vector<char> bytes(static_cast<size_t>(file.tellg()));
                file.seekg(0);
                if (!file.read(bytes.data(), bytes.size()))
                    throw std::runtime_error("source read");
                const auto start = std::chrono::steady_clock::now();
                out.error =
                    mooncake_store_put(client, req.key.c_str(), bytes.data(),
                                       bytes.size(), nullptr);
                out.api_ms = std::chrono::duration<double, std::milli>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
                out.bytes = bytes.size();
                out.sha256 = Hash(bytes);
            } else if (req.op == "query" || req.op == "get") {
                const auto status =
                    mooncake_store_get_replica_status(client, req.key.c_str());
                if (status < 0) {
                    out.error = status;
                } else {
                    out.found = true;
                    out.memory = (status & 1) != 0;
                    out.disk = (status & 2) != 0;
                    out.bytes =
                        mooncake_store_get_size(client, req.key.c_str());
                    if (out.bytes < 0) throw std::runtime_error("get_size");
                    if (req.op == "get") {
                        if (out.memory != 0 || out.disk != 1 ||
                            out.bytes <= 0 || out.bytes > (1025LL << 20))
                            throw std::runtime_error(
                                "expected one cloud-backed replica and no "
                                "memory replica");
                        std::vector<char> bytes(out.bytes);
                        if (mooncake_store_register_buffer(client, bytes.data(),
                                                           bytes.size()) != 0)
                            throw std::runtime_error("register buffer");
                        const auto start = std::chrono::steady_clock::now();
                        const auto n =
                            mooncake_store_get_into(client, req.key.c_str(),
                                                    bytes.data(), bytes.size());
                        out.api_ms =
                            std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - start)
                                .count();
                        mooncake_store_unregister_buffer(client, bytes.data());
                        if (n != out.bytes)
                            out.error = n < 0 ? static_cast<int>(n) : -1;
                        if (!out.error) {
                            std::ofstream file(
                                req.path, std::ios::binary | std::ios::trunc);
                            if (!file.write(bytes.data(), bytes.size()))
                                throw std::runtime_error("output write");
                            out.sha256 = Hash(bytes);
                        }
                    }
                }
            } else {
                throw std::runtime_error("unknown operation");
            }
        } catch (const std::exception& error) {
            std::cerr << "business command failed: " << error.what()
                      << std::endl;
            out.error = -9999;
        }
        std::string response_json;
        struct_json::to_json(out, response_json);
        std::cout << response_json << std::endl;
    }
    mooncake_store_destroy(client);
}
