#pragma once

#include <charconv>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace mooncake {

// Keep existing LAN defaults; WAN deployments can explicitly budget each
// metadata operation without changing unrelated transfer deadlines.
struct HttpMetadataTimeouts {
    long request_ms;
    long connect_ms;

    static long Read(const char* name, long fallback) {
        const char* raw = std::getenv(name);
        if (!raw) return fallback;
        const std::string_view value(raw);
        long parsed = 0;
        const auto result =
            std::from_chars(value.data(), value.data() + value.size(), parsed);
        if (result.ec != std::errc{} ||
            result.ptr != value.data() + value.size() || parsed <= 0 ||
            parsed > 300000) {
            throw std::invalid_argument(
                std::string(name) + " must be an integer in [1, 300000] ms");
        }
        return parsed;
    }

    static HttpMetadataTimeouts FromEnv() {
        HttpMetadataTimeouts config{
            Read("MC_METADATA_HTTP_TIMEOUT_MS", 3000),
            Read("MC_METADATA_HTTP_CONNECT_TIMEOUT_MS", 1500)};
        if (config.connect_ms > config.request_ms) {
            throw std::invalid_argument(
                "metadata HTTP connect timeout exceeds request timeout");
        }
        return config;
    }
};

}  // namespace mooncake
