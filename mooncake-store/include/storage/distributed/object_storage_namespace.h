#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {

inline constexpr uint32_t kDurableReadFenceProtocolVersion = 3;
inline constexpr uint64_t kMaxDurableReadLeaseMs = 5000;

// Non-secret routing identity reported by a credential-owning provider. Exact
// comparison is intentional: endpoint aliases must be configured consistently,
// and different physical prefixes must never be silently treated as equivalent.
struct DurableObjectStorageNamespace {
    uint32_t protocol_version = 1;
    std::string backend;
    std::string endpoint;
    std::string bucket;
    std::string region;
    std::string key_prefix;
    uint32_t max_scoped_key_bytes = 0;

    // Canonical length-delimited identity: field separators in a prefix cannot
    // alias another namespace. Credentials must never be present in this tuple.
    std::string Identity() const {
        std::string result;
        for (const auto& field :
             {std::to_string(protocol_version), backend, endpoint, bucket,
              region, key_prefix, std::to_string(max_scoped_key_bytes)}) {
            result += std::to_string(field.size()) + ":" + field;
        }
        return result;
    }

    bool IsValid() const {
        constexpr auto marker_bytes = std::string_view("/deletions/").size();
        if ((protocol_version != 1 && protocol_version != 2 &&
             protocol_version != 3) ||
            (protocol_version >= 2 && key_prefix.size() > 909) ||
            backend != "s3" || bucket.empty() || region.empty() ||
            key_prefix.empty() || max_scoped_key_bytes == 0 ||
            !(endpoint.starts_with("https://") ||
              endpoint.starts_with("http://")) ||
            endpoint.find_first_of("@?#\r\n") != std::string::npos ||
            endpoint.find('\0') != std::string::npos ||
            key_prefix.size() > 1024 - marker_bytes - 2 ||
            max_scoped_key_bytes >
                (1024 - key_prefix.size() - marker_bytes) / 2) {
            return false;
        }
        const auto authority_start = endpoint.find("://") + 3;
        const auto authority_end = endpoint.find('/', authority_start);
        if (authority_start >= endpoint.size() ||
            authority_end == authority_start)
            return false;
        return Identity().size() <= 1024;
    }

    bool operator==(const DurableObjectStorageNamespace&) const = default;
};

}  // namespace mooncake
