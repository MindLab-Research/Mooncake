#pragma once

#include <string>

namespace mooncake {

struct ReplicaSelectionConfig {
    bool remote_scoring_enabled = false;
    std::string required_offload_endpoint;

    static ReplicaSelectionConfig FromEnvironment();
};

}  // namespace mooncake
