#include "http_metadata_config.h"
#include <cstdlib>

static void check(bool condition) {
    if (!condition) std::abort();
}

int main() {
    using mooncake::HttpMetadataTimeouts;
    unsetenv("MC_METADATA_HTTP_TIMEOUT_MS");
    unsetenv("MC_METADATA_HTTP_CONNECT_TIMEOUT_MS");
    auto c = HttpMetadataTimeouts::FromEnv();
    check(c.request_ms == 3000 && c.connect_ms == 1500);
    setenv("MC_METADATA_HTTP_TIMEOUT_MS", "30000", 1);
    setenv("MC_METADATA_HTTP_CONNECT_TIMEOUT_MS", "10000", 1);
    c = HttpMetadataTimeouts::FromEnv();
    check(c.request_ms == 30000 && c.connect_ms == 10000);
    for (const char* invalid :
         {"", "0", "-1", "300001", "10ms", " 100", "9999999999999999999999"}) {
        setenv("MC_METADATA_HTTP_CONNECT_TIMEOUT_MS", invalid, 1);
        bool rejected = false;
        try {
            (void)HttpMetadataTimeouts::FromEnv();
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        check(rejected);
    }
    setenv("MC_METADATA_HTTP_CONNECT_TIMEOUT_MS", "30001", 1);
    bool rejected = false;
    try {
        (void)HttpMetadataTimeouts::FromEnv();
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    check(rejected);
}
