#!/usr/bin/env bash
set -euo pipefail

# Run inside the pinned native builder; reuses its configured dependencies.
source_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
build_dir="${1:-/build/mooncake}"
cmake -S "$source_dir" -B "$build_dir" \
    -DBUILD_UNIT_TESTS=ON -DMOONCAKE_ENABLE_TEST_FAILPOINTS=ON
cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-8}" \
    --target durable_delete_journal_test master_service_ssd_test \
             durable_provider_rpc_test s3_store_smoke
ctest --test-dir "$build_dir" --output-on-failure \
    -R "^(durable_delete_journal_test|master_service_ssd_test|durable_provider_rpc_test)$"
# Real cloud validation is deliberately opt-in and uses only the smoke program
# allowlisted endpoint/prefix. Pass --with-oss after the build directory; the
# existing SDK credentials come from the container environment.
if [[ "${2:-}" == --with-oss ]]; then
    "$build_dir/mooncake-store/src/s3_store_smoke" native-scoped-delete
fi
