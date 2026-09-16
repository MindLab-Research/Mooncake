#!/usr/bin/env bash
# CPU-only native regression gate. No cloud credentials or production services.
# Live OSS, Mint API and two-region acceptance remain separate gates.
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <configured-cmake-build-directory>" >&2
  exit 2
fi
build_dir=$(cd "$1" && pwd)
source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
configured_source=$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "$build_dir/CMakeCache.txt")
if [[ "$configured_source" != "$source_dir" ]]; then
  echo "Build directory belongs to another source checkout: $configured_source" >&2
  exit 1
fi
targets=(
  master_service_ssd_test durable_delete_journal_test durable_provider_rpc_test
  transfer_task_test tcp_write_visibility_test object_storage_adapter_test
  file_storage_test file_storage_config_test replica_selection_test
  replica_selection_config_test http_metadata_config_test
)
cmake --build "$build_dir" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}" \
  --target "${targets[@]}"
# Fail if CTest registration is absent; a zero-test invocation is not a pass.
listing=$(ctest --test-dir "$build_dir" -N)
for target in "${targets[@]}" replica_selection_env_opt_in_test; do
  if ! printf '%s\n' "$listing" | grep -E ": ${target}$" >/dev/null; then
    echo "Missing CTest registration: $target" >&2
    exit 1
  fi
done
pattern=$(IFS='|'; echo "${targets[*]}|replica_selection_env_opt_in_test")
ctest --test-dir "$build_dir" --output-on-failure --timeout 300 \
  --no-tests=error -R "^(${pattern})$"
"${PYTHON:-python3}" -m unittest discover \
  -s "$source_dir/mooncake-store/tests" -p 'test_repair_delete_journal.py' -v
"${PYTHON:-python3}" -m unittest discover \
  -s "$source_dir/mooncake-store/tests" -p 'test_s3_dev_validation.py' -v
