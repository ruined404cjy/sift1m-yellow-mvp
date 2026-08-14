#!/usr/bin/env bash
# 执行全表扫描正确性和性能测试。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"
state_dir="${MVP_STATE_DIR:-$root_dir/state}"

mkdir -p "$state_dir"
python3 "$root_dir/bin/benchmark.py" \
  --mode fullscan \
  --query-dop 1 \
  --nq "${MVP_TEST_NQ:-100}" \
  --k "${MVP_TEST_K:-10}" \
  --warmup "${MVP_TEST_WARMUP:-5}" \
  --query-sampling "${MVP_QUERY_SAMPLING:-first}" \
  --output "$state_dir/fullscan.json"
