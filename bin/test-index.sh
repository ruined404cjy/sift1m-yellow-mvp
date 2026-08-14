#!/usr/bin/env bash
# 校验指定索引路径并执行 Recall 与延迟测试。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
profile="${1:-}"
scope="${2:-quick}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"
state_dir="${MVP_STATE_DIR:-$root_dir/state}"

case "$profile" in
  pq)
    expected_type=ivf_pq
    expected_implementation=ivf_pq
    ;;
  flat)
    expected_type=ivf_flat
    expected_implementation=ivf
    ;;
  *)
    echo "Usage: bash bin/test-index.sh <pq|flat> [quick|recall]" >&2
    exit 2
    ;;
esac
case "$scope" in
  quick)
    test_nq="${MVP_TEST_NQ:-100}"
    output="$state_dir/index-$profile.json"
    ;;
  recall)
    test_nq="${MVP_RECALL_NQ:-10000}"
    output="$state_dir/index-$profile-recall.json"
    ;;
  *)
    echo "Usage: bash bin/test-index.sh <pq|flat> [quick|recall]" >&2
    exit 2
    ;;
esac
if [[ "${MVP_INDEX_TYPE:-}" != "$expected_type" || \
      "${MVP_INDEX_IMPLEMENTATION:-}" != "$expected_implementation" ]]; then
  echo "ERROR: 当前 mvp.env 与 $profile 路径不匹配；先执行 bash bin/configure-index.sh $profile" >&2
  exit 1
fi

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
index_name="${MVP_INDEX_NAME:?MVP_INDEX_NAME 未配置}"
gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"
identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$namespace" "$table" "$index_name"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: 非法标识符: $value" >&2
    exit 1
  fi
done
actual="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT index_type, implementation, index_status FROM iceberg_catalog.table_indexes WHERE namespace='$namespace' AND table_name='$table' AND index_name='$index_name';" \
  | tr -d '[:space:]')"
if [[ "$actual" != "$expected_type|$expected_implementation|active" ]]; then
  echo "ERROR: $profile 索引状态为 ${actual:-<empty>}，期望 $expected_type|$expected_implementation|active" >&2
  exit 1
fi

mkdir -p "$state_dir"
python3 "$root_dir/bin/benchmark.py" \
  --mode index \
  --query-dop 1 \
  --nq "$test_nq" \
  --k "${MVP_TEST_K:-10}" \
  --warmup "${MVP_TEST_WARMUP:-5}" \
  --query-sampling "${MVP_QUERY_SAMPLING:-first}" \
  --nprobe "${MVP_NPROBE:-10}" \
  --output "$output"
