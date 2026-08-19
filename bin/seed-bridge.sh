#!/usr/bin/env bash
# 按当前数据集配置通过 bridge ABI 生成 Iceberg v3 fixture。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

dataset="${MVP_DATASET:-sift1m}"
state_dir="${MVP_STATE_DIR:-$root_dir/state}"
case "$dataset" in
  sift1m)
    verifier=verify-sift1m.sh
    ;;
  gist1m)
    verifier=verify-gist1m.sh
    ;;
  *)
    echo "ERROR: Bridge 供数不支持数据集 $dataset" >&2
    exit 2
    ;;
esac

: "${MVP_BASE_FILE:?MVP_BASE_FILE 未配置}"
: "${MVP_VECTOR_DIM:?MVP_VECTOR_DIM 未配置}"
: "${MVP_ROW_COUNT:?MVP_ROW_COUNT 未配置}"
: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
: "${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
: "${MVP_TABLE:?MVP_TABLE 未配置}"
: "${MVP_BRIDGE_SOURCE:?MVP_BRIDGE_SOURCE 未配置}"

partition_buckets="${MVP_PARTITION_BUCKETS:-32}"
if [[ ! "$partition_buckets" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: bridge provider 要求 MVP_PARTITION_BUCKETS 为正整数" >&2
  exit 1
fi
base_file="$MVP_BASE_FILE"
if [[ "$base_file" != /* ]]; then
  base_file="$root_dir/$base_file"
fi

manifest="$MVP_BRIDGE_SOURCE/Cargo.toml"
example_dir="$MVP_BRIDGE_SOURCE/examples"
fixture_source="$root_dir/src/seed_fvecs_bridge.rs"
fixture_target="$example_dir/fvecs_bridge_fixture.rs"
if [[ ! -f "$manifest" || ! -d "$example_dir" ]]; then
  echo "ERROR: MVP_BRIDGE_SOURCE 不是完整的 iceberg-rust-bridge 工作树: $MVP_BRIDGE_SOURCE" >&2
  exit 1
fi
if [[ -e "$fixture_target" || -L "$fixture_target" ]]; then
  echo "ERROR: bridge examples 中已存在 $fixture_target，拒绝覆盖" >&2
  exit 1
fi

bash "$root_dir/bin/$verifier"
mkdir -p "$MVP_WAREHOUSE_DIR" "$state_dir"
ln -s "$fixture_source" "$fixture_target"
cleanup() {
  if [[ -L "$fixture_target" && "$(readlink -f "$fixture_target")" == "$fixture_source" ]]; then
    rm -f "$fixture_target"
  fi
}
trap cleanup EXIT

cargo_bin="${MVP_CARGO_BIN:-cargo}"
seed_log="$state_dir/seed-bridge.log"
# 隔离 openGauss 构建环境中的定制编译器；Cargo 依赖使用系统工具链构建。
env -u CC -u CXX -u LD_LIBRARY_PATH "$cargo_bin" run \
  --offline --locked --release --quiet \
  --manifest-path "$manifest" \
  --example fvecs_bridge_fixture -- \
  "$base_file" \
  "$MVP_WAREHOUSE_DIR" \
  "$MVP_NAMESPACE" \
  "$MVP_TABLE" \
  "$dataset" \
  "$MVP_VECTOR_DIM" \
  "$MVP_ROW_COUNT" \
  "${MVP_ID_BASE:-1}" \
  "${MVP_BRIDGE_BATCH_ROWS:-16384}" \
  "${MVP_COMPRESSION:-uncompressed}" \
  "$partition_buckets" \
  "${MVP_TARGET_FILE_SIZE_BYTES:-1073741824}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: Bridge fixture 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$state_dir/metadata_location.txt"
printf '%s\n' bridge > "$state_dir/provider.txt"
echo "Metadata 已保存到 $state_dir/metadata_location.txt"
