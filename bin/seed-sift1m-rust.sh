#!/usr/bin/env bash
# 复用 bridge 工作树及其 Cargo.lock 编译 Rust SDK fixture，并保存 metadata location。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$root_dir/mvp.env"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
: "${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
: "${MVP_TABLE:?MVP_TABLE 未配置}"
: "${MVP_BRIDGE_SOURCE:?MVP_BRIDGE_SOURCE 未配置}"

identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$MVP_NAMESPACE" "$MVP_TABLE"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: namespace/table 仅支持小写字母、数字和下划线: $value" >&2
    exit 1
  fi
done
if [[ "${MVP_PARTITION_BUCKETS:-0}" != "0" ]]; then
  echo "ERROR: Rust fixture 当前只支持非分区串行表；请设置 MVP_PARTITION_BUCKETS=0" >&2
  exit 1
fi
manifest="$MVP_BRIDGE_SOURCE/Cargo.toml"
example_dir="$MVP_BRIDGE_SOURCE/examples"
fixture_source="$root_dir/src/seed_sift1m_rust.rs"
fixture_target="$example_dir/sift1m_yellow_fixture.rs"
if [[ ! -f "$manifest" || ! -d "$example_dir" ]]; then
  echo "ERROR: MVP_BRIDGE_SOURCE 不是完整的 iceberg-rust-bridge 工作树: $MVP_BRIDGE_SOURCE" >&2
  exit 1
fi
if [[ -e "$fixture_target" || -L "$fixture_target" ]]; then
  echo "ERROR: bridge examples 中已存在 $fixture_target，拒绝覆盖" >&2
  exit 1
fi

bash "$root_dir/bin/verify-sift1m.sh"
mkdir -p "$MVP_WAREHOUSE_DIR" "$root_dir/state"
ln -s "$fixture_source" "$fixture_target"
cleanup() {
  if [[ -L "$fixture_target" && "$(readlink -f "$fixture_target")" == "$fixture_source" ]]; then
    rm -f "$fixture_target"
  fi
}
trap cleanup EXIT

cargo_bin="${MVP_CARGO_BIN:-cargo}"
seed_log="$root_dir/state/seed-rust.log"
env -u LD_LIBRARY_PATH "$cargo_bin" run \
  --offline --locked --release --quiet \
  --manifest-path "$manifest" \
  --example sift1m_yellow_fixture -- \
  "$root_dir/downloads/sift_base.fvecs" \
  "$MVP_WAREHOUSE_DIR" \
  "$MVP_NAMESPACE" \
  "$MVP_TABLE" \
  "${MVP_RUST_BATCH_ROWS:-131072}" \
  "${MVP_COMPRESSION:-uncompressed}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: Rust fixture 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$root_dir/state/metadata_location.txt"
printf '%s\n' rust-fixture > "$root_dir/state/provider.txt"
echo "Metadata 已保存到 $root_dir/state/metadata_location.txt"
