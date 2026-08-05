#!/usr/bin/env bash
# 使用包内隔离 PyIceberg 环境写入 SIFT1M，并保存最新 metadata location。
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

python_bin="${MVP_PYTHON_BIN:-$root_dir/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
  echo "ERROR: 找不到 PyIceberg Python: $python_bin" >&2
  echo "ERROR: 先执行 bash bin/install-pyiceberg-offline.sh" >&2
  exit 1
fi

bash "$root_dir/bin/verify-sift1m.sh"
mkdir -p "$root_dir/state"
seed_log="$root_dir/state/seed-pyiceberg.log"

env -u LD_LIBRARY_PATH "$python_bin" "$root_dir/src/seed_sift1m_pyiceberg.py" \
  --input "$root_dir/downloads/sift_base.fvecs" \
  --warehouse "$MVP_WAREHOUSE_DIR" \
  --namespace "$MVP_NAMESPACE" \
  --table "$MVP_TABLE" \
  --batch-rows "${MVP_PYICEBERG_BATCH_ROWS:-131072}" \
  --compression "${MVP_COMPRESSION:-uncompressed}" \
  --partition-buckets "${MVP_PARTITION_BUCKETS:-0}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: PyIceberg 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$root_dir/state/metadata_location.txt"
printf '%s\n' pyiceberg > "$root_dir/state/provider.txt"
echo "Metadata 已保存到 $root_dir/state/metadata_location.txt"
