#!/usr/bin/env bash
# 使用独立 GIST 配置通过 PyIceberg 写入 bucket[32] 表。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/gist.env}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"
state_dir="${MVP_STATE_DIR:-$root_dir/state/gist1m}"

if [[ "${MVP_DATASET:-}" != "gist1m" ]]; then
  echo "ERROR: GIST 供数要求 MVP_DATASET=gist1m" >&2
  exit 1
fi
: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
: "${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
: "${MVP_TABLE:?MVP_TABLE 未配置}"

python_bin="${MVP_PYTHON_BIN:-$root_dir/.venv/bin/python}"
if [[ ! -x "$python_bin" ]]; then
  echo "ERROR: 找不到 PyIceberg Python: $python_bin" >&2
  echo "ERROR: 先执行 bash bin/install-pyiceberg-offline.sh" >&2
  exit 1
fi

bash "$root_dir/bin/verify-gist1m.sh"
mkdir -p "$state_dir"
seed_log="$state_dir/seed-pyiceberg.log"

env -u LD_LIBRARY_PATH "$python_bin" "$root_dir/src/seed_sift1m_pyiceberg.py" \
  --input "$root_dir/downloads/gist_base.fvecs" \
  --warehouse "$MVP_WAREHOUSE_DIR" \
  --namespace "$MVP_NAMESPACE" \
  --table "$MVP_TABLE" \
  --dataset gist1m \
  --dimension 960 \
  --rows 1000000 \
  --id-base "${MVP_ID_BASE:-1}" \
  --batch-rows "${MVP_PYICEBERG_BATCH_ROWS:-1000000}" \
  --compression "${MVP_COMPRESSION:-uncompressed}" \
  --partition-buckets "${MVP_PARTITION_BUCKETS:-32}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: PyIceberg 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$state_dir/metadata_location.txt"
printf '%s\n' pyiceberg > "$state_dir/provider.txt"
echo "Metadata 已保存到 $state_dir/metadata_location.txt"
