#!/usr/bin/env bash
# 调用黄区现有 Spark/runtime 写入 SIFT1M，并保存 metadata location。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$root_dir/mvp.env"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

: "${SPARK_HOME:?SPARK_HOME 未配置}"
: "${ICEBERG_SPARK_RUNTIME_JAR:?ICEBERG_SPARK_RUNTIME_JAR 未配置}"
: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
: "${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
: "${MVP_TABLE:?MVP_TABLE 未配置}"

bash "$root_dir/bin/verify-sift1m.sh"
mkdir -p "$root_dir/state"
seed_log="$root_dir/state/seed.log"

env -u LD_LIBRARY_PATH \
  "$SPARK_HOME/bin/spark-submit" \
  --master 'local[*]' \
  --jars "$ICEBERG_SPARK_RUNTIME_JAR" \
  "$root_dir/src/seed_sift1m.py" \
  --input "$root_dir/downloads/sift_base.fvecs" \
  --warehouse "$MVP_WAREHOUSE_DIR" \
  --namespace "$MVP_NAMESPACE" \
  --table "$MVP_TABLE" \
  --id-base "${MVP_ID_BASE:-1}" \
  --data-files "${MVP_DATA_FILES:-8}" \
  --compression "${MVP_COMPRESSION:-uncompressed}" \
  --partition-buckets "${MVP_PARTITION_BUCKETS:-32}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: Spark 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$root_dir/state/metadata_location.txt"
printf '%s\n' spark > "$root_dir/state/provider.txt"
echo "Metadata 已保存到 $root_dir/state/metadata_location.txt"
