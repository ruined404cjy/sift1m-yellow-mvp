#!/usr/bin/env bash
# 按当前数据集配置使用 Spark 写入 Iceberg v3，并保存 metadata location。
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
    dimension="${MVP_VECTOR_DIM:-128}"
    rows="${MVP_ROW_COUNT:-1000000}"
    base_file="${MVP_BASE_FILE:-downloads/sift_base.fvecs}"
    verifier=verify-sift1m.sh
    ;;
  gist1m)
    dimension="${MVP_VECTOR_DIM:-960}"
    rows="${MVP_ROW_COUNT:-1000000}"
    base_file="${MVP_BASE_FILE:-downloads/gist_base.fvecs}"
    verifier=verify-gist1m.sh
    ;;
  *)
    echo "ERROR: Spark 供数不支持数据集 $dataset" >&2
    exit 2
    ;;
esac
if [[ "$base_file" != /* ]]; then
  base_file="$root_dir/$base_file"
fi

: "${SPARK_HOME:?SPARK_HOME 未配置}"
: "${ICEBERG_SPARK_RUNTIME_JAR:?ICEBERG_SPARK_RUNTIME_JAR 未配置}"
: "${JAVA_HOME:?JAVA_HOME 未配置}"
: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
: "${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
: "${MVP_TABLE:?MVP_TABLE 未配置}"

bash "$root_dir/bin/$verifier"
mkdir -p "$state_dir"
seed_log="$state_dir/seed-spark.log"

env -u LD_LIBRARY_PATH \
  "$SPARK_HOME/bin/spark-submit" \
  --master "${MVP_SPARK_MASTER:-local[*]}" \
  --driver-memory "${MVP_SPARK_DRIVER_MEMORY:-8g}" \
  --jars "$ICEBERG_SPARK_RUNTIME_JAR" \
  "$root_dir/src/seed_sift1m.py" \
  --input "$base_file" \
  --warehouse "$MVP_WAREHOUSE_DIR" \
  --namespace "$MVP_NAMESPACE" \
  --table "$MVP_TABLE" \
  --dataset "$dataset" \
  --dimension "$dimension" \
  --rows "$rows" \
  --id-base "${MVP_ID_BASE:-1}" \
  --data-files "${MVP_DATA_FILES:-32}" \
  --compression "${MVP_COMPRESSION:-uncompressed}" \
  --partition-buckets "${MVP_PARTITION_BUCKETS:-32}" \
  --target-file-size-bytes "${MVP_TARGET_FILE_SIZE_BYTES:-1073741824}" \
  2>&1 | tee "$seed_log"

metadata="$(sed -n 's/^MVP_METADATA_LOCATION=//p' "$seed_log" | tail -1)"
if [[ -z "$metadata" ]]; then
  echo "ERROR: Spark 已退出，但日志中没有 metadata location" >&2
  exit 1
fi
printf '%s\n' "$metadata" > "$state_dir/metadata_location.txt"
printf '%s\n' spark > "$state_dir/provider.txt"
echo "Metadata 已保存到 $state_dir/metadata_location.txt"
