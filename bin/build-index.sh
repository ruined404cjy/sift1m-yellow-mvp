#!/usr/bin/env bash
# 按 index ABI 契约构建 IVF-Flat、IVF-PQ 或 BTree 索引。
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

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
index_name="${MVP_INDEX_NAME:-}"
index_type="${MVP_INDEX_TYPE:-ivf_pq}"
implementation="${MVP_INDEX_IMPLEMENTATION:-ivf_pq}"
num_clusters="${MVP_NUM_CLUSTERS:-256}"
sample_rate="${MVP_SAMPLE_RATE:-100000}"
workers="${MVP_BUILD_WORKERS:-1}"
num_sub_quantizers="${MVP_NUM_SUB_QUANTIZERS:-}"
pq_nbits="${MVP_PQ_NBITS:-}"
vector_dim="${MVP_VECTOR_DIM:-128}"
gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"

case "$index_type:$implementation" in
  ivf_flat:ivf)
    index_profile=flat
    default_index_name="${MVP_INDEX_FLAT_NAME:-idx_sift_ivfflat}"
    index_column=embedding
    resolved_implementation=builtin.ivf_flat@2
    index_properties="{\"vector_column\":\"embedding\",\"num_clusters\":$num_clusters,\"sample_rate\":$sample_rate}"
    ;;
  ivf_pq:ivf_pq)
    index_profile=pq
    default_index_name="${MVP_INDEX_PQ_NAME:-idx_sift_ivfpq}"
    index_column=embedding
    resolved_implementation=builtin.ivf_pq@1
    index_properties="{\"vector_column\":\"embedding\",\"num_clusters\":$num_clusters,\"sample_rate\":$sample_rate}"
    if [[ -n "$num_sub_quantizers" ]]; then
      index_properties="${index_properties%\}},\"num_sub_quantizers\":$num_sub_quantizers}"
    fi
    if [[ -n "$pq_nbits" ]]; then
      index_properties="${index_properties%\}},\"nbits\":$pq_nbits}"
    fi
    ;;
  btree:btree)
    index_profile=btree
    default_index_name=idx_sift_btree
    index_column=id
    resolved_implementation=BTree
    index_properties='{"key_column":"id"}'
    ;;
  *)
    echo "ERROR: 索引组合仅支持 ivf_flat+ivf、ivf_pq+ivf_pq、btree+btree" >&2
    exit 1
    ;;
esac
index_name="${index_name:-$default_index_name}"
identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$namespace" "$table" "$index_name" "$index_type" "$implementation"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: 非法标识符或索引枚举值: $value" >&2
    exit 1
  fi
done
for value in "$num_clusters" "$sample_rate" "$workers" "$vector_dim"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: 索引数值参数必须是正整数: $value" >&2
    exit 1
  fi
done
for value in "$num_sub_quantizers" "$pq_nbits"; do
  if [[ -n "$value" && ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: PQ 数值参数必须是正整数: $value" >&2
    exit 1
  fi
done
if [[ -n "$pq_nbits" && "$pq_nbits" -gt 8 ]]; then
  echo "ERROR: MVP_PQ_NBITS 必须在 1..8 范围内: $pq_nbits" >&2
  exit 1
fi
if [[ -n "$num_sub_quantizers" && $((vector_dim % num_sub_quantizers)) -ne 0 ]]; then
  echo "ERROR: MVP_NUM_SUB_QUANTIZERS=$num_sub_quantizers 必须整除维度 $vector_dim" >&2
  exit 1
fi

sql_file="$(mktemp "/tmp/${dataset}-index.XXXXXX.sql")"
trap 'rm -f "$sql_file"' EXIT
cat > "$sql_file" <<SQL
\set ON_ERROR_STOP on
\timing on
SELECT iceberg_catalog.create_index(
  '$namespace', '$table', '$index_name',
  '["$index_column"]'::jsonb,
  '$index_type', '$implementation',
  '$index_properties'::jsonb,
  p_is_async => false,
  p_num_workers => $workers
);

SELECT index_name, index_status, index_type, implementation
FROM iceberg_catalog.table_indexes
WHERE namespace='$namespace'
  AND table_name='$table'
  AND index_name='$index_name';
SQL

mkdir -p "$state_dir"
log_file="$state_dir/build-index-$index_profile.log"
started_ms="$(date +%s%3N)"
"$gsql_bin" -X -d "$db" -p "$port" -f "$sql_file" 2>&1 | tee "$log_file"
finished_ms="$(date +%s%3N)"
elapsed_ms="$((finished_ms - started_ms))"
printf 'MVP_BUILD_WALL_MS=%s\n' "$elapsed_ms" | tee -a "$log_file"
status="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT index_status FROM iceberg_catalog.table_indexes WHERE namespace='$namespace' AND table_name='$table' AND index_name='$index_name';" \
  | tr -d '[:space:]')"
printf 'MVP_INDEX_STATUS=%s\n' "$status" | tee -a "$log_file"
if [[ "$status" != "active" ]]; then
  echo "ERROR: 索引状态为 ${status:-<empty>}，期望 active" >&2
  exit 1
fi
actual_contract="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT index_type, implementation FROM iceberg_catalog.table_indexes WHERE namespace='$namespace' AND table_name='$table' AND index_name='$index_name';" \
  | tr -d '[:space:]')"
if [[ "$actual_contract" != "$index_type|$implementation" ]]; then
  echo "ERROR: Catalog 索引契约为 ${actual_contract:-<empty>}，期望 $index_type|$implementation" >&2
  exit 1
fi
printf 'MVP_INDEX_TYPE=%s\n' "$index_type" | tee -a "$log_file"
printf 'MVP_INDEX_IMPLEMENTATION=%s\n' "$implementation" | tee -a "$log_file"
printf 'MVP_INDEX_RESOLVED_IMPLEMENTATION=%s\n' "$resolved_implementation" | tee -a "$log_file"
if [[ "$index_profile" == "flat" || "$index_profile" == "pq" ]]; then
  metadata_location="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
    "SELECT metadata_location FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
    | tr -d '\r\n')"
  artifact_command=(
    python3 "$root_dir/bin/clean-index-artifacts.py"
    --metadata-location "$metadata_location"
    --expect-index-name "$index_name"
    --expect-index-type "$index_type"
    --expect-dimension "$vector_dim"
  )
  if [[ "$index_profile" == "pq" && -n "$num_sub_quantizers" ]]; then
    artifact_command+=(--expect-num-sub-quantizers "$num_sub_quantizers")
  fi
  if [[ "$index_profile" == "pq" && -n "$pq_nbits" ]]; then
    artifact_command+=(--expect-nbits "$pq_nbits")
  fi
  "${artifact_command[@]}" | tee -a "$log_file"
fi
echo "$index_profile 索引构建完成且状态为 active，墙钟耗时 ${elapsed_ms} ms"
