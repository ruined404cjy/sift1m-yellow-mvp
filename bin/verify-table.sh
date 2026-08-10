#!/usr/bin/env bash
# 校验可复用表的 Catalog head、relid、向量类型和数据范围。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$root_dir/mvp.env"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
partition_buckets="${MVP_PARTITION_BUCKETS:-32}"
gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"
identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$namespace" "$table"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: 非法标识符: $value" >&2
    exit 1
  fi
done
if [[ ! "$partition_buckets" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MVP_PARTITION_BUCKETS 必须是非负整数: $partition_buckets" >&2
  exit 1
fi

catalog_state="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), sum(CASE WHEN relid='$namespace.$table'::regclass THEN 1 ELSE 0 END), sum(CASE WHEN metadata_location LIKE 'file:///%.metadata.json' THEN 1 ELSE 0 END) FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
  | tr -d '[:space:]')"
if [[ "$catalog_state" != "1|1|1" ]]; then
  echo "ERROR: Catalog 表头状态为 ${catalog_state:-<empty>}，期望 1|1|1" >&2
  exit 1
fi

actual_type="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='$namespace.$table'::regclass AND attname='embedding' AND NOT attisdropped;" \
  | tr -d '[:space:]')"
if [[ "$actual_type" != "vector(128)" && "$actual_type" != "floatvector(128)" ]]; then
  echo "ERROR: embedding 类型为 ${actual_type:-<empty>}，期望 vector(128) 或 floatvector(128)" >&2
  exit 1
fi

metadata="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT metadata_location FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
  | tr -d '\r\n')"
metadata_path="${metadata#file://}"
if [[ "$metadata" != file:///*.metadata.json || ! -r "$metadata_path" ]]; then
  echo "ERROR: 当前 metadata 不可读取: ${metadata:-<empty>}" >&2
  exit 1
fi
python3 - "$metadata_path" "$partition_buckets" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    metadata = json.load(handle)
partition_buckets = int(sys.argv[2])
default_spec_id = metadata.get("default-spec-id", 0)
specs = metadata.get("partition-specs")
if isinstance(specs, list):
    current_spec = next(
        (spec for spec in specs if spec.get("spec-id") == default_spec_id),
        None,
    )
    fields = current_spec.get("fields", []) if current_spec else []
else:
    fields = metadata.get("partition-spec", [])

if partition_buckets == 0:
    if fields:
        raise SystemExit("ERROR: 当前表配置为非分区，但 metadata partition spec 非空")
else:
    expected = f"bucket[{partition_buckets}]"
    if len(fields) != 1 or not (
        fields[0].get("source-id") == 1
        and fields[0].get("name") == "id_bucket"
        and fields[0].get("transform") == expected
    ):
        raise SystemExit(
            f"ERROR: 当前表要求 bucket(id, {partition_buckets})，实际为 {fields!r}"
        )
PY

data_range="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), min(id), max(id) FROM $namespace.$table;" \
  | tr -d '[:space:]')"
if [[ "$data_range" != "1000000|1|1000000" ]]; then
  echo "ERROR: 数据范围为 ${data_range:-<empty>}，期望 1000000|1|1000000" >&2
  exit 1
fi
echo "表复用门禁通过: $namespace.$table, $actual_type, bucket=$partition_buckets, $data_range"
