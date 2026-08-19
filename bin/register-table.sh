#!/usr/bin/env bash
# 通过 Catalog register_table 原生接入带表级向量维度属性的 producer fixture。
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
metadata_file="$state_dir/metadata_location.txt"
if [[ ! -f "$metadata_file" ]]; then
  echo "ERROR: 缺少 $metadata_file" >&2
  exit 1
fi

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
partition_buckets="${MVP_PARTITION_BUCKETS:-32}"
dimension="${MVP_VECTOR_DIM:-128}"
row_count="${MVP_ROW_COUNT:-1000000}"
id_base="${MVP_ID_BASE:-1}"
dataset="${MVP_DATASET:-sift1m}"
metadata="$(head -1 "$metadata_file")"
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
if [[ "$metadata" != file:///*.metadata.json || "$metadata" == *"'"* ]]; then
  echo "ERROR: metadata 必须是具体、无单引号的 file:///...metadata.json URI" >&2
  exit 1
fi
if [[ ! "$partition_buckets" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MVP_PARTITION_BUCKETS 必须是非负整数: $partition_buckets" >&2
  exit 1
fi
for value in "$dimension" "$row_count"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: 维度和行数必须是正整数: $value" >&2
    exit 1
  fi
done
if [[ "$id_base" != "0" && "$id_base" != "1" ]]; then
  echo "ERROR: MVP_ID_BASE 仅支持 0 或 1" >&2
  exit 1
fi
metadata_path="${metadata#file://}"
if [[ ! -r "$metadata_path" ]]; then
  echo "ERROR: 当前用户不能读取 metadata: $metadata_path" >&2
  exit 1
fi

fixture_contract="$(python3 - "$metadata_path" "$partition_buckets" "$dimension" <<'PY'
import json
import sys

metadata_path = sys.argv[1]
partition_buckets = int(sys.argv[2])
dimension = int(sys.argv[3])
with open(metadata_path, "r", encoding="utf-8") as handle:
    metadata = json.load(handle)

format_version = metadata.get("format-version")
if format_version not in (2, 3):
    raise SystemExit(
        f"ERROR: fixture format-version 仅支持 2 或 3，实际为 {format_version!r}"
    )
snapshot_id = metadata.get("current-snapshot-id")
if type(snapshot_id) is not int:
    raise SystemExit(f"ERROR: current-snapshot-id 必须是整数，实际为 {snapshot_id!r}")

properties = metadata.get("properties")
audit_dim = properties.get("vector_dim.embedding") if isinstance(properties, dict) else None
if audit_dim != str(dimension):
    raise SystemExit(
        "ERROR: fixture 要求字符串审计属性 "
        f"vector_dim.embedding={str(dimension)!r}，实际为 {audit_dim!r}"
    )

table_uuid = metadata.get("table-uuid")
if not isinstance(table_uuid, str) or not table_uuid:
    raise SystemExit(f"ERROR: table-uuid 必须是非空字符串，实际为 {table_uuid!r}")

schemas = metadata.get("schemas") or [metadata.get("schema")]
current_schema_id = metadata.get(
    "current-schema-id", (metadata.get("schema") or {}).get("schema-id")
)
current_schema = next(
    (
        schema
        for schema in schemas
        if isinstance(schema, dict) and schema.get("schema-id") == current_schema_id
    ),
    None,
)
if current_schema is None and len(schemas) == 1 and isinstance(schemas[0], dict):
    current_schema = schemas[0]
if current_schema is None:
    raise SystemExit("ERROR: 找不到 fixture 当前 schema")

fields = current_schema.get("fields", [])
id_field = next((field for field in fields if field.get("name") == "id"), None)
embedding = next((field for field in fields if field.get("name") == "embedding"), None)
if id_field is None or id_field.get("type") != "long":
    raise SystemExit("ERROR: fixture schema 要求 id long")
embedding_type = embedding.get("type") if isinstance(embedding, dict) else None
if not (
    isinstance(embedding_type, dict)
    and embedding_type.get("type") == "list"
    and embedding_type.get("element") == "float"
):
    raise SystemExit("ERROR: fixture schema 要求 embedding list<float>")

field_vector_dim = embedding.get("vector_dim")
if field_vector_dim is not None and field_vector_dim not in (dimension, str(dimension)):
    raise SystemExit(
        f"ERROR: fixture embedding.vector_dim 存在时必须为 {dimension}，"
        f"实际为 {field_vector_dim!r}"
    )
field_dim_display = "<absent>" if field_vector_dim is None else str(field_vector_dim)

default_spec_id = metadata.get("default-spec-id", 0)
specs = metadata.get("partition-specs")
if isinstance(specs, list):
    current_spec = next(
        (spec for spec in specs if spec.get("spec-id") == default_spec_id),
        None,
    )
    spec_fields = current_spec.get("fields", []) if current_spec else []
else:
    spec_fields = metadata.get("partition-spec", [])
if partition_buckets == 0:
    if spec_fields:
        raise SystemExit("ERROR: MVP_PARTITION_BUCKETS=0，但 fixture partition spec 非空")
else:
    expected_transform = f"bucket[{partition_buckets}]"
    if len(spec_fields) != 1 or not (
        spec_fields[0].get("source-id") == 1
        and spec_fields[0].get("name") == "id_bucket"
        and spec_fields[0].get("transform") == expected_transform
    ):
        raise SystemExit(
            f"ERROR: fixture 要求 bucket(id, {partition_buckets}) partition spec，"
            f"实际为 {spec_fields!r}"
        )

print(
    f"{format_version}|{snapshot_id}|{table_uuid}|{field_dim_display}|"
    f"{audit_dim}|{partition_buckets}"
)
PY
)"
IFS='|' read -r format_version snapshot_id table_uuid field_vector_dim audit_vector_dim actual_partition_buckets <<< "$fixture_contract"
echo "Fixture metadata: format=v$format_version, snapshot=$snapshot_id, uuid=$table_uuid, field vector_dim=$field_vector_dim, table property vector_dim.embedding=$audit_vector_dim, partition buckets=$actual_partition_buckets"

catalog_count="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT count(*) FROM pg_extension WHERE extname='iceberg_catalog';" \
  | tr -d '[:space:]')"
if [[ "$catalog_count" == "0" ]]; then
  namespace_count=0
else
  namespace_count="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
    "SELECT count(*) FROM iceberg_catalog.namespaces WHERE catalog_name=current_database() AND namespace='$namespace';" \
    | tr -d '[:space:]')"
fi
if [[ "$namespace_count" != "0" && "$namespace_count" != "1" ]]; then
  echo "ERROR: Catalog namespace 状态异常: $namespace_count" >&2
  exit 1
fi

sql_file="$(mktemp "/tmp/${dataset}-attach.XXXXXX.sql")"
trap 'rm -f "$sql_file"' EXIT
cat > "$sql_file" <<SQL
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS iceberg_catalog;
CREATE EXTENSION IF NOT EXISTS iceberg_fdw;

SQL
if [[ "$namespace_count" == "0" ]]; then
  printf "SELECT iceberg_catalog.create_namespace('%s', '{}'::jsonb);\n" "$namespace" >> "$sql_file"
fi
cat >> "$sql_file" <<SQL
SELECT jsonb_typeof(iceberg_catalog.register_table(
  '$namespace',
  '$table',
  '$metadata'
)) AS register_result_type;

SELECT attname, format_type(atttypid, atttypmod) AS sql_type
FROM pg_attribute
WHERE attrelid='$namespace.$table'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;

SELECT count(*) AS row_count, min(id) AS min_id, max(id) AS max_id
FROM $namespace.$table;
SQL

mkdir -p "$state_dir"
"$gsql_bin" -X -d "$db" -p "$port" -f "$sql_file" \
  2>&1 | tee "$state_dir/register-table.log"

actual="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), min(id), max(id) FROM $namespace.$table;" | tr -d '[:space:]')"
expected_max="$((id_base + row_count - 1))"
expected_range="$row_count|$id_base|$expected_max"
if [[ "$actual" != "$expected_range" ]]; then
  echo "ERROR: fixture 接入后数据校验失败，实际为 $actual，期望 $expected_range" >&2
  exit 1
fi

actual_type="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='$namespace.$table'::regclass AND attname='embedding' AND NOT attisdropped;" \
  | tr -d '[:space:]')"
if [[ "$actual_type" != "vector($dimension)" && "$actual_type" != "floatvector($dimension)" ]]; then
  echo "ERROR: embedding 类型为 ${actual_type:-<empty>}，期望 vector($dimension) 或 floatvector($dimension)" >&2
  echo "ERROR: Catalog register_table 必须从表级 vector_dim.embedding=$dimension 创建向量列" >&2
  exit 1
fi

catalog_head="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), sum(CASE WHEN relid='$namespace.$table'::regclass THEN 1 ELSE 0 END), sum(CASE WHEN metadata_location='$metadata' AND current_snapshot_id=$snapshot_id THEN 1 ELSE 0 END), sum(CASE WHEN table_uuid='$table_uuid' THEN 1 ELSE 0 END) FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
  | tr -d '[:space:]')"
if [[ "$catalog_head" != "1|1|1|1" ]]; then
  echo "ERROR: Catalog 表头校验失败，实际为 $catalog_head，期望 1|1|1|1" >&2
  exit 1
fi

catalog_dim="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), min(field_vector_dim), max(field_vector_dim) FROM iceberg_catalog.table_schemas s JOIN iceberg_catalog.tables_internal t USING (table_uuid) WHERE t.namespace='$namespace' AND t.table_name='$table' AND s.field_name='embedding';" \
  | tr -d '[:space:]')"
if [[ "$catalog_dim" != "1|$dimension|$dimension" ]]; then
  echo "ERROR: Catalog schema 向量维度校验失败，实际为 $catalog_dim，期望 1|$dimension|$dimension" >&2
  exit 1
fi

printf '%s.%s\n' "$namespace" "$table" > "$state_dir/table.txt"
echo "Fixture 原生注册完成，Catalog 向量类型、维度、UUID、metadata、snapshot、relid 和数据范围均通过校验: $namespace.$table"
