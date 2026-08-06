#!/usr/bin/env bash
# 通过 Catalog 主动建表并接入 producer fixture，保留字段级向量类型和 Delta 伴生表。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$root_dir/mvp.env"
metadata_file="$root_dir/state/metadata_location.txt"
if [[ ! -f "$env_file" || ! -f "$metadata_file" ]]; then
  echo "ERROR: 需要 mvp.env 和 state/metadata_location.txt" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
vector_type="${MVP_VECTOR_TYPE:-floatvector}"
warehouse_dir="${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
metadata="$(head -1 "$metadata_file")"
gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"
bootstrap_dir="${MVP_CATALOG_BOOTSTRAP_DIR:-$warehouse_dir/.catalog-bootstrap}/$namespace/$table"
bootstrap_uri="file://$bootstrap_dir"

identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$namespace" "$table" "$vector_type"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: 非法标识符: $value" >&2
    exit 1
  fi
done
if [[ "$metadata" != file:///*.metadata.json || "$metadata" == *"'"* ]]; then
  echo "ERROR: metadata 必须是具体、无单引号的 file:///...metadata.json URI" >&2
  exit 1
fi
if [[ "$bootstrap_dir" != /* || "$bootstrap_uri" == *"'"* ]]; then
  echo "ERROR: Catalog bootstrap 目录必须是无单引号的绝对路径: $bootstrap_dir" >&2
  exit 1
fi
metadata_path="${metadata#file://}"
if [[ ! -r "$metadata_path" ]]; then
  echo "ERROR: 当前用户不能读取 metadata: $metadata_path" >&2
  exit 1
fi

fixture_contract="$(python3 - "$metadata_path" <<'PY'
import json
import sys

metadata_path = sys.argv[1]
with open(metadata_path, "r", encoding="utf-8") as handle:
    metadata = json.load(handle)

snapshot_id = metadata.get("current-snapshot-id")
if type(snapshot_id) is not int:
    raise SystemExit(f"ERROR: current-snapshot-id 必须是整数，实际为 {snapshot_id!r}")

properties = metadata.get("properties")
audit_dim = properties.get("vector_dim.embedding") if isinstance(properties, dict) else None
if audit_dim != "128":
    raise SystemExit(
        "ERROR: fixture 要求字符串审计属性 "
        f"vector_dim.embedding='128'，实际为 {audit_dim!r}"
    )

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
if field_vector_dim is not None and field_vector_dim not in (128, "128"):
    raise SystemExit(
        "ERROR: fixture embedding.vector_dim 存在时必须为 128，"
        f"实际为 {field_vector_dim!r}"
    )
field_dim_display = "<absent>" if field_vector_dim is None else str(field_vector_dim)
print(f"{snapshot_id}|{field_dim_display}|{audit_dim}")
PY
)"
IFS='|' read -r snapshot_id field_vector_dim audit_vector_dim <<< "$fixture_contract"
echo "Fixture metadata: snapshot=$snapshot_id, field vector_dim=$field_vector_dim, table property vector_dim.embedding=$audit_vector_dim"

sql_file="$(mktemp /tmp/sift1m-attach.XXXXXX.sql)"
trap 'rm -f "$sql_file"' EXIT
cat > "$sql_file" <<SQL
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS iceberg_catalog;
CREATE EXTENSION IF NOT EXISTS iceberg_fdw;

SELECT iceberg_catalog.create_namespace('$namespace', '{}'::jsonb);
SELECT jsonb_typeof(iceberg_catalog.create_table(
  '$namespace',
  '$table',
  '{"type":"struct","fields":['
    '{"id":1,"name":"id","type":"long","required":true},'
    '{"id":2,"name":"embedding","type":{"type":"list","element-id":3,"element":"float","element-required":true},"required":true,"vector_dim":128}'
  ']}'::jsonb,
  '$bootstrap_uri'::text,
  NULL,
  NULL,
  FALSE,
  '{"format-version":"2","vector_dim.embedding":"128"}'::jsonb
)) AS create_result_type;

UPDATE iceberg_catalog.tables_internal
SET metadata_location='$metadata', current_snapshot_id=$snapshot_id
WHERE namespace='$namespace' AND table_name='$table';

SELECT attname, format_type(atttypid, atttypmod) AS sql_type
FROM pg_attribute
WHERE attrelid='$namespace.$table'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;

SELECT count(*) AS row_count, min(id) AS min_id, max(id) AS max_id
FROM $namespace.$table;
SQL

mkdir -p "$root_dir/state"
"$gsql_bin" -X -d "$db" -p "$port" -f "$sql_file" \
  2>&1 | tee "$root_dir/state/register-table.log"

actual="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), min(id), max(id) FROM $namespace.$table;" | tr -d '[:space:]')"
if [[ "$actual" != "1000000|1|1000000" ]]; then
  echo "ERROR: fixture 接入后数据校验失败，实际为 $actual，期望 1000000|1|1000000" >&2
  exit 1
fi

actual_type="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='$namespace.$table'::regclass AND attname='embedding' AND NOT attisdropped;" \
  | tr -d '[:space:]')"
if [[ "$actual_type" != "vector(128)" && "$actual_type" != "floatvector(128)" ]]; then
  echo "ERROR: embedding 类型为 ${actual_type:-<empty>}，期望 vector(128) 或 floatvector(128)" >&2
  echo "ERROR: Catalog create_table 必须从字段级 vector_dim=128 创建向量列" >&2
  exit 1
fi

catalog_head="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), sum(CASE WHEN relid='$namespace.$table'::regclass THEN 1 ELSE 0 END), sum(CASE WHEN metadata_location='$metadata' AND current_snapshot_id=$snapshot_id THEN 1 ELSE 0 END) FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
  | tr -d '[:space:]')"
if [[ "$catalog_head" != "1|1|1" ]]; then
  echo "ERROR: Catalog 表头校验失败，实际为 $catalog_head，期望 1|1|1" >&2
  exit 1
fi

delta_table="${table}_delta"
delta_rel="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT COALESCE(to_regclass('$namespace.$delta_table')::text, '');" | tr -d '[:space:]')"
if [[ -n "$delta_rel" ]]; then
  delta_type="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
    "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='$namespace.$delta_table'::regclass AND attname='embedding' AND NOT attisdropped;" \
    | tr -d '[:space:]')"
  if [[ "$delta_type" != "$actual_type" ]]; then
    echo "ERROR: Delta 伴生表 embedding 类型为 ${delta_type:-<empty>}，基础表为 $actual_type" >&2
    exit 1
  fi
  echo "Delta 伴生表类型校验通过: $namespace.$delta_table embedding $delta_type"
else
  echo "Delta 伴生表未创建；当前数据库会话未启用 Delta create hook"
fi

printf '%s.%s\n' "$namespace" "$table" > "$root_dir/state/table.txt"
echo "Fixture 接入完成，Catalog 向量类型、metadata、snapshot、relid 和数据范围均通过校验: $namespace.$table"
