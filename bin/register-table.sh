#!/usr/bin/env bash
# 注册 producer 快照；按模式校验原生映射或重建固定维度向量外表。
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
register_mode="${MVP_REGISTER_MODE:-manual-vector}"
metadata="$(head -1 "$metadata_file")"
gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"

identifier_pattern='^[a-z_][a-z0-9_]*$'
for value in "$namespace" "$table" "$vector_type"; do
  if [[ ! "$value" =~ $identifier_pattern ]]; then
    echo "ERROR: 非法标识符: $value" >&2
    exit 1
  fi
done
if [[ "$register_mode" != "auto" && "$register_mode" != "manual-vector" ]]; then
  echo "ERROR: MVP_REGISTER_MODE 仅支持 auto|manual-vector" >&2
  exit 1
fi
if [[ "$metadata" != file:///*.metadata.json || "$metadata" == *"'"* ]]; then
  echo "ERROR: metadata 必须是具体、无单引号的 file:///...metadata.json URI" >&2
  exit 1
fi
metadata_path="${metadata#file://}"
if [[ ! -r "$metadata_path" ]]; then
  echo "ERROR: 当前用户不能读取 metadata: $metadata_path" >&2
  exit 1
fi

python3 - "$metadata_path" "$register_mode" <<'PY'
import json
import sys

metadata_path = sys.argv[1]
register_mode = sys.argv[2]
with open(metadata_path, "r", encoding="utf-8") as handle:
    metadata = json.load(handle)
properties = metadata.get("properties")
actual = properties.get("vector_dim.embedding") if isinstance(properties, dict) else None
if register_mode == "manual-vector" and actual != "128":
    raise SystemExit(
        "ERROR: manual-vector 模式要求字符串审计属性 "
        f"vector_dim.embedding='128'，实际为 {actual!r}"
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
fields = current_schema.get("fields", []) if current_schema else []
embedding = next((field for field in fields if field.get("name") == "embedding"), None)
if embedding is None:
    raise SystemExit("ERROR: 当前 Iceberg schema 缺少 embedding 字段")
field_vector_dim = embedding.get("vector_dim")
if register_mode == "auto" and field_vector_dim not in (128, "128"):
    raise SystemExit(
        "ERROR: auto 模式要求 schema.fields[embedding].vector_dim=128，"
        f"实际为 {field_vector_dim!r}"
    )
print(
    "Metadata 向量维度: "
    f"field vector_dim={field_vector_dim!r}, "
    f"table property vector_dim.embedding={actual!r}"
)
PY

sql_file="$(mktemp /tmp/sift1m-register.XXXXXX.sql)"
trap 'rm -f "$sql_file"' EXIT
cat > "$sql_file" <<SQL
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS iceberg_catalog;
CREATE EXTENSION IF NOT EXISTS iceberg_fdw;

SELECT iceberg_catalog.create_namespace('$namespace', '{}'::jsonb);
SELECT jsonb_typeof(iceberg_catalog.register_table(
  '$namespace', '$table', '$metadata'
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

if [[ "$register_mode" == "manual-vector" ]]; then
  cat >> "$sql_file" <<SQL

DROP FOREIGN TABLE $namespace.$table;
CREATE FOREIGN TABLE $namespace.$table (
  id bigint,
  embedding $vector_type(128)
) SERVER iceberg_catalog_server
OPTIONS (namespace '$namespace', table_name '$table');
UPDATE iceberg_catalog.tables_internal
SET relid='$namespace.$table'::regclass
WHERE namespace='$namespace' AND table_name='$table';

SELECT attname, format_type(atttypid, atttypmod) AS sql_type
FROM pg_attribute
WHERE attrelid='$namespace.$table'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;
SQL
fi

mkdir -p "$root_dir/state"
"$gsql_bin" -X -d "$db" -p "$port" -f "$sql_file" \
  2>&1 | tee "$root_dir/state/register-table.log"

actual="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -F '|' -c \
  "SELECT count(*), min(id), max(id) FROM $namespace.$table;" | tr -d '[:space:]')"
if [[ "$actual" != "1000000|1|1000000" ]]; then
  echo "ERROR: 注册后数据校验失败，实际为 $actual，期望 1000000|1|1000000" >&2
  exit 1
fi

actual_type="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='$namespace.$table'::regclass AND attname='embedding' AND NOT attisdropped;" \
  | tr -d '[:space:]')"
expected_type="${vector_type}(128)"
if [[ "$actual_type" != "$expected_type" ]]; then
  echo "ERROR: embedding 类型为 ${actual_type:-<empty>}，期望 $expected_type" >&2
  echo "ERROR: auto 模式检查字段级 vector_dim；manual-vector 模式检查重建外表 DDL" >&2
  exit 1
fi

relid_count="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT count(*) FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table' AND relid='$namespace.$table'::regclass;" \
  | tr -d '[:space:]')"
if [[ "$relid_count" != "1" ]]; then
  echo "ERROR: Catalog relid 未指向外表 $namespace.$table" >&2
  exit 1
fi

printf '%s.%s\n' "$namespace" "$table" > "$root_dir/state/table.txt"
echo "注册完成，模式=$register_mode，向量类型、relid 和数据范围均通过校验: $namespace.$table"
