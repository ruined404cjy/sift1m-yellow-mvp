#!/usr/bin/env bash
# 部署并验证套件直接依赖的 Catalog 与 FDW 数据库扩展。
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

gsql_bin="${MVP_GSQL_BIN:-gsql}"
db="${MVP_DB:-postgres}"
port="${MVP_PORT:-37000}"
mkdir -p "$state_dir"

"$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 \
  2>&1 <<'SQL' | tee "$state_dir/deploy.log"
CREATE EXTENSION IF NOT EXISTS iceberg_catalog;
CREATE EXTENSION IF NOT EXISTS iceberg_fdw;

SELECT p.proname, p.probin
FROM pg_proc p
JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE n.nspname='iceberg_catalog'
  AND p.proname IN ('create_table', 'register_table', 'create_index', 'drop_index', 'drop_table', 'vacuum_index')
ORDER BY p.proname;
SQL

function_count="$("$gsql_bin" -X -d "$db" -p "$port" -t -A -c \
  "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='iceberg_catalog' AND p.proname IN ('create_table','register_table','create_index','drop_index','drop_table','vacuum_index');" \
  | tr -d '[:space:]')"
if [[ "$function_count" != "6" ]]; then
  echo "ERROR: Catalog 关键函数数量为 ${function_count:-<empty>}，期望 6" >&2
  exit 1
fi
echo "Catalog 与 FDW 部署检查通过。"
