#!/usr/bin/env bash
# 分级清理索引、测试结果或当前表的全部供数状态。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$root_dir/mvp.env"
level="${1:-}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

case "$level" in
  index|results|all) ;;
  *)
    echo "Usage: bash bin/clean.sh <index|results|all>" >&2
    exit 2
    ;;
esac

namespace="${MVP_NAMESPACE:?MVP_NAMESPACE 未配置}"
table="${MVP_TABLE:?MVP_TABLE 未配置}"
warehouse_dir="${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
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
if [[ "$warehouse_dir" != /* || "$warehouse_dir" == "/" ]]; then
  echo "ERROR: MVP_WAREHOUSE_DIR 必须是非根目录的裸绝对路径" >&2
  exit 1
fi
warehouse_dir="${warehouse_dir%/}"

catalog_count=""
load_catalog_state() {
  catalog_count="$("$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -t -A -c \
    "SELECT count(*) FROM pg_extension WHERE extname='iceberg_catalog';" \
    | tr -d '[:space:]')"
  if [[ "$catalog_count" != "0" && "$catalog_count" != "1" ]]; then
    echo "ERROR: Catalog 扩展状态异常: ${catalog_count:-<empty>}" >&2
    exit 1
  fi
}

table_count() {
  "$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -t -A -c \
    "SELECT count(*) FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
    | tr -d '[:space:]'
}

clean_indexes() {
  if [[ "$catalog_count" == "0" ]]; then
    echo "Catalog 中没有 $namespace.$table，索引清理跳过。"
    return
  fi
  count="$(table_count)"
  if [[ "$count" == "0" ]]; then
    echo "Catalog 中没有 $namespace.$table，索引清理跳过。"
    return
  elif [[ "$count" != "1" ]]; then
    echo "ERROR: Catalog 中存在 $count 条同名表记录" >&2
    exit 1
  fi
  index_output="$("$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -t -A -c \
    "SELECT index_name FROM iceberg_catalog.table_indexes WHERE namespace='$namespace' AND table_name='$table' ORDER BY index_name;")"
  index_names=()
  if [[ -n "$index_output" ]]; then
    mapfile -t index_names <<< "$index_output"
  fi
  if [[ "${#index_names[@]}" -eq 0 ]]; then
    echo "Catalog 中没有待清理索引，继续检查落盘残留。"
  else
    for index_name in "${index_names[@]}"; do
      index_name="${index_name//[[:space:]]/}"
      if [[ ! "$index_name" =~ $identifier_pattern ]]; then
        echo "ERROR: Catalog 返回非法索引名: $index_name" >&2
        exit 1
      fi
      "$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -c \
        "SELECT iceberg_catalog.drop_index('$namespace', '$table', '$index_name');"
    done

    "$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -c \
      "SELECT iceberg_catalog.vacuum_index('$namespace', '$table', NULL, INTERVAL '0 seconds', FALSE, TRUE);"
  fi
  remaining="$("$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -t -A -c \
    "SELECT count(*) FROM iceberg_catalog.table_indexes WHERE namespace='$namespace' AND table_name='$table';" \
    | tr -d '[:space:]')"
  if [[ "$remaining" != "0" ]]; then
    echo "ERROR: 索引清理后仍有 $remaining 条 Catalog 记录" >&2
    exit 1
  fi
  current_metadata="$("$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -t -A -c \
    "SELECT metadata_location FROM iceberg_catalog.tables_internal WHERE namespace='$namespace' AND table_name='$table';" \
    | tr -d '\r\n')"
  python3 "$root_dir/bin/clean-index-artifacts.py" \
    --metadata-location "$current_metadata"
  echo "索引定义已删除；当前 metadata 指向空 Registry，残留索引 artifact 已清理。"
}

clean_results() {
  mkdir -p "$root_dir/state"
  find "$root_dir/state" -mindepth 1 -depth \
    ! -name .gitkeep \
    ! -name metadata_location.txt \
    ! -name provider.txt \
    ! -name table.txt \
    -delete
  echo "测试结果已清理，供数定位状态已保留。"
}

delete_table_tree() {
  local scope_root="${1%/}"
  local target="$2"
  if [[ "$scope_root" != /* || "$scope_root" == "/" || "$target" != "$scope_root/"* ]]; then
    echo "ERROR: 拒绝清理越界目录: $target" >&2
    exit 1
  fi
  if [[ -e "$target" || -L "$target" ]]; then
    find "$target" -depth -delete
    echo "已删除表目录: $target"
  fi
}

clean_all() {
  if [[ "$catalog_count" == "1" ]]; then
    count="$(table_count)"
    if [[ "$count" == "1" ]]; then
      clean_indexes
      "$gsql_bin" -X -d "$db" -p "$port" -v ON_ERROR_STOP=1 -c \
        "SELECT iceberg_catalog.drop_table('$namespace', '$table', FALSE);"
    elif [[ "$count" != "0" ]]; then
      echo "ERROR: Catalog 中存在 $count 条同名表记录" >&2
      exit 1
    fi
  fi

  delete_table_tree "$warehouse_dir" "$warehouse_dir/$namespace/$table"
  delete_table_tree "$warehouse_dir" "$warehouse_dir/${namespace}.db/$table"
  bootstrap_root="${MVP_CATALOG_BOOTSTRAP_DIR:-$warehouse_dir/.catalog-bootstrap}"
  if [[ "$bootstrap_root" != /* || "$bootstrap_root" == "/" ]]; then
    echo "ERROR: MVP_CATALOG_BOOTSTRAP_DIR 必须是非根目录的绝对路径" >&2
    exit 1
  fi
  bootstrap_root="${bootstrap_root%/}"
  delete_table_tree "$bootstrap_root" "$bootstrap_root/$namespace/$table"

  find "$root_dir/state" -mindepth 1 -depth ! -name .gitkeep -delete
  echo "Catalog 表、producer 表目录、bootstrap metadata 和运行状态已清理。"
  echo "SIFT1M 原始文件保留在 $root_dir/downloads。"
}

case "$level" in
  index)
    load_catalog_state
    clean_indexes
    ;;
  results) clean_results ;;
  all)
    load_catalog_state
    clean_all
    ;;
esac
