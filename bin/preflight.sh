#!/usr/bin/env bash
# 检查 ARM EulerOS、producer、数据库连接和 bridge 安装副本。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 请先复制 config/mvp.env.example 为 mvp.env 并修改" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"

provider="${1:-all}"
dataset="${MVP_DATASET:-sift1m}"
state_dir="${MVP_STATE_DIR:-$root_dir/state}"
if [[ "$provider" != "all" && "$provider" != "spark" && "$provider" != "pyiceberg" && "$provider" != "rust" ]]; then
  echo "Usage: bash bin/preflight.sh [all|spark|pyiceberg|rust]" >&2
  exit 2
fi

mkdir -p "$state_dir"
exec > >(tee "$state_dir/preflight.log") 2>&1

: "${MVP_WAREHOUSE_DIR:?MVP_WAREHOUSE_DIR 未配置}"
if [[ "$MVP_WAREHOUSE_DIR" != /* || "$MVP_WAREHOUSE_DIR" == "/" ]]; then
  echo "ERROR: MVP_WAREHOUSE_DIR 必须是非根目录的裸绝对路径" >&2
  exit 1
fi
case "$dataset" in
  sift1m) bash "$root_dir/bin/verify-sift1m.sh" ;;
  gist1m) bash "$root_dir/bin/verify-gist1m.sh" ;;
  *)
    echo "ERROR: 不支持的数据集: $dataset" >&2
    exit 1
    ;;
esac

if [[ "$(uname -m)" != "aarch64" && "${MVP_ALLOW_NON_AARCH64:-0}" != "1" ]]; then
  echo "ERROR: 当前架构为 $(uname -m)，本 MVP 的目标架构是 aarch64" >&2
  echo "ERROR: 蓝区验证可显式设置 MVP_ALLOW_NON_AARCH64=1" >&2
  exit 1
fi
if [[ "$(uname -m)" != "aarch64" ]]; then
  echo "WARN: 已启用非 aarch64 蓝区验证开关，结果不代表黄区 ARM 性能" >&2
fi
echo "架构: $(uname -m)"
echo "数据集: $dataset"

if [[ -r /etc/euleros-release ]]; then
  echo "系统: $(cat /etc/euleros-release)"
elif [[ -r /etc/os-release ]]; then
  grep -E '^(NAME|VERSION)=' /etc/os-release || true
fi

echo "CPU/NUMA/内存摘要:"
if command -v lscpu >/dev/null 2>&1; then
  lscpu | grep -E '^(Architecture|CPU\(s\)|Thread|Core|Socket|NUMA node\(s\)|NUMA node[0-9]+ CPU\(s\)):' || true
fi
if command -v free >/dev/null 2>&1; then
  free -h
fi
echo "进程资源上限: open_files=$(ulimit -n), max_user_processes=$(ulimit -u)"
echo "Warehouse 存储摘要:"
warehouse_probe="$MVP_WAREHOUSE_DIR"
while [[ ! -e "$warehouse_probe" && "$warehouse_probe" != "/" ]]; do
  warehouse_probe="$(dirname -- "$warehouse_probe")"
done
df -hT "$warehouse_probe"
if command -v lsblk >/dev/null 2>&1; then
  lsblk -o NAME,ROTA,TYPE,SIZE,FSTYPE,MOUNTPOINTS || true
fi

for command_name in python3 sha256sum stat; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "ERROR: 缺少命令 $command_name" >&2
    exit 1
  }
done

if [[ "$provider" == "all" || "$provider" == "spark" ]]; then
  : "${SPARK_HOME:?SPARK_HOME 未配置}"
  : "${ICEBERG_SPARK_RUNTIME_JAR:?ICEBERG_SPARK_RUNTIME_JAR 未配置}"
  if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
    echo "ERROR: 找不到 $SPARK_HOME/bin/spark-submit" >&2
    exit 1
  fi
  if [[ ! -f "$ICEBERG_SPARK_RUNTIME_JAR" ]]; then
    echo "ERROR: 找不到 runtime jar: $ICEBERG_SPARK_RUNTIME_JAR" >&2
    exit 1
  fi

  echo "Java:"
  java_version="$(env -u LD_LIBRARY_PATH java -version 2>&1)"
  sed -n '1,3p' <<< "$java_version"
  echo "Spark:"
  spark_version="$(env -u LD_LIBRARY_PATH "$SPARK_HOME/bin/spark-submit" --version 2>&1)"
  sed -n '1,12p' <<< "$spark_version"
  echo "Iceberg runtime: $ICEBERG_SPARK_RUNTIME_JAR"
  sha256sum "$ICEBERG_SPARK_RUNTIME_JAR"

  case "$(basename "$ICEBERG_SPARK_RUNTIME_JAR")" in
    *spark-runtime-3.5_2.12*) ;;
    *)
      echo "WARN: runtime jar 文件名不是 Spark 3.5 / Scala 2.12 形式，请人工核对版本矩阵" >&2
      ;;
  esac
fi

if [[ "$provider" == "all" || "$provider" == "pyiceberg" ]]; then
  python_bin="${MVP_PYTHON_BIN:-$root_dir/.venv/bin/python}"
  if [[ ! -x "$python_bin" ]]; then
    echo "ERROR: 找不到 PyIceberg Python: $python_bin" >&2
    exit 1
  fi
  env -u LD_LIBRARY_PATH "$python_bin" - <<'PY'
import pyarrow
import pyiceberg
import sqlalchemy

print(
    f"PyIceberg: pyiceberg={pyiceberg.__version__} "
    f"pyarrow={pyarrow.__version__} sqlalchemy={sqlalchemy.__version__}"
)
PY
  env -u LD_LIBRARY_PATH "$python_bin" -m pip check
fi

if [[ "$provider" == "all" || "$provider" == "rust" ]]; then
  : "${MVP_BRIDGE_SOURCE:?MVP_BRIDGE_SOURCE 未配置}"
  cargo_bin="${MVP_CARGO_BIN:-cargo}"
  if ! command -v "$cargo_bin" >/dev/null 2>&1 && [[ ! -x "$cargo_bin" ]]; then
    echo "ERROR: 找不到 Cargo: $cargo_bin" >&2
    exit 1
  fi
  if [[ ! -f "$MVP_BRIDGE_SOURCE/Cargo.toml" || ! -f "$MVP_BRIDGE_SOURCE/Cargo.lock" ]]; then
    echo "ERROR: bridge 工作树缺少 Cargo.toml 或 Cargo.lock: $MVP_BRIDGE_SOURCE" >&2
    exit 1
  fi
  echo "Rust: $("$cargo_bin" --version)"
  echo "Bridge source: $MVP_BRIDGE_SOURCE"
  git -C "$MVP_BRIDGE_SOURCE" rev-parse HEAD 2>/dev/null || true
fi

mkdir -p "$MVP_WAREHOUSE_DIR"
if [[ ! -w "$MVP_WAREHOUSE_DIR" ]]; then
  echo "ERROR: warehouse 不可写: $MVP_WAREHOUSE_DIR" >&2
  exit 1
fi
echo "Warehouse: $MVP_WAREHOUSE_DIR"

gsql_bin="${MVP_GSQL_BIN:-gsql}"
if ! command -v "$gsql_bin" >/dev/null 2>&1 && [[ ! -x "$gsql_bin" ]]; then
  echo "ERROR: 找不到 gsql: $gsql_bin" >&2
  exit 1
fi
gsql_version="$("$gsql_bin" --version)"
sed -n '1p' <<< "$gsql_version"
"$gsql_bin" -X -d "${MVP_DB:-postgres}" -p "${MVP_PORT:-37000}" \
  -t -A -c 'SELECT 1;' >/dev/null
echo "数据库连接: OK"
echo "数据库性能参数:"
"$gsql_bin" -X -d "${MVP_DB:-postgres}" -p "${MVP_PORT:-37000}" -t -A -F '|' -c \
  "SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_process_memory','shared_buffers','work_mem','enable_thread_pool','thread_pool_attr','enable_dynamic_workload','use_workload_manager') ORDER BY name;" || true
echo "Catalog C 函数实际绑定:"
"$gsql_bin" -X -d "${MVP_DB:-postgres}" -p "${MVP_PORT:-37000}" \
  -c "SELECT p.proname, p.probin FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='iceberg_catalog' AND p.proname='create_table';"

gausshome="${MVP_GAUSSHOME:-${GAUSSHOME:-}}"
if [[ -n "$gausshome" && -d "$gausshome" ]]; then
  echo "GAUSSHOME bridge 候选文件及 SHA-256:"
  bridge_count=0
  while IFS= read -r bridge_file; do
    bridge_count=$((bridge_count + 1))
    sha256sum "$bridge_file"
  done < <(find "$gausshome" -maxdepth 5 -type f \
    \( -name 'rustbridge.so' -o -name '*iceberg*rust*bridge*.so' \) | sort)
  if [[ "$bridge_count" -eq 0 ]]; then
    echo "WARN: GAUSSHOME 下未发现 bridge .so" >&2
  fi
  echo "GAUSSHOME Catalog 命名安装副本及 SHA-256:"
  catalog_count=0
  while IFS= read -r catalog_file; do
    catalog_count=$((catalog_count + 1))
    sha256sum "$catalog_file"
  done < <(find "$gausshome" -maxdepth 6 -type f -name 'iceberg_catalog.so' | sort)
  if [[ "$catalog_count" -eq 0 ]]; then
    echo "WARN: GAUSSHOME 下未发现命名为 iceberg_catalog.so 的安装副本" >&2
  fi
else
  echo "WARN: 未配置有效的 MVP_GAUSSHOME，未检查已安装 bridge 副本" >&2
fi

echo "前置检查通过。"
echo "预检记录: $state_dir/preflight.log"
