#!/usr/bin/env bash
# 从模板创建 MVP 或 perf 配置，并按当前 shell 环境补充可确定的路径。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
profile="${1:-}"
provider="${2:-}"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"

case "$profile" in
  mvp) template="$root_dir/config/mvp.env.example" ;;
  perf) template="$root_dir/config/perf.env.example" ;;
  *)
    echo "Usage: bash bin/init-env.sh <mvp|perf> [spark|pyiceberg|rust|bridge|all]" >&2
    exit 2
    ;;
esac
if [[ -n "$provider" && "$provider" != "all" && "$provider" != "spark" && \
      "$provider" != "pyiceberg" && "$provider" != "rust" && "$provider" != "bridge" ]]; then
  echo "Usage: bash bin/init-env.sh <mvp|perf> [spark|pyiceberg|rust|bridge|all]" >&2
  exit 2
fi

read_profile() {
  sed -n 's/^[[:space:]]*\(export[[:space:]]\+\)\?MVP_CONFIG_PROFILE=//p' "$1" \
    | tail -1 | tr -d '[:space:]'
}

replace_value() {
  local key="$1"
  local value="$2"
  local tmp_file
  tmp_file="$(mktemp "${env_file}.XXXXXX")"
  awk -v key="$key" -v value="$value" '
    $0 ~ "^[[:space:]]*(export[[:space:]]+)?" key "=" {
      print "export " key "=" value
      found=1
      next
    }
    { print }
    END { if (!found) print "export " key "=" value }
  ' "$env_file" > "$tmp_file"
  chmod --reference="$env_file" "$tmp_file"
  mv "$tmp_file" "$env_file"
}

if [[ -f "$env_file" ]]; then
  current_profile="$(read_profile "$env_file")"
  if [[ "$current_profile" != "$profile" ]]; then
    echo "ERROR: $env_file 的 MVP_CONFIG_PROFILE=${current_profile:-<未配置>}，期望 $profile" >&2
    echo "ERROR: 请先备份或移走现有 mvp.env，再重新初始化配置。" >&2
    exit 1
  fi
  echo "复用现有 $profile 配置: $env_file"
else
  if [[ "$provider" == "pyiceberg" || "$provider" == "all" ]] && \
      [[ -z "${MVP_PYTHON_BIN:-}" && ! -x "$root_dir/.venv/bin/python" ]]; then
    if compgen -G "$root_dir/wheelhouse/*.whl" >/dev/null && \
        [[ -f "$root_dir/wheelhouse/SHA256SUMS" ]]; then
      echo "检测到已校验 wheelhouse，正在创建锁定 PyIceberg venv。"
      bash "$root_dir/bin/install-pyiceberg-offline.sh"
    else
      echo "WARN: 缺少包内 PyIceberg venv 和完整 wheelhouse，当前无法自动安装。" >&2
      echo "WARN: 联网同构主机先执行 bin/download-pyiceberg-wheelhouse.sh，目标机再执行 bin/install-pyiceberg-offline.sh。" >&2
    fi
  fi
  cp "$template" "$env_file"

  # 已由调用 shell 明确提供的值优先于模板占位值。
  for key in \
    JAVA_HOME SPARK_HOME ICEBERG_SPARK_RUNTIME_JAR MVP_SPARK_MASTER \
    MVP_SPARK_DRIVER_MEMORY MVP_PYTHON_BIN MVP_BRIDGE_SOURCE MVP_BRIDGE_BATCH_ROWS \
    MVP_CARGO_BIN MVP_GSQL_BIN MVP_DB MVP_PORT MVP_GAUSSHOME \
    MVP_WAREHOUSE_DIR MVP_NAMESPACE MVP_TABLE MVP_VECTOR_TYPE MVP_TARGET_FILE_SIZE_BYTES \
    MVP_ALLOW_NON_AARCH64; do
    value="${!key:-}"
    if [[ -n "$value" ]]; then
      replace_value "$key" "$value"
    fi
  done
  if [[ -z "${MVP_PYTHON_BIN:-}" && -x "$root_dir/.venv/bin/python" ]]; then
    replace_value MVP_PYTHON_BIN "$root_dir/.venv/bin/python"
  fi
  echo "已创建 $profile 配置: $env_file"
fi

echo "请核对 mvp.env 中的 producer、GAUSSHOME/gsql、warehouse、namespace 和 table。"
if [[ -n "$provider" ]]; then
  MVP_ENV_FILE="$env_file" bash "$root_dir/bin/preflight.sh" "$provider"
else
  echo "环境检查: bash bin/preflight.sh <spark|pyiceberg|rust|bridge|all>"
fi
