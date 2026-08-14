#!/usr/bin/env bash
# 编排当前数据集 IVF-Flat、IVF-PQ 和全扫的代表性性能测试。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
mode="${1:-fresh}"
provider="${2:-pyiceberg}"

if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  echo "ERROR: 先执行 bash bin/init-env.sh perf $provider，并按提示补全配置。" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"
dataset="${MVP_DATASET:-sift1m}"
state_dir="${MVP_STATE_DIR:-$root_dir/state}"
if [[ "${MVP_CONFIG_PROFILE:-}" != "perf" ]]; then
  echo "ERROR: run-perf.sh 要求 MVP_CONFIG_PROFILE=perf" >&2
  echo "ERROR: 请使用 config/perf.env.example 重新创建 mvp.env。" >&2
  exit 1
fi
if [[ "$mode" != "fresh" && "$mode" != "reuse" ]]; then
  echo "Usage: bash bin/run-perf.sh [fresh|reuse] [spark|pyiceberg|rust]" >&2
  exit 2
fi
if [[ "$provider" != "spark" && "$provider" != "pyiceberg" && "$provider" != "rust" ]]; then
  echo "Usage: bash bin/run-perf.sh [fresh|reuse] [spark|pyiceberg|rust]" >&2
  exit 2
fi
if [[ "$dataset" == "gist1m" && "$provider" != "pyiceberg" ]]; then
  echo "ERROR: GIST1M 首版仅支持 PyIceberg 供数" >&2
  exit 2
fi

perf_k="${MVP_PERF_K:-10,100}"
perf_dop="${MVP_PERF_DOP:-1,8}"
perf_nq="${MVP_PERF_NQ:-100}"
perf_rounds="${MVP_PERF_ROUNDS:-1}"
perf_warmup="${MVP_PERF_WARMUP:-5}"
sampling="${MVP_QUERY_SAMPLING:-equidistant}"
nprobe="${MVP_NPROBE:-10}"

run_matrix() {
  local mode_name="$1"
  local output_dir="$2"
  local -a matrix_command=(
    python3 "$root_dir/bin/run-matrix.py"
    --modes "$mode_name"
    --k "$perf_k"
    --dop "$perf_dop"
    --nq "$perf_nq"
    --rounds "$perf_rounds"
    --warmup "$perf_warmup"
    --query-sampling "$sampling"
    --output-dir "$output_dir"
  )
  if [[ "$mode_name" == "index" ]]; then
    matrix_command+=(--nprobe "$nprobe")
  fi
  "${matrix_command[@]}"
}

run_steps() {
  echo "性能测试数据集: $dataset；模式: $mode；供数路径: $provider"
  echo "代表性矩阵: K=$perf_k, DOP=$perf_dop, nq=$perf_nq, rounds=$perf_rounds, sampling=$sampling"
  bash "$root_dir/bin/preflight.sh" "$provider"
  bash "$root_dir/bin/deploy.sh"

  if [[ "$mode" == "fresh" ]]; then
    bash "$root_dir/bin/clean.sh" all
    bash "$root_dir/bin/supply-data.sh" "$provider"
    bash "$root_dir/bin/register-table.sh"
  else
    bash "$root_dir/bin/clean.sh" results
    bash "$root_dir/bin/clean.sh" index
  fi

  bash "$root_dir/bin/verify-table.sh"
  bash "$root_dir/bin/configure-index.sh" flat
  bash "$root_dir/bin/build-index.sh"
  run_matrix index "$state_dir/perf/flat"

  bash "$root_dir/bin/clean.sh" index
  bash "$root_dir/bin/configure-index.sh" pq
  bash "$root_dir/bin/build-index.sh"
  run_matrix index "$state_dir/perf/pq"

  bash "$root_dir/bin/clean.sh" index
  run_matrix fullscan "$state_dir/perf/fullscan"
  echo "一键性能测试完成；当前未保留索引，索引配置保持 PQ。"
}

run_log="$(mktemp "/tmp/${dataset}-perf.XXXXXX.log")"
set +e
(set -euo pipefail; run_steps) 2>&1 | tee "$run_log"
status="${PIPESTATUS[0]}"
set -e
mkdir -p "$state_dir"
cp "$run_log" "$state_dir/run-perf.log"
rm -f "$run_log"
if [[ "$status" -ne 0 ]]; then
  echo "ERROR: 一键性能测试失败，日志见 $state_dir/run-perf.log" >&2
  exit "$status"
fi
echo "总日志: $state_dir/run-perf.log"
