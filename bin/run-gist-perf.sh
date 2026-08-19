#!/usr/bin/env bash
# 使用独立配置和状态目录运行 GIST1M Flat、PQ、FullScan 基线。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/gist.env}"
mode="${1:-fresh}"
provider="${2:-spark}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  echo "ERROR: 先复制 config/gist-perf.env.example 为 gist.env 并修改绝对路径" >&2
  exit 1
fi

export MVP_ENV_FILE="$env_file"
export MVP_STATE_DIR="${MVP_STATE_DIR:-$root_dir/state/gist1m}"
# shellcheck source=/dev/null
source "$env_file"
if [[ "${MVP_DATASET:-}" != "gist1m" ]]; then
  echo "ERROR: GIST 入口要求 MVP_DATASET=gist1m" >&2
  exit 1
fi
if [[ "${MVP_CONFIG_PROFILE:-}" != "perf" ]]; then
  echo "ERROR: GIST 入口要求 MVP_CONFIG_PROFILE=perf" >&2
  exit 1
fi
for contract in \
  "MVP_VECTOR_DIM:${MVP_VECTOR_DIM:-}:960" \
  "MVP_ROW_COUNT:${MVP_ROW_COUNT:-}:1000000" \
  "MVP_QUERY_COUNT:${MVP_QUERY_COUNT:-}:1000" \
  "MVP_GT_K:${MVP_GT_K:-}:100" \
  "MVP_PARTITION_BUCKETS:${MVP_PARTITION_BUCKETS:-}:32"; do
  IFS=: read -r key actual expected <<< "$contract"
  if [[ "$actual" != "$expected" ]]; then
    echo "ERROR: GIST 基线要求 $key=$expected，实际为 ${actual:-<未配置>}" >&2
    exit 1
  fi
done

if [[ "$provider" != "spark" && "$provider" != "pyiceberg" && "$provider" != "bridge" ]]; then
  echo "Usage: bash bin/run-gist-perf.sh [fresh|reuse] [spark|pyiceberg|bridge]" >&2
  exit 2
fi

exec bash "$root_dir/bin/run-perf.sh" "$mode" "$provider"
