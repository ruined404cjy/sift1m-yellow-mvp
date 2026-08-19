#!/usr/bin/env bash
# 将 provider 参数分派到当前数据集的供数入口。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${MVP_ENV_FILE:-$root_dir/mvp.env}"
provider="${1:-}"
if [[ ! -f "$env_file" ]]; then
  echo "ERROR: 缺少 $env_file" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$env_file"
dataset="${MVP_DATASET:-sift1m}"

case "$dataset:$provider" in
  sift1m:spark)
    script=seed-sift1m.sh
    ;;
  sift1m:pyiceberg)
    script=seed-sift1m-pyiceberg.sh
    ;;
  sift1m:rust)
    script=seed-sift1m-rust.sh
    ;;
  sift1m:bridge|gist1m:bridge)
    script=seed-bridge.sh
    ;;
  gist1m:spark)
    script=seed-spark.sh
    ;;
  gist1m:pyiceberg)
    script=seed-gist1m-pyiceberg.sh
    ;;
  *)
    echo "ERROR: $dataset 不支持供数路径 $provider" >&2
    exit 2
    ;;
esac

echo "开始供数: dataset=$dataset, provider=$provider"
bash "$root_dir/bin/$script"
