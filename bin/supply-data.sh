#!/usr/bin/env bash
# 将 provider 参数分派到现有的三条供数路径。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
provider="${1:-}"

case "$provider" in
  spark)
    script=seed-sift1m.sh
    ;;
  pyiceberg)
    script=seed-sift1m-pyiceberg.sh
    ;;
  rust)
    script=seed-sift1m-rust.sh
    ;;
  *)
    echo "Usage: bash bin/supply-data.sh <spark|pyiceberg|rust>" >&2
    exit 2
    ;;
esac

echo "开始供数: $provider"
bash "$root_dir/bin/$script"
