#!/usr/bin/env bash
# 兼容既有 SIFT 命令，转发到公共 Spark producer。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export MVP_ENV_FILE="${MVP_ENV_FILE:-$root_dir/mvp.env}"
exec bash "$root_dir/bin/seed-spark.sh"
