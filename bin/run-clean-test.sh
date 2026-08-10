#!/usr/bin/env bash
# 编排从零供数或复用供数数据的 Flat、PQ、全表一键测试。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-}"
provider="${2:-}"
if [[ "$mode" != "fresh" && "$mode" != "reuse" ]]; then
  echo "Usage: bash bin/run-clean-test.sh <fresh|reuse> <spark|pyiceberg|rust>" >&2
  exit 2
fi
if [[ "$provider" != "spark" && "$provider" != "pyiceberg" && "$provider" != "rust" ]]; then
  echo "Usage: bash bin/run-clean-test.sh <fresh|reuse> <spark|pyiceberg|rust>" >&2
  exit 2
fi

run_steps() {
  echo "测试模式: $mode；供数路径: $provider"
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
  bash "$root_dir/bin/test-fullscan.sh"

  bash "$root_dir/bin/configure-index.sh" flat
  bash "$root_dir/bin/build-index.sh"
  bash "$root_dir/bin/test-index.sh" flat

  bash "$root_dir/bin/clean.sh" index
  bash "$root_dir/bin/configure-index.sh" pq
  bash "$root_dir/bin/build-index.sh"
  bash "$root_dir/bin/test-index.sh" pq
  echo "一键测试完成；当前保留 PQ 索引。"
}

run_log="$(mktemp /tmp/sift1m-clean-test.XXXXXX.log)"
set +e
(set -euo pipefail; run_steps) 2>&1 | tee "$run_log"
status="${PIPESTATUS[0]}"
set -e
mkdir -p "$root_dir/state"
cp "$run_log" "$root_dir/state/run-clean-test.log"
rm -f "$run_log"
if [[ "$status" -ne 0 ]]; then
  echo "ERROR: 一键测试失败，日志见 $root_dir/state/run-clean-test.log" >&2
  exit "$status"
fi
echo "总日志: $root_dir/state/run-clean-test.log"
