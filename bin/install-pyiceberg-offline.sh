#!/usr/bin/env bash
# 仅从包内 wheelhouse 创建隔离 PyIceberg venv。
set -euo pipefail

root_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
base_python="${MVP_BASE_PYTHON:-python3}"
venv_dir="${MVP_PYICEBERG_VENV:-$root_dir/.venv}"
wheelhouse="$root_dir/wheelhouse"
requirements="$root_dir/requirements/pyiceberg-lock.txt"

if ! compgen -G "$wheelhouse/*.whl" >/dev/null; then
  echo "ERROR: $wheelhouse 中没有 wheel；请先在同架构、同 Python ABI 的联网机器下载" >&2
  exit 1
fi
if [[ -f "$wheelhouse/SHA256SUMS" ]]; then
  (cd "$wheelhouse" && sha256sum -c SHA256SUMS)
else
  echo "ERROR: 缺少 $wheelhouse/SHA256SUMS" >&2
  exit 1
fi

if [[ ! -x "$venv_dir/bin/python" ]]; then
  env -u LD_LIBRARY_PATH "$base_python" -m venv "$venv_dir"
fi
env -u LD_LIBRARY_PATH "$venv_dir/bin/python" -m pip install \
  --disable-pip-version-check \
  --no-index \
  --find-links "$wheelhouse" \
  --requirement "$requirements"
env -u LD_LIBRARY_PATH "$venv_dir/bin/python" -m pip check
env -u LD_LIBRARY_PATH "$venv_dir/bin/python" - <<'PY'
import pyarrow
import pyiceberg
import sqlalchemy

print(
    f"PyIceberg 离线环境就绪: pyiceberg={pyiceberg.__version__} "
    f"pyarrow={pyarrow.__version__} sqlalchemy={sqlalchemy.__version__}"
)
PY
echo "MVP_PYTHON_BIN=$venv_dir/bin/python"
