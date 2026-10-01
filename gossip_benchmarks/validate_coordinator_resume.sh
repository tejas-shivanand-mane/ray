#!/usr/bin/env bash
# Local correctness gate only. No benchmark or plotting is run by this script.
set -euo pipefail
cd "$(dirname "$0")/.."
export RAY_TRAIN_V2_ENABLED=1
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$TMPDIR"
export RAY_TMPDIR="$TMPDIR"
exec python -m pytest -q python/ray/tests/test_resumable_split.py "$@"
