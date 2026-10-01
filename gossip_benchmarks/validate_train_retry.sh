#!/usr/bin/env bash
# Focused regressions and matched normal/failure runs; no native rebuild.
set -euo pipefail
cd "$(dirname "$0")/.."
export RAY_TRAIN_V2_ENABLED=1
python -m pytest -q \
  python/ray/tests/test_train_selective_retry.py \
  python/ray/tests/test_selective_xgboost_active.py \
  python/ray/tests/test_selective_xgboost_boundary.py \
  python/ray/tests/test_fixed_r_train_comparison.py
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$result_root" "$TMPDIR"
for directory in "$result_root" "$TMPDIR"; do
  case "$(stat -f -c %T "$directory")" in
    tmpfs|ramfs) echo "Use disk-backed result and temporary directories." >&2; exit 2 ;;
  esac
done
export RAY_TMPDIR="$TMPDIR"
result_dir="$(mktemp -d "$result_root/train-retry.XXXXXX")"
exec python gossip_benchmarks/run_train_retry_comparison.py \
  --result-directory "$result_dir" --output "$result_root/train-retry-comparison.json" "$@"
