#!/usr/bin/env bash
# One command for the existing source build; no package installation or rebuild.
set -euo pipefail
cd "$(dirname "$0")/.."
python -m pytest -q \
  python/ray/tests/test_fixed_r_train_comparison.py \
  python/ray/tests/test_fixed_r_train_coverage.py \
  python/ray/tests/test_fixed_r_worker_node.py \
  python/ray/tests/test_selective_xgboost_boundary.py
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$result_root" "$TMPDIR"
for directory in "$result_root" "$TMPDIR"; do
  case "$(stat -f -c %T "$directory")" in
    tmpfs|ramfs) echo "Use disk-backed result and temporary directories." >&2; exit 2 ;;
  esac
done
export RAY_TMPDIR="$TMPDIR"
result_dir="$(mktemp -d "$result_root/selective-xgboost.XXXXXX")"
exec python gossip_benchmarks/run_selective_xgboost_comparison.py \
  --result-directory "$result_dir" --output "$result_root/selective-xgboost.json" "$@"
