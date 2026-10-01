#!/usr/bin/env bash
# Local validation only; plotting is a separate JSON-only command.
set -euo pipefail
cd "$(dirname "$0")/.."
export RAY_TRAIN_V2_ENABLED=1
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$result_root" "$TMPDIR"
for directory in "$result_root" "$TMPDIR"; do
  case "$(stat -f -c %T "$directory")" in
    tmpfs|ramfs) echo "Use disk-backed results and temporary storage." >&2; exit 2 ;;
  esac
done
export RAY_TMPDIR="$TMPDIR"
python -m pytest -q python/ray/tests/test_coordinator_training_comparison.py
result_dir="$(mktemp -d "$result_root/coordinator-training.XXXXXX")"
exec python gossip_benchmarks/run_coordinator_training_comparison.py \
  --result-directory "$result_dir" --output "$result_root/coordinator-training-comparison.json" "$@"
