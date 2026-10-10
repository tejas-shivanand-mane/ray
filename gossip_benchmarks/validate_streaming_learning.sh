#!/usr/bin/env bash
# Run locally in ray-dev. Plot separately after inspecting the JSON.
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
comparison=fixed-r
previous=""
for argument in "$@"; do
  if [[ "$previous" == "--comparison" ]]; then comparison="$argument"; fi
  case "$argument" in --comparison=*) comparison="${argument#--comparison=}" ;; esac
  previous="$argument"
done
if [[ "$comparison" == "checkpoints" ]]; then
  python -m pytest -q python/ray/tests/test_streaming_learning_comparison.py
else
python -m pytest -q \
  python/ray/tests/test_train_selective_retry.py \
  python/ray/tests/test_fashion_training_comparison.py \
  python/ray/tests/test_streaming_recovery_timing.py \
  python/ray/tests/test_streaming_recovery_helper_reuse.py \
  python/ray/tests/test_fixed_r_automatic_data.py::test_unschedulable_helper_only_fails_over_before_begin \
  python/ray/tests/test_streaming_learning_comparison.py
fi
result_dir="$(mktemp -d "$result_root/streaming-learning.XXXXXX")"
exec python gossip_benchmarks/run_streaming_learning_comparison.py \
  --result-directory "$result_dir" --output "$result_root/streaming-learning-comparison.json" "$@"
