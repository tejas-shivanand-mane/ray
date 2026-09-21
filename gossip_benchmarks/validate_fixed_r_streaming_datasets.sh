#!/usr/bin/env bash
# Run from the ray-dev environment AFTER rebuilding this checkout's native Ray.
set -euo pipefail
if [[ $# -gt 1 || ( $# -eq 1 && "${1:-}" != "--benchmarks-only" ) ]]; then
  echo "Usage: $0 [--benchmarks-only]" >&2
  exit 2
fi
cd "$(dirname "$0")/.."
result_dir="${RAY_RECOVERY_OUTPUT_DIR:-/tmp/fixed-r-streaming-datasets}"
mkdir -p "$result_dir"

if [[ "${1:-}" != "--benchmarks-only" ]]; then
  bazel test //src/ray/common/streaming_recovery:streaming_recovery_test \
    //src/ray/core_worker/tests:task_manager_test --test_output=errors

  python -m pytest -q --tb=long \
    python/ray/tests/test_streaming_recovery_consumer.py \
    python/ray/tests/test_streaming_recovery_submission.py \
    python/ray/tests/test_streaming_recovery_owner_loss.py \
    python/ray/tests/test_fixed_r_automatic_data.py \
    python/ray/tests/test_fixed_r_streaming_data.py
fi

TEST_OUTPUT_JSON="$result_dir/worker-scaling.json" \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  --worker-type tasks --num-workers 4 --blocks-per-worker 4 \
  --num-scalar-cols 128 --num-array-cols 32 --num-operators 2 \
  --recovery-plan dataset --recovery-mode suite --recovery-output-mode streaming \
  --recovery-failure-stage map --recovery-failure-operator 1 \
  --local-executor-nodes 2 --local-object-store-mb 512 --recovery-timeout-s 180

TEST_OUTPUT_JSON="$result_dir/backpressure.json" \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer --recovery-plan runtime --recovery-mode suite \
  --num-input-blocks 16 --output-batches-per-input-batch 8 \
  --output-batch-rows 32 --output-row-bytes 1048576 --consumer-sleep-s 0.1 \
  --local-executor-nodes 2 --local-object-store-mb 512 --recovery-timeout-s 180
