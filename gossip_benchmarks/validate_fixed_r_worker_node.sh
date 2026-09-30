#!/usr/bin/env bash
# One local command: regressions, then matched node-loss and prediction checks.
set -euo pipefail
cd "$(dirname "$0")/.."
python -m pytest -q \
  python/ray/tests/test_fixed_r_train_comparison.py \
  python/ray/tests/test_fixed_r_train_coverage.py \
  python/ray/tests/test_fixed_r_worker_node.py
# Use eight input blocks for a small smoke case with a default 120s budget.
# Larger performance comparisons remain available through --input-blocks.
bash gossip_benchmarks/run_fixed_r_train_comparison.sh \
  --scenario worker-node --include-prediction --input-blocks 8 "$@"
