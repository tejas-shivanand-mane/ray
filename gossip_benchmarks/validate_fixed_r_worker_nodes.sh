#!/usr/bin/env bash
# Reuse the single-node validation entry point for two sequential node losses.
set -euo pipefail
cd "$(dirname "$0")/.."
exec bash gossip_benchmarks/validate_fixed_r_worker_node.sh \
  --worker-node-failures 2 --checkpoint-frequency 3 --failure-after-round 3 \
  --max-failures 2 "$@"
