#!/usr/bin/env bash
set -u

BENCH="${BENCH:-gossip_benchmarks/22_succession_vs_lazy_baseline_v2.py}"
OUTROOT="${OUTROOT:-gossip_benchmarks/results/22_baseline_opt_bisect}"
mkdir -p "$OUTROOT"

OPTS=(
  RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS
  RAY_RECOVERY_BASELINE_COMPACT_METADATA
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY
  RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE
  RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST
  RAY_RECOVERY_BASELINE_FAST_RECEIVER
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION
  RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC
  RAY_RECOVERY_BASELINE_BATCH_SWAP
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION
  RAY_RECOVERY_TASKMANAGER_PIN
)

clear_opts() {
  for v in "${OPTS[@]}"; do
    unset "$v" || true
  done
  export RAY_RECOVERY_PROFILING=1
  unset RAY_RECOVERY_CERTIFICATE_ADMISSION || true
}

run_case() {
  local name="$1"
  shift
  clear_opts
  while [[ $# -gt 0 ]]; do
    export "$1"
    shift
  done

  local json="$OUTROOT/${name}.json"
  rm -f "$json"

  echo
  echo "================================================================"
  echo "CASE: $name"
  echo "================================================================"
  env | grep -E '^RAY_RECOVERY_(BASELINE|TASKMANAGER_PIN|PROFILING)' | sort || true

  python "$BENCH" _single-run \
    --single-method witness_baseline \
    --single-borrower-count 1 \
    --single-padding-name 1KiB \
    --single-padding-bytes 1024 \
    --single-repetition 1 \
    --single-output-json "$json" \
    --warmup-seconds 0 \
    --duration-seconds 1 \
    --inflight 16 \
    --state-task-count 1 \
    --protection-timeout-seconds 2 \
    --profile-quiescence-timeout-seconds 5 \
    --profile-stable-seconds 0.2 \
    --drain-timeout-seconds 20

  local rc=$?
  if [[ $rc -eq 0 && -f "$json" ]]; then
    echo "RESULT: PASS ($name)"
  else
    echo "RESULT: FAIL ($name), exit=$rc"
  fi
  return 0
}

# Control: must pass.
run_case "00_original"

# Low-risk normal-path/local CPU optimizations only.
run_case "01_safe_cpu" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1

# Add transport batching, but not request Swap.
run_case "02_add_batching" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1 \
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING=1

# Add moving queued logical updates into the batch.
run_case "03_add_batch_swap" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1 \
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING=1 \
  RAY_RECOVERY_BASELINE_BATCH_SWAP=1

# Serialize-once, retaining the manifest inside the serialized TaskSpec.
run_case "04_add_serialize_once" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1 \
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING=1 \
  RAY_RECOVERY_BASELINE_BATCH_SWAP=1 \
  RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE=1

# Move/Swap parsed TaskSpec into witness storage.
run_case "05_add_witness_move" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1 \
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING=1 \
  RAY_RECOVERY_BASELINE_BATCH_SWAP=1 \
  RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE=1 \
  RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC=1

# Separate manifest storage is deliberately added last because it changes the
# in-memory representation at the holder while preserving replay semantics.
run_case "06_add_separate_manifest" \
  RAY_RECOVERY_BASELINE_COMPACT_METADATA=1 \
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY=1 \
  RAY_RECOVERY_BASELINE_FAST_RECEIVER=1 \
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION=1 \
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION=1 \
  RAY_RECOVERY_TASKMANAGER_PIN=1 \
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING=1 \
  RAY_RECOVERY_BASELINE_BATCH_SWAP=1 \
  RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE=1 \
  RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC=1 \
  RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST=1

# Exact combined configuration that failed.
run_case "07_all_switch" \
  RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS=1

echo
echo "==================== SUMMARY ===================="
for f in "$OUTROOT"/*.json; do
  [[ -e "$f" ]] || continue
  echo "PASS: $(basename "$f" .json)"
done
echo "Any CASE without a JSON file failed."
