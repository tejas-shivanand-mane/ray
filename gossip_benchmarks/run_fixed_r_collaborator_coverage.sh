#!/usr/bin/env bash
# One bounded coverage run on the 16-thread / 32-GiB development machine.
# Use the existing ray-dev environment and its native Fixed-R source build.
set -euo pipefail
cd "$(dirname "$0")/.."
profile=full
if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [--failed-only|--training-only|--actors-only|--xgboost-only|--xgboost-multi-only|--xgboost-long-training-only|--entrypoints-only|--entrypoints-resume]" >&2; exit 2
fi
case "${1:-}" in
  "") ;;
  --failed-only) profile=failed-only ;;
  --training-only) profile=training-only ;;
  --actors-only) profile=actors-only ;;
  --xgboost-only) profile=xgboost-only ;;
  --xgboost-multi-only) profile=xgboost-multi-only ;;
  --xgboost-long-training-only) profile=xgboost-long-training-only ;;
  --entrypoints-only) profile=entrypoints-only ;;
  --entrypoints-resume) profile=entrypoints-resume ;;
  *) echo "Usage: $0 [--failed-only|--training-only|--actors-only|--xgboost-only|--xgboost-multi-only|--xgboost-long-training-only|--entrypoints-only|--entrypoints-resume]" >&2; exit 2 ;;
esac
result_root="${RAY_RECOVERY_OUTPUT_DIR:-$HOME/ray-coverage}"
mkdir -p "$result_root"
case "$(stat -f -c %T "$result_root")" in
  tmpfs|ramfs) echo "Set RAY_RECOVERY_OUTPUT_DIR to a disk-backed directory." >&2; exit 2 ;;
esac
result_dir="$(mktemp -d "$result_root/run.XXXXXX")"
# Keep socket paths short even if the result directory has a long pathname.
# Ray appends its session/socket directories to this root.
export TMPDIR="${RAY_RECOVERY_TEMP_DIR:-$HOME/raytmp}"
mkdir -p "$TMPDIR"
case "$(stat -f -c %T "$TMPDIR")" in
  tmpfs|ramfs) echo "Set RAY_RECOVERY_TEMP_DIR to a disk-backed directory." >&2; exit 2 ;;
esac
export RAY_TMPDIR="$TMPDIR"
if [[ "$profile" == entrypoints-only || "$profile" == entrypoints-resume ]]; then
  resume_args=()
  if [[ "$profile" == entrypoints-resume ]]; then
    # Preserve the source report before writing the combined resumed result.
    cp "$result_root/coverage-entrypoints.json" "$result_dir/previous-entrypoints.json"
    resume_args=(--resume-from "$result_dir/previous-entrypoints.json")
  fi
  exec env TEST_OUTPUT_JSON="$result_root/coverage-entrypoints.json" \
    python gossip_benchmarks/run_fixed_r_entrypoint_coverage.py --result-directory "$result_dir" "${resume_args[@]}"
fi
failed=0
backpressure=(
  --recovery-plan runtime --local-executor-nodes 8 --local-object-store-mb 512
  --num-input-blocks 16 --output-batches-per-input-batch 8
  --output-batch-rows 32 --output-row-bytes 1048576 --consumer-sleep-s 0.1
  --recovery-timeout-s 120
)
worker=(
  --worker-type tasks --num-workers 8 --blocks-per-worker 4
  --num-scalar-cols 128 --num-array-cols 32
  --recovery-plan dataset --recovery-output-mode streaming
  --local-executor-nodes 8 --local-object-store-mb 512 --recovery-timeout-s 120
)
training_mode=suite
chain_mode=(--recovery-mode suite)
summary_name=coverage.json
if [[ "$profile" == failed-only ]]; then
  # The five gaps in the uploaded coverage report; retain the six passed cases.
  training_mode=fixed_r_head_failure
  chain_mode=(--recovery-mode fixed_r_head_failure --recovery-failure-stage map)
  summary_name=coverage-retry.json
fi
if [[ "$profile" == training-only ]]; then
  training_mode=fixed_r_head_failure
  summary_name=coverage-training.json
fi

if [[ "$profile" == xgboost-only || "$profile" == xgboost-multi-only || "$profile" == xgboost-long-training-only ]]; then
summary_name=coverage-xgboost.json
train_workers=1
train_timeout=120
train_args=()
if [[ "$profile" == xgboost-multi-only ]]; then
  summary_name=coverage-xgboost-multi.json
  train_workers=2
fi
if [[ "$profile" == xgboost-long-training-only ]]; then
  summary_name=coverage-xgboost-long-training.json
  train_workers=2
  train_timeout=300
  train_args=(--num-boost-round 100 --failure-phase boosting --failure-after-round 50)
  # Match the longer no-failure measurement's native thread limits.
  export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
  echo "One 100-round run with two workers; fail head after round 50; 300-second case budget."
fi
TEST_OUTPUT_JSON="$result_dir/xgboost.json" \
RAY_TRAIN_V2_ENABLED=1 RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S=30 \
RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S=30 RAY_TRAIN_COLLECTIVE_TIMEOUT_S=30 \
python gossip_benchmarks/run_fixed_r_train_coverage.py \
  --result-directory "$result_dir/xgboost-workload" --recovery-timeout-s "$train_timeout" \
  --num-train-workers "$train_workers" "${train_args[@]}" || failed=1
elif [[ "$profile" == actors-only ]]; then
summary_name=coverage-actors.json
# Original actor UDF, pool and wide schema; reduce only local scale. Protect the
# reads and verify the same initialized actor processes survive head replacement.
TEST_OUTPUT_JSON="$result_dir/worker-scaling-actors.json" \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  --worker-type actors --num-workers 8 --num-operators 2 --blocks-per-worker 4 \
  --num-scalar-cols 200 --num-array-cols 400 \
  --recovery-plan dataset --recovery-output-mode streaming \
  --local-executor-nodes 4 --local-object-store-mb 512 --recovery-timeout-s 120 \
  --recovery-mode fixed_r_head_failure --recovery-failure-stage read \
  --recovery-head-timing early || failed=1
else
# New workload first: copy + enrolled no-failure + asynchronous producer failure.
TEST_OUTPUT_JSON="$result_dir/training-prefetch.json" \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  "${backpressure[@]}" --case training-prefetch --num-trainers 8 --prefetch-batches 2 \
  --recovery-mode "$training_mode" --recovery-head-timing middle || failed=1

# Cover protected reads and a two-stage task map chain in one fresh-cluster suite.
if [[ "$profile" != training-only ]]; then
TEST_OUTPUT_JSON="$result_dir/worker-scaling-chain.json" \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  "${worker[@]}" --num-operators 2 --recovery-failure-operator 1 \
  "${chain_mode[@]}" --recovery-head-timing early || failed=1

# The original single-stage task variant uses an unbounded pool.
if [[ "$profile" == full ]]; then
TEST_OUTPUT_JSON="$result_dir/worker-scaling-single.json" \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  "${worker[@]}" --num-operators 1 --recovery-failure-operator 0 \
  --recovery-mode fixed_r_head_failure --recovery-failure-stage map \
  --recovery-head-timing middle || failed=1
fi

# Do not repeat the already-passing backpressure baselines or paused injections.
TEST_OUTPUT_JSON="$result_dir/backpressure-async.json" \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  "${backpressure[@]}" --case fast-producer-slow-consumer \
  --recovery-mode fixed_r_head_failure --recovery-head-timing suite || failed=1
fi
fi

python - "$result_dir" "$result_root/$summary_name" "$profile" <<'PY' || failed=1
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
profile = sys.argv[3]
names = {
    "actors-only": ["worker-scaling-actors"], "xgboost-only": ["xgboost"],
    "xgboost-multi-only": ["xgboost"],
    "xgboost-long-training-only": ["xgboost"],
}.get(profile, ["training-prefetch"])
if profile in ("full", "failed-only"):
    names.extend(["worker-scaling-chain", "backpressure-async"])
if profile == "full":
    names.append("worker-scaling-single")
expected_count = {
    "full": 11, "failed-only": 5, "training-only": 1, "actors-only": 1, "xgboost-only": 1,
    "xgboost-multi-only": 1,
    "xgboost-long-training-only": 1,
}[profile]
summary = {"result_directory": str(directory), "profile": profile,
           "expected_case_count": expected_count, "cases": {}, "missing_results": []}
for name in names:
    path = directory / (name + ".json")
    if not path.exists():
        summary["missing_results"].append(name)
        continue
    try:
        summary["cases"].update(json.loads(path.read_text()))
    except (ValueError, OSError) as exc:
        summary["missing_results"].append(f"{name}: {exc}")
summary["failed_cases"] = [key for key, value in summary["cases"].items()
                           if value.get("validation_status") != "passed"]
summary["validation_status"] = (
    "passed" if len(summary["cases"]) == expected_count and not summary["failed_cases"]
    and not summary["missing_results"] else "failed"
)
output = Path(sys.argv[2])
temporary = output.with_suffix(".json.tmp")
temporary.write_text(json.dumps(summary, indent=2))
temporary.replace(output)
for key, value in summary["cases"].items():
    print(f"{value.get('validation_status', 'missing'):>7}  {key}")
print(f"Combined result: {output}")
sys.exit(0 if summary["validation_status"] == "passed" else 1)
PY
printf 'Individual results: %s\n' "$result_dir"
exit "$failed"
