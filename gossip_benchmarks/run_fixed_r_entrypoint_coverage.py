"""One acceptance pass through the three normal collaborator entry points.

The shared launcher owns recovery, supervision and runtime observations. These
case definitions supply only local workload sizes, paths and environment.
"""

import argparse
import json
import os
from pathlib import Path

from ray.experimental.recovery._launcher import run_script

ROOT = Path(__file__).resolve().parents[1]


def make_input(directory):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir()
    rng = np.random.default_rng(42)
    for index in range(32):
        values = rng.normal(size=(1024, 16)).astype(np.float32)
        columns = {f"feature_{i}": values[:, i] for i in range(16)}
        columns["labels"] = (values[:, 0] + values[:, 1] > 0).astype(np.int32)
        pq.write_table(pa.table(columns), directory / f"part-{index:04d}.parquet")


def validate_predictions(work):
    """Validate the normal benchmark's persisted artifacts outside its code."""
    import numpy as np
    import pyarrow.parquet as pq
    import xgboost as xgb

    checkpoints = sorted((work / "checkpoints").rglob("model.ubj"))
    if len(checkpoints) != 1:
        raise ValueError(f"Expected one final XGBoost checkpoint, found {len(checkpoints)}")
    model = xgb.Booster()
    model.load_model(str(checkpoints[0]))
    if model.num_boosted_rounds() != 10 or model.num_features() != 16:
        raise ValueError("The checkpoint has the wrong rounds or feature count")
    frame = pq.read_table(work / "input").to_pandas()
    predicted = pq.read_table(work / "predictions")
    if predicted.column_names != ["predictions"] or predicted.num_rows != len(frame):
        raise ValueError("Persisted predictions have the wrong schema or row count")
    values = predicted["predictions"].to_numpy()
    expected = model.predict(xgb.DMatrix(frame.drop("labels", axis=1)))
    if not np.isfinite(values).all():
        raise ValueError("Nonfinite prediction values")
    np.testing.assert_allclose(np.sort(values), np.sort(expected), rtol=1e-6, atol=1e-7)
    return {"validated_prediction_rows": len(values), "checkpoint_rounds": model.num_boosted_rounds()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--resume-from", type=Path)
    args = parser.parse_args()
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    work = directory / "xgboost-workload"
    work.mkdir()
    make_input(work / "input")
    cases = [
        ("backpressure", "release/nightly_tests/dataset/backpressure_benchmark.py", [
            "--case", "fast-producer-slow-consumer", "--num-input-blocks", "16",
            "--output-batches-per-input-batch", "8", "--output-batch-rows", "16",
            "--output-row-bytes", "1048576", "--consumer-sleep-s", "0.1",
        ]),
        ("worker-scaling-actors", "release/nightly_tests/dataset/worker_scaling_benchmark.py", [
            "--worker-type", "actors", "--num-workers", "8", "--num-operators", "2",
            "--blocks-per-worker", "4", "--num-scalar-cols", "200", "--num-array-cols", "400",
            "--output-dir", str(directory / "profiling"), "--skip-upload",
        ]),
        ("xgboost-multi", "release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py", [
            "xgboost", "--data-path", str(work / "input"), "--num-workers", "2",
            "--cpus-per-worker", "1", "--placement-strategy", "STRICT_SPREAD",
            "--storage-path", str(work / "checkpoints"),
            "--prediction-output-path", str(work / "predictions"),
            "--read-blocks", "32", "--small-blocks", "--disable-check",
        ]),
    ]
    results = {}
    output = Path(os.environ["TEST_OUTPUT_JSON"])
    previous = None
    if args.resume_from:
        previous = json.loads(args.resume_from.read_text())
        if previous.get("profile") != "entrypoints-only":
            parser.error("--resume-from must be an entrypoints-only report")
    reused = []

    def write_summary():
        summary = {
            "profile": "entrypoints-only", "cases": results,
            "expected_case_count": len(cases), "result_directory": str(directory),
            "retained_cases": reused,
            "retained_from": str(args.resume_from.resolve()) if args.resume_from else None,
            "validation_status": "passed" if len(results) == len(cases) and all(
                r["validation_status"] == "passed" for r in results.values()) else "failed",
        }
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(summary, indent=2))
        temporary.replace(output)

    for name, script, script_args in cases:
        prior = (previous or {}).get("cases", {}).get(name)
        if prior and prior.get("validation_status") == "passed":
            # Retain historical evidence only for the same workload arguments.
            # Output/data directories differ between fresh local runs.
            old_directory = prior.get("case_result_directory", previous["result_directory"])
            normalized_args = [
                str(directory) + arg[len(old_directory):]
                if arg.startswith(old_directory + "/") else arg
                for arg in prior.get("argv", [])
            ]
            if (prior.get("script", "").endswith("/" + script)
                    and normalized_args == script_args and prior.get("exit_code") == 0
                    and prior.get("original_head_processes_exited") is True):
                from ray.experimental.recovery._observe import validate

                validate(prior["observation"], require_replay=True)
                prior["case_result_directory"] = old_directory
                results[name] = prior
                reused.append(name)
                write_summary()
                print(f" retained  {name} (earlier report; not rerun)", flush=True)
                continue
        print(f"Running {name} (120-second processing deadline)", flush=True)
        result = run_script(
            ROOT / script, script_args, local=True, inject_head_failure=True,
            timeout_s=120, report=directory / f"{name}-recovery.json",
            env={
                "TEST_OUTPUT_JSON": str(directory / f"{name}-benchmark.json"),
                "RAY_TRAIN_V2_ENABLED": "1", "RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S": "30",
                "RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S": "30", "RAY_TRAIN_COLLECTIVE_TIMEOUT_S": "30",
                "PROFILER_MODE": "none", "PYSPY_ENABLED": "0", "PERF_PROFILING_ENABLED": "0",
                "GPU_MONITOR_ENABLED": "0", "NET_MONITOR_ENABLED": "0", "OBJECT_STORE_MONITOR_ENABLED": "0",
                # ReadRange is too short to reliably catch after two outputs.
                # Request failure upon admission; never pause the task or UDF.
                "RAY_RECOVERY_FAILURE_TRIGGER": (
                    "task-submission" if name == "worker-scaling-actors" else "output"
                ),
            },
        )
        if name == "xgboost-multi" and result["validation_status"] == "passed":
            try:
                result.update(validate_predictions(work))
            except Exception as exc:
                result.update(validation_status="failed", error_type=type(exc).__name__, error=str(exc))
        result["case_result_directory"] = str(directory)
        results[name] = result
        write_summary()
        print(f"{result['validation_status']:>7}  {name}")
        if result["validation_status"] != "passed":
            break  # Fail fast; do not spend minutes on later cases after a shared failure.
    print(f"Combined result: {output}")
    return 0 if len(results) == len(cases) and all(
        r["validation_status"] == "passed" for r in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
