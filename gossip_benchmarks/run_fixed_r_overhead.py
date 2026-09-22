"""Paired no-failure timings through the normal collaborator entry points.

Run with the existing source-built Ray. Each observation uses a fresh local
cluster. Native flags and Data recovery are OFF versus ON; no fault observer,
injection, replay validation or application monkeypatch is used.
"""

import argparse
import csv
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
import traceback

import ray
from ray.experimental.recovery import system_config
from ray.experimental.recovery._launcher import run_script
from ray.experimental.recovery._local import local_head_failure_cluster
from run_fixed_r_entrypoint_coverage import make_input, validate_predictions
from plot_fixed_r_overhead import require_matplotlib, render_report

ROOT = Path(__file__).resolve().parents[1]
CASES = ("backpressure", "worker-scaling-actors", "xgboost-single", "xgboost-multi")


def workload(name, directory, input_path, num_boost_round=10):
    if name == "backpressure":
        return "release/nightly_tests/dataset/backpressure_benchmark.py", [
            "--case", "fast-producer-slow-consumer", "--num-input-blocks", "16",
            "--output-batches-per-input-batch", "8", "--output-batch-rows", "16",
            "--output-row-bytes", "1048576", "--consumer-sleep-s", "0.1",
        ], 2048
    if name == "worker-scaling-actors":
        return "release/nightly_tests/dataset/worker_scaling_benchmark.py", [
            "--worker-type", "actors", "--num-workers", "8", "--num-operators", "2",
            "--blocks-per-worker", "4", "--num-scalar-cols", "200", "--num-array-cols", "400",
            "--output-dir", str(directory / "profiling"), "--skip-upload", "--skip-state-api-stats",
        ], 10304
    return "release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py", [
        "xgboost", "--data-path", str(input_path),
        "--num-workers", "1" if name == "xgboost-single" else "2",
        "--num-boost-round", str(num_boost_round),
        "--cpus-per-worker", "1", "--placement-strategy", "STRICT_SPREAD",
        "--storage-path", str(directory / "checkpoints"),
        "--prediction-output-path", str(directory / "predictions"),
        "--read-blocks", "32", "--small-blocks", "--disable-check",
    ], 32768


def timings(name, raw):
    if name.startswith("xgboost-"):
        result = {"training": raw["training_time"], "prediction": raw["prediction_time"]}
        result["total"] = result["training"] + result["prediction"]
    else:
        key = "fast-producer-slow-consumer" if name == "backpressure" else "worker_scaling"
        result = {"total": raw[key]["time"]}
        if name == "worker-scaling-actors" and raw[key]["num_rows"] != 10304:
            raise ValueError("Unexpected worker-scaling workload size")
        if name == "worker-scaling-actors" and raw[key].get("state_api_stats_enabled", True):
            raise ValueError("Overhead measurements must skip State API statistics in both modes")
    if any(not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0
           for v in result.values()):
        raise ValueError(f"Invalid benchmark timing: {result}")
    return result


def summarize(samples):
    rows = []
    for name in CASES:
        matched = {}
        for sample in samples:
            if sample["case"] == name and sample["status"] == "passed":
                matched.setdefault(sample["pair"], {})[sample["mode"]] = sample
        pairs = [pair for pair in matched.values() if set(pair) == {"off", "on"}]
        if not pairs:
            continue
        for phase in pairs[0]["off"]["benchmark_seconds"]:
            off = [pair["off"]["benchmark_seconds"][phase] for pair in pairs]
            on = [pair["on"]["benchmark_seconds"][phase] for pair in pairs]
            overhead = [100 * (b / a - 1) for a, b in zip(off, on)]
            count = pairs[0]["off"]["logical_rows"]
            off_rate = [count / value for value in off]
            on_rate = [count / value for value in on]
            rows.append({
                "case": name, "phase": phase, "pairs": len(pairs),
                "off_seconds_mean": statistics.mean(off),
                "on_seconds_mean": statistics.mean(on),
                "off_seconds_stdev": statistics.stdev(off) if len(off) > 1 else None,
                "on_seconds_stdev": statistics.stdev(on) if len(on) > 1 else None,
                "runtime_overhead_pct_mean": statistics.mean(overhead),
                "runtime_overhead_pct_stdev": statistics.stdev(overhead) if len(overhead) > 1 else None,
                "off_rows_per_second_mean": statistics.mean(off_rate),
                "on_rows_per_second_mean": statistics.mean(on_rate),
                "throughput_change_pct_mean": statistics.mean(
                    100 * (a / b - 1) for a, b in zip(off, on)
                ),
                "paired_runtime_overhead_pct": overhead,
            })
    return rows


def save_report(report, output):
    report["summary"] = summarize(report["samples"])
    serialized = json.dumps(report, indent=2)
    archive = Path(report["result_directory"]) / "overhead.json"
    for target in {output, archive}:
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(serialized)
        temporary.replace(target)
    csv_path = output.with_suffix(".csv")
    if report["summary"]:
        fields = [key for key in report["summary"][0] if key != "paired_runtime_overhead_pct"]
        with csv_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(report["summary"])
    else:
        # A new/failed run must not leave the previous run's CSV looking current.
        csv_path.write_text("case,phase,pairs,off_seconds_mean,on_seconds_mean,runtime_overhead_pct_mean\n")
    # Plot outside every measurement interval. Also archive the plot alongside
    # its source JSON so subsequent runs cannot separate figures from data.
    render_report(report, output.with_suffix(""))
    for extension in (".png", ".pdf"):
        source = output.with_suffix(extension)
        destination = archive.with_suffix(extension)
        if source != destination:
            destination.write_bytes(source.read_bytes())


def run_observation(name, pair, mode, directory, input_path, timeout_s, num_boost_round=10):
    directory.mkdir(parents=True)
    enabled = mode == "on"
    script, script_args, count = workload(name, directory, input_path, num_boost_round)
    sample = {"case": name, "pair": pair, "mode": mode, "logical_rows": count,
              "directory": str(directory), "status": "failed"}
    started = time.monotonic()
    try:
        args = argparse.Namespace(
            local_executor_nodes=4, local_object_store_mb=512,
            owner_node_id=None, executor_node_ids=None, producer_concurrency=None,
            recovery_timeout_s=30,
        )
        # Optional State API telemetry is explicitly skipped in BOTH modes.
        # Local Dataset statistics remain; frontend assets are not required.
        with local_head_failure_cluster(
            args, coordinator_cpus=0, recovery_enabled=enabled, include_dashboard=False,
        ) as (_, _unused_failure_function):
            sample["cluster_startup_seconds"] = time.monotonic() - started
            before = {node["NodeID"] for node in ray.nodes() if node["Alive"]}
            coordinator = ray.get_runtime_context().get_node_id()
            result = run_script(
                ROOT / script, script_args,
                address=ray.get_runtime_context().gcs_address,
                node_ip_address="127.0.0.3", recovery_enabled=enabled,
                timeout_s=timeout_s, report=directory / "launcher.json",
                env={
                    "TEST_OUTPUT_JSON": str(directory / "benchmark.json"),
                    "RAY_RECOVERY_STATE_REPORT": str(directory / "settings.json"),
                    "RAY_TRAIN_V2_ENABLED": "1",
                    "RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S": "30",
                    "RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S": "30",
                    "RAY_TRAIN_COLLECTIVE_TIMEOUT_S": "30",
                    "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                    "PROFILER_MODE": "none", "PYSPY_ENABLED": "0", "PERF_PROFILING_ENABLED": "0",
                    "GPU_MONITOR_ENABLED": "0", "NET_MONITOR_ENABLED": "0", "OBJECT_STORE_MONITOR_ENABLED": "0",
                },
            )
            sample["launcher"] = result
            if result["validation_status"] != "passed":
                raise RuntimeError(result.get("error", "Benchmark process failed"))
            after = {node["NodeID"] for node in ray.nodes() if node["Alive"]}
            if before != after:
                raise ValueError("Node membership changed during a no-failure measurement")
            audit = json.loads((directory / "settings.json").read_text())
            expected = system_config()
            for key, value in expected.items():
                if key.startswith("enable_"):
                    value = enabled
                if audit["native_settings"].get(key) != value:
                    raise ValueError(f"Unexpected native setting {key}: {audit}")
            if (audit["data_recovery_enabled"] != enabled or audit["launcher_observer_enabled"]
                    or audit["driver_node_id"] != coordinator):
                raise ValueError(f"Invalid application recovery/placement settings: {audit}")
            sample["settings"] = audit
            sample["all_nodes_survived"] = True
            raw = json.loads((directory / "benchmark.json").read_text())
            if name.startswith("xgboost-") and raw.get("num_boost_round") != num_boost_round:
                raise ValueError("Benchmark reported a different boosting-round count")
            sample["benchmark_metrics"] = raw
            sample["benchmark_seconds"] = timings(name, raw)
        # Check prediction artifacts outside both the benchmark timer and the
        # cluster lifetime. The common input link is only for this validator.
        if name.startswith("xgboost-"):
            (directory / "input").symlink_to(input_path, target_is_directory=True)
            sample["prediction_validation"] = validate_predictions(
                directory, expected_rounds=num_boost_round,
            )
        sample["status"] = "passed"
    except Exception as exc:
        sample.update(error_type=type(exc).__name__, error=str(exc), traceback=traceback.format_exc())
        traceback.print_exc()
    sample["observation_wall_seconds"] = time.monotonic() - started
    (directory / "sample.json").write_text(json.dumps(sample, indent=2))
    return sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=1, help="OFF/ON pairs per case; 1 is preliminary")
    parser.add_argument("--case", action="append", choices=CASES, help="Repeat to select cases; default all four")
    parser.add_argument("--num-boost-round", type=int, default=10,
                        help="XGBoost rounds in BOTH modes; dataset and worker counts stay fixed")
    parser.add_argument("--timeout-s", type=float, default=120,
                        help="Per-process deadline in seconds; explicitly increase for longer training")
    args = parser.parse_args()
    if args.repeats < 1 or not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use --repeats >= 1 and a finite --timeout-s > 0")
    if args.num_boost_round < 1:
        parser.error("--num-boost-round must be positive")
    if sys.platform != "linux":
        parser.error("The local process-cluster fixture requires Linux")
    # Prevent inherited launcher/callback settings from contaminating the
    # parent fixture or either arm. Native settings are explicit per cluster.
    for key in list(os.environ):
        if key.startswith("RAY_RECOVERY_") or key == "RAY_EXPERIMENTAL_RECOVERY":
            os.environ.pop(key)
    # Ray workers inherit the raylet environment, not just the script's env.
    # Apply these before starting either arm's cluster as well as to the child.
    os.environ.update(OMP_NUM_THREADS="1", MKL_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1")
    if os.environ.get("RAY_DATA_EXECUTION_CALLBACKS"):
        parser.error("Unset RAY_DATA_EXECUTION_CALLBACKS for an observer-free comparison")
    # Fail immediately if plotting support is missing, before any cluster work.
    require_matplotlib()
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    input_path = directory / "input"
    selected = list(dict.fromkeys(args.case or CASES))
    if any(name.startswith("xgboost-") for name in selected):
        make_input(input_path)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    report = {
        "profile": "fixed-r-no-failure-overhead", "status": "running",
        "git_commit": commit, "python": sys.version, "platform": platform.platform(),
        "result_directory": str(directory), "cases": selected, "repeats": args.repeats,
        "expected_observations": 2 * args.repeats * len(selected), "timeout_s": args.timeout_s,
        "xgboost_num_boost_round": args.num_boost_round,
        "method": "fresh cluster/process per observation; alternating paired order; no warmup",
        "primary_timing": "benchmark-reported wall time, excluding cluster startup/cleanup",
        "comparison": "ordinary native/Data recovery OFF vs full Fixed-R native/Data recovery ON",
        "topology": {"head_cpus": 0, "driver_node_cpus": 0,
                     "executor_nodes": 4, "cpus_per_executor": 2,
                     "object_store_mb_per_node": 512, "dashboard_enabled": False},
        "state_api_statistics": "skipped in both modes; local Dataset statistics retained",
        "runtime_overhead_formula": "100 * (ON seconds / OFF seconds - 1), mean of paired ratios",
        "throughput_change_formula": "100 * (OFF seconds / ON seconds - 1), mean of paired ratios",
        "throughput_unit": "logical rows/s; producer rows for backpressure; input rows otherwise",
        "scope": "local cold-job cost including worker startup, placement/fusion/copying policy; not isolated native overhead or an unmodified upstream build",
        "preliminary": args.repeats == 1, "samples": [],
    }
    save_report(report, output)
    if any(name.startswith("xgboost-") for name in selected):
        print(f"XGBoost: {args.num_boost_round} boosting rounds per observation; fixed input data", flush=True)
    for case_index, name in enumerate(selected):
        for pair in range(1, args.repeats + 1):
            modes = ("off", "on") if (case_index + pair) % 2 else ("on", "off")
            for mode in modes:
                print(f"{name}: pair {pair}/{args.repeats}, recovery {mode.upper()} "
                      f"({args.timeout_s:g}s process limit)", flush=True)
                sample = run_observation(
                    name, pair, mode, directory / name / f"pair-{pair}-{mode}", input_path,
                    args.timeout_s, args.num_boost_round,
                )
                report["samples"].append(sample)
                if sample["status"] != "passed":
                    report["status"] = "failed"
                    save_report(report, output)
                    print(f"Stopped at first failure. Partial report: {output}", flush=True)
                    return 1
                save_report(report, output)
                print(f"  {sample['benchmark_seconds']['total']:.3f}s benchmark time", flush=True)
    report["status"] = "passed"
    save_report(report, output)
    print("\nCase / phase                         OFF(s)     ON(s)   overhead")
    for row in report["summary"]:
        label = f"{row['case']} / {row['phase']}"
        print(f"{label:36} {row['off_seconds_mean']:8.3f}  {row['on_seconds_mean']:8.3f}  "
              f"{row['runtime_overhead_pct_mean']:+8.2f}%")
    if report["preliminary"]:
        print("One pair per case: preliminary measurements, no variance estimate.")
    print(f"JSON: {output}\nCSV: {output.with_suffix('.csv')}\n"
          f"Plots: {output.with_suffix('.png')} and {output.with_suffix('.pdf')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
