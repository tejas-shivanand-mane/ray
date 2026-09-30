"""Matched, observed OFF/ON training and checkpoint-recovery comparisons.

Default: one two-worker, ten-round, training-only OFF/ON pair, checkpointing
every five rounds. Each observation gets a fresh subprocess and local cluster.
No worker-node failure or physical-machine resilience is claimed here.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from training_provenance import runtime_provenance, source_provenance

SCENARIOS = ("none", "worker", "head", "head-worker")


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def summarize(samples):
    rows = []
    for scenario in SCENARIOS:
        matched = {}
        for sample in samples:
            if sample["scenario"] == scenario and sample["status"] == "passed":
                arm = matched.setdefault(sample["pair"], {})
                if sample["mode"] in arm:
                    raise ValueError("Duplicate comparison arm")
                arm[sample["mode"]] = sample
        pairs = [p for p in matched.values() if set(p) == {"off", "on"}]
        for metric in ("training_s", "prediction_s", "pipeline_s"):
            values = []
            for pair in pairs:
                a, b = pair["off"].get(metric), pair["on"].get(metric)
                if a is None or b is None:
                    continue
                if not all(isinstance(v, (int, float)) and math.isfinite(v) and v > 0 for v in (a, b)):
                    raise ValueError("Invalid successful comparison timing")
                off_provenance, on_provenance = pair["off"]["provenance"], pair["on"]["provenance"]
                for key in ("source_sha256", "native_extension_sha256", "python", "platform",
                            "ray_version", "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version"):
                    if off_provenance[key] != on_provenance[key]:
                        raise ValueError(f"Cannot pair observations with different {key}")
                values.append((a, b, 100 * (b / a - 1)))
            if values:
                ratios = [v[2] for v in values]
                rows.append({
                    "scenario": scenario, "metric": metric, "pairs": len(values),
                    "off_seconds_mean": statistics.mean(v[0] for v in values),
                    "on_seconds_mean": statistics.mean(v[1] for v in values),
                    "on_vs_off_pct_mean": statistics.mean(ratios),
                    "on_vs_off_pct_stdev": statistics.stdev(ratios) if len(ratios) > 1 else None,
                    "paired_on_vs_off_pct": ratios,
                    "interpretation": "normal_run_overhead" if scenario == "none" else "fault_case_completion_change",
                })
    return rows


def child_case(path):
    payload = json.loads(Path(path).read_text())
    directory = Path(payload["directory"])
    output = directory / "case.json"
    options = payload["options"]
    diagnostics = {}
    ray = None

    def terminate(_signum, _frame):
        raise TimeoutError("Parent comparison deadline expired")

    signal.signal(signal.SIGTERM, terminate)
    try:
        # Clear inherited opt-ins before importing Ray/Data as well as before
        # cluster startup; both arms configure recovery explicitly.
        for key in list(os.environ):
            if key.startswith("RAY_RECOVERY_") or key in ("RAY_EXPERIMENTAL_RECOVERY", "RAY_DATA_EXECUTION_CALLBACKS", "RAY_ADDRESS"):
                os.environ.pop(key)
        import ray
        from train_comparison import run_case

        provenance = {**source_provenance(ROOT), **runtime_provenance(ROOT)}
        diagnostics["provenance"] = provenance
        if provenance["source_sha256"] != payload["expected_source_sha256"]:
            raise ValueError("Source changed before observation startup")
        if not provenance["ray_from_checkout"]:
            raise ValueError("Use the Ray build imported from this checkout in ray-dev")
        result = run_case(options, directory, diagnostics)
        if source_provenance(ROOT)["source_sha256"] != provenance["source_sha256"]:
            raise ValueError("Source changed during observation")
        if runtime_provenance(ROOT)["native_extension_sha256"] != provenance["native_extension_sha256"]:
            raise ValueError("Native extension changed during observation")
        result = {**diagnostics, **result}
    except Exception as exc:
        traceback.print_exc()
        result = {
            **diagnostics, "validation_status": "failed", "error_type": type(exc).__name__,
            "error": str(exc), "traceback": traceback.format_exc(),
        }
    finally:
        if ray is not None and ray.is_initialized():
            ray.shutdown()
    write_json(output, result)
    return 0 if result["validation_status"] == "passed" else 1


def stop_child(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def run_observation(options, pair, directory, provenance):
    directory.mkdir(parents=True)
    payload = directory / "options.json"
    write_json(payload, {"options": options, "directory": str(directory),
                         "expected_source_sha256": provenance["source_sha256"]})
    sample = {
        "scenario": options["scenario"], "mode": options["mode"], "pair": pair,
        "status": "failed", "directory": str(directory), "log": str(directory / "case.log"),
    }
    env = {
        **os.environ, "RAY_TRAIN_V2_ENABLED": "1", "PYTHONUNBUFFERED": "1",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
        "RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S": "30",
        "RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S": "30", "RAY_TRAIN_COLLECTIVE_TIMEOUT_S": "30",
    }
    process = None
    started = time.monotonic()
    try:
        with (directory / "case.log").open("wb") as log:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--_case", str(payload)],
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            try:
                code = process.wait(timeout=options["timeout_s"])
            except subprocess.TimeoutExpired:
                sample["timeout"] = True
                stop_child(process)
                raise TimeoutError(f"Observation exceeded {options['timeout_s']:g}s including startup and validation")
            result = json.loads((directory / "case.json").read_text())
            sample.update(result)
            if code or result["validation_status"] != "passed":
                raise RuntimeError(result.get("error", f"Comparison subprocess exited {code}"))
            sample["status"] = "passed"
    except Exception as exc:
        sample.update(error_type=type(exc).__name__, error=str(exc))
        # Retain partial timeline/provenance on timeout; never promote its status.
        if (directory / "case.json").exists():
            partial = json.loads((directory / "case.json").read_text())
            for key in ("observation", "provenance", "fault", "head_replacement", "native_settings"):
                if key in partial:
                    sample[key] = partial[key]
    finally:
        if process is not None:
            stop_child(process)
    sample["observation_wall_s"] = time.monotonic() - started
    write_json(directory / "sample.json", sample)
    return sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--_case", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result-directory", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scenario", choices=SCENARIOS, action="append")
    parser.add_argument("--mode", choices=("off", "on"), action="append")
    parser.add_argument("--repeats", type=int, default=1, help="Matched pairs; 1 is preliminary")
    parser.add_argument("--num-train-workers", type=int, choices=(1, 2), default=2)
    parser.add_argument("--num-boost-round", type=int, default=10)
    parser.add_argument("--checkpoint-frequency", type=int, default=5)
    parser.add_argument("--max-failures", type=int, default=1, help="Identical Train worker retry budget in both modes")
    parser.add_argument("--failure-after-round", type=int)
    parser.add_argument("--ungated", action="store_true", help="Observe a training trigger without pausing the controller; late injection fails")
    parser.add_argument("--include-prediction", action="store_true", help="Also time and validate the separate original inference pipeline")
    parser.add_argument("--timeout-s", type=float, default=120, help="Per-observation budget including startup/validation; cleanup may add 15s")
    args = parser.parse_args()
    if args._case:
        return child_case(args._case)
    scenarios = list(dict.fromkeys(args.scenario or ["none"]))
    modes = list(dict.fromkeys(args.mode or ["off", "on"]))
    if sys.platform != "linux":
        parser.error("Local RocksDB/process comparisons require Linux")
    if (args.repeats < 1 or args.num_boost_round < 1 or args.checkpoint_frequency < 0
            or args.max_failures < 0 or not math.isfinite(args.timeout_s) or args.timeout_s <= 0):
        parser.error("Use positive repetitions/rounds/timeout and nonnegative checkpoint frequency/retry budget")
    needs_fault = any(s != "none" for s in scenarios)
    needs_restart = any(s in ("worker", "head-worker") for s in scenarios)
    failure_round = args.failure_after_round
    if needs_fault and failure_round is None:
        failure_round = max(args.checkpoint_frequency, args.num_boost_round // 2)
    if needs_fault and not 1 <= failure_round < args.num_boost_round - 2:
        parser.error("Fault cases require 1 <= --failure-after-round < --num-boost-round - 2")
    if needs_restart and (args.max_failures < 1 or args.checkpoint_frequency < 1
                          or failure_round < args.checkpoint_frequency):
        parser.error("Worker faults require a positive retry budget and a checkpoint before the fault round")
    if not needs_fault and (args.failure_after_round is not None or args.ungated):
        parser.error("Fault trigger options require a fault scenario")
    directory = (args.result_directory or Path.home() / "ray-coverage" / (
        "training-comparison-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    )).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    output = (args.output or directory / "comparison.json").resolve()
    provenance = source_provenance(ROOT)
    settings = {
        "num_train_workers": args.num_train_workers, "num_boost_round": args.num_boost_round,
        "checkpoint_frequency": args.checkpoint_frequency, "max_failures": args.max_failures,
        "failure_after_round": failure_round, "gated": needs_fault and not args.ungated,
        "include_prediction": args.include_prediction, "timeout_s": args.timeout_s,
    }
    report = {
        "profile": "fixed-r-observed-training-comparison", "status": "running",
        "settings": settings, "scenarios": scenarios, "modes": modes, "repeats": args.repeats,
        "source_provenance": provenance, "result_directory": str(directory), "samples": [],
        "preliminary": args.repeats == 1,
        "measurement_scope": "instrumented training wall time excluding cluster startup; matched observers in both modes",
        "failure_scope": "head processes and/or one training actor; shared local GCS/checkpoint storage survives",
        "clock_scope": "one Linux host's monotonic clock; not synchronized multi-machine timestamps",
        "comparison": "same fork and external RocksDB head replacement in both arms; native/Data protection OFF versus ON",
        "topology": {"head_cpus": 0, "coordinator_cpus": 0, "executor_nodes": 4,
                     "cpus_per_executor": 2, "object_store_mb_per_node": 512},
    }

    def save():
        try:
            report["summary"] = summarize(report["samples"])
        except ValueError as exc:
            # Preserve every observation even when pairing evidence is invalid.
            report["summary"] = []
            report["comparison_error"] = str(exc)
        report["failed_observations"] = [
            {"scenario": s["scenario"], "pair": s["pair"], "mode": s["mode"], "error": s.get("error")}
            for s in report["samples"] if s["status"] != "passed"
        ]
        for path in {output, directory / "comparison.json"}:
            write_json(path, report)

    save()
    for index, scenario in enumerate(scenarios):
        for pair in range(1, args.repeats + 1):
            order = modes if (index + pair) % 2 else list(reversed(modes))
            for mode in order:
                options = {**settings, "scenario": scenario, "mode": mode}
                print(f"{scenario}: pair {pair}/{args.repeats}, recovery {mode.upper()} ({args.timeout_s:g}s)", flush=True)
                sample = run_observation(options, pair, directory / scenario / f"pair-{pair}-{mode}", provenance)
                report["samples"].append(sample)
                save()
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('training_s', '')) + 's training')}", flush=True)
    report["status"] = "failed" if report["failed_observations"] or report.get("comparison_error") else "passed"
    save()
    for row in report["summary"]:
        print(f"{row['scenario']} / {row['metric']}: OFF {row['off_seconds_mean']:.3f}s, "
              f"ON {row['on_seconds_mean']:.3f}s, change {row['on_vs_off_pct_mean']:+.2f}% ({row['pairs']} pairs)")
    print(f"Report: {output}")
    if report.get("comparison_error"):
        print(f"Invalid comparison: {report['comparison_error']}")
    if report["preliminary"]:
        print("One pair: preliminary result, no variance estimate.")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
