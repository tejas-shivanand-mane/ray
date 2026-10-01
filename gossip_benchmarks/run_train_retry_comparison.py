"""Compare standard Train retries with opt-in selective CPU retries.

Use --workload python/ray/train/examples/pytorch/torch_regression_example.py
to run the existing PyTorch example, with a small local CSV and Fixed-R OFF
by default. This exercises real Ray Data shards without a custom train loop.
Use --mode on to also enroll non-keyed repartition and random-shuffle exchange
tasks in Fixed-R; the report requires evidence of those protected stages.
Other CPU TorchTrainer scripts can use --workload-arg=... for their arguments;
they must already save and restore synchronous checkpoints. The harness
checks retry mechanics, not arbitrary workload semantics. The regression
example additionally checks epoch progress and final prediction agreement.
--comparison fixed-r compares ordinary Data OFF with protected Data ON, using
standard Train retries in both. It includes a head-process failure while the
first random-shuffle map task is held before exporting its output.

Without --workload, retain the matched fixed-partition XGBoost benchmark.
"""

import argparse
import math
from pathlib import Path
import statistics
import sys

import numpy as np

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from training_provenance import source_provenance


def compare_pair(full, selective):
    if any(s["status"] != "passed" for s in (full, selective)):
        raise ValueError("Both policies must pass before timing comparison")
    keys = ["scenario", "mode", "native_settings"]
    keys += (["workload_sha256", "torch_version"] if "workload_sha256" in full else ["final_tree_sha256"])
    for key in keys:
        if full[key] != selective[key]:
            raise ValueError(f"Cannot compare different {key}")
    for key in ("source_sha256", "native_extension_sha256", "python", "platform",
                "ray_version", "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version"):
        if full["provenance"][key] != selective["provenance"][key]:
            raise ValueError(f"Cannot compare different {key}")
    predictions_match = None
    if full.get("numerical_probe", True) and selective.get("numerical_probe", True):
        np.testing.assert_allclose(
            np.load(Path(full["directory"]) / "predictions.npy", allow_pickle=False),
            np.load(Path(selective["directory"]) / "predictions.npy", allow_pickle=False),
            rtol=1e-5 if "workload_sha256" in full else 1e-6, atol=1e-7)
        predictions_match = True
    before, after = full["training_s"], selective["training_s"]
    if not all(math.isfinite(v) and v > 0 for v in (before, after)):
        raise ValueError("Invalid training timing")
    return {"full_s": before, "selective_s": after, "predictions_match": predictions_match,
            "selective_vs_full_pct": 100 * (after / before - 1)}


def compare_fixed_r_pair(off, on):
    if any(s["status"] != "passed" for s in (off, on)):
        raise ValueError("Incomplete OFF/ON pair; inspect failure evidence before claiming a benefit")
    for key in ("scenario", "workload_sha256", "torch_version", "restart_scope"):
        if off[key] != on[key]:
            raise ValueError(f"Cannot compare different {key}")
    if off["mode"] != "off" or on["mode"] != "on" or on["restart_scope"] != "full":
        raise ValueError("Fixed-R comparison requires OFF/ON with standard Train retries")
    for key in ("source_sha256", "native_extension_sha256", "python", "platform",
                "ray_version", "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version"):
        if off["provenance"][key] != on["provenance"][key]:
            raise ValueError(f"Cannot compare different {key}")
    if set(off["native_settings"]) != set(on["native_settings"]):
        raise ValueError("Native setting keys differ")
    for key, value in off["native_settings"].items():
        other = on["native_settings"][key]
        if (key.startswith("enable_") and (value is not False or other is not True)) or (
            not key.startswith("enable_") and value != other
        ):
            raise ValueError(f"Unmatched native setting {key}")
    if on["scenario"] == "data-owner":
        for sample in (off, on):
            fault = sample.get("data_owner_fault", {})
            head = fault.get("head_replacement", {})
            if (not fault.get("completed") or fault.get("stage") != "RandomShuffle.map"
                    or not head.get("original_head_processes_exited")
                    or head.get("failure_scope") != "all_head_processes_with_surviving_gcs_storage"
                    or fault["target"]["blocked_ns"] > fault["request_ns"]):
                raise ValueError("Missing matched head-failure evidence")
        target_id = on["data_owner_fault"]["target"]["task_id"]
        if not on["data_owner_fault"].get("fixed_r_submission_batch_settled"):
            raise ValueError("Owner failure raced an unfinished submission batch")
        if not any(detail["task_id"] == target_id
                   for op in on["data_exchanges"] if op["operator"].startswith("RandomShuffle")
                   for detail in op.get("fixed_r_recovered_task_details", [])):
            raise ValueError("ON did not replay the blocked owner-lost shuffle task")
    if not all(s.get("numerical_probe") for s in (off, on)):
        raise ValueError("Fixed-R comparison requires a workload numerical probe")
    np.testing.assert_allclose(
        np.load(Path(off["directory"]) / "predictions.npy", allow_pickle=False),
        np.load(Path(on["directory"]) / "predictions.npy", allow_pickle=False),
        rtol=1e-5, atol=1e-7,
    )
    before, after = off["workload_s"], on["workload_s"]
    if not all(math.isfinite(v) and v > 0 for v in (before, after)):
        raise ValueError("Invalid workload timing")
    return {"off_s": before, "on_s": after, "on_vs_off_pct": 100 * (after / before - 1),
            "off_completed": True, "on_completed": True, "predictions_match": True,
            "on_replayed_tasks": sum(op.get("fixed_r_recovered_tasks", 0)
                                     for op in on["data_exchanges"])}


def run_fixed_r_workload_comparison(args, directory, provenance):
    report = {
        "profile": "torch-workload-fixed-r-owner-comparison", "status": "running",
        "source_provenance": provenance, "samples": [], "pairs": [],
        "failed_observations": [], "preliminary": args.repeats == 1,
        "comparison_axis": "ordinary Ray Data OFF versus Fixed-R Data ON; standard Train retry in both",
        "measurement_scope": "script execution including data preparation, training and head replacement; excludes cluster startup and final numerical probe",
        "limitations": [
            "both arms use pull-based random shuffle with preserved input order",
            "OFF owns data outputs on the surviving driver and may survive head loss too",
            "ON adds owner helpers, protected submission and coordinator copies; overhead includes the full integration",
            "both arms use external head replacement with surviving local RocksDB storage",
            "logical nodes share one machine and filesystem; not whole-machine head failure",
            "one random-shuffle map task is gated before exporting output; training begins afterward",
            "ON injects only after the current submission batch has finished enrolling",
            "this tests data-owner recovery, not training-worker selective retry",
        ],
    }
    controls = {}

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    save()
    for scenario in ("none", "data-owner"):
        if scenario not in (args.scenario or ("none", "data-owner")):
            continue
        for pair in range(1, args.repeats + 1):
            samples = {}
            for mode in (("off", "on") if pair % 2 else ("on", "off")):
                options = {"training_strategy": "ray-train-workload", "scenario": scenario,
                           "mode": mode, "restart_scope": "full", "timeout_s": args.timeout_s,
                           "workload": str(args.workload.resolve()), "workload_args": args.workload_arg,
                           "comparison": "fixed-r"}
                print(f"{scenario}: pair {pair}/{args.repeats}, Fixed-R {mode.upper()} ({args.timeout_s:g}s)", flush=True)
                sample = run_observation(options, pair, directory / f"{scenario}-{pair}-{mode}", provenance)
                sample["restart_scope"] = "full"
                if sample["status"] == "passed" and scenario == "data-owner":
                    try:
                        control = controls[(pair, mode)]
                        if control["status"] != "passed":
                            raise ValueError("No successful no-failure control for this arm")
                        np.testing.assert_allclose(
                            np.load(Path(sample["directory"]) / "predictions.npy", allow_pickle=False),
                            np.load(Path(control["directory"]) / "predictions.npy", allow_pickle=False),
                            rtol=1e-5, atol=1e-7,
                        )
                        sample["matches_no_failure_predictions"] = True
                    except (KeyError, ValueError, AssertionError, OSError) as exc:
                        sample.update(status="failed", error=f"No-failure comparison: {exc}")
                if scenario == "none":
                    controls[(pair, mode)] = sample
                samples[mode] = sample
                report["samples"].append(sample)
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                if sample["status"] != "passed":
                    report["failed_observations"].append({
                        "scenario": scenario, "pair": pair, "mode": mode, "error": sample.get("error")
                    })
                save()
            if all(s["status"] == "passed" for s in samples.values()):
                try:
                    values = compare_fixed_r_pair(samples["off"], samples["on"])
                    report["pairs"].append({"scenario": scenario, "pair": pair, **values})
                    print(f"  both completed; ON versus OFF: {values['on_vs_off_pct']:+.2f}%", flush=True)
                except (ValueError, AssertionError, OSError) as exc:
                    report["failed_observations"].append({"scenario": scenario, "pair": pair,
                                                          "error": f"Comparison: {exc}"})
                save()
    report["summary"] = []
    for scenario in ("none", "data-owner"):
        rows = [p for p in report["pairs"] if p["scenario"] == scenario]
        if rows:
            changes = [p["on_vs_off_pct"] for p in rows]
            report["summary"].append({
                "scenario": scenario, "pairs": len(rows),
                "off_s_mean": statistics.mean(p["off_s"] for p in rows),
                "on_s_mean": statistics.mean(p["on_s"] for p in rows),
                "on_vs_off_pct_mean": statistics.mean(changes),
                "on_vs_off_pct_stdev": statistics.stdev(changes) if len(rows) > 1 else None,
            })
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}")
    return int(report["status"] != "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--comparison", choices=("retry", "fixed-r"), default="retry")
    parser.add_argument("--mode", choices=("on", "off"),
                        help="Fixed-R mode; default ON for XGBoost, OFF for existing scripts")
    parser.add_argument("--scenario", action="append", choices=("none", "worker", "worker-node", "data-owner"))
    parser.add_argument("--workload", type=Path,
                        help="Run an existing CPU TorchTrainer script through the external harness")
    parser.add_argument("--workload-arg", action="append", default=[],
                        help="Script argument; use --workload-arg=--flag for flags")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=120)
    args = parser.parse_args()
    if args.comparison == "fixed-r":
        if not args.workload or args.mode is not None:
            parser.error("Fixed-R comparison requires --workload and selects both modes itself")
        if args.scenario and ("none" not in args.scenario or any(
            s not in ("none", "data-owner") for s in args.scenario
        )):
            parser.error("Use none and optionally data-owner; a no-failure control is required")
    elif args.scenario and "data-owner" in args.scenario:
        parser.error("data-owner requires --comparison fixed-r")
    args.mode = args.mode or ("off" if args.workload else "on")
    scenarios = args.scenario or (["none", "worker"] if args.workload else ["none", "worker-node"])
    if (args.workload and "worker-node" in scenarios) or (not args.workload and "worker" in scenarios):
        parser.error("Existing-workload harness supports worker-process loss; XGBoost harness uses worker-node loss")
    if args.workload and not args.workload.is_file():
        parser.error("Workload script does not exist")
    if sys.platform != "linux" or args.repeats < 1 or not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use Linux, positive repeats and a finite positive timeout")
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    provenance = source_provenance(ROOT)
    if args.comparison == "fixed-r":
        return run_fixed_r_workload_comparison(args, directory, provenance)
    report = {"profile": "ray-train-selective-xgboost", "status": "running",
              "source_provenance": provenance, "samples": [], "pairs": [], "failed_observations": [],
              "preliminary": args.repeats == 1,
              "comparison_axis": "standard Ray Train retry versus selective retry; Fixed-R identical",
              "measurement_scope": "trainer.fit including ingestion, retries, final integrity probe and teardown; excludes cluster startup",
              "limitations": ["fixed CPU partitions; explicit input cache helper; no shared streaming shards",
                              "controlled callback allreduces; not arbitrary native tree-building failures",
                              "logical nodes on one machine; shared storage and input service survive",
                              "Fixed-R independent contribution requires a separate OFF/ON comparison"]}
    if args.workload:
        report.update(profile="ray-train-existing-workload", workload=str(args.workload.resolve()),
                      measurement_scope="trainer.fit including ingestion, retries and teardown; excludes cluster startup, driver preprocessing and final numerical probe",
                      limitations=["CPU Gloo; synchronous, application-restored checkpoints",
                                   "worker-process failure after a committed checkpoint; not node or head loss",
                                   "fresh dataset iterators; no retained streaming position or cached-partition guarantee",
                                   "existing regression example has a numerical probe; other scripts need workload-specific correctness checks",
                                   "Fixed-R is identical in both policies; its independent benefit is not measured"])

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    save()
    for scenario in dict.fromkeys(scenarios):
        for pair in range(1, args.repeats + 1):
            samples = {}
            for policy in (("full", "selective") if pair % 2 else ("selective", "full")):
                options = {"training_strategy": "ray-train-selective", "scenario": scenario,
                           "mode": args.mode, "restart_scope": policy, "timeout_s": args.timeout_s}
                if args.workload:
                    options.update(training_strategy="ray-train-workload",
                                   workload=str(args.workload.resolve()), workload_args=args.workload_arg)
                print(f"{scenario}: pair {pair}/{args.repeats}, {policy}, Fixed-R {args.mode.upper()} ({args.timeout_s:g}s)", flush=True)
                sample = run_observation(options, pair, directory / f"{scenario}-{pair}-{policy}", provenance)
                sample["restart_scope"] = policy
                report["samples"].append(sample)
                samples[policy] = sample
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('training_s')) + 's training')}", flush=True)
                if sample["status"] != "passed":
                    report["failed_observations"].append({"scenario": scenario, "pair": pair,
                                                          "policy": policy, "error": sample.get("error")})
                save()
            if all(s["status"] == "passed" for s in samples.values()):
                try:
                    values = compare_pair(samples["full"], samples["selective"])
                    report["pairs"].append({"scenario": scenario, "pair": pair,
                                            **values})
                    print(f"  matched change: {values['selective_vs_full_pct']:+.2f}%", flush=True)
                except (ValueError, AssertionError, OSError) as exc:
                    report["failed_observations"].append({"scenario": scenario, "pair": pair,
                                                          "error": f"Comparison: {exc}"})
                save()
    report["summary"] = []
    for scenario in dict.fromkeys(scenarios):
        rows = [p for p in report["pairs"] if p["scenario"] == scenario]
        if rows:
            changes = [r["selective_vs_full_pct"] for r in rows]
            report["summary"].append({"scenario": scenario, "pairs": len(rows),
                                      "full_s_mean": statistics.mean(r["full_s"] for r in rows),
                                      "selective_s_mean": statistics.mean(r["selective_s"] for r in rows),
                                      "selective_vs_full_pct_mean": statistics.mean(changes),
                                      "selective_vs_full_pct_stdev": statistics.stdev(changes) if len(rows) > 1 else None})
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}")
    return int(report["status"] != "passed")


if __name__ == "__main__":
    sys.exit(main())
