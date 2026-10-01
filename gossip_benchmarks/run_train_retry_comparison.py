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
Add --owner-placement head for a controlled owner-loss comparison: ordinary
shuffle maps are submitted through head actors, while the driver survives.
This changes OFF's ownership topology explicitly; it is not default Ray Data.

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
    placement = on.get("owner_placement", "default")
    if off.get("owner_placement", "default") != placement:
        raise ValueError("Cannot compare different owner placement")
    owner_loss = placement == "head" and on["scenario"] == "data-owner"
    if on["status"] != "passed" or (off["status"] != "passed" and not owner_loss):
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
    if placement == "head":
        for sample in (off, on):
            owner = sample.get("shuffle_owner", {})
            if (not owner.get("task_id") or not owner.get("owner_worker_id")
                    or not owner.get("owner_node_id")
                    or owner["owner_node_id"] != sample.get("selected_owner_node_id")):
                raise ValueError("Missing observed head ownership")
    if on["scenario"] == "data-owner":
        for sample in (off, on):
            fault = sample.get("data_owner_fault", {})
            head = fault.get("head_replacement", {})
            if (not fault.get("completed") or fault.get("stage") != "RandomShuffle.map"
                    or not head.get("original_head_processes_exited")
                    or head.get("failure_scope") != "all_head_processes_with_surviving_gcs_storage"
                    or fault["target"]["blocked_ns"] > fault["request_ns"]):
                raise ValueError("Missing matched head-failure evidence")
            if owner_loss:
                owner = sample["shuffle_owner"]
                if (fault.get("ownership") != owner
                        or owner["task_id"] != fault["target"]["task_id"]
                        or owner["owner_node_id"] != head.get("original_head_node_id")
                        or owner["recorded_ns"] > fault["request_ns"]
                        or not fault.get("submission_batch_settled")
                        or not sample.get("no_failure_control_passed")):
                    raise ValueError("Fault did not verify matched owner loss and a passing control")
        target_id = on["data_owner_fault"]["target"]["task_id"]
        if not on["data_owner_fault"].get("fixed_r_submission_batch_settled"):
            raise ValueError("Owner failure raced an unfinished submission batch")
        if not any(detail["task_id"] == target_id
                   for op in on["data_exchanges"] if op["operator"].startswith("RandomShuffle")
                   for detail in op.get("fixed_r_recovered_task_details", [])):
            raise ValueError("ON did not replay the blocked owner-lost shuffle task")
    if not all(s.get("numerical_probe") for s in (off, on)):
        raise ValueError("Fixed-R comparison requires a workload numerical probe")
    if owner_loss and not on.get("matches_no_failure_predictions"):
        raise ValueError("Recovered ON predictions must match its no-failure control")
    if off["status"] != "passed":
        loss, owner = off.get("ordinary_owner_loss", {}), off["shuffle_owner"]
        if (off.get("timeout") or off.get("workload_completed") is not False
                or off.get("validation_status") != "failed"
                or loss.get("error_type") != "OwnerDiedError"
                or loss.get("source") != "shuffle_metadata_fetch"
                or not owner.get("object_ref_hex")
                or any(loss.get(k) != owner[k] for k in (
                    "object_ref_hex", "owner_node_id", "owner_worker_id"))
                or loss.get("observed_ns", 0) <= off["data_owner_fault"].get("replacement_ready_ns", 0)):
            raise ValueError("OFF failure is not verified owner loss of the exact blocked task")
        after, failed_after = on["workload_s"], off["workload_s"]
        if not all(math.isfinite(v) and v > 0 for v in (after, failed_after)):
            raise ValueError("Invalid workload timing")
        return {"off_s": None, "on_s": after, "off_failure_s": failed_after,
                "on_vs_off_pct": None, "off_completed": False, "on_completed": True,
                "predictions_match": None, "on_matches_no_failure_predictions": True,
                "owner_loss_demonstrated": True,
                "on_replayed_tasks": sum(op.get("fixed_r_recovered_tasks", 0)
                                         for op in on["data_exchanges"])}
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
        "owner_placement": args.owner_placement,
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
    if args.owner_placement == "head":
        report["profile"] = "torch-workload-matched-owner-loss"
        report["limitations"][1] = (
            "OFF uses ordinary tasks submitted through head actors to match ON's shuffle-map owner placement; not default Ray Data topology"
        )
        report["limitations"][6] = "both arms wait for the shuffle-map submission batch before owner failure"
        report["limitations"].append(
            "verified OFF OwnerDiedError is an expected experimental outcome, not a completed workload or a speedup measurement"
        )
    controls = {}
    validated_control_pairs = set()

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
                           "comparison": "fixed-r", "owner_placement": args.owner_placement}
                print(f"{scenario}: pair {pair}/{args.repeats}, Fixed-R {mode.upper()} ({args.timeout_s:g}s)", flush=True)
                sample = run_observation(options, pair, directory / f"{scenario}-{pair}-{mode}", provenance)
                sample["restart_scope"] = "full"
                if scenario == "data-owner":
                    sample["no_failure_control_passed"] = (
                        pair in validated_control_pairs
                        and controls.get((pair, mode), {}).get("status") == "passed"
                    )
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
                save()
            try:
                values = compare_fixed_r_pair(samples["off"], samples["on"])
                report["pairs"].append({"scenario": scenario, "pair": pair, **values})
                if scenario == "none":
                    validated_control_pairs.add(pair)
                if values.get("owner_loss_demonstrated"):
                    samples["off"]["expected_owner_loss"] = True
                    print("  OFF: verified OwnerDiedError; ON: completed with replay and matching control predictions", flush=True)
                else:
                    print(f"  both completed; ON versus OFF: {values['on_vs_off_pct']:+.2f}%", flush=True)
            except (KeyError, ValueError, AssertionError, OSError) as exc:
                report["failed_observations"].append({"scenario": scenario, "pair": pair,
                                                      "error": f"Comparison: {exc}"})
            for mode, sample in samples.items():
                if sample["status"] != "passed" and not sample.get("expected_owner_loss"):
                    report["failed_observations"].append({
                        "scenario": scenario, "pair": pair, "mode": mode, "error": sample.get("error")
                    })
            save()
    report["summary"] = []
    for scenario in ("none", "data-owner"):
        rows = [p for p in report["pairs"] if p["scenario"] == scenario]
        if rows:
            timings = [p for p in rows if p["off_completed"] and p["on_completed"]]
            changes = [p["on_vs_off_pct"] for p in timings]
            report["summary"].append({
                "scenario": scenario, "pairs": len(rows),
                "off_completed_pairs": sum(p["off_completed"] for p in rows),
                "on_completed_pairs": sum(p["on_completed"] for p in rows),
                "verified_owner_loss_pairs": sum(p.get("owner_loss_demonstrated", False) for p in rows),
                "off_s_mean": statistics.mean(p["off_s"] for p in timings) if timings else None,
                "on_s_mean": statistics.mean(p["on_s"] for p in rows),
                "on_vs_off_pct_mean": statistics.mean(changes) if changes else None,
                "on_vs_off_pct_stdev": statistics.stdev(changes) if len(changes) > 1 else None,
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
    parser.add_argument("--owner-placement", choices=("default", "head"), default="default",
                        help="With --comparison fixed-r, head matches shuffle-map owners on the failed head in both modes")
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
    if args.owner_placement != "default" and args.comparison != "fixed-r":
        parser.error("--owner-placement head requires --comparison fixed-r")
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
