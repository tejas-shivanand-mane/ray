"""Compare CPU training recovery after abrupt logical worker-node loss.

Default: four epochs, one middle-of-epoch node loss, one paired repetition,
with healthy controls. Both arms use Fixed-R OFF and ordinary checkpoint retry;
the selective arm retains healthy actors. Shared durable input/checkpoints and
spare capacity survive. This is a single-machine simulation of spot-worker
loss, not cloud instance termination or autoscaler provisioning.
Use --comparison integrated for the historical OFF/full vs ON/selective study.
Plot saved JSON separately with plot_fashion_training.py.
"""

import argparse
import math
from pathlib import Path
import statistics
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from training_provenance import source_provenance
from fashion_comparison import compare_control, compare_pair, input_identity


def failure_epochs(epochs, points):
    if type(epochs) is not int or epochs < 4:
        raise ValueError("Use at least four fixed epochs")
    choices = {"early": 1, "middle": epochs // 2, "late": epochs - 1}
    return {point: choices[point] for point in dict.fromkeys(points)}


def comparison_cases(epochs, points, kinds):
    milestones = failure_epochs(epochs, points)
    kinds = list(dict.fromkeys(kinds))
    if not kinds or any(k not in ("head-node", "worker-node", "worker") for k in kinds):
        raise ValueError("Select head-node, worker-node or worker-process failure")
    return ([{"scenario": "none", "failure_point": "none", "fault_after_epoch": 0}]
            + [{"scenario": kind, "failure_point": point, "fault_after_epoch": epoch}
               for kind in kinds for point, epoch in milestones.items()])


def run_comparison(args, directory, provenance):
    points = {"none": 0, **failure_epochs(args.epochs, args.failure_point)}
    kinds = list(dict.fromkeys(args.failure_kind))
    cases = comparison_cases(args.epochs, args.failure_point, kinds)
    identity = input_identity(args.data_directory)
    comparison = getattr(args, "comparison", "integrated")
    retry_only = comparison == "retry"
    timing = getattr(args, "failure_timing", "boundary")
    step = getattr(args, "fault_after_step", 59)
    report = {
        "profile": "fashion-mnist-failure-matrix", "status": "running",
        "source_provenance": provenance, "input_identity": identity,
        "training_epochs": args.epochs, "failure_epochs": points,
        "failure_kinds": kinds, "cases": cases,
        "failure_timing": timing, "fault_after_step": step if timing == "active" else None,
        "observations_per_repetition": 2 * len(cases), "placement_strategy": "STRICT_SPREAD",
        "samples": [], "pairs": [], "failed_observations": [], "preliminary": args.repeats == 1,
        "comparison": comparison,
        "comparison_axis": ("Fixed-R OFF: full-group versus selective Train retry" if retry_only else
                            "ordinary Ray OFF/full retry versus Fixed-R ON/selective retry"),
        "failure_model": {
            "worker_loss": "abrupt logical-node process loss without advance notice",
            "storage": "shared input and checkpoints survive independently of failed workers",
            "capacity": "replacement actors use already available surviving executors",
            "physical_instance_termination_tested": False,
        },
        "arm_labels": {"ordinary": "Ordinary Ray: OFF/full retry",
                       "integrated": "Fixed-R OFF/selective retry" if retry_only else "Fixed-R ON/selective retry"},
        "measurement_scope": "workload execution: data read/normalization/shuffle, training, validation, checkpoints and teardown; excludes download, cluster startup and final correctness probe",
        "limitations": [
            "two CPU Gloo workers, 235146-parameter MLP; one physical machine and shared surviving storage",
            "head-node cases replace head processes during training; local GCS RocksDB storage, driver and executors survive",
            "worker-node cases kill one executor's raylet, object store and children; replacement workers use surviving executors",
            "both arms use STRICT_SPREAD across logical worker nodes, with one Train retry and application checkpoints",
            (f"failure follows {step} optimizer updates per rank inside the next uncommitted epoch; both ranks gated, not an in-flight-kernel fault"
             if timing == "active" else "failure is gated immediately after a committed epoch; unfinished minibatch work is not measured"),
            "failure-to-next-report includes the next training epoch, validation and checkpoint/report costs",
            "preprocessing is materialized before training; default ownership, no forced head-owner topology or guaranteed OFF failure",
            "head recovery may continue without restarting training workers; the actual retry and replay evidence is recorded",
            ("isolates selective retry with Fixed-R OFF in both arms; surviving actors still restore the application checkpoint"
             if retry_only else "combined policy comparison; individual contributions require separate ablations"),
            "real dataset but a modest CPU model; not evidence for large distributed training",
            "all repetitions retained; failed or timed-out observations are never completed timings",
        ],
    }

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    save()
    controls, valid_controls = {}, set()
    for case in cases:
        scenario, point, epoch = case["scenario"], case["failure_point"], case["fault_after_epoch"]
        for pair in range(1, args.repeats + 1):
            samples = {}
            for arm in (("ordinary", "integrated") if pair % 2 else ("integrated", "ordinary")):
                mode, scope = ("off", "full") if arm == "ordinary" else ("off" if retry_only else "on", "selective")
                options = {
                    "training_strategy": "ray-train-workload", "comparison": comparison,
                    "scenario": scenario, "placement_strategy": "STRICT_SPREAD",
                    "mode": mode, "restart_scope": scope, "owner_placement": "default",
                    "timeout_s": args.timeout_s, "training_epochs": args.epochs,
                    "fault_after_epoch": epoch, "data_directory": str(args.data_directory),
                    "failure_timing": timing, "fault_after_step": step,
                    "input_identity": identity,
                    "workload": str(ROOT / "gossip_benchmarks/workloads/fashion_mnist.py"),
                    "workload_args": ["--data-directory", str(args.data_directory), "--epochs", str(args.epochs)],
                }
                print(f"{scenario}/{point}: pair {pair}/{args.repeats}, {arm} ({mode.upper()}/{scope}), "
                      f"{args.epochs} epochs, timeout {args.timeout_s:g}s", flush=True)
                sample = run_observation(options, pair, directory / f"{scenario}-{point}-{pair}-{arm}", provenance)
                sample.update(arm=arm, comparison=comparison, failure_point=point, fault_after_epoch=epoch,
                              restart_scope=scope)
                if sample["status"] == "passed" and point != "none":
                    try:
                        if pair not in valid_controls:
                            raise ValueError("Matched no-failure comparison did not pass")
                        compare_control(sample, controls[(pair, arm)])
                    except (KeyError, ValueError, AssertionError, OSError, IndexError) as exc:
                        sample.update(status="failed", error=f"Control comparison: {exc}")
                if point == "none":
                    controls[(pair, arm)] = sample
                samples[arm] = sample
                report["samples"].append(sample)
                if sample["status"] != "passed":
                    report["failed_observations"].append({"scenario": scenario, "failure_point": point, "pair": pair,
                                                          "arm": arm, "error": sample.get("error")})
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                save()
            try:
                values = compare_pair(samples["ordinary"], samples["integrated"], comparison=comparison)
                report["pairs"].append({"scenario": scenario, "failure_point": point, "pair": pair, **values})
                if point == "none":
                    valid_controls.add(pair)
                print(f"  matched workload change: {values['workload_s_change_pct']:+.2f}%", flush=True)
            except (KeyError, ValueError, AssertionError, OSError) as exc:
                report["failed_observations"].append({"scenario": scenario, "failure_point": point, "pair": pair,
                                                      "error": f"Pair comparison: {exc}"})
            save()
        if point == "none" and len(valid_controls) != args.repeats:
            # There is no interpretable recovery comparison without passing
            # controls. Preserve evidence and avoid spending more benchmark time.
            report["skipped_failure_points"] = list(points)[1:]
            report["skipped_cases"] = cases[1:]
            break
    report["summary"] = []
    for case in cases:
        scenario, point = case["scenario"], case["failure_point"]
        rows = [p for p in report["pairs"] if p["failure_point"] == point and p["scenario"] == scenario]
        observations = [s for s in report["samples"] if s["failure_point"] == point and s["scenario"] == scenario]
        if not observations:
            continue
        changes = [p["workload_s_change_pct"] for p in rows]
        report["summary"].append({
            "scenario": scenario, "failure_point": point, "valid_pairs": len(rows), "requested_pairs": args.repeats,
            "ordinary_completed": sum(s.get("workload_completed", False) for s in observations if s["arm"] == "ordinary"),
            "integrated_completed": sum(s.get("workload_completed", False) for s in observations if s["arm"] == "integrated"),
            "workload_s_change_pct_mean": statistics.mean(changes) if changes else None,
            "workload_s_change_pct_stdev": statistics.stdev(changes) if len(changes) > 1 else None,
        })
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}", flush=True)
    return int(report["status"] != "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--comparison", choices=("retry", "integrated"), default="retry")
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--failure-point", action="append", choices=("early", "middle", "late"))
    parser.add_argument("--failure-kind", action="append", choices=("head-node", "worker-node", "worker"),
                        help="Default: worker-node; explicit head-node and worker process experiments remain available")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=420)
    parser.add_argument("--failure-timing", choices=("boundary", "active"), default="active",
                        help="Active: fault inside an unfinished epoch, after real optimizer updates")
    parser.add_argument("--fault-after-step", type=int, default=59,
                        help="For active mode: completed updates per rank in the uncheckpointed epoch (1..117)")
    args = parser.parse_args()
    if sys.platform != "linux" or args.repeats < 1 or args.epochs < 4:
        parser.error("Use Linux, positive repeats and at least four epochs")
    if not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use a finite positive observation timeout")
    args.failure_point = args.failure_point or ["middle"]
    args.failure_kind = args.failure_kind or ["worker-node"]
    if args.failure_timing == "active" and (
            "worker" in args.failure_kind or not 0 < args.fault_after_step < 118):
        parser.error("Active timing supports head-node/worker-node and steps 1..117")
    args.data_directory = args.data_directory.resolve()
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return run_comparison(args, directory, source_provenance(ROOT))


if __name__ == "__main__":
    sys.exit(main())
