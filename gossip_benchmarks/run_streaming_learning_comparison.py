"""Short CPU CIFAR-10/ResNet comparison; plot its JSON separately.

Both arms use standard Train checkpoint retry and default data ownership.
Head cases replace logical head processes while storage/driver/executors survive.
Worker-node cases are coverage probes, not promised Fixed-R executor recovery.
"""

import argparse
import math
from pathlib import Path
import statistics
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from run_fashion_training_comparison import comparison_cases
from streaming_learning import compare_samples, input_identity
from training_provenance import source_provenance


def run_comparison(args):
    identity = input_identity(args.data_directory)
    if identity["training_rows"] % (2 * args.batch_size):
        raise ValueError("Training image count must be divisible by two worker batches")
    steps = identity["training_rows"] // (2 * args.batch_size)
    step = args.fault_after_step if args.fault_after_step is not None else steps // 2
    if not 0 < step < steps:
        raise ValueError("Fault step must leave real work before and after the fault in its epoch")
    cases = comparison_cases(args.epochs, args.failure_point, args.failure_kind)
    if args.controls_only:
        cases = cases[:1]
    provenance = source_provenance(ROOT)
    report = {
        "profile": "cifar-streaming-learning", "status": "running", "cases": cases,
        "input_identity": identity, "source_provenance": provenance,
        "training_epochs": args.epochs, "batch_size": args.batch_size,
        "steps_per_epoch": steps, "fault_after_step": step, "repeats": args.repeats,
        "observations_per_repetition": 2 * len(cases), "preliminary": args.repeats == 1,
        "samples": [], "pairs": [], "failed_observations": [],
        "comparison_axis": "Fixed-R OFF/full Train retry versus Fixed-R ON/full Train retry",
        "limitations": [
            "one physical machine; CPU Gloo, two training workers, shared surviving data/checkpoint storage",
            "CIFAR-10 subset and ResNet-18 with a 3x3 stride-1 stem, no initial max pool; short correctness run, not convergence/scale evidence",
            "PNG decode/normalization repeats lazily each epoch; no materialization, random augmentation or artificial sleeps",
            "4 MiB Data object-store scheduling budget and two data CPUs in both arms; a scheduling budget, not a physical memory cap",
            "all arms allow one standard full-group Train retry; application checkpoints every completed epoch",
            "node fault follows a real optimizer update with both ranks gated, not an arbitrary in-flight collective/kernel",
            "head processes are replaced using surviving local GCS RocksDB; driver/controller and executors survive",
            "Fixed-R still requires the data coordinator and protected executors to survive; worker-node loss can expose unsupported coverage",
            "default ownership; head loss need not fail ordinary Ray and ON completion need not involve task replay",
            "streaming_split does not guarantee identical batch assignment; report accuracy differences, do not assert equal SGD trajectories",
            "per-task and per-update local telemetry is included in workload time in both arms; clocks comparable only on this physical machine",
            "workload time excludes cluster startup and final checkpoint verification; observation_wall_s includes those costs and cleanup",
        ],
    }

    def save():
        write_json(args.output, report)
        write_json(args.result_directory / "comparison.json", report)

    save()
    controls, valid_controls = {}, set()
    for case in cases:
        scenario, point, epoch = case["scenario"], case["failure_point"], case["fault_after_epoch"]
        for pair in range(1, args.repeats + 1):
            samples = {}
            for mode in (("off", "on") if pair % 2 else ("on", "off")):
                options = {
                    "training_strategy": "ray-train-workload", "streaming_learning": True,
                    "comparison": "fixed-r", "scenario": scenario, "mode": mode,
                    "restart_scope": "full", "owner_placement": "default", "placement_strategy": "STRICT_SPREAD",
                    "timeout_s": args.timeout_s, "training_epochs": args.epochs,
                    "fault_after_epoch": epoch, "fault_after_step": step, "failure_timing": "active",
                    "steps_per_epoch": steps, "batch_size": args.batch_size,
                    "data_directory": str(args.data_directory), "input_identity": identity,
                    "workload": str(ROOT / "gossip_benchmarks/workloads/cifar_streaming.py"),
                    "workload_args": ["--data-directory", str(args.data_directory), "--epochs", str(args.epochs),
                                      "--batch-size", str(args.batch_size)],
                }
                print(f"{scenario}/{point}: pair {pair}/{args.repeats}, Fixed-R {mode.upper()}, full retry; "
                      f"timeout {args.timeout_s:g}s", flush=True)
                sample = run_observation(options, pair,
                                         args.result_directory / f"{scenario}-{point}-{pair}-{mode}", provenance)
                sample.update(failure_point=point, fault_after_epoch=epoch, fault_after_step=step,
                              restart_scope="full", training_epochs=args.epochs)
                if scenario == "none":
                    controls[(pair, mode)] = sample
                elif sample["status"] == "passed":
                    try:
                        if pair not in valid_controls:
                            raise ValueError("Missing passing control pair")
                        compare_samples(sample, controls[(pair, mode)], control=True)
                        sample["control_accuracy_difference_pp"] = 100 * (
                            sample["final_accuracy"] - controls[(pair, mode)]["final_accuracy"])
                        sample["workload_increase_vs_control_s"] = sample["workload_s"] - controls[(pair, mode)]["workload_s"]
                    except (ValueError, KeyError, AssertionError) as exc:
                        sample.update(status="failed", error=f"Control comparison: {exc}")
                samples[mode] = sample
                report["samples"].append(sample)
                write_json(Path(sample["directory"]) / "sample.json", sample)
                if sample["status"] != "passed":
                    report["failed_observations"].append({"scenario": scenario, "point": point, "mode": mode,
                                                          "pair": pair, "error": sample.get("error")})
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                save()
            try:
                values = compare_samples(samples["off"], samples["on"])
                report["pairs"].append({**case, "pair": pair, **values})
                if scenario == "none":
                    valid_controls.add(pair)
            except (ValueError, KeyError, AssertionError) as exc:
                report["failed_observations"].append({"scenario": scenario, "point": point, "pair": pair,
                                                      "error": f"Pair comparison: {exc}"})
            save()
        if scenario == "none" and len(valid_controls) != args.repeats:
            report["skipped_cases"] = cases[1:]
            break
    report["summary"] = []
    for case in cases:
        pairs = [p for p in report["pairs"] if p["scenario"] == case["scenario"] and p["failure_point"] == case["failure_point"]]
        changes = [p["workload_s_change_pct"] for p in pairs]
        report["summary"].append({**case, "valid_pairs": len(pairs), "requested_pairs": args.repeats,
                                  "workload_s_change_pct_mean": statistics.mean(changes) if changes else None,
                                  "workload_s_change_pct_stdev": statistics.stdev(changes) if len(changes) > 1 else None})
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}", flush=True)
    return int(report["status"] != "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--fault-after-step", type=int)
    parser.add_argument("--failure-point", action="append", choices=("early", "middle", "late"))
    parser.add_argument("--failure-kind", action="append", choices=("head-node", "worker-node"))
    parser.add_argument("--controls-only", action="store_true")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=300)
    args = parser.parse_args()
    if (sys.platform != "linux" or args.epochs < 4 or args.repeats < 1 or args.batch_size < 2
            or not math.isfinite(args.timeout_s) or args.timeout_s <= 0):
        parser.error("Use Linux, >=4 epochs, positive repeats, batch size >=2 and finite positive timeout")
    args.failure_point = args.failure_point or ["middle"]
    args.failure_kind = args.failure_kind or ["head-node"]
    args.data_directory = args.data_directory.resolve()
    args.result_directory = args.result_directory.resolve()
    args.result_directory.mkdir(parents=True, exist_ok=True)
    return run_comparison(args)


if __name__ == "__main__":
    sys.exit(main())
