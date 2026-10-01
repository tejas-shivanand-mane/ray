"""Short CIFAR/ResNet coordinator-process comparison; plot the JSON separately."""

import argparse
from datetime import datetime, timezone
import math
from pathlib import Path
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from coordinator_comparison import compare_samples
from streaming_learning import input_identity
from training_provenance import source_provenance


def run_comparison(args):
    identity = input_identity(args.data_directory)
    if identity["training_rows"] % (2 * args.batch_size):
        raise ValueError("Training images must divide evenly into two worker batches")
    steps = identity["training_rows"] // (2 * args.batch_size)
    step = args.fault_after_step if args.fault_after_step is not None else steps // 2
    if not 0 < step < steps:
        raise ValueError("Leave optimizer work before and after the fault within its epoch")
    modes = ["ordinary", "resume"]
    if args.include_deterministic_baseline:
        modes.insert(1, "deterministic")
    cases = ["none"] if args.controls_only else ["none", "coordinator-process"]
    provenance = source_provenance(ROOT)
    report = {
        "profile": "cifar-coordinator-resume", "status": "running", "modes": modes,
        "scenarios": cases, "repeats": args.repeats, "preliminary": args.repeats == 1,
        "training_epochs": args.epochs, "steps_per_epoch": steps, "batch_size": args.batch_size,
        "fault_after_epoch": args.fault_after_epoch, "fault_after_step": step,
        "observations_per_repetition": len(cases) * len(modes), "timeout_s": args.timeout_s,
        "input_identity": identity, "source_provenance": provenance,
        "samples": [], "comparisons": [], "control_comparisons": [], "skipped": [], "comparison_errors": [],
        "limitations": [
            "Fixed-R OFF and ordinary full-group Train retry enabled in every arm; input resume is an independent Ray Data prototype",
            "only the original data-coordinator process is killed, at its natural placement; no head/node/driver/storage failure",
            "two CPU/Gloo training workers on separate logical nodes on one physical machine; all nodes and shared storage survive",
            "existing CIFAR-10 subset/ResNet-18 workload unchanged; real learning and lazy PNG decode, not convergence or scale evidence",
            "failure after a real Adam update with both ranks gated in an unfinished epoch, not during an arbitrary collective/kernel",
            "application model/Adam/epoch/RNG checkpoints every epoch and one full-group Train retry in all modes",
            "ordinary and resumable splitters can assign different batches to ranks; only deterministic arms demand exact checkpoint and sample-order agreement with their own controls",
            "optional deterministic baseline uses the prototype splitter with zero coordinator restarts; it is not ordinary Ray",
            "instrumentation and fault-gate time included equally; workload excludes cluster startup and final verification, observation time includes them and cleanup",
            "prefix replay can be expensive; no speed or overhead advantage is assumed; one repetition is preliminary",
        ],
    }

    def save():
        write_json(args.output, report)
        write_json(args.result_directory / "comparison.json", report)

    save()
    controls = {}
    for scenario in cases:
        for pair in range(1, args.repeats + 1):
            samples = {}
            order = modes if pair % 2 else list(reversed(modes))
            for mode in order:
                if scenario != "none" and (pair, mode) not in controls:
                    report["skipped"].append({"scenario": scenario, "pair": pair, "mode": mode,
                                              "reason": "Matching control did not pass"})
                    save()
                    continue
                options = {
                    "training_strategy": "coordinator-input-resume", "streaming_learning": True,
                    "mode": mode, "scenario": scenario, "failure_timing": "active",
                    "training_epochs": args.epochs, "batch_size": args.batch_size, "steps_per_epoch": steps,
                    "fault_after_epoch": args.fault_after_epoch, "fault_after_step": step,
                    "timeout_s": args.timeout_s, "data_directory": str(args.data_directory),
                    "input_identity": identity,
                }
                print(f"{scenario}: pair {pair}/{args.repeats}, {mode}, full Train retry; "
                      f"timeout {args.timeout_s:g}s", flush=True)
                sample = run_observation(options, pair, args.result_directory / scenario / f"pair-{pair}-{mode}", provenance)
                sample.update(fault_after_epoch=args.fault_after_epoch, fault_after_step=step)
                report["samples"].append(sample)
                samples[mode] = sample
                if sample["status"] == "passed":
                    if scenario == "none":
                        controls[pair, mode] = sample
                    else:
                        try:
                            comparison = compare_samples(controls[pair, mode], sample, control=True)
                            report["control_comparisons"].append({"mode": mode, "pair": pair, **comparison})
                        except ValueError as exc:
                            sample.update(status="failed", validation_status="failed", error=str(exc))
                            report["comparison_errors"].append(str(exc))
                save()
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                if sample.get("recovery"):
                    r = sample["recovery"]
                    print(f"  {r['kind']}; observed repeated optimizer updates/rank: "
                          f"{r['repeated_optimizer_updates_per_rank']}", flush=True)
            pair_modes = [("ordinary", mode) for mode in modes if mode != "ordinary"]
            if "deterministic" in modes:
                pair_modes.append(("deterministic", "resume"))
            for left_mode, right_mode in pair_modes:
                left, right = samples.get(left_mode), samples.get(right_mode)
                if not left or not right or any(s["status"] != "passed" for s in (left, right)):
                    continue
                try:
                    report["comparisons"].append({"scenario": scenario, "pair": pair,
                                                  "left": left_mode, "right": right_mode,
                                                  **compare_samples(left, right)})
                except ValueError as exc:
                    report["comparison_errors"].append(str(exc))
            save()
    report["status"] = "failed" if (report["skipped"] or report["comparison_errors"]
                                      or any(s["status"] != "passed" for s in report["samples"])) else "passed"
    save()
    print(f"Report: {args.output}")
    for row in report["comparisons"]:
        print(f"{row['scenario']}, pair {row['pair']}: {row['right']} versus {row['left']} workload time "
              f"{row['workload_s_change_pct']:+.2f}%")
    return 0 if report["status"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--fault-after-epoch", type=int, default=1)
    parser.add_argument("--fault-after-step", type=int)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=300)
    parser.add_argument("--controls-only", action="store_true")
    parser.add_argument("--include-deterministic-baseline", action="store_true")
    parser.add_argument("--result-directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (sys.platform != "linux" or args.epochs < 2 or args.batch_size < 1 or args.repeats < 1
            or not 1 <= args.fault_after_epoch < args.epochs
            or not math.isfinite(args.timeout_s) or args.timeout_s <= 0):
        parser.error("Use Linux, >=2 epochs, positive batch/repeats/timeout and a completed epoch before the fault")
    args.data_directory = args.data_directory.expanduser().resolve()
    args.result_directory = (args.result_directory or Path.home() / "ray-coverage" / (
        "coordinator-training-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f"))).resolve()
    args.result_directory.mkdir(parents=True, exist_ok=True)
    args.output = (args.output or args.result_directory / "comparison.json").resolve()
    return run_comparison(args)


if __name__ == "__main__":
    sys.exit(main())
