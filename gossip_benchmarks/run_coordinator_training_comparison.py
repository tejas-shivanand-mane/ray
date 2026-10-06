"""Short CIFAR/ResNet coordinator-process comparison; plot the JSON separately."""

import argparse
from datetime import datetime, timezone
import math
from pathlib import Path
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from coordinator_comparison import compare_samples, failure_cases, summarize_comparisons
from streaming_learning import input_identity
from training_provenance import source_provenance


def run_comparison(args):
    identity = input_identity(args.data_directory)
    if identity["training_rows"] % (2 * args.batch_size):
        raise ValueError("Training images must divide evenly into two worker batches")
    steps = identity["training_rows"] // (2 * args.batch_size)
    cases = failure_cases(steps, args.failure_point, args.fault_after_step, args.controls_only)
    failure_scope = getattr(args, "failure_scope", "process")
    if failure_scope == "node":
        if not args.same_sharding_only:
            raise ValueError("--failure-scope node requires --same-sharding-only for matched placement")
        for case in cases:
            if case["scenario"] != "none":
                case["scenario"] = "coordinator-node"
    modes = ["deterministic", "resume"] if args.same_sharding_only else ["ordinary", "resume"]
    if args.include_deterministic_baseline:
        modes.insert(1, "deterministic")
    pair_modes = [("ordinary", mode) for mode in modes if mode != "ordinary"] if "ordinary" in modes else []
    if "deterministic" in modes:
        pair_modes.append(("deterministic", "resume"))
    provenance = source_provenance(ROOT)
    report = {
        "profile": "cifar-coordinator-resume", "status": "running", "modes": modes,
        "scenarios": list(dict.fromkeys(c["scenario"] for c in cases)), "cases": cases,
        "repeats": args.repeats, "preliminary": args.repeats == 1,
        "comparison_modes": pair_modes,
        "training_epochs": args.epochs, "steps_per_epoch": steps, "batch_size": args.batch_size,
        "fault_after_epoch": args.fault_after_epoch,
        "failure_scope": failure_scope,
        "fault_after_step": cases[-1]["fault_after_step"] if len(cases) <= 2 else None,
        "observations_per_repetition": len(cases) * len(modes), "timeout_s": args.timeout_s,
        "input_identity": identity, "source_provenance": provenance,
        "samples": [], "comparisons": [], "control_comparisons": [], "skipped": [], "comparison_errors": [],
        "limitations": [
            "Fixed-R OFF and ordinary full-group Train retry enabled in every arm; input resume is an independent Ray Data prototype",
            ("one dedicated logical coordinator node is removed in both arms; controlled separate-owner placement, not default Ray placement"
             if failure_scope == "node" else "only the original data-coordinator process is killed, at its natural placement; no node failure"),
            "two CPU/Gloo training workers on separate logical nodes on one physical machine; driver, head, actor owner, training nodes and shared storage survive",
            "existing CIFAR-10 subset/ResNet-18 workload unchanged; real learning and lazy PNG decode, not convergence or scale evidence",
            "failure after a real Adam update with both ranks gated in an unfinished epoch, not during an arbitrary collective/kernel",
            "early/middle/late refer to progress within the epoch after the selected checkpoint, not fractions of total job time",
            "one fresh control per mode and repetition is shared across requested fault points; comparisons to that control are correlated",
            "application model/Adam/epoch/RNG checkpoints every epoch and one full-group Train retry in all modes",
            "ordinary and resumable splitters can assign different batches to ranks; only deterministic arms demand exact checkpoint and sample-order agreement with their own controls",
            "optional deterministic baseline uses the prototype splitter with zero coordinator restarts; it is not ordinary Ray",
            "instrumentation and fault-gate time included equally; workload excludes cluster startup and final verification, observation time includes them and cleanup",
            "prefix replay can be expensive; no speed or overhead advantage is assumed; one repetition is preliminary",
        ],
    }

    def save():
        report["summary"] = summarize_comparisons(report)
        write_json(args.output, report)
        write_json(args.result_directory / "comparison.json", report)

    save()
    controls = {}
    for case in cases:
        scenario, point, step = case["scenario"], case["failure_point"], case["fault_after_step"]
        for pair in range(1, args.repeats + 1):
            samples = {}
            order = modes if pair % 2 else list(reversed(modes))
            for mode in order:
                if scenario != "none" and (pair, mode) not in controls:
                    report["skipped"].append({"scenario": scenario, "failure_point": point, "pair": pair, "mode": mode,
                                              "reason": "Matching control did not pass"})
                    save()
                    continue
                options = {
                    "training_strategy": "coordinator-input-resume", "streaming_learning": True,
                    "mode": mode, "scenario": scenario, "failure_point": point, "failure_timing": "active",
                    "failure_scope": failure_scope,
                    "training_epochs": args.epochs, "batch_size": args.batch_size, "steps_per_epoch": steps,
                    "fault_after_epoch": args.fault_after_epoch, "fault_after_step": step,
                    "timeout_s": args.timeout_s, "data_directory": str(args.data_directory),
                    "input_identity": identity,
                }
                print(f"{scenario}/{point}: pair {pair}/{args.repeats}, {mode}, full Train retry; "
                      f"timeout {args.timeout_s:g}s", flush=True)
                sample = run_observation(options, pair, args.result_directory / scenario / point / f"pair-{pair}-{mode}", provenance)
                sample.update(failure_point=point, fault_after_epoch=args.fault_after_epoch, fault_after_step=step)
                report["samples"].append(sample)
                samples[mode] = sample
                if sample["status"] == "passed":
                    if scenario == "none":
                        controls[pair, mode] = sample
                    else:
                        try:
                            comparison = compare_samples(controls[pair, mode], sample, control=True)
                            report["control_comparisons"].append({"mode": mode, "failure_point": point, "pair": pair, **comparison})
                        except ValueError as exc:
                            sample.update(status="failed", validation_status="failed", error=str(exc))
                            report["comparison_errors"].append(str(exc))
                save()
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                if sample.get("recovery"):
                    r = sample["recovery"]
                    print(f"  {r['kind']}; observed repeated optimizer updates/rank: "
                          f"{r['repeated_optimizer_updates_per_rank']}", flush=True)
            for left_mode, right_mode in pair_modes:
                left, right = samples.get(left_mode), samples.get(right_mode)
                if not left or not right or any(s["status"] != "passed" for s in (left, right)):
                    continue
                try:
                    report["comparisons"].append({"scenario": scenario, "failure_point": point, "pair": pair,
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
        print(f"{row['scenario']}/{row['failure_point']}, pair {row['pair']}: {row['right']} versus {row['left']} workload time "
              f"{row['workload_s_change_pct']:+.2f}%")
    for row in report["summary"]:
        mean, stdev = row["paired_change_pct"]["mean"], row["paired_change_pct"]["stdev"]
        spread = f", sample SD {stdev:.2f} percentage points" if stdev is not None else ", SD unavailable"
        timing = f"mean paired change {mean:+.2f}%{spread}" if mean is not None else "no valid timing pairs"
        print(f"{row['scenario']}/{row['failure_point']}: {row['right']} versus {row['left']}: {timing}; "
              f"{row['completed_pairs']}/{row['requested_pairs']} valid pairs")
    return 0 if report["status"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--fault-after-epoch", type=int, default=1)
    points = parser.add_mutually_exclusive_group()
    points.add_argument("--fault-after-step", type=int)
    points.add_argument("--failure-point", action="append", choices=("early", "middle", "late", "all"),
                        help="Repeat to select points within the next epoch; default: middle")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=300)
    parser.add_argument("--controls-only", action="store_true")
    parser.add_argument("--failure-scope", choices=("process", "node"), default="process",
                        help="Node loss uses an explicit separate-coordinator topology in both arms")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--include-deterministic-baseline", action="store_true")
    modes.add_argument("--same-sharding-only", action="store_true",
                       help="Compare deterministic checkpoint retry with input resume; omit ordinary Ray")
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
