"""Measure ordinary application restart versus Fixed-R through verified completion.

Both arms include one initial cluster startup, preprocessing, training, validation
and cleanup. OFF reruns the whole application once after verified owner loss on
the SAME repaired cluster. Standard Train retries remain enabled in both arms.
Use --feature-directory for real frozen MobileNet image feature extraction.
Plotting is a separate command and never runs here.
"""

import argparse
import math
from pathlib import Path
import statistics
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from run_fashion_owner_comparison import POINTS, compare_owner_pair, match_control, verify_progress
from fashion_comparison import feature_identity, input_identity, predictions_match
from train_restart import verify_owner_loss
from training_provenance import source_provenance


def check_trial(sample, control=None):
    if sample["status"] != "passed" or sample.get("timeout") or not sample.get("workload_completed"):
        raise ValueError("Trial did not reach verified completion within its total budget")
    attempts = sample["attempts"]
    if type(sample["whole_workload_restarts"]) is not int or sample["whole_workload_restarts"] not in (0, 1):
        raise ValueError("Invalid restart count")
    expected = 2 if sample["whole_workload_restarts"] else 1
    if len(attempts) != expected or expected not in (1, 2):
        raise ValueError("Invalid application attempt count")
    if expected == 2:
        verify_owner_loss(attempts[0])
        if (sample["mode"] != "off" or attempts[1]["scenario"] != "none"
                or attempts[1]["failure_point"] != "none"
                or attempts[0]["driver_identity"] != attempts[1]["driver_identity"]
                or attempts[1]["selected_owner_node_id"] != attempts[0]["data_owner_fault"]["head_replacement"]["replacement_head_node_id"]
                or attempts[1]["attempt_started_ns"] < attempts[0]["attempt_finished_ns"]):
            raise ValueError("Restart did not reuse the repaired cluster and surviving driver")
    final = attempts[-1]
    for attempt in attempts:
        verify_progress(attempt)
        if sample.get("feature_identity") != attempt.get("feature_identity"):
            raise ValueError("Feature weights differ between attempts")
        if sample.get("feature_identity") is not None:
            progress = attempt["feature_progress"]
            if (progress["training_images"] != 60000 or progress["feature_width"] != 576
                    or not attempt["workload_started_ns"] <= progress["feature_started_ns"] < progress["feature_ready_ns"]):
                raise ValueError("Missing full training-image feature extraction")
            if attempt["scenario"] == "data-owner" and progress["feature_ready_ns"] >= attempt["data_owner_fault"]["request_ns"]:
                raise ValueError("Owner failure preceded the completed feature work")
    if final["status"] != "passed" or final["scenario"] not in ("none", "data-owner"):
        raise ValueError("Last attempt failed")
    if control is not None:
        for attempt in attempts:
            match_control(attempt, control["attempts"][-1])
    elif sample["scenario"] != "none":
        raise ValueError("Fault trial lacks a matched no-failure control")
    elapsed = sample["observation_wall_s"]
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError("Invalid end-to-end duration")
    if not sample["observation_started_ns"] <= attempts[0]["attempt_started_ns"] < final["attempt_finished_ns"] <= sample["observation_finished_ns"]:
        raise ValueError("Attempt timing lies outside observation timing")
    return elapsed


def compare_trials(off, on, controls=None):
    times = [check_trial(s, controls[s["mode"]] if controls else None) for s in (off, on)]
    if off.get("feature_identity") != on.get("feature_identity"):
        raise ValueError("Different feature workloads")
    # Validate the original owner-loss event against ON replay, independently
    # of OFF's later application restart. Then compare the actual final outputs.
    initial = compare_owner_pair(off["attempts"][0], on["attempts"][0])
    error = predictions_match(off["attempts"][-1], on["attempts"][-1])
    return {"off_completion_wall_s": times[0], "on_completion_wall_s": times[1],
            "on_vs_off_pct": 100 * (times[1] / times[0] - 1),
            "off_application_restarts": off["whole_workload_restarts"],
            "on_application_restarts": on["whole_workload_restarts"],
            "predictions_max_abs_difference": error,
            "owner_loss_demonstrated": initial.get("owner_loss_demonstrated", False),
            "on_replayed_tasks": initial["on_replayed_tasks"]}


def run_comparison(args, directory, provenance):
    identity = input_identity(args.data_directory)
    features = feature_identity(args.feature_directory) if args.feature_directory else None
    points = {"none": 0, **{p: POINTS[p] for p in dict.fromkeys(args.failure_point)}}
    report = {"profile": "fashion-mnist-owner-restart", "status": "running", "samples": [], "pairs": [],
              "failed_comparisons": [], "source_provenance": provenance, "input_identity": identity,
              "feature_identity": features, "failure_points": points, "training_epochs": args.epochs,
              "repeats": args.repeats, "preliminary": args.repeats == 1, "timeout_s": args.timeout_s,
              "measurement_scope": "parent wall time through verified completion: initial startup, all application attempts, validation and cleanup",
              "restart_scope": "same repaired cluster and driver; one fresh application invocation after verified OFF owner loss",
              "limitations": [
                  "controlled map ordering and matched head ownership in both arms, including controls",
                  "GCS disk, off-head driver, executors and original inputs survive; no physical-machine loss",
                  "failure follows image feature extraction when enabled, but precedes training; not interrupted-training recovery",
                  "OFF restart preserves the cluster and OS caches; no application training-feature cache or preprocessing checkpoint",
                  "same standard Train checkpoint retry policy; one injected failure per trial, not per attempt",
                  "end-to-end timing includes identical correctness probes and cleanup; reported separately from workload-only durations",
                  "neither a general advantage nor a break-even failure rate is established by a single pair",
              ]}

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    controls = {}
    save()
    for point, index in points.items():
        for pair in range(1, args.repeats + 1):
            samples = {}
            for mode in (("off", "on") if pair % 2 else ("on", "off")):
                name = "fashion_features.py" if features else "fashion_mnist.py"
                options = {
                    "training_strategy": "ray-train-workload-restart", "comparison": "fixed-r",
                    "scenario": "none" if point == "none" else "data-owner", "failure_point": point,
                    "mode": mode, "restart_scope": "full", "owner_placement": "head", "placement_strategy": "STRICT_SPREAD",
                    "timeout_s": args.timeout_s, "training_epochs": args.epochs, "fault_after_epoch": 0,
                    "data_directory": str(args.data_directory), "input_identity": identity,
                    "owner_progress_plan": {"map_count": 4, "target_index": index, "inject": point != "none"},
                    "workload": str(ROOT / "gossip_benchmarks/workloads" / name),
                    "workload_args": ["--data-directory", str(args.data_directory), "--epochs", str(args.epochs)],
                }
                if features:
                    options.update(feature_directory=str(args.feature_directory), feature_identity=features)
                    options["workload_args"] += ["--feature-directory", str(args.feature_directory)]
                print(f"{point}: pair {pair}/{args.repeats}, {mode.upper()}, total trial budget {args.timeout_s:g}s", flush=True)
                sample = run_observation(options, pair, directory / f"{point}-{pair}-{mode}", provenance)
                sample.update(failure_point=point)
                samples[mode] = sample
                report["samples"].append(sample)
                print(f"  {sample['status']}: {sample['observation_wall_s']:.2f}s total; "
                      f"{sample.get('whole_workload_restarts', '?')} application restarts", flush=True)
                save()
            try:
                values = compare_trials(samples["off"], samples["on"], controls.get(pair))
                report["pairs"].append({"failure_point": point, "pair": pair, **values})
                if point == "none":
                    controls[pair] = samples
                print(f"  ON versus OFF restart: {values['on_vs_off_pct']:+.2f}% total time", flush=True)
            except (KeyError, ValueError, AssertionError, OSError, IndexError) as exc:
                report["failed_comparisons"].append({"failure_point": point, "pair": pair, "error": str(exc)})
                print(f"  Invalid comparison: {exc}", flush=True)
            save()
        if point == "none" and len(controls) != args.repeats:
            report["skipped_failure_points"] = list(points)[1:]
            break
    report["summary"] = []
    for point in points:
        pairs = [p for p in report["pairs"] if p["failure_point"] == point]
        ratios = [p["on_vs_off_pct"] for p in pairs]
        report["summary"].append({"failure_point": point, "valid_pairs": len(pairs), "requested_pairs": args.repeats,
                                  "on_vs_off_pct_mean": statistics.mean(ratios) if ratios else None,
                                  "on_vs_off_pct_stdev": statistics.stdev(ratios) if len(ratios) > 1 else None})
    report["status"] = "failed" if report["failed_comparisons"] else "passed"
    save()
    print(f"Report: {args.output}", flush=True)
    return int(report["status"] != "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--feature-directory", type=Path, help="Prepared MobileNet weights; omit for the original small workload")
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--failure-point", action="append", choices=tuple(POINTS))
    parser.add_argument("--timeout-s", type=float,
                        help="Total budget for both OFF attempts; default 1800s with features, 180s with raw pixels")
    args = parser.parse_args()
    if args.timeout_s is None:
        args.timeout_s = 1800 if args.feature_directory else 180
    if sys.platform != "linux" or args.epochs < 4 or args.repeats < 1 or not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use Linux, at least four epochs, positive repeats and a finite positive timeout")
    args.failure_point = args.failure_point or list(POINTS)
    args.data_directory = args.data_directory.resolve()
    if args.feature_directory:
        args.feature_directory = args.feature_directory.resolve()
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return run_comparison(args, directory, source_provenance(ROOT))


if __name__ == "__main__":
    sys.exit(main())
