"""Matched head-owner loss during Fashion-MNIST preprocessing; writes JSON only.

Both arms use standard Train checkpoint retry. OFF/ON differ in Fixed-R.
Four shuffle maps compute in a controlled order in every run. Faults precede
map indices 0, 2 and 3, after 0/4, 2/4 and 3/4 maps have computed partitions.
This is an explicit head-ownership topology on one physical machine. Head
processes are replaced; GCS disk, the driver, executors and input files survive.
"""

import argparse
import math
from pathlib import Path
import statistics
import sys

from run_fixed_r_train_comparison import ROOT, run_observation, write_json
from run_train_retry_comparison import compare_fixed_r_pair
from fashion_comparison import PROVENANCE_KEYS, input_identity, predictions_match
from training_provenance import source_provenance


POINTS = {"early": 0, "middle": 2, "late": 3}


def verify_progress(sample):
    plan = sample["owner_progress_plan"]
    index = plan["target_index"]
    progress = sample["map_progress"]
    point = sample["failure_point"]
    expected_index = 0 if point == "none" else POINTS[point]
    if (plan["map_count"] != 4 or type(index) is not int or not 0 <= index < 4
            or index != expected_index
            or sample["scenario"] != ("none" if point == "none" else "data-owner")
            or [e["index"] for e in progress] != list(range(len(progress)))
            or len(progress) > 4):
        raise ValueError("Invalid ordered map progress")
    previous = sample["workload_started_ns"]
    for event in progress:
        if event["time_ns"] <= previous or not event["task_id"] or not event["node_id"]:
            raise ValueError("Invalid map computation timestamp or identity")
        previous = event["time_ns"]
    if sample["status"] == "passed" and (
            len(progress) != 4 or sample.get("timeout") or sample.get("workload_completed") is not True):
        raise ValueError("Completed workload lacks completion evidence or four computed maps")
    if sample["scenario"] == "none":
        if plan["inject"]:
            raise ValueError("Control requested an injected failure")
        return
    fault = sample["data_owner_fault"]
    prefix = fault["completed_maps_before_failure"]
    head = fault["head_replacement"]
    if (plan["inject"] is not True or fault["progress_plan"] != plan
            or fault["target"]["map_index"] != index
            or prefix != progress[:index] or len(prefix) != index
            or any(e["time_ns"] > fault["target"]["blocked_ns"] for e in prefix)
            or not sample["workload_started_ns"] <= fault["target"]["blocked_ns"]
            <= fault["request_ns"] < fault["replacement_ready_ns"]
            or head["gcs_storage_backend"] != "rocksdb"
            or head["original_head_node_id"] == head["replacement_head_node_id"]
            or head["original_gcs_pid"] == head["replacement_gcs_pid"]
            or fault["target"]["node_id"] not in head["surviving_node_ids"]):
        raise ValueError("Failure did not verify the requested preprocessing point")
    if sample.get("reports") and sample["reports"][0]["time_ns"] <= fault["replacement_ready_ns"]:
        raise ValueError("Owner failure did not precede training")


def match_control(sample, control):
    if control["status"] != "passed" or control["scenario"] != "none":
        raise ValueError("Missing successful matching control")
    for key in ("mode", "restart_scope", "input_identity", "training_epochs", "workload_sha256",
                "torch_version", "owner_placement", "placement_strategy", "native_settings"):
        if sample[key] != control[key]:
            raise ValueError(f"Control differs in {key}")
    for key in PROVENANCE_KEYS:
        if sample["provenance"][key] != control["provenance"][key]:
            raise ValueError(f"Control provenance differs in {key}")
    sample["no_failure_control_passed"] = True
    if sample["status"] == "passed":
        sample["control_predictions_max_abs_difference"] = predictions_match(sample, control)
        sample["matches_no_failure_predictions"] = True


def compare_owner_pair(off, on):
    for key in ("input_identity", "failure_point", "owner_progress_plan", "placement_strategy"):
        if off[key] != on[key]:
            raise ValueError(f"Unmatched owner experiment: {key}")
    if off["owner_placement"] != "head" or off["placement_strategy"] != "STRICT_SPREAD":
        raise ValueError("Expected matched head ownership and spread training workers")
    for sample in (off, on):
        verify_progress(sample)
    # Existing checks require the exact owner/object failure, real head loss,
    # matched settings/build, target task replay and control predictions.
    values = compare_fixed_r_pair(off, on)
    if off["status"] == "passed":
        values["predictions_max_abs_difference"] = predictions_match(off, on)
    return values


def run_comparison(args, directory, provenance):
    points = {"none": 0, **{p: POINTS[p] for p in dict.fromkeys(args.failure_point)}}
    identity = input_identity(args.data_directory)
    report = {
        "profile": "fashion-mnist-owner-replay", "status": "running",
        "source_provenance": provenance, "input_identity": identity,
        "training_epochs": args.epochs, "map_count": 4, "failure_points": points,
        "repeats": args.repeats, "preliminary": args.repeats == 1,
        "observations_per_repetition": 2 * len(points), "samples": [], "pairs": [],
        "failed_comparisons": [],
        "comparison_axis": "Fixed-R OFF versus ON; standard full-group Train retry in both",
        "limitations": [
            "explicit matched head ownership and ordered map computation in controls and faults; not default Ray Data placement",
            "early/middle/late refer to 0/4, 2/4 and 3/4 shuffle map computations, not wall-time fractions or training epochs",
            "computed maps are not necessarily consumed or durably copied outputs",
            "head processes and owners die; local GCS RocksDB disk, driver/controller, executors and input/checkpoint files survive",
            "head replacement is external and identical in both arms; not physical-machine or driver recovery",
            "training starts after preprocessing; checkpoints cannot restore a training run that has not started",
            "failed OFF observations remain failed; verified OwnerDiedError can satisfy comparison criteria without a speedup",
            "full Fashion-MNIST with a modest CPU MLP; no claim about large distributed training or adaptive Succession",
        ],
    }

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    save()
    controls, valid_controls = {}, set()
    for point, index in points.items():
        for pair in range(1, args.repeats + 1):
            samples = {}
            for mode in (("off", "on") if pair % 2 else ("on", "off")):
                options = {
                    "training_strategy": "ray-train-workload", "comparison": "fixed-r",
                    "scenario": "none" if point == "none" else "data-owner",
                    "mode": mode, "restart_scope": "full", "owner_placement": "head",
                    "placement_strategy": "STRICT_SPREAD", "timeout_s": args.timeout_s,
                    "training_epochs": args.epochs, "fault_after_epoch": 0,
                    "data_directory": str(args.data_directory), "input_identity": identity,
                    "owner_progress_plan": {"map_count": 4, "target_index": index, "inject": point != "none"},
                    "workload": str(ROOT / "gossip_benchmarks/workloads/fashion_mnist.py"),
                    "workload_args": ["--data-directory", str(args.data_directory), "--epochs", str(args.epochs)],
                }
                print(f"{point}: pair {pair}/{args.repeats}, Fixed-R {mode.upper()}, "
                      f"{index}/4 maps before fault, full Train retry ({args.timeout_s:g}s)", flush=True)
                sample = run_observation(options, pair, directory / f"{point}-{pair}-{mode}", provenance)
                sample.update(failure_point=point, arm="ordinary" if mode == "off" else "integrated")
                samples[mode] = sample
                report["samples"].append(sample)
                if point == "none":
                    controls[(pair, mode)] = sample
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s')) + 's workload')}", flush=True)
                save()
            try:
                if point != "none":
                    if pair not in valid_controls:
                        raise ValueError("Matched controls did not pass")
                    for mode, sample in samples.items():
                        match_control(sample, controls[(pair, mode)])
                values = compare_owner_pair(samples["off"], samples["on"])
                report["pairs"].append({"failure_point": point, "pair": pair, **values})
                if point == "none":
                    valid_controls.add(pair)
                elif values.get("owner_loss_demonstrated"):
                    samples["off"]["expected_owner_loss"] = True
                    print("  Verified OFF OwnerDiedError; ON replayed and matched control predictions.", flush=True)
            except (KeyError, ValueError, AssertionError, OSError, IndexError) as exc:
                report["failed_comparisons"].append({"failure_point": point, "pair": pair, "error": str(exc)})
                print(f"  Invalid comparison: {exc}", flush=True)
            save()
        if point == "none" and len(valid_controls) != args.repeats:
            report["skipped_failure_points"] = list(points)[1:]
            break
    report["summary"] = []
    for point in points:
        pairs = [p for p in report["pairs"] if p["failure_point"] == point]
        changes = [p["on_vs_off_pct"] for p in pairs if p["on_vs_off_pct"] is not None]
        report["summary"].append({
            "failure_point": point, "valid_pairs": len(pairs), "requested_pairs": args.repeats,
            "off_completed": sum(p["off_completed"] for p in pairs),
            "on_completed": sum(p["on_completed"] for p in pairs),
            "verified_owner_loss_pairs": sum(p.get("owner_loss_demonstrated", False) for p in pairs),
            "completion_change_pct_mean": statistics.mean(changes) if changes else None,
            "completion_change_pct_stdev": statistics.stdev(changes) if len(changes) > 1 else None,
        })
    report["status"] = "failed" if report["failed_comparisons"] else "passed"
    save()
    print(f"Report: {args.output}", flush=True)
    return int(report["status"] != "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--failure-point", action="append", choices=tuple(POINTS))
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=180)
    args = parser.parse_args()
    if sys.platform != "linux" or args.epochs < 4 or args.repeats < 1:
        parser.error("Use Linux, at least four epochs and positive repeats")
    if not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use a finite positive timeout")
    args.failure_point = args.failure_point or list(POINTS)
    args.data_directory = args.data_directory.resolve()
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return run_comparison(args, directory, source_provenance(ROOT))


if __name__ == "__main__":
    sys.exit(main())
