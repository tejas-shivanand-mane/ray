"""Compare full and selective worker replacement at XGBoost checkpoint boundaries.

This experimental controller is separate from Ray Train's ordinary retry path.
Both policies use the same Fixed-R setting (ON by default), two fixed data
partitions, three distributed segments, and node failures at rounds 3 and 6.
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
    if full["status"] != "passed" or selective["status"] != "passed":
        raise ValueError("Both replacement policies must pass before comparison")
    for key in ("source_sha256", "native_extension_sha256", "python", "platform",
                "xgboost_version", "ray_version", "numpy_version", "pyarrow_version", "pandas_version"):
        if full["provenance"][key] != selective["provenance"][key]:
            raise ValueError(f"Cannot compare different {key}")
    if full["native_settings"] != selective["native_settings"]:
        raise ValueError("Cannot compare different protection settings")
    a = np.load(Path(full["directory"]) / "predictions.npy", allow_pickle=False)
    b = np.load(Path(selective["directory"]) / "predictions.npy", allow_pickle=False)
    np.testing.assert_allclose(b, a, rtol=1e-6, atol=1e-7)
    rows = []
    for metric in ("training_s", "prediction_s", "pipeline_s"):
        before, after = full[metric], selective[metric]
        if not all(math.isfinite(value) and value > 0 for value in (before, after)):
            raise ValueError("Invalid comparison timing")
        rows.append({"metric": metric, "full_s": before, "selective_s": after,
                     "selective_vs_full_pct": 100 * (after / before - 1)})
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("off", "on"), default="on",
                        help="Same native/Data protection in both policies; not the comparison axis")
    parser.add_argument("--timeout-s", type=float, default=120)
    parser.add_argument("--repeats", type=int, default=1)
    args = parser.parse_args()
    if sys.platform != "linux" or args.repeats < 1 or not math.isfinite(args.timeout_s) or args.timeout_s <= 0:
        parser.error("Use Linux, positive repeats and a finite positive timeout")
    directory = args.result_directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    options = {"scenario": "worker-node", "mode": args.mode, "timeout_s": args.timeout_s,
               "training_strategy": "checkpoint-boundary"}
    provenance = source_provenance(ROOT)
    report = {"profile": "experimental-selective-xgboost-checkpoint-boundary", "status": "running",
              "source_provenance": provenance, "settings": options, "samples": [], "pairs": [],
              "preliminary": args.repeats == 1,
              "comparison_axis": "full versus selective replacement; Fixed-R setting identical",
              "scope": "two sequential logical node losses after finalized collectives; shared local storage survives",
              "limitations": ["explicit checkpoint boundaries, not failures inside collectives",
                              "experimental controller, not Ray Train retry integration",
                              "fixed partitions; healthy ranks pause and join a new collective",
                              "DMatrix rebuilt on every rank; loaded input and healthy model retained"],
              "measurement_scope": "instrumented segments, ingestion, checkpointing and recovery; excludes cluster startup",
              "prediction_scope": "both surviving worker models evaluate all rows; this is an integrity probe, not the original inference pipeline",
              "failure_rounds": [3, 6], "target_rounds": 10, "failed_observations": []}

    def save():
        for path in {args.output.resolve(), directory / "comparison.json"}:
            write_json(path, report)

    save()
    for pair in range(1, args.repeats + 1):
        observations = {}
        for policy in (("full", "selective") if pair % 2 else ("selective", "full")):
            print(f"pair {pair}/{args.repeats}: {policy} replacement, Fixed-R {args.mode.upper()} ({args.timeout_s:g}s)", flush=True)
            sample = run_observation({**options, "restart_scope": policy}, pair,
                                     directory / f"pair-{pair}-{policy}", provenance)
            sample["restart_scope"] = policy
            report["samples"].append(sample)
            observations[policy] = sample
            if sample["status"] != "passed":
                report["failed_observations"].append({"pair": pair, "restart_scope": policy, "error": sample.get("error")})
            print(f"  {sample['status']}: {sample.get('error', str(sample.get('training_s')) + 's training')}", flush=True)
            for recovery in sample.get("recoveries", []):
                print(f"    checkpoint {recovery['checkpoint_round']}: preserved ranks "
                      f"{recovery.get('preserved_ranks')}, resumed round {recovery.get('first_resumed_round')}", flush=True)
            save()
        if all(s["status"] == "passed" for s in observations.values()):
            try:
                report["pairs"].append({"pair": pair, "predictions_match": True,
                                        "metrics": compare_pair(observations["full"], observations["selective"])})
            except (ValueError, AssertionError, OSError) as exc:
                report["failed_observations"].append({"pair": pair, "error": f"Comparison validation: {exc}"})
            save()
    report["summary"] = []
    for metric in ("training_s", "prediction_s", "pipeline_s"):
        values = [r for pair in report["pairs"] for r in pair["metrics"] if r["metric"] == metric]
        if values:
            changes = [v["selective_vs_full_pct"] for v in values]
            row = {"metric": metric, "pairs": len(values),
                   "full_s_mean": statistics.mean(v["full_s"] for v in values),
                   "selective_s_mean": statistics.mean(v["selective_s"] for v in values),
                   "selective_vs_full_pct_mean": statistics.mean(changes),
                   "selective_vs_full_pct_stdev": statistics.stdev(changes) if len(changes) > 1 else None}
            report["summary"].append(row)
            print(f"{metric}: full {row['full_s_mean']:.3f}s, selective {row['selective_s_mean']:.3f}s, "
                  f"change {row['selective_vs_full_pct_mean']:+.2f}%")
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
