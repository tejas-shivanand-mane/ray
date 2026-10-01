"""Compare standard Train retries with opt-in selective CPU retries.

Use --workload python/ray/train/examples/pytorch/torch_regression_example.py
to run the existing PyTorch example, with a small local CSV and Fixed-R OFF
by default. This exercises real Ray Data shards without a custom train loop.
Other CPU TorchTrainer scripts can use --workload-arg=... for their arguments;
they must already save and restore synchronous checkpoints. The harness
checks retry mechanics, not arbitrary workload semantics. The regression
example additionally checks epoch progress and final prediction agreement.

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("on", "off"),
                        help="Fixed-R mode; default ON for XGBoost, OFF for existing scripts")
    parser.add_argument("--scenario", action="append", choices=("none", "worker", "worker-node"))
    parser.add_argument("--workload", type=Path,
                        help="Run an existing CPU TorchTrainer script through the external harness")
    parser.add_argument("--workload-arg", action="append", default=[],
                        help="Script argument; use --workload-arg=--flag for flags")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=120)
    args = parser.parse_args()
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
