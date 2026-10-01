"""Compare standard XGBoostTrainer retries with opt-in selective CPU retries."""

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
    for key in ("scenario", "mode", "native_settings", "final_tree_sha256"):
        if full[key] != selective[key]:
            raise ValueError(f"Cannot compare different {key}")
    for key in ("source_sha256", "native_extension_sha256", "python", "platform",
                "ray_version", "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version"):
        if full["provenance"][key] != selective["provenance"][key]:
            raise ValueError(f"Cannot compare different {key}")
    np.testing.assert_allclose(
        np.load(Path(full["directory"]) / "predictions.npy", allow_pickle=False),
        np.load(Path(selective["directory"]) / "predictions.npy", allow_pickle=False),
        rtol=1e-6, atol=1e-7)
    before, after = full["training_s"], selective["training_s"]
    if not all(math.isfinite(v) and v > 0 for v in (before, after)):
        raise ValueError("Invalid training timing")
    return {"full_s": before, "selective_s": after,
            "selective_vs_full_pct": 100 * (after / before - 1)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("on", "off"), default="on")
    parser.add_argument("--scenario", action="append", choices=("none", "worker-node"))
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout-s", type=float, default=120)
    args = parser.parse_args()
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

    def save():
        write_json(args.output, report)
        write_json(directory / "comparison.json", report)

    save()
    for scenario in dict.fromkeys(args.scenario or ["none", "worker-node"]):
        for pair in range(1, args.repeats + 1):
            samples = {}
            for policy in (("full", "selective") if pair % 2 else ("selective", "full")):
                options = {"training_strategy": "ray-train-selective", "scenario": scenario,
                           "mode": args.mode, "restart_scope": policy, "timeout_s": args.timeout_s}
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
                                            "predictions_match": True, **values})
                    print(f"  matched change: {values['selective_vs_full_pct']:+.2f}%", flush=True)
                except (ValueError, AssertionError, OSError) as exc:
                    report["failed_observations"].append({"scenario": scenario, "pair": pair,
                                                          "error": f"Comparison: {exc}"})
                save()
    report["summary"] = []
    for scenario in ("none", "worker-node"):
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
