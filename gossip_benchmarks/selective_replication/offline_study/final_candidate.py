"""Reproduce the selected candidate's stronger baseline and frozen-plan checks."""
import argparse
import json
from pathlib import Path

import sensitivity
from dense_timing_sweep import scenarios
from offline_sweep import dump
from stronger_baseline import solve


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True)
    args = parser.parse_args()
    root = Path(args.results)
    spec = json.loads((root / "design.json").read_text())["spec"]
    case = json.loads((root / "case-030.json").read_text())
    strong = solve(spec, case, [1, 2, 5, 10, 20, 25, 33, 50, 75, 100])
    dump(root / "case-030-stronger-baseline.json", strong)
    selected = {n: v for n, v in case["policies"].items() if v is not None}
    selected["risk_aware_sequential"] = strong["result"]
    out = []
    for grid in [50, 100]:
        for delay, scale in [
            (0, 1.25),
            (8, 1.25),
            (12, 1.25),
            (8, 1.1),
            (8, 1.35),
            (8, 1.4),
        ]:
            scen = scenarios(0.005, 0.005, [(i + 0.5) / grid for i in range(grid)])
            rows = {
                n: sensitivity.evaluate(
                    v["plan"],
                    spec["tasks"],
                    case["machines"],
                    spec["profile"],
                    scen,
                    185,
                    case["horizon_s"],
                    detection_s=delay,
                    epoch_scale=scale,
                    dispatch_s=1,
                )
                for n, v in selected.items()
            }
            out.append(
                dict(
                    grid=grid,
                    delay=delay,
                    epoch_scale=scale,
                    scenarios=scen,
                    policies=rows,
                )
            )
    dump(root / "case-030-heldout.json", out)
    joint = case["policies"]["joint_enumeration"]
    assert (
        abs(
            joint["expected_worker_busy_s"] - strong["result"]["expected_worker_busy_s"]
        )
        < 1e-8
    )
    assert all(
        r["policies"]["joint_enumeration"]["success_probability"] >= 0.99 for r in out
    )
    print(
        "PASS: stronger sequential baseline ties joint; all 12 frozen-plan sensitivity checks meet 99%."
    )


if __name__ == "__main__":
    main()
