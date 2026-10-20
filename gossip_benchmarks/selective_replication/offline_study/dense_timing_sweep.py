"""Final discovery check: dense failure times, uniform checkpoint costs.

Same profile-22 compute speeds, five-second checkpoints on all workers/tasks.
Probability and target grid is written before evaluation. All candidate policies
see the same dense failure distribution and checkpoint menu.
"""
import argparse
import itertools
import json
import time
from pathlib import Path

import sensitivity
from offline_sweep import classify, dump
from reference import planner


def scenarios(safe_p, corr_p, fractions):
    result = [dict(name="healthy", probability=0.5, failures={})]
    single_p = (0.5 - safe_p - corr_p) / 2
    for f in fractions:
        for label, p, failed in [
            ("safe", safe_p, ["safe"]),
            ("single_a", single_p, ["spot_a"]),
            ("single_b", single_p, ["spot_b"]),
            ("correlated", corr_p, ["spot_a", "spot_b"]),
        ]:
            result.append(
                dict(
                    name=f"{label}-t{f:.4f}",
                    probability=p / len(fractions),
                    failures={m: 157 * f for m in failed},
                )
            )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spec = json.loads(Path(args.input).read_text())
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    for p in spec["profile"].values():
        for v in p.values():
            v["save_s"] = 5
    fractions = [(i + 0.5) / 20 for i in range(20)]
    choices = [1, 2, 5, 10, 20, 25, 33, 50, 75, 100]
    grid = list(
        itertools.product(
            [0, 0.005, 0.02], [0.005, 0.02, 0.08], [0.95, 0.99], [165, 175, 185, 195]
        )
    )
    dump(
        out / "design.json",
        dict(
            spec={k: spec[k] for k in ["tasks", "machines", "profile"]},
            fractions=fractions,
            failure_window_s=157,
            checkpoint_choices=choices,
            grid=[
                dict(
                    safe_probability=a, correlated_probability=b, target=c, deadline_s=d
                )
                for a, b, c, d in grid
            ],
            epoch_scale=1.25,
            dispatch_s=1,
            detection_s=8,
            limitation="Synthetic discovery; probabilities are assumptions, all cloud roles and VM failures untested.",
        ),
    )
    original_simulate = planner.simulate
    original_evaluate = planner.evaluate
    planner.simulate = lambda *args: sensitivity.simulate(
        *args, detection_s=8, epoch_scale=1.25, dispatch_s=1
    )
    cached = {}

    def evaluate(plan, tasks, machines, profile, scenario_list, deadline, horizon):
        key = planner.plan_key(plan)
        if key not in cached:
            cached[key] = [
                planner.simulate(plan, tasks, machines, profile, s["failures"], horizon)
                for s in scenario_list
            ]
        rows = cached[key]
        assert all(r["complete"] and r["elapsed_s"] <= horizon for r in rows)
        return dict(
            plan=plan,
            scenarios=rows,
            success_probability=sum(
                s["probability"]
                for s, r in zip(scenario_list, rows)
                if r["elapsed_s"] <= deadline
            ),
            expected_worker_busy_s=sum(
                s["probability"] * r["worker_busy_s"]
                for s, r in zip(scenario_list, rows)
            ),
            expected_elapsed_s=sum(
                s["probability"] * r["elapsed_s"] for s, r in zip(scenario_list, rows)
            ),
            expected_cost=sum(
                s["probability"] * r["cost"] for s, r in zip(scenario_list, rows)
            ),
        )

    planner.evaluate = evaluate
    started = time.monotonic()
    rows = []
    try:
        for i, (safe_p, corr_p, target, deadline) in enumerate(grid):
            case = planner.search(
                spec["tasks"],
                spec["machines"],
                spec["profile"],
                scenarios(safe_p, corr_p, fractions),
                deadline,
                target=target,
                checkpoint_choices={t: choices for t in spec["tasks"]},
                objective="expected_worker_busy_s",
            )
            comparison = classify(case)
            row = dict(
                index=i,
                safe_probability=safe_p,
                correlated_probability=corr_p,
                target=target,
                deadline_s=deadline,
                **comparison,
            )
            rows.append(row)
            dump(out / f"case-{i:03d}.json", case)
            if (
                comparison.get("improvement_percent", 0) > 5
                or comparison["outcome"] == "joint_only_feasible"
            ):
                print("GAIN", row, flush=True)
    finally:
        planner.simulate = original_simulate
        planner.evaluate = original_evaluate
    counts = {}
    for r in rows:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    dump(
        out / "summary.json",
        dict(wall_s=time.monotonic() - started, counts=counts, cases=rows),
    )
    print("DONE", counts, time.monotonic() - started, flush=True)


if __name__ == "__main__":
    main()
