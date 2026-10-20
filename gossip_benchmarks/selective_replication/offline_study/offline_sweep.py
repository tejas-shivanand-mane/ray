"""Bounded discovery study. Model outputs are not measured training results.

The upstream planner is unchanged. Cache identical simulations across probability
and deadline settings; all baseline policy families still use planner.search.
"""

import argparse
import copy
import hashlib
import itertools
import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from reference import planner

SHAPES = {
    "balanced_merge": (600, 600, 300),
    "long_branch_merge": (300, 1200, 300),
    "long_final_merge": (300, 300, 1200),
    "independent_trials": (600, 600, 600),
}
SPEEDS = ("measured", "safe_2x_slower", "safe_branch_b_3x_slower")
SAVES = ("measured", 0.5, 2.0)
RISKS = (0.1, 0.5)
DEADLINES = (1.15, 1.5, 2.0)
FRACTIONS = (0.25, 0.65)
BASELINES = (
    "checkpoint_retry",
    "fixed_placement_replication",
    "placement_first",
    "uniform_replication",
    "greedy_replication",
    "round_robin_retry",
)


def dump(path, data):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def make_problem(measured, shape, speed, save):
    tasks = copy.deepcopy(measured["tasks"])
    estimates = copy.deepcopy(measured["estimates"])
    for name, epochs in zip(tasks, SHAPES[shape]):
        tasks[name]["epochs"] = epochs
        tasks[name]["checkpoint_every"] = 100
        if shape == "independent_trials":
            tasks[name].pop("depends_on", None)
        for machine, p in estimates[name].items():
            if speed == "safe_2x_slower" and machine == "safe":
                p["epoch_s"] *= 2
            if (
                speed == "safe_branch_b_3x_slower"
                and machine == "safe"
                and name == "shard-b"
            ):
                p["epoch_s"] *= 3
            if save != "measured":
                p["save_s"] = save
    machines = {
        m: {"cost_per_hour": 1.0 if m == "safe" else 0.25} for m in measured["machines"]
    }
    checkpoints = {t: sorted({100, v["epochs"]}) for t, v in tasks.items()}
    # Optimistic critical path with final-only saving, independent of policy.
    earliest = {}
    for t in planner.topological_order(tasks):
        duration = min(
            p["init_s"] + tasks[t]["epochs"] * p["epoch_s"] + p["save_s"]
            for p in estimates[t].values()
        )
        earliest[t] = duration + max(
            (earliest[d] for d in tasks[t].get("depends_on", [])), default=0
        )
    return tasks, machines, estimates, checkpoints, max(earliest.values())


def scenarios_for(lower_bound, risk, fractions=FRACTIONS):
    rows = [dict(name="healthy", probability=1 - risk, failures={})]
    for fraction in fractions:
        for label, weight, failed in [
            ("single_a", 0.4, ["spot_a"]),
            ("single_b", 0.4, ["spot_b"]),
            ("correlated", 0.2, ["spot_a", "spot_b"]),
        ]:
            rows.append(
                dict(
                    name=f"{label}-t{fraction}",
                    probability=risk * weight / len(fractions),
                    failures=dict.fromkeys(failed, lower_bound * fraction),
                )
            )
    return rows


def classify(case):
    joint = case["policies"]["joint_enumeration"]
    if joint is None:
        return dict(outcome="joint_infeasible")
    feasible = {
        n: case["policies"][n] for n in BASELINES if case["policies"][n] is not None
    }
    if not feasible:
        return dict(outcome="joint_only_feasible", feasible_baselines=[])
    best_name = min(feasible, key=lambda n: feasible[n]["expected_worker_busy_s"])
    best = feasible[best_name]["expected_worker_busy_s"]
    improvement = 100 * (best - joint["expected_worker_busy_s"]) / best
    return dict(
        outcome="strict_gain" if improvement > 1e-7 else "tie",
        best_baseline=best_name,
        improvement_percent=improvement,
        feasible_baselines=list(feasible),
        infeasible_baselines=[n for n in BASELINES if n not in feasible],
    )


def run_model(args):
    profile_path, shape, speed, save, output = args
    started = time.monotonic()
    measured = json.loads(Path(profile_path).read_text())
    tasks, machines, estimates, choices, lower = make_problem(
        measured, shape, speed, save
    )
    cached = {}
    original_evaluate = planner.evaluate

    def cached_evaluate(plan, tasks, machines, profile, scenarios, deadline, horizon):
        key = planner.plan_key(plan)
        if key not in cached:
            cached[key] = [
                planner.simulate(plan, tasks, machines, profile, s["failures"], horizon)
                for s in scenarios
            ]
        rows = cached[key]
        # Only reuse complete traces that finish within this requested horizon.
        if not all(r["complete"] and r["elapsed_s"] <= horizon for r in rows):
            return original_evaluate(
                plan, tasks, machines, profile, scenarios, deadline, horizon
            )
        return dict(
            plan=plan,
            scenarios=rows,
            success_probability=sum(
                s["probability"]
                for s, r in zip(scenarios, rows)
                if r["complete"] and r["elapsed_s"] <= deadline
            ),
            **{
                "expected_"
                + out: sum(s["probability"] * r[key] for s, r in zip(scenarios, rows))
                for out, key in [
                    ("cost", "cost"),
                    ("elapsed_s", "elapsed_s"),
                    ("worker_busy_s", "worker_busy_s"),
                ]
            },
        )

    planner.evaluate = cached_evaluate
    cases = []
    try:
        for risk, multiplier in itertools.product(RISKS, DEADLINES):
            case = planner.search(
                tasks,
                machines,
                estimates,
                scenarios_for(lower, risk),
                lower * multiplier,
                target=0.95,
                accounting=dict(bill_warm_pool=True, shared_storage_per_hour=2.0),
                checkpoint_choices=choices,
                objective="expected_worker_busy_s",
            )
            case.update(risk=risk, deadline_multiplier=multiplier)
            case["comparison"] = classify(case)
            cases.append(case)
    finally:
        planner.evaluate = original_evaluate
    model_id = f"{shape}__{speed}__save-{save}"
    result = dict(
        model_id=model_id,
        shape=shape,
        speed=speed,
        save=save,
        tasks=tasks,
        estimates=estimates,
        machines=machines,
        lower_bound_s=lower,
        cases=cases,
        wall_s=time.monotonic() - started,
    )
    dump(Path(output) / (model_id + ".json"), result)
    return {k: result[k] for k in ["model_id", "shape", "speed", "save", "wall_s"]} | {
        "cases": [
            {
                "risk": c["risk"],
                "deadline_multiplier": c["deadline_multiplier"],
                **c["comparison"],
            }
            for c in cases
        ]
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output", default="results")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    specs = list(itertools.product(SHAPES, SPEEDS, SAVES))
    dump(
        out / "design.json",
        dict(
            kind="synthetic_profile_discovery_not_measured_training",
            upstream_commit="226d9be",
            planner_sha256=hashlib.sha256(
                Path(planner.__file__).read_bytes()
            ).hexdigest(),
            profile_sha256=hashlib.sha256(Path(args.profile).read_bytes()).hexdigest(),
            shapes=SHAPES,
            speeds=SPEEDS,
            saves=SAVES,
            risks=RISKS,
            deadlines=DEADLINES,
            fractions=FRACTIONS,
            target=0.95,
            model_count=len(specs),
            case_count=len(specs) * len(RISKS) * len(DEADLINES),
            selection="Report entire grid. Strict gain means lower expected busy time than every feasible baseline; infeasible baselines are reported separately.",
            limitations=[
                "Synthetic timing transformations, not measured models.",
                "Safe worker and shared checkpoint storage never fail.",
                "Instant failure detection in the unchanged upstream simulator.",
                "Chosen after seeing the pilot: exploratory discovery, not confirmatory evidence.",
            ],
        ),
    )
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_model, (args.profile, *s, str(out))) for s in specs]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                len(results),
                result["model_id"],
                [
                    (
                        c["risk"],
                        c["deadline_multiplier"],
                        c["outcome"],
                        round(c.get("improvement_percent", 0), 2),
                    )
                    for c in result["cases"]
                ],
                flush=True,
            )
            dump(out / "progress.json", results)
    counts = {}
    for r in results:
        for c in r["cases"]:
            counts[c["outcome"]] = counts.get(c["outcome"], 0) + 1
    dump(
        out / "summary.json",
        dict(
            wall_s=time.monotonic() - started,
            counts=counts,
            models=sorted(results, key=lambda r: r["model_id"]),
        ),
    )
    print("DONE", counts, "seconds", time.monotonic() - started, flush=True)


if __name__ == "__main__":
    main()
