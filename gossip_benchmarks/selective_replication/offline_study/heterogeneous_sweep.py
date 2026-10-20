"""Follow-up discovery: two independent trials on heterogeneous workers.

Finite seeded synthetic profiles; reports every case, not just selected wins.
The additional delay model is applied equally to every policy family.
"""
import argparse
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import sensitivity
from offline_sweep import classify, dump
from reference import planner


def problem_specs():
    rng = random.Random(412)
    machines = {
        m: dict(cost_per_hour=1 if m == "safe" else 0.25)
        for m in ["safe", "spot_a", "spot_b"]
    }
    tasks = {t: dict(epochs=100, checkpoint_every=25) for t in ["trial-a", "trial-b"]}
    for i in range(60):
        profile = {
            t: {
                m: dict(
                    epoch_s=rng.choice([20, 40, 80, 120]) / 100,
                    save_s=rng.choice([0.1, 1, 5]),
                    init_s=1,
                )
                for m in machines
            }
            for t in tasks
        }
        yield dict(index=i, tasks=tasks, machines=machines, profile=profile)


def run(spec):
    rows = []
    original_simulate = planner.simulate
    for delay in [0, 8]:
        # This second phase explicitly models startup and prediction uncertainty.
        def sim(*args):
            return sensitivity.simulate(
                *args, detection_s=delay, epoch_scale=1.25, dispatch_s=1
            )

        planner.simulate = sim
        lower = max(
            min(2 + 100 * v["epoch_s"] * 1.25 + v["save_s"] for v in p.values())
            for p in spec["profile"].values()
        )
        for risk in [0.1, 0.5]:
            scenarios = [dict(name="healthy", probability=1 - risk, failures={})]
            for f in [0.35, 0.75]:
                for label, w, failed in [
                    ("single_a", 0.4, ["spot_a"]),
                    ("single_b", 0.4, ["spot_b"]),
                    ("correlated", 0.2, ["spot_a", "spot_b"]),
                ]:
                    scenarios.append(
                        dict(
                            name=f"{label}-t{f}",
                            probability=risk * w / 2,
                            failures={m: lower * f for m in failed},
                        )
                    )
            for multiplier in [1.1, 1.3, 1.6]:
                case = planner.search(
                    spec["tasks"],
                    spec["machines"],
                    spec["profile"],
                    scenarios,
                    lower * multiplier,
                    checkpoint_choices={t: [25, 100] for t in spec["tasks"]},
                    objective="expected_worker_busy_s",
                )
                case.update(
                    detection_s=delay,
                    epoch_scale=1.25,
                    dispatch_s=1,
                    risk=risk,
                    deadline_multiplier=multiplier,
                    lower_bound_s=lower,
                )
                case["comparison"] = classify(case)
                rows.append(case)
    planner.simulate = original_simulate
    return {**spec, "cases": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="heterogeneous_results")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    specs = list(problem_specs())
    dump(
        out / "design.json",
        dict(
            kind="exploratory_synthetic_profiles",
            seed=412,
            profiles=specs,
            detection_s=[0, 8],
            epoch_scale=1.25,
            dispatch_s=1,
            risks=[0.1, 0.5],
            deadline_multipliers=[1.1, 1.3, 1.6],
            target=0.95,
            failure_fractions=[0.35, 0.75],
            conditional_failure_weights=[0.4, 0.4, 0.2],
            limitations=[
                "Per-trial/per-machine speed and checkpoint cost assumptions are unmeasured.",
                "Safe worker and shared checkpoint store never fail.",
                "Two independent training trials; no distributed gradient communication.",
                "Same unchanged planner policy families; delay model shared by all.",
            ],
        ),
    )
    started = time.monotonic()
    results = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        for future in as_completed([pool.submit(run, spec) for spec in specs]):
            r = future.result()
            dump(out / f"profile-{r['index']:03d}.json", r)
            rows = [
                dict(
                    index=r["index"],
                    detection_s=c["detection_s"],
                    risk=c["risk"],
                    deadline_multiplier=c["deadline_multiplier"],
                    **c["comparison"],
                )
                for c in r["cases"]
            ]
            results.extend(rows)
            gains = [
                c
                for c in rows
                if c.get("improvement_percent", 0) > 5
                or c["outcome"] == "joint_only_feasible"
            ]
            if gains:
                print("GAIN", gains, flush=True)
    counts = {}
    for row in results:
        counts[row["outcome"]] = counts.get(row["outcome"], 0) + 1
    dump(
        out / "summary.json",
        dict(
            wall_s=time.monotonic() - started,
            counts=counts,
            cases=sorted(
                results,
                key=lambda r: (
                    r["index"],
                    r["detection_s"],
                    r["risk"],
                    r["deadline_multiplier"],
                ),
            ),
        ),
    )
    print("DONE", counts, time.monotonic() - started, flush=True)


if __name__ == "__main__":
    main()
