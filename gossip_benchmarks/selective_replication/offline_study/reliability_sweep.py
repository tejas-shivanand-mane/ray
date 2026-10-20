"""Explicit follow-up hypothesis: the on-demand worker is not infallible.

Use the same seeded profiles as heterogeneous_sweep. Scenario mass: healthy .5,
safe loss .02, each spot loss .2, correlated spot loss .08. These are illustrative
per-workflow probabilities, not measured infrastructure reliability.
"""
import argparse
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import sensitivity
from heterogeneous_sweep import problem_specs
from offline_sweep import classify, dump
from reference import planner


def run(spec):
    original = planner.simulate
    cases = []
    for delay in [0, 8]:

        def sim(*args):
            return sensitivity.simulate(
                *args, detection_s=delay, epoch_scale=1.25, dispatch_s=1
            )

        planner.simulate = sim
        lower = max(
            min(2 + 100 * v["epoch_s"] * 1.25 + v["save_s"] for v in p.values())
            for p in spec["profile"].values()
        )
        scenarios = [dict(name="healthy", probability=0.5, failures={})]
        for f in [0.35, 0.75]:
            for label, w, failed in [
                ("safe", 0.02, ["safe"]),
                ("single_a", 0.2, ["spot_a"]),
                ("single_b", 0.2, ["spot_b"]),
                ("correlated", 0.08, ["spot_a", "spot_b"]),
            ]:
                scenarios.append(
                    dict(
                        name=f"{label}-t{f}",
                        probability=w / 2,
                        failures={m: lower * f for m in failed},
                    )
                )
        for target in [0.95, 0.99]:
            for multiplier in [1.1, 1.3, 1.6, 2.0]:
                case = planner.search(
                    spec["tasks"],
                    spec["machines"],
                    spec["profile"],
                    scenarios,
                    lower * multiplier,
                    target=target,
                    checkpoint_choices={t: [25, 100] for t in spec["tasks"]},
                    objective="expected_worker_busy_s",
                )
                case.update(
                    detection_s=delay,
                    epoch_scale=1.25,
                    dispatch_s=1,
                    deadline_multiplier=multiplier,
                    lower_bound_s=lower,
                )
                case["comparison"] = classify(case)
                cases.append(case)
    planner.simulate = original
    return {**spec, "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="reliability_results")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    specs = list(problem_specs())
    dump(
        out / "design.json",
        dict(
            kind="exploratory_synthetic_profiles_nonzero_safe_failure",
            seed=412,
            targets=[0.95, 0.99],
            safe_failure_probability=0.02,
            each_spot_failure_probability=0.2,
            correlated_probability=0.08,
            failure_fractions=[0.35, 0.75],
            detection_s=[0, 8],
            epoch_scale=1.25,
            dispatch_s=1,
            deadline_multipliers=[1.1, 1.3, 1.6, 2.0],
            profiles=specs,
            limitations=[
                "Assumed probabilities and synthetic timings, not measured reliability or runtime.",
                "Safe worker may fail; shared storage and controller still survive.",
                "This changes the previous GCP experiment failure model explicitly.",
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
                    target=c["target"],
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
    for r in results:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
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
                    r["target"],
                    r["deadline_multiplier"],
                ),
            ),
        ),
    )
    print("DONE", counts, time.monotonic() - started, flush=True)


if __name__ == "__main__":
    main()
