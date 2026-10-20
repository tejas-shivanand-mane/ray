"""Re-optimize all baseline families around discovered synthetic profile 22.

Wider checkpoint menu and nearby assumptions guard against a single grid point.
Also evaluate frozen nominal plans on new failure times; no retuning there.
"""
import argparse
import copy
import json
from pathlib import Path

import sensitivity
from offline_sweep import classify, dump
from reference import planner


def solve(spec, scenarios, deadline, delay, scale, choices):
    old = planner.simulate
    planner.simulate = lambda *args: sensitivity.simulate(
        *args, detection_s=delay, epoch_scale=scale, dispatch_s=1
    )
    try:
        return planner.search(
            spec["tasks"],
            spec["machines"],
            spec["profile"],
            scenarios,
            deadline,
            target=0.95,
            checkpoint_choices={t: choices for t in spec["tasks"]},
            objective="expected_worker_busy_s",
        )
    finally:
        planner.simulate = old


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spec = json.loads(Path(args.input).read_text())
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    base = next(
        c
        for c in spec["cases"]
        if c["detection_s"] == 8
        and c["target"] == 0.95
        and c["deadline_multiplier"] == 1.6
    )
    choices = [10, 20, 25, 50, 100]
    nominal = solve(spec, base["scenarios"], base["deadline_s"], 8, 1.25, choices)
    dump(
        out / "nominal.json",
        dict(
            spec={k: spec[k] for k in ["tasks", "machines", "profile"]},
            case=nominal,
            comparison=classify(nominal),
        ),
    )
    variants = []
    # One-at-a-time sensitivities preserve a clear interpretation.
    settings = [dict(name="nominal", deadline=base["deadline_s"], delay=8, scale=1.25)]
    settings += [
        dict(name=f"deadline-{d}", deadline=d, delay=8, scale=1.25)
        for d in [160, 165, 175, 180, 190]
    ]
    settings += [
        dict(name=f"detection-{d}", deadline=base["deadline_s"], delay=d, scale=1.25)
        for d in [0, 4, 12]
    ]
    settings += [
        dict(name=f"epoch-scale-{s}", deadline=base["deadline_s"], delay=8, scale=s)
        for s in [1.1, 1.35, 1.4]
    ]
    settings += [
        dict(
            name=f"uniform-save-{s}",
            deadline=base["deadline_s"],
            delay=8,
            scale=1.25,
            uniform_save=s,
        )
        for s in [0.1, 1, 5]
    ]
    settings += [
        dict(
            name="equal-worker-speeds",
            deadline=base["deadline_s"],
            delay=8,
            scale=1.25,
            equal_speeds=True,
        )
    ]
    settings += [
        dict(
            name="safe-never-fails",
            deadline=base["deadline_s"],
            delay=8,
            scale=1.25,
            safe_never_fails=True,
        )
    ]
    for setting in settings:
        problem = copy.deepcopy(spec)
        scenarios = copy.deepcopy(base["scenarios"])
        if "uniform_save" in setting:
            for p in problem["profile"].values():
                for v in p.values():
                    v["save_s"] = setting["uniform_save"]
        if setting.get("equal_speeds"):
            for p in problem["profile"].values():
                fastest = min(v["epoch_s"] for v in p.values())
                for v in p.values():
                    v["epoch_s"] = fastest
        if setting.get("safe_never_fails"):
            extra = sum(s["probability"] for s in scenarios if "safe" in s["failures"])
            scenarios = [s for s in scenarios if "safe" not in s["failures"]]
            scenarios[0]["probability"] += extra
        case = solve(
            problem,
            scenarios,
            setting["deadline"],
            setting["delay"],
            setting["scale"],
            choices,
        )
        comparison = classify(case)
        variants.append(dict(setting=setting, comparison=comparison, case=case))
        print(setting["name"], comparison, flush=True)
    dump(out / "reoptimized_variants.json", variants)
    frozen = []
    for fractions in [(0.2, 0.6), (0.3, 0.7), (0.4, 0.8), (0.1, 0.3, 0.5, 0.7, 0.9)]:
        scenarios = [dict(name="healthy", probability=0.5, failures={})]
        for f in fractions:
            for label, w, failed in [
                ("safe", 0.02, ["safe"]),
                ("single_a", 0.2, ["spot_a"]),
                ("single_b", 0.2, ["spot_b"]),
                ("correlated", 0.08, ["spot_a", "spot_b"]),
            ]:
                scenarios.append(
                    dict(
                        name=f"{label}-t{f}",
                        probability=w / len(fractions),
                        failures={m: 107 * f for m in failed},
                    )
                )
        policies = {}
        for name, row in nominal["policies"].items():
            if row is not None:
                policies[name] = sensitivity.evaluate(
                    row["plan"],
                    spec["tasks"],
                    spec["machines"],
                    spec["profile"],
                    scenarios,
                    base["deadline_s"],
                    nominal["horizon_s"],
                    detection_s=8,
                    epoch_scale=1.25,
                    dispatch_s=1,
                )
        frozen.append(dict(fractions=fractions, scenarios=scenarios, policies=policies))
    dump(out / "frozen_new_failure_times.json", frozen)
    print(
        "FROZEN",
        [
            (
                r["fractions"],
                {
                    k: round(v["success_probability"], 3)
                    for k, v in r["policies"].items()
                },
            )
            for r in frozen
        ],
        flush=True,
    )


if __name__ == "__main__":
    main()
