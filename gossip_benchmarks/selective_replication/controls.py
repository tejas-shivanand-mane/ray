"""Constructed controls test the harness, not practical benefit or research novelty."""

import argparse
import copy
import json
import random
import statistics
from pathlib import Path

from replay_runtime import replay_once
from run_study import profile
from workload import write_json


def controls(config, folder, repeats=3):
    if repeats < 2:
        raise ValueError("Use at least two repetitions")
    folder = Path(folder)
    # A late failure loses all progress: checkpoint only at completion. This is
    # deliberate and ONLY a mechanism control; workload studies retain checkpoints.
    task = copy.deepcopy(config["task"])
    task["checkpoint_every"] = task["epochs"]
    measured = profile(
        dict(
            tasks={"train": task},
            machines=config["machines"],
            profile_repeats=config.get("profile_repeats", 2),
        ),
        folder,
    )
    durations = {
        m: statistics.median(r["observed_s"] for r in rows)
        for m, rows in measured["samples"]["train"].items()
    }
    primary, backup = config["primary"], config["backup"]
    failure_at = config.get("failure_fraction", 0.65) * durations[primary]
    deadline = config.get("deadline_multiplier", 1.25) * max(durations.values())
    machines = {
        m: dict(v, accounting=config.get("accounting", {}))
        for m, v in config["machines"].items()
    }
    case = dict(machines=machines, deadline_s=deadline, horizon_s=deadline * 5)
    plans = {
        "fixed_retry": dict(order=["train"], copies={"train": [primary]}),
        "fixed_replica": dict(order=["train"], copies={"train": [primary, backup]}),
    }
    scenarios = [
        dict(name="negative-healthy", failures={}, probability=None),
        dict(
            name="positive-late-loss", failures={primary: failure_at}, probability=None
        ),
    ]
    jobs = [
        (name, scenario, repeat)
        for name in plans
        for scenario in scenarios
        for repeat in range(repeats)
    ]
    random.Random(735).shuffle(jobs)
    rows = []
    for name, scenario, repeat in jobs:
        row = replay_once(
            measured,
            case,
            plans[name],
            scenario,
            folder / f"{name}-{scenario['name']}-{repeat}",
        )
        rows.append(dict(policy=name, repeat=repeat, **row))
        write_json(folder / "controls-partial.json", dict(rows=rows))
        print(
            f"control {name}/{scenario['name']}/{repeat}: correct={row['correct']} deadline={row['met_deadline']} {row['elapsed_s']:.3f}s",
            flush=True,
        )

    def subset(policy, scenario):
        return [
            r
            for r in rows
            if r["policy"] == policy and r["scenario"]["name"] == scenario
        ]

    positive_retry = subset("fixed_retry", "positive-late-loss")
    positive_replica = subset("fixed_replica", "positive-late-loss")
    healthy_retry = subset("fixed_retry", "negative-healthy")
    healthy_replica = subset("fixed_replica", "negative-healthy")

    def med(rows, key):
        return statistics.median(r[key] for r in rows)

    checks = {
        "all_states_correct": all(r["correct"] for r in rows),
        "positive_fault_hits_active_training": all(
            r["fault_evidence"].get(primary, {}).get("active_task") == "train"
            for r in positive_retry + positive_replica
        ),
        "positive_replica_faster": med(positive_replica, "elapsed_s")
        < 0.9 * med(positive_retry, "elapsed_s"),
        "positive_replica_meets_deadline": all(
            r["met_deadline"] for r in positive_replica
        ),
        "positive_retry_misses_deadline": all(
            not r["met_deadline"] for r in positive_retry
        ),
        "negative_both_meet_deadline": all(
            r["met_deadline"] for r in healthy_retry + healthy_replica
        ),
        "negative_replica_uses_more_worker_time": med(healthy_replica, "worker_busy_s")
        > 1.5 * med(healthy_retry, "worker_busy_s"),
    }
    result = dict(
        schema_version=2,
        kind="constructed_mechanism_controls",
        config=config,
        checks=checks,
        passed=all(checks.values()),
        rows=rows,
        durations=durations,
        deadline_s=deadline,
        failure_at_s=failure_at,
    )
    write_json(folder / "controls.json", result)
    print(
        json.dumps({"passed": result["passed"], "checks": checks}, indent=2), flush=True
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--folder", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    result = controls(
        json.loads(Path(args.config).read_text()), args.folder, args.repeats
    )
    if not result["passed"]:
        raise SystemExit(
            "Controls inconclusive/failed. Inspect controls.json before interpreting workload comparisons."
        )
