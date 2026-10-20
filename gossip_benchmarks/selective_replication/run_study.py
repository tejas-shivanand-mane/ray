"""Profile real trials, enumerate policies, and replay process-loss scenarios."""

import argparse
import copy
import importlib.metadata
import json
import random
import statistics
import time
from pathlib import Path

from planner import plan_key, search, topological_order
from replay_runtime import replay_once
from workload import Workers, prepare_data, validate_state, write_json


def read(path):
    return json.loads(Path(path).read_text())


def profile(config, folder, workers_factory=Workers):
    if config["profile_repeats"] < 1:
        raise ValueError("Need at least one profiling repeat")
    folder = Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=False)
    write_json(folder / "config.json", config)
    data_path, digest = prepare_data(folder)
    tasks = copy.deepcopy(config["tasks"])
    for task in tasks.values():
        task["dataset_sha256"] = digest
    workers = workers_factory(config["machines"], data_path)
    samples = {t: {m: [] for m in config["machines"]} for t in tasks}
    references = {}
    try:
        for name in topological_order(tasks):
            task = tasks[name]
            for machine in config["machines"]:
                for repeat in range(config["profile_repeats"]):
                    attempt = folder / "profile" / f"{name}-{machine}-{repeat}"
                    begin = time.monotonic()
                    workers.submit(
                        machine,
                        name,
                        task,
                        None,
                        attempt,
                        [references[parent] for parent in task.get("depends_on", [])],
                    )
                    while True:
                        connection = workers.connections[machine]
                        if not connection.poll(config.get("profile_timeout_s", 120)):
                            raise TimeoutError("Profiling trial timed out")
                        event = connection.recv()
                        if event["kind"] == "error":
                            raise RuntimeError(event["traceback"])
                        if event["kind"] != "result":
                            continue
                        observed = time.monotonic() - begin
                        reference = references.setdefault(name, event["path"])
                        error = validate_state(event["path"], reference)
                        samples[name][machine].append(
                            {**event, "observed_s": observed, "max_abs_error": error}
                        )
                        print(
                            f"profile {name}/{machine}/{repeat}: {observed:.3f}s accuracy={event['accuracy']:.3f}",
                            flush=True,
                        )
                        # A remote worker can deliver its result before its
                        # training thread has acknowledged becoming idle.
                        while True:
                            if not connection.poll(30):
                                raise TimeoutError(
                                    "Worker did not settle after profiling"
                                )
                            if connection.recv()["kind"] == "idle":
                                break
                        break
    finally:
        workers.close()
    estimates = {name: {} for name in tasks}
    for name, per_machine in samples.items():
        for machine, rows in per_machine.items():
            estimates[name][machine] = {
                "epoch_s": statistics.median(
                    statistics.mean(r["epoch_times"]) for r in rows
                ),
                "save_s": statistics.median(
                    statistics.mean(r["save_times"]) for r in rows
                ),
                "init_s": statistics.median(
                    max(
                        r["init_s"],
                        r["observed_s"] - sum(r["epoch_times"]) - sum(r["save_times"]),
                    )
                    for r in rows
                ),
            }
    result = dict(
        schema_version=2,
        config=config,
        tasks=tasks,
        machines=config["machines"],
        data_path=data_path,
        references=references,
        estimates=estimates,
        samples=samples,
        workers=workers.ready,
        versions={
            m: importlib.metadata.version(m) for m in ("torch", "numpy", "scikit-learn")
        },
    )
    write_json(folder / "profile.json", result)
    return result


def enumerate_study(folder):
    folder = Path(folder)
    if (folder / "search.json").exists():
        raise FileExistsError("Search exists; use a new study folder")
    measured = read(folder / "profile.json")
    if measured.get("schema_version") != 2:
        raise ValueError(
            "Use a new profile folder for the revised benchmark; old results are preserved"
        )
    config, tasks, estimates = (
        measured["config"],
        measured["tasks"],
        measured["estimates"],
    )
    # A common deadline scale, independent of which policy is being evaluated.
    earliest = {}
    for name in topological_order(tasks):
        task = tasks[name]
        duration = min(
            p["init_s"]
            + task["epochs"] * p["epoch_s"]
            + (
                (task["epochs"] + task["checkpoint_every"] - 1)
                // task["checkpoint_every"]
            )
            * p["save_s"]
            for p in estimates[name].values()
        )
        earliest[name] = duration + max(
            (earliest[parent] for parent in task.get("depends_on", [])), default=0
        )
    parallel_lower_bound = max(earliest.values())
    cases = []
    for discount in config["spot_cost_multipliers"]:
        machines = copy.deepcopy(config["machines"])
        for m in config["failure_machines"]:
            machines[m]["cost_per_hour"] *= discount
        for probability in config["failure_probabilities"]:
            fractions = config.get(
                "fault_time_fractions", [config.get("fault_time_fraction", 0.45)]
            )
            if not fractions or any(f < 0 for f in fractions):
                raise ValueError("Need nonnegative failure-time fractions")
            a, b = config["failure_machines"]
            scenarios = [dict(name="healthy", probability=1 - probability, failures={})]
            for fraction in fractions:
                fault_at = parallel_lower_bound * fraction
                for name, weight, failed in [
                    (f"loss-{a}", 0.4, [a]),
                    (f"loss-{b}", 0.4, [b]),
                    ("correlated-loss", 0.2, [a, b]),
                ]:
                    scenarios.append(
                        dict(
                            name=f"{name}-t{fraction}",
                            probability=probability * weight / len(fractions),
                            failures=dict.fromkeys(failed, fault_at),
                        )
                    )
            for multiplier in config["deadline_multipliers"]:
                case = search(
                    tasks,
                    machines,
                    estimates,
                    scenarios,
                    parallel_lower_bound * multiplier,
                    config["success_target"],
                    config.get("accounting"),
                )
                case[
                    "name"
                ] = f"price-{discount}-risk-{probability}-deadline-{multiplier}"
                cases.append(case)
                print(
                    case["name"],
                    {
                        name: None if row is None else round(row["expected_cost"], 7)
                        for name, row in case["policies"].items()
                    },
                    flush=True,
                )
    result = dict(
        schema_version=2,
        kind="modeled_finite_scenario_results",
        parallel_lower_bound_s=parallel_lower_bound,
        cases=cases,
    )
    write_json(folder / "search.json", result)
    return result


def replay(folder, case_indices=None, repeats=2, holdout_fraction=0.8):
    if repeats < 1:
        raise ValueError("Need at least one replay repeat")
    folder = Path(folder).resolve()
    measured, searched = read(folder / "profile.json"), read(folder / "search.json")
    if measured.get("schema_version") != 2:
        raise ValueError("Use a new profile folder for the revised benchmark")
    if holdout_fraction < 0 or holdout_fraction in measured["config"].get(
        "fault_time_fractions", [measured["config"].get("fault_time_fraction", 0.45)]
    ):
        raise ValueError(
            "Use a nonnegative holdout time different from the planning time"
        )
    replay_dir = folder / "replays"
    replay_dir.mkdir(exist_ok=False)
    cases = searched["cases"]
    if case_indices is None:
        # Pick the largest modeled advantage, explicitly as a selected case.
        # Always replay a second case if possible; do not call this representative.
        def gap(i):
            policies = cases[i]["policies"]
            joint = policies["joint_enumeration"]
            other = [
                r["expected_cost"]
                for n, r in policies.items()
                if n != "joint_enumeration" and r is not None
            ]
            return (
                -1
                if joint is None
                else (min(other) / joint["expected_cost"] - 1 if other else 1)
            )

        best = max(range(len(cases)), key=gap)
        feasible = [
            i
            for i, c in enumerate(cases)
            if c["policies"]["joint_enumeration"] is not None
        ]
        contrast = next((i for i in reversed(feasible) if i != best), best)
        case_indices = sorted({best, contrast}) if feasible else []
    jobs = []
    for index in case_indices:
        case = cases[index]
        unique = {}
        # Replay modeled-infeasible baselines too; otherwise difficult cases
        # silently exclude the conventional scheduler from measured comparisons.
        for name, feasible_row in case["policies"].items():
            row = feasible_row or case.get("unconstrained_policies", {}).get(name)
            if row is not None:
                entry = unique.setdefault(
                    plan_key(row["plan"]),
                    {"plan": row["plan"], "policies": [], "modeled_feasible": {}},
                )
                entry["policies"].append(name)
                entry["modeled_feasible"][name] = feasible_row is not None
        for number, entry in enumerate(unique.values()):
            # Replay all planning scenarios and held-out single/correlated loss.
            # Holdouts do not participate in policy selection.
            fault_time = searched["parallel_lower_bound_s"] * holdout_fraction
            scenarios = [
                *case["scenarios"],
                dict(
                    name="holdout-correlated-loss",
                    probability=None,
                    failures=dict.fromkeys(
                        measured["config"]["failure_machines"], fault_time
                    ),
                ),
                dict(
                    name="holdout-single-loss",
                    probability=None,
                    failures={measured["config"]["failure_machines"][0]: fault_time},
                ),
            ]
            for scenario in scenarios:
                for repeat in range(repeats):
                    jobs.append((index, number, entry, scenario, repeat))
    random.Random(735).shuffle(jobs)
    print(
        f"Replaying {len(jobs)} runs; startup and validation add time beyond the profiled training duration.",
        flush=True,
    )
    rows = []
    for index, number, entry, scenario, repeat in jobs:
        result = replay_once(
            measured,
            cases[index],
            entry["plan"],
            scenario,
            replay_dir / f"case-{index}-plan-{number}-{scenario['name']}-{repeat}",
        )
        rows.append(
            dict(
                case_index=index,
                policies=entry["policies"],
                modeled_feasible=entry["modeled_feasible"],
                repeat=repeat,
                **result,
            )
        )
        write_json(
            folder / "replay.json",
            dict(
                kind="measured_local_process_replay",
                selected_case_indices=case_indices,
                rows=rows,
            ),
        )
        print(
            f"replay {index}/{entry['policies']}/{scenario['name']}/{repeat}: correct={result['correct']} deadline={result['met_deadline']} {result['elapsed_s']:.3f}s",
            flush=True,
        )
    if not rows:
        write_json(
            folder / "replay.json",
            dict(
                kind="measured_local_process_replay",
                selected_case_indices=case_indices,
                rows=[],
            ),
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=["profile", "search", "replay"])
    parser.add_argument("--folder", required=True)
    parser.add_argument("--config")
    parser.add_argument("--cases", nargs="*", type=int)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--holdout-fraction", type=float, default=0.8)
    args = parser.parse_args()
    if args.stage == "profile":
        profile(read(args.config), args.folder)
    elif args.stage == "search":
        enumerate_study(args.folder)
    else:
        replay(args.folder, args.cases, args.repeats, args.holdout_fraction)


if __name__ == "__main__":
    main()
