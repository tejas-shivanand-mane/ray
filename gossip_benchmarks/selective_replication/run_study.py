"""Profile real trials, enumerate policies, and replay process-loss scenarios."""

import argparse
import copy
import importlib.metadata
import json
import random
import statistics
import time
from pathlib import Path

from planner import Schedule, lease_cost, plan_key, search, simulate
from workload import Workers, prepare_data, validate_state, write_json


def read(path):
    return json.loads(Path(path).read_text())


def profile(config, folder):
    if config["profile_repeats"] < 1:
        raise ValueError("Need at least one profiling repeat")
    folder = Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=False)
    write_json(folder / "config.json", config)
    data_path, digest = prepare_data(folder)
    tasks = copy.deepcopy(config["tasks"])
    for task in tasks.values():
        task["dataset_sha256"] = digest
    workers = Workers(config["machines"], data_path)
    samples = {t: {m: [] for m in config["machines"]} for t in tasks}
    references = {}
    try:
        for name, task in tasks.items():
            for machine in config["machines"]:
                for repeat in range(config["profile_repeats"]):
                    attempt = folder / "profile" / f"{name}-{machine}-{repeat}"
                    begin = time.monotonic()
                    workers.submit(machine, name, task, None, attempt)
                    while True:
                        connection = workers.connections[machine]
                        if not connection.poll(120):
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
    config, tasks, estimates = (
        measured["config"],
        measured["tasks"],
        measured["estimates"],
    )
    # A common deadline scale, independent of which policy is being evaluated.
    parallel_lower_bound = max(
        min(
            p["init_s"]
            + task["epochs"] * p["epoch_s"]
            + (
                (task["epochs"] + task["checkpoint_every"] - 1)
                // task["checkpoint_every"]
            )
            * p["save_s"]
            for p in estimates[name].values()
        )
        for name, task in tasks.items()
    )
    cases = []
    for discount in config["spot_cost_multipliers"]:
        machines = copy.deepcopy(config["machines"])
        for m in config["failure_machines"]:
            machines[m]["cost_per_hour"] *= discount
        for probability in config["failure_probabilities"]:
            fault_at = parallel_lower_bound * config["fault_time_fraction"]
            a, b = config["failure_machines"]
            scenarios = [
                dict(name="healthy", probability=1 - probability, failures={}),
                dict(
                    name=f"loss-{a}",
                    probability=probability * 0.4,
                    failures={a: fault_at},
                ),
                dict(
                    name=f"loss-{b}",
                    probability=probability * 0.4,
                    failures={b: fault_at},
                ),
                dict(
                    name="correlated-loss",
                    probability=probability * 0.2,
                    failures={a: fault_at, b: fault_at},
                ),
            ]
            for multiplier in config["deadline_multipliers"]:
                case = search(
                    tasks,
                    machines,
                    estimates,
                    scenarios,
                    parallel_lower_bound * multiplier,
                    config["success_target"],
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
        kind="modeled_finite_scenario_results",
        parallel_lower_bound_s=parallel_lower_bound,
        cases=cases,
    )
    write_json(folder / "search.json", result)
    return result


def replay_once(measured, case, plan, scenario, output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    tasks, machines = measured["tasks"], case["machines"]
    # Warm the controller's tensor validator as well as the workers. Otherwise
    # the first result pays a one-off torch import and can delay fault injection.
    for reference in measured["references"].values():
        validate_state(reference, reference)
    workers = Workers(machines, measured["data_path"])
    schedule = Schedule(plan, machines)
    checkpoints, committed, starts, tokens = {}, {}, {}, {}
    errors, injections, fault_evidence = {}, {}, {}
    attempts = 0
    begin = time.monotonic()
    horizon = case["horizon_s"]
    events_file = (output / "events.jsonl").open("w")

    def log(event):
        events_file.write(
            json.dumps({"elapsed_s": time.monotonic() - begin, **event}) + "\n"
        )
        events_file.flush()

    try:
        while len(schedule.done) < len(tasks):
            now = time.monotonic() - begin
            for machine, failure_at in scenario["failures"].items():
                if now >= failure_at and machine not in schedule.dead:
                    killed_task = schedule.active.get(machine)
                    evidence = workers.kill(machine)
                    injections[machine] = time.monotonic() - begin
                    fault_evidence[machine] = dict(
                        requested_s=failure_at,
                        observed_s=injections[machine],
                        active_task=killed_task,
                        **evidence,
                    )
                    log(
                        dict(
                            kind="process_killed",
                            machine=machine,
                            requested_s=failure_at,
                            active_task=killed_task,
                            **evidence,
                        )
                    )
                    schedule.fail(machine)
                    tokens.pop(machine, None)
            for name, machine in schedule.dispatch():
                attempts += 1
                starts.setdefault(machine, time.monotonic() - begin)
                checkpoint = checkpoints.get(name, {}).get("path")
                tokens[machine] = workers.submit(
                    machine,
                    name,
                    tasks[name],
                    checkpoint,
                    output / f"attempt-{attempts}",
                )
                log(
                    dict(
                        kind="dispatch",
                        machine=machine,
                        task=name,
                        checkpoint=checkpoint,
                        generation=tokens[machine],
                    )
                )
            for machine in machines:
                if machine in schedule.dead:
                    continue
                connection = workers.connections[machine]
                while connection.poll():
                    event = connection.recv()
                    if event["kind"] == "error":
                        raise RuntimeError(event["traceback"])
                    if tokens.get(machine) != event.get("generation"):
                        continue
                    name = event["task"]
                    log({**event, "machine": machine})
                    if event["kind"] == "checkpoint":
                        if event["epoch"] > checkpoints.get(name, {}).get("epoch", 0):
                            checkpoints[name] = event
                    if event["kind"] == "result":
                        errors[name] = validate_state(
                            event["path"], measured["references"][name]
                        )
                        if name in committed:
                            raise AssertionError("Duplicate task commit")
                        committed[name] = event
                        for cancelled in schedule.finish(name):
                            workers.cancel(cancelled)
                            tokens.pop(cancelled, None)
                        log(dict(kind="commit", task=name, max_abs_error=errors[name]))
                if not workers.processes[machine].is_alive():
                    raise RuntimeError(f"Uninjected worker exit: {machine}")
            if time.monotonic() - begin > horizon:
                break
            if not schedule.active and not schedule.pending:
                break
            if len(schedule.dead) == len(machines):
                break
            time.sleep(0.001)
        elapsed = time.monotonic() - begin
        complete = len(committed) == len(tasks)
        selected = (
            max(sorted(committed), key=lambda t: committed[t]["accuracy"])
            if complete
            else None
        )
        reference_accuracy = {
            t: next(iter(measured["samples"][t].values()))[0]["accuracy"] for t in tasks
        }
        expected_selected = max(sorted(tasks), key=lambda t: reference_accuracy[t])
        if complete and selected != expected_selected:
            raise AssertionError("Hyperparameter selection changed")
        predicted = simulate(
            plan, tasks, machines, measured["estimates"], scenario["failures"], horizon
        )
        result = dict(
            kind="measured_local_process_replay",
            scenario=scenario,
            plan=plan,
            complete=complete,
            correct=complete,
            met_deadline=complete and elapsed <= case["deadline_s"],
            elapsed_s=elapsed,
            predicted=predicted,
            assumed_cost=lease_cost(machines, starts, injections, elapsed),
            deadline_s=case["deadline_s"],
            injections=injections,
            fault_evidence=fault_evidence,
            attempts=attempts,
            max_abs_error=errors,
            selected_trial=selected,
            workers=workers.ready,
        )
        write_json(output / "result.json", result)
        return result
    finally:
        events_file.close()
        workers.close()


def replay(folder, case_indices=None, repeats=2, holdout_fraction=0.8):
    if repeats < 1:
        raise ValueError("Need at least one replay repeat")
    folder = Path(folder).resolve()
    measured, searched = read(folder / "profile.json"), read(folder / "search.json")
    if (
        holdout_fraction < 0
        or holdout_fraction == measured["config"]["fault_time_fraction"]
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
        for name, row in case["policies"].items():
            if row is not None:
                entry = unique.setdefault(
                    plan_key(row["plan"]), {"plan": row["plan"], "policies": []}
                )
                entry["policies"].append(name)
        for number, entry in enumerate(unique.values()):
            # Healthy, training-scenario single loss, and an unseen-time
            # correlated loss. Holdout does not participate in policy selection.
            fault_time = searched["parallel_lower_bound_s"] * holdout_fraction
            scenarios = [
                case["scenarios"][0],
                case["scenarios"][1],
                dict(
                    name="holdout-correlated-loss",
                    probability=None,
                    failures=dict.fromkeys(
                        measured["config"]["failure_machines"], fault_time
                    ),
                ),
            ]
            for scenario in scenarios:
                for repeat in range(repeats):
                    jobs.append((index, number, entry, scenario, repeat))
    random.Random(735).shuffle(jobs)
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
            dict(case_index=index, policies=entry["policies"], repeat=repeat, **result)
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
