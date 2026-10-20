"""Selective checkpoints on t1 || (t2; t3), with measured process-loss replay.

Finite enumeration is an exact small-instance reference, not an SMT novelty
claim. No task is executed redundantly. See STATE_PROTECTION_EXPERIMENT.md.
"""

import argparse
import fcntl
import hashlib
import itertools
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import planner
from replay_runtime import replay_once
from run_study import profile
from training_experiment import read, stop_group
from workload import fingerprint, write_json

HERE = Path(__file__).resolve().parent
PRESETS = ("native", "storage", "homogeneous", "correlated")
POLICIES = (
    "restart_only",
    "uniform_checkpoint",
    "selective_fixed_placement",
    "risk_aware_sequential",
    "joint_checkpoint",
)
METRICS = ("elapsed_s", "worker_busy_s", "assumed_cost", "met_deadline")


def sources():
    return {
        name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
        for name in (
            Path(__file__).name,
            "planner.py",
            "workload.py",
            "run_study.py",
            "replay_runtime.py",
            "training_experiment.py",
        )
    }


def configuration(args):
    target = args.target_seconds if args.target_seconds is not None else 2.0
    repeats = args.repeats if args.repeats is not None else 3
    if not math.isfinite(target) or target <= 0 or repeats < 1:
        raise ValueError("Target seconds must be positive and finite; repeats >= 1")
    smoke = bool(args.smoke)
    return dict(
        schema_version=1,
        presets=list(PRESETS) if args.preset in (None, "all") else [args.preset],
        target_seconds=0.12 if smoke else target,
        repeats=1 if smoke else repeats,
        profile_repeats=1 if smoke else 2,
        smoke=smoke,
        width=16 if smoke else 128,
        source_hashes=sources(),
        success_target=0.95,
        deadline_multipliers=[1.25, 1.5, 2.0],
        replay_deadline_multiplier=1.5,
        planning_fractions=[0.2, 0.5, 0.8],
        replay_fractions=[0.35, 0.65],
        limitations=[
            "Real deterministic digits MLP training, with a sequential fine-tuning dependency; not distributed SGD, Ray, or TorchFT.",
            "SIGKILL of local CPU workers; input data, controller and checkpoint storage survive. This is not VM/storage failure.",
            "Native preset has no artificial compute or storage delay. Other presets add explicit checkpoint latency, not model state padding.",
            "Scenario probabilities and $/hour rates are assumptions, not measured spot failure rates or cloud bills.",
            "Uniform means a shared epoch interval or shared fraction of task work, whichever plans better. All tasks have the same architecture and batches.",
            "Only one modeled loss event per node; correlated events explicitly kill both spot workers.",
            "Transfer is shared-file access. There is no modeled network topology or data-locality speedup.",
            "Finite policy-menu optimum is not a global optimum over all schedules, backup mechanisms or checkpoint intervals.",
            "Final task outputs are always durable. Selectivity concerns intermediate model/optimizer/progress checkpoints, not permission to recover.",
            "Time excludes common warm-pool startup and post-run validation, includes task initialization, checkpoint writes and recovery.",
            "Weighted deadline fractions are scenario summaries, not empirical reliability guarantees. Repeats quantify timing variation only.",
        ],
    )


def case_config(config, preset, tasks):
    # Controlled checkpoint latency is tied to workload scale and applied to
    # every save, including mandatory final saves and profiling.
    delay = 0.0 if preset == "native" else config["target_seconds"] * 0.15
    return dict(
        tasks=tasks,
        final_tasks=["t1", "t3"],
        profile_repeats=config["profile_repeats"],
        profile_timeout_s=120,
        machines={
            machine: dict(
                cost_per_hour=1.0 if machine == "safe" else 0.25,
                accounting={"bill_warm_pool": True},
                execution_by_task={
                    t: dict(compute_scale=1.0, checkpoint_delay_s=delay) for t in tasks
                },
            )
            for machine in ("safe", "spot_a", "spot_b")
        },
    )


def calibrate(root, config):
    tasks = {
        name: dict(
            width=config["width"],
            epochs=6,
            batch_size=64,
            lr=0.03,
            seed=123 + index,
            checkpoint_every=6,
            dtype="float64",
            depends_on=["t2"] if name == "t3" else [],
        )
        for index, name in enumerate(("t1", "t2", "t3"))
    }
    pilot_config = case_config(config, "native", tasks)
    pilot_config["machines"] = {"safe": pilot_config["machines"]["safe"]}
    pilot_config["profile_repeats"] = 1
    pilot = profile(pilot_config, root / f"pilot-{uuid4().hex[:8]}")
    # Same architecture, batch count and epochs per unit across every task and
    # preset. t1 is twice as long as either task in the sequential branch.
    epoch_s = statistics.median(pilot["estimates"][t]["safe"]["epoch_s"] for t in tasks)
    unit = max(8, min(2000, math.ceil(config["target_seconds"] / epoch_s)))
    for name, task in tasks.items():
        task.update(epochs=unit * (2 if name == "t1" else 1))
        task["checkpoint_every"] = max(1, unit // 4)
    write_json(root / "calibration.json", dict(tasks=tasks, epoch_s=epoch_s))


def failure_scenarios(preset, fractions, window):
    # The total probability of any failure is held at 0.5 in all presets.
    weights = {
        "native": (0.01, 0.22, 0.22, 0.05),
        "storage": (0.01, 0.22, 0.22, 0.05),
        "homogeneous": (0.15, 0.15, 0.15, 0.05),
        "correlated": (0.01, 0.07, 0.07, 0.35),
    }[preset]
    rows = [dict(name="healthy", probability=0.5, failures={})]
    for label, nodes, probability in zip(
        ("safe", "spot_a", "spot_b", "correlated"),
        (("safe",), ("spot_a",), ("spot_b",), ("spot_a", "spot_b")),
        weights,
    ):
        for fraction in fractions:
            rows.append(
                dict(
                    name=f"{label}-{fraction:g}",
                    probability=probability / len(fractions),
                    failures=dict.fromkeys(nodes, window * fraction),
                )
            )
    return rows


def menus(tasks):
    names = tuple(tasks)
    unit = min(t["epochs"] for t in tasks.values())
    uniform = [{t: tasks[t]["epochs"] for t in names}]
    for divisor in (2, 4, 8):
        uniform.append({t: max(1, unit // divisor) for t in names})
        uniform.append({t: max(1, tasks[t]["epochs"] // divisor) for t in names})
    uniform = list({fingerprint(p): p for p in uniform}.values())
    choices = {t: sorted({p[t] for p in uniform}) for t in names}
    return uniform, choices


def ranking(row):
    return (
        row["expected_elapsed_s"],
        row["expected_cost"],
        row["expected_worker_busy_s"],
        fingerprint(row["plan"]),
    )


def select(rows, target):
    feasible = [r for r in rows if r["success_probability"] + 1e-12 >= target]
    return (
        min(feasible, key=ranking)
        if feasible
        else min(rows, key=lambda r: (-r["success_probability"], *ranking(r))),
        bool(feasible),
    )


def pareto(rows, target):
    """Time/cost frontier of feasible rows, one deterministic plan per tie."""
    result, lowest_cost = [], math.inf
    for row in sorted(
        (r for r in rows if r["success_probability"] + 1e-12 >= target),
        key=ranking,
    ):
        if row["expected_cost"] < lowest_cost - 1e-15:
            result.append(row)
            lowest_cost = row["expected_cost"]
    return result


def plan_case(root, config, preset):
    measured = read(root / preset / "profile.json")
    tasks, machines = measured["tasks"], measured["machines"]
    uniform, choices = menus(tasks)
    uniform_keys = {fingerprint(c) for c in uniform}
    final = {t: v["epochs"] for t, v in tasks.items()}
    orders = [
        order
        for order in itertools.permutations(tasks)
        if order.index("t2") < order.index("t3")
    ]
    placements = [
        dict(zip(tasks, assignment))
        for assignment in itertools.product(machines, repeat=len(tasks))
    ]
    nominal = []
    for placement, order in itertools.product(placements, orders):
        plan = dict(
            copies={t: [m] for t, m in placement.items()},
            order=order,
            checkpoint_every=final,
        )
        row = planner.simulate(plan, tasks, machines, measured["estimates"], {}, 1e6)
        nominal.append((row["elapsed_s"], row["cost"], fingerprint(plan), plan))
    best_nominal = min(nominal)
    window = best_nominal[0]
    fixed_copies = best_nominal[3]["copies"]
    planning = failure_scenarios(preset, config["planning_fractions"], window)
    replay = failure_scenarios(preset, config["replay_fractions"], window)
    horizon = max(30.0, window * 8)
    rows = []
    # Simulate each plan once, then evaluate all predeclared deadlines against
    # those same scenarios. Avoid choosing the replay case from modeled gains.
    for placement, order, intervals in itertools.product(
        placements, orders, itertools.product(*(choices[t] for t in tasks))
    ):
        plan = dict(
            copies={t: [m] for t, m in placement.items()},
            order=order,
            checkpoint_every=dict(zip(tasks, intervals)),
        )
        row = planner.evaluate(
            plan,
            tasks,
            machines,
            measured["estimates"],
            planning,
            window * 1.5,
            horizon,
        )
        rows.append(row)
    deadline_cases = []
    for multiplier in config["deadline_multipliers"]:
        candidates = []
        for original in rows:
            row = {
                **original,
                "success_probability": sum(
                    scenario["probability"]
                    for scenario, result in zip(planning, original["scenarios"])
                    if result["complete"] and result["elapsed_s"] <= window * multiplier
                ),
            }
            candidates.append(row)
        groups = {
            "restart_only": [
                r for r in candidates if r["plan"]["checkpoint_every"] == final
            ],
            "uniform_checkpoint": [
                r
                for r in candidates
                if fingerprint(r["plan"]["checkpoint_every"]) in uniform_keys
            ],
            "selective_fixed_placement": [
                r for r in candidates if r["plan"]["copies"] == fixed_copies
            ],
            "joint_checkpoint": candidates,
        }
        uniform_best, _ = select(groups["uniform_checkpoint"], config["success_target"])
        groups["risk_aware_sequential"] = [
            r
            for r in candidates
            if r["plan"]["copies"] == uniform_best["plan"]["copies"]
        ]
        policies = {}
        for name in POLICIES:
            best, feasible = select(groups[name], config["success_target"])
            policies[name] = dict(row=best, feasible=feasible)
        deadline_cases.append(
            dict(
                multiplier=multiplier,
                deadline_s=window * multiplier,
                policies=policies,
                pareto={
                    name: pareto(group, config["success_target"])
                    for name, group in groups.items()
                },
            )
        )
    chosen = next(
        c
        for c in deadline_cases
        if c["multiplier"] == config["replay_deadline_multiplier"]
    )
    plans = {}
    for name, entry in chosen["policies"].items():
        plan = entry["row"]["plan"]
        key = fingerprint(plan)
        record = plans.setdefault(
            key, dict(plan=plan, policies=[], modeled_feasible={})
        )
        record["policies"].append(name)
        record["modeled_feasible"][name] = entry["feasible"]
    jobs = [
        dict(plan_id=p, scenario=s, repeat=r)
        for r in range(config["repeats"])
        for p in plans
        for s in replay
    ]
    random.Random(873).shuffle(jobs)
    for job in jobs:
        job["id"] = fingerprint(job)[:20]
    result = dict(
        preset=preset,
        deadline_cases=deadline_cases,
        plans=plans,
        jobs=jobs,
        case=dict(
            machines=machines, deadline_s=chosen["deadline_s"], horizon_s=horizon
        ),
        window_s=window,
        scenarios=replay,
        planning_scenarios=planning,
        choices=choices,
        uniform_choices=uniform,
        enumerated_plans=len(rows),
        nominal_plan=best_nominal[3],
        selection_rule="Fixed 1.5x healthy reference deadline; minimize expected elapsed time, then cost. No replay-driven selection.",
    )
    write_json(root / preset / "search.json", result)
    print(
        f"{preset}: {len(rows)} candidates, {len(plans)} distinct plans, {len(jobs)} replays",
        flush=True,
    )


def replay_case(root, config, preset):
    folder = root / preset
    measured, search = read(folder / "profile.json"), read(folder / "search.json")
    runs = folder / "runs"
    runs.mkdir(exist_ok=True)
    for index, job in enumerate(search["jobs"]):
        saved = runs / f"{job['id']}.json"
        if saved.exists():
            record = read(saved)
            if record["job"] != job:
                raise ValueError(f"Saved job mismatch: {saved}")
            continue
        selected = search["plans"][job["plan_id"]]
        print(
            f"{preset} {index + 1}/{len(search['jobs'])}: {selected['policies']} {job['scenario']['name']}",
            flush=True,
        )
        attempt = runs / f"{job['id']}-{uuid4().hex[:8]}"
        result = replay_once(
            measured,
            search["case"],
            selected["plan"],
            job["scenario"],
            attempt,
            kind="measured_selective_state_checkpoints",
        )
        # Incomplete runs are retained as failures, not discarded. State
        # mismatches raise from replay_once and stop the experiment.
        events = read(attempt / "events.json")
        result["checkpoint_commits"] = sum(e["kind"] == "checkpoint" for e in events)
        result["checkpoint_bytes"] = sum(
            Path(e["path"]).stat().st_size for e in events if e["kind"] == "checkpoint"
        )
        write_json(saved, dict(job=job, result=result, attempt=str(attempt)))
        report_case(root, config, preset)


def summarize_records(records, repeats):
    per_repeat = []
    for repeat in range(repeats):
        group = [r for r in records if r["job"]["repeat"] == repeat]
        per_repeat.append(
            {
                k: sum(
                    r["job"]["scenario"]["probability"] * r["result"][k] for r in group
                )
                for k in METRICS
            }
        )
    return dict(
        weighted={k: statistics.mean(r[k] for r in per_repeat) for k in METRICS},
        per_repeat=per_repeat,
        all_correct=all(r["result"]["correct"] for r in records),
        active_faults=sum(
            e.get("active_task") is not None
            for r in records
            for e in r["result"]["fault_evidence"].values()
        ),
        requested_faults=sum(len(r["job"]["scenario"]["failures"]) for r in records),
        injected_faults=sum(len(r["result"]["fault_evidence"]) for r in records),
        restored_attempts=sum(
            e["restored_epoch"] > 0
            for r in records
            for e in r["result"]["restorations"]
        ),
        observed_checkpoint_bytes=sum(r["result"]["checkpoint_bytes"] for r in records),
        scenario_results={
            name: [
                {k: r["result"][k] for k in (*METRICS, "correct")}
                for r in records
                if r["job"]["scenario"]["name"] == name
            ]
            for name in sorted({r["job"]["scenario"]["name"] for r in records})
        },
    )


def comparison(joint, baseline, config):
    if joint["plan_id"] == baseline["plan_id"]:
        return dict(verdict="identical_plan", elapsed_gain_fraction=0.0)
    j, b = joint["weighted"], baseline["weighted"]
    gain = 1 - j["elapsed_s"] / b["elapsed_s"]
    adequate = (
        joint["all_correct"]
        and baseline["all_correct"]
        and j["met_deadline"] + 1e-12 >= config["success_target"]
        and j["met_deadline"] + 1e-12 >= b["met_deadline"]
    )
    gains = [
        1 - a["elapsed_s"] / b["elapsed_s"]
        for a, b in zip(joint["per_repeat"], baseline["per_repeat"])
    ]
    if not adequate:
        verdict = "no_claim_deadline_or_correctness"
    elif gain >= 0.05:
        verdict = (
            "candidate_time_advantage"
            if len(gains) >= 3 and min(gains) > 0
            else "preliminary_time_advantage"
        )
    elif gain <= -0.05:
        verdict = "time_loss"
    else:
        verdict = "within_five_percent"
    return dict(
        verdict=verdict,
        elapsed_gain_fraction=gain,
        per_repeat_gains=gains,
        assumed_cost_gain_fraction=1 - j["assumed_cost"] / b["assumed_cost"],
        note="Descriptive comparison, not a significance test. Inspect cost, reliability and each failure scenario as well.",
    )


def report_case(root, config, preset):
    folder = root / preset
    search = read(folder / "search.json")
    available = {}
    for job in search["jobs"]:
        path = folder / "runs" / f"{job['id']}.json"
        if path.exists():
            available[job["id"]] = read(path)
    policies = {}
    for key, entry in search["plans"].items():
        jobs = [j for j in search["jobs"] if j["plan_id"] == key]
        records = [available[j["id"]] for j in jobs if j["id"] in available]
        row = dict(
            complete=len(records) == len(jobs), observations=len(records), plan_id=key
        )
        if row["complete"]:
            row.update(summarize_records(records, config["repeats"]))
        for name in entry["policies"]:
            policies[name] = {
                **row,
                "modeled_feasible": entry["modeled_feasible"][name],
            }
    comparisons = {}
    if all(r["complete"] for r in policies.values()):
        comparisons = {
            name: comparison(policies["joint_checkpoint"], row, config)
            for name, row in policies.items()
            if name != "joint_checkpoint"
        }
    result = dict(
        preset=preset,
        complete=len(available) == len(search["jobs"]),
        completed_observations=len(available),
        expected_observations=len(search["jobs"]),
        policies=policies,
        joint_comparisons=comparisons,
        artificial_checkpoint_delay_s=0
        if preset == "native"
        else config["target_seconds"] * 0.15,
        limitations=config["limitations"],
        smoke=config["smoke"],
    )
    write_json(folder / "summary.json", result)
    lines = [
        f"# State protection: {preset}",
        "",
        f"Observations: {len(available)}/{len(search['jobs'])}. Smoke: {config['smoke']}.",
        "",
        "| Policy | Weighted elapsed s | Worker busy s | Assumed cost | Deadline fraction | Active faults | Restored attempts |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, row in policies.items():
        if not row["complete"]:
            lines.append(f"| {name} | incomplete | | | | | |")
            continue
        v = row["weighted"]
        lines.append(
            f"| {name} | {v['elapsed_s']:.3f} | {v['worker_busy_s']:.3f} | {v['assumed_cost']:.6f} | {v['met_deadline']:.4f} | {row['active_faults']} | {row['restored_attempts']} |"
        )
    lines += ["", "Joint versus each baseline:", ""]
    for name, row in comparisons.items():
        lines.append(
            f"- {name}: {row['verdict']}; elapsed gain {100 * row['elapsed_gain_fraction']:.2f}%."
        )
    lines += [
        "",
        "These are assumed-weight scenario summaries, not measured cloud reliability. Identical plans share observations. Five percent is a predeclared practical threshold, not statistical significance.",
        "",
        *config["limitations"],
        "",
    ]
    (folder / "summary.md").write_text("\n".join(lines))


def stage(root, config, preset, name):
    if name == "calibrate":
        calibrate(root, config)
    elif name == "profile":
        tasks = read(root / "calibration.json")["tasks"]
        measured = profile(
            case_config(config, preset, tasks),
            root / preset / f"profile-{uuid4().hex[:8]}",
        )
        write_json(root / preset / "profile.json", measured)
    elif name == "plan":
        plan_case(root, config, preset)
    elif name == "replay":
        replay_case(root, config, preset)
    else:
        raise ValueError(name)


def package(root, config):
    summaries = {
        p: read(root / p / "summary.json")
        for p in config["presets"]
        if (root / p / "summary.json").exists()
    }
    write_json(
        root / "summary.json",
        dict(
            complete=len(summaries) == len(config["presets"])
            and all(s["complete"] for s in summaries.values()),
            cases=summaries,
            limitations=config["limitations"],
        ),
    )
    with zipfile.ZipFile(
        root / "analysis.zip.tmp", "w", zipfile.ZIP_DEFLATED
    ) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in (".json", ".md", ".log"):
                archive.write(path, path.relative_to(root))
    (root / "analysis.zip.tmp").replace(root / "analysis.zip")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    location = parser.add_mutually_exclusive_group()
    location.add_argument("--output", type=Path)
    location.add_argument("--resume", type=Path)
    parser.add_argument("--preset", choices=("all", *PRESETS))
    parser.add_argument("--smoke", action="store_true", default=None)
    parser.add_argument("--target-seconds", type=float)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--max-minutes", type=float, default=28)
    parser.add_argument(
        "--stage",
        choices=("calibrate", "profile", "plan", "replay"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--case", choices=PRESETS, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.stage:
        root = args.output.resolve()
        stage(root, read(root / "config.json"), args.case, args.stage)
        return 0
    if not hasattr(os, "sched_getaffinity") or len(os.sched_getaffinity(0)) < 3:
        parser.error("Requires Linux and at least three available CPU cores")
    if not 0 < args.max_minutes <= 28:
        parser.error("--max-minutes must be in (0, 28]")
    if args.resume:
        if any(
            getattr(args, k) is not None
            for k in ("preset", "smoke", "target_seconds", "repeats")
        ):
            parser.error("Resume uses saved settings; only --max-minutes may change")
        root = args.resume.resolve()
        config = read(root / "config.json")
        if config["source_hashes"] != sources():
            parser.error(
                "Source changed: resume with the original commit or start a new run"
            )
    else:
        try:
            config = configuration(args)
        except ValueError as exc:
            parser.error(str(exc))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        root = (
            args.output
            or Path("ray_gcp_results/state-protection") / f"{stamp}-{uuid4().hex[:8]}"
        ).resolve()
        root.mkdir(parents=True, exist_ok=False)
        write_json(root / "config.json", config)
    lock = (root / "run.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("Another runner is active here")
    print("State-protection results:", root, flush=True)
    start = time.monotonic()
    status = dict(state="running")
    jobs = [(None, "calibrate", root / "calibration.json")]
    for preset in config["presets"]:
        (root / preset).mkdir(exist_ok=True)
        jobs += [
            (preset, s, root / preset / f"{s if s != 'plan' else 'search'}.json")
            for s in ("profile", "plan", "replay")
        ]
    try:
        for preset, name, marker in jobs:
            if marker.exists():
                continue
            remaining = args.max_minutes * 60 - (time.monotonic() - start)
            if remaining <= 0:
                raise subprocess.TimeoutExpired(name, args.max_minutes * 60)
            status["active"] = f"{preset or 'suite'}/{name}"
            write_json(root / "status.json", status)
            log_path = (root / preset if preset else root) / f"{name}.log"
            command = [
                sys.executable,
                "-u",
                str(Path(__file__).resolve()),
                "--output",
                str(root),
                "--stage",
                name,
            ]
            if preset:
                command += ["--case", preset]
            print(f"Running {status['active']}; log: {log_path}", flush=True)
            with log_path.open("a") as log:
                process = subprocess.Popen(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env={
                        **os.environ,
                        "OMP_NUM_THREADS": "1",
                        "MKL_NUM_THREADS": "1",
                        "OPENBLAS_NUM_THREADS": "1",
                    },
                )
                try:
                    code = process.wait(timeout=remaining)
                except (KeyboardInterrupt, subprocess.TimeoutExpired):
                    stop_group(process)
                    raise
            if code:
                stop_group(process)
                raise RuntimeError(f"{status['active']} failed: see {log_path}")
            if name == "replay":
                write_json(marker, dict(complete=True))
        status["state"] = "complete"
    except subprocess.TimeoutExpired:
        status["state"] = "timed_out"
    except KeyboardInterrupt:
        status["state"] = "interrupted"
    except Exception as exc:
        status.update(state="failed", error=str(exc))
    finally:
        status["wall_s"] = time.monotonic() - start
        write_json(root / "status.json", status)
        package(root, config)
        lock.close()
    print(json.dumps(status), flush=True)
    print("Upload:", root / "analysis.zip", flush=True)
    if status["state"] != "complete":
        print(
            f"Resume: {sys.executable} {Path(__file__).resolve()} --resume {root}",
            flush=True,
        )
    return 1 if status["state"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
