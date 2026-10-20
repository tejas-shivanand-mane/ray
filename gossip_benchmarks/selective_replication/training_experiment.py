"""Measured two-trial training experiment with resumable, bounded process-loss runs."""

import argparse
import copy
import fcntl
import hashlib
import itertools
import json
import math
import os
import random
import signal
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
from workload import fingerprint, write_json

HERE = Path(__file__).resolve().parent
STAGES = ("calibrate", "profile", "plan", "replay", "report")
FAMILIES = ("checkpoint_retry", "fixed_placement_replication", "uniform_replication")


def read(path):
    return json.loads(Path(path).read_text())


def source_hashes():
    return {
        name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
        for name in (
            "training_experiment.py",
            "workload.py",
            "planner.py",
            "replay_runtime.py",
            "run_study.py",
        )
    }


def make_config(args):
    smoke = bool(args.smoke)
    native = bool(args.native_workers)
    delay = args.checkpoint_delay_s or 0.0
    seconds = (
        args.target_seconds
        if args.target_seconds is not None
        else (0.15 if smoke else 4.0)
    )
    repeats = args.repeats if args.repeats is not None else 1
    profile_repeats = (
        args.profile_repeats
        if args.profile_repeats is not None
        else (1 if smoke else 2)
    )
    if (
        not math.isfinite(seconds)
        or seconds <= 0
        or not math.isfinite(delay)
        or delay < 0
    ):
        raise ValueError(
            "Target seconds must be positive and checkpoint delay nonnegative, both finite"
        )
    if repeats < 1 or profile_repeats < 1:
        raise ValueError("Repeat counts must be positive")
    tasks = {
        name: dict(
            width=width,
            epochs=5,
            batch_size=64,
            lr=0.03,
            seed=123,
            checkpoint_every=1,
            dtype="float64",
        )
        for name, width in zip(
            ("trial-a", "trial-b"), ((16, 24) if smoke else (128, 256))
        )
    }
    machines = {
        m: dict(
            cost_per_hour=1.0,
            execution_by_task={
                t: dict(
                    compute_scale=(1.0 if native or m == "spot_a" else scale),
                    checkpoint_delay_s=delay,
                )
                for t, scale in zip(tasks, (3.0, 1.5))
            },
        )
        for m in ("safe", "spot_a", "spot_b")
    }
    return dict(
        schema_version=1,
        kind="measured_training_controlled_worker_pacing"
        if not native
        else "measured_training_native_workers",
        smoke=smoke,
        tasks=tasks,
        machines=machines,
        target_fast_compute_s=seconds,
        repeats=repeats,
        profile_repeats=profile_repeats,
        profile_timeout_s=120,
        success_target=0.99,
        deadline_multipliers=[1.05, 1.2, 1.5],
        planning_fractions=[0.2, 0.4, 0.6, 0.8],
        replay_fractions=[0.3, 0.7],
        probabilities=dict(
            healthy=0.5, safe=0.005, single_a=0.245, single_b=0.245, correlated=0.005
        ),
        source_hashes=source_hashes(),
        limitations=[
            "Real CPU learning on sklearn digits; small independent trials, not distributed gradient training.",
            "Local worker processes are killed; controller and shared checkpoint storage survive. No VM loss is tested.",
            "Worker-speed differences are explicit per-task sleep pacing unless native workers are requested.",
            "Worker busy time includes pacing and storage waits; it is not CPU utilization or cloud cost.",
            "Checkpoint delay is explicitly artificial when nonzero; default is zero.",
            "Failure weights are experiment assumptions, not measured cloud rates. Few runs cannot establish 99% reliability.",
            "Plans and deadline are chosen from profiling before replay; all candidate searches and negative results are retained.",
        ],
    )


def scenarios(config, window, fractions):
    probs = config["probabilities"]
    result = [dict(name="healthy", probability=probs["healthy"], failures={})]
    for f in fractions:
        for label, failed in (
            ("safe", ["safe"]),
            ("single_a", ["spot_a"]),
            ("single_b", ["spot_b"]),
            ("correlated", ["spot_a", "spot_b"]),
        ):
            result.append(
                dict(
                    name=f"{label}-t{f:g}",
                    probability=probs[label] / len(fractions),
                    failures=dict.fromkeys(failed, window * f),
                )
            )
    return result


def calibrate(root, config):
    # Fast-worker calibration uses real compute with no artificial delay. All
    # full profiles below include the selected pacing and actual checkpoint IO.
    pilot_config = copy.deepcopy(config)
    pilot_config["machines"] = {"spot_a": dict(cost_per_hour=1.0)}
    pilot_config["profile_repeats"] = 1
    pilot = profile(pilot_config, root / f"calibration-{uuid4().hex[:8]}")
    tasks = copy.deepcopy(config["tasks"])
    for index, name in enumerate(tasks):
        epoch_s = pilot["estimates"][name]["spot_a"]["epoch_s"]
        epochs = max(
            2,
            min(
                4000, math.ceil(config["target_fast_compute_s"] * (index + 1) / epoch_s)
            ),
        )
        tasks[name].update(epochs=epochs, checkpoint_every=max(1, epochs // 4))
    write_json(root / "calibration.json", dict(tasks=tasks, pilot=pilot))
    print("Calibrated epochs:", {n: t["epochs"] for n, t in tasks.items()}, flush=True)


def full_profile(root, config):
    updated = {**config, "tasks": read(root / "calibration.json")["tasks"]}
    measured = profile(updated, root / f"profile-{uuid4().hex[:8]}")
    write_json(root / "profile.json", measured)


def sequential(measured, case, choices):
    seed = (
        case["policies"]["checkpoint_retry"]
        or case["unconstrained_policies"]["checkpoint_retry"]
    )
    fixed = seed["plan"]
    tasks = measured["tasks"]
    rows = []
    backup_options = [
        [(), *((m,) for m in case["machines"] if m != fixed["copies"][t][0])]
        for t in tasks
    ]
    for backups, order, cps in itertools.product(
        itertools.product(*backup_options),
        itertools.permutations(tasks),
        itertools.product(*(choices[t] for t in tasks)),
    ):
        plan = dict(
            copies={t: [fixed["copies"][t][0], *b] for t, b in zip(tasks, backups)},
            order=order,
            checkpoint_every=dict(zip(tasks, cps)),
        )
        rows.append(
            planner.evaluate(
                plan,
                tasks,
                case["machines"],
                measured["estimates"],
                case["scenarios"],
                case["deadline_s"],
                case["horizon_s"],
            )
        )
    feasible = planner.choose(rows, case["target"], "expected_worker_busy_s")
    best_effort = min(
        rows, key=lambda r: (-r["success_probability"], r["expected_worker_busy_s"])
    )
    return dict(single_copy_seed=seed, feasible=feasible, best_effort=best_effort)


def plan_study(root, config):
    measured = read(root / "profile.json")
    tasks = measured["tasks"]
    choices = {
        t: sorted({max(1, v["epochs"] // d) for d in (1, 2, 4, 8)})
        for t, v in tasks.items()
    }
    # Reference duration uses the slower reliable worker, independent of policy.
    window = max(
        measured["estimates"][t]["safe"]["init_s"]
        + tasks[t]["epochs"] * measured["estimates"][t]["safe"]["epoch_s"]
        + measured["estimates"][t]["safe"]["save_s"]
        for t in tasks
    )
    planning = scenarios(config, window, config["planning_fractions"])
    cases = []
    for multiplier in config["deadline_multipliers"]:
        case = planner.search(
            tasks,
            measured["machines"],
            measured["estimates"],
            planning,
            window * multiplier,
            target=config["success_target"],
            checkpoint_choices=choices,
            objective="expected_worker_busy_s",
        )
        strong = sequential(measured, case, choices)
        case["policies"]["risk_aware_sequential"] = strong["feasible"]
        case["unconstrained_policies"]["risk_aware_sequential"] = strong["best_effort"]
        case["sequential_seed"] = strong["single_copy_seed"]
        joint = case["policies"]["joint_enumeration"]
        baselines = [
            case["policies"][n] for n in FAMILIES if case["policies"][n] is not None
        ]
        gain = None
        if joint and baselines:
            best = min(r["expected_worker_busy_s"] for r in baselines)
            gain = (best - joint["expected_worker_busy_s"]) / best
        case.update(deadline_multiplier=multiplier, gain_over_best_nonselective=gain)
        cases.append(case)
    # Predeclared discovery rule; record every case, including no-gain outcomes.
    eligible = [
        i for i, c in enumerate(cases) if c["gain_over_best_nonselective"] is not None
    ]
    index = (
        max(eligible, key=lambda i: (cases[i]["gain_over_best_nonselective"], -i))
        if eligible
        else 1
    )
    case = cases[index]
    plans = {}
    for name, feasible in case["policies"].items():
        row = feasible or case["unconstrained_policies"][name]
        key = fingerprint(row["plan"])
        entry = plans.setdefault(
            key, dict(plan=row["plan"], policies=[], modeled_feasible={})
        )
        entry["policies"].append(name)
        entry["modeled_feasible"][name] = feasible is not None
    replay_scenarios = scenarios(config, window, config["replay_fractions"])
    jobs = [
        dict(plan_id=p, scenario=s, repeat=r)
        for r in range(config["repeats"])
        for p in plans
        for s in replay_scenarios
    ]
    # Healthy correctness controls run first; failure runs are seeded and shuffled.
    healthy = [j for j in jobs if not j["scenario"]["failures"]]
    failures = [j for j in jobs if j["scenario"]["failures"]]
    random.Random(412).shuffle(healthy)
    random.Random(413).shuffle(failures)
    for job in healthy + failures:
        job["id"] = fingerprint(job)[:20]
    result = dict(
        cases=cases,
        selected_case=index,
        plans=plans,
        jobs=healthy + failures,
        replay_scenarios=replay_scenarios,
        failure_window_s=window,
        checkpoint_choices=choices,
        selection_rule="Maximum modeled gain over best feasible checkpoint/fixed/uniform baseline; ties prefer earliest deadline. Fallback is middle deadline. No replay result influences selection.",
    )
    write_json(root / "search.json", result)
    print(
        f"Selected deadline {case['deadline_s']:.2f}s; {len(plans)} distinct plans; {len(jobs)} measured runs",
        flush=True,
    )
    print(
        "Modeled gain over best nonselective baseline:",
        case["gain_over_best_nonselective"],
        flush=True,
    )


def replay_study(root, config):
    measured, searched = read(root / "profile.json"), read(root / "search.json")
    case = searched["cases"][searched["selected_case"]]
    runs = root / "runs"
    runs.mkdir(exist_ok=True)
    for index, job in enumerate(searched["jobs"]):
        saved = runs / f"{job['id']}.json"
        if saved.exists():
            record = read(saved)
            if record["job"] != job or not record["result"]["correct"]:
                raise ValueError(f"Invalid saved observation: {saved}")
            print(f"Reuse {index + 1}/{len(searched['jobs'])}: {job['id']}", flush=True)
            continue
        selected = searched["plans"][job["plan_id"]]
        attempt = runs / f"{job['id']}-{uuid4().hex[:8]}"
        print(
            f"Run {index + 1}/{len(searched['jobs'])}: {selected['policies']} / {job['scenario']['name']}",
            flush=True,
        )
        result = replay_once(
            measured,
            case,
            selected["plan"],
            job["scenario"],
            attempt,
            kind=config["kind"],
        )
        if not result["correct"]:
            raise AssertionError(f"Incomplete/incorrect training: {attempt}")
        write_json(
            saved,
            dict(
                job=job,
                policies=selected["policies"],
                result=result,
                attempt=str(attempt),
            ),
        )
        report(root, config)
    report(root, config)


def report(root, config):
    searched = read(root / "search.json")
    case = searched["cases"][searched["selected_case"]]
    available = {}
    for job in searched["jobs"]:
        path = root / "runs" / f"{job['id']}.json"
        if path.exists():
            available[job["id"]] = read(path)
    summaries = {}
    for plan_id, entry in searched["plans"].items():
        jobs = [j for j in searched["jobs"] if j["plan_id"] == plan_id]
        records = [available[j["id"]] for j in jobs if j["id"] in available]
        if len(records) != len(jobs):
            for name in entry["policies"]:
                summaries[name] = dict(
                    complete=False,
                    observations=len(records),
                    expected_observations=len(jobs),
                )
            continue
        weighted = {
            k: sum(
                r["job"]["scenario"]["probability"] * r["result"][k] / config["repeats"]
                for r in records
            )
            for k in ("elapsed_s", "worker_busy_s", "met_deadline")
        }
        groups = {}
        for scenario in searched["replay_scenarios"]:
            matched = [
                r["result"]
                for r in records
                if r["job"]["scenario"]["name"] == scenario["name"]
            ]
            groups[scenario["name"]] = dict(
                observations=len(matched),
                elapsed_s=statistics.mean(r["elapsed_s"] for r in matched),
                worker_busy_s=statistics.mean(r["worker_busy_s"] for r in matched),
                met_deadline=sum(r["met_deadline"] for r in matched),
                all_requested_faults_injected=all(
                    r["fault_coverage_complete"] for r in matched
                ),
                active_faults=sum(
                    e.get("active_task") is not None
                    for r in matched
                    for e in r["fault_evidence"].values()
                ),
            )
        for name in entry["policies"]:
            summaries[name] = dict(
                complete=True,
                observations=len(records),
                modeled_feasible=entry["modeled_feasible"][name],
                weighted_observed=weighted,
                scenarios=groups,
                all_correct=all(r["result"]["correct"] for r in records),
                plan_id=plan_id,
            )
    result = dict(
        kind=config["kind"],
        smoke=config["smoke"],
        complete=len(available) == len(searched["jobs"]),
        completed_observations=len(available),
        expected_observations=len(searched["jobs"]),
        deadline_s=case["deadline_s"],
        policies=summaries,
        limitations=config["limitations"],
        comparison_note="Weighted observations use assumed failure weights. Shared plan IDs reuse identical measurements. Compare deadline attainment before claiming a resource advantage; report untriggered and idle-worker faults.",
    )
    write_json(root / "summary.json", result)
    lines = [
        "# Measured training experiment",
        "",
        f"Completed observations: {len(available)}/{len(searched['jobs'])}.",
        f"Kind: {config['kind']}. Smoke test: {config['smoke']}. Deadline: {case['deadline_s']:.3f}s.",
        "",
        "| Policy | Weighted busy seconds | Weighted elapsed seconds | Weighted deadline fraction |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, row in summaries.items():
        if row["complete"]:
            v = row["weighted_observed"]
            lines.append(
                f"| {name} | {v['worker_busy_s']:.3f} | {v['elapsed_s']:.3f} | {v['met_deadline']:.4f} |"
            )
        else:
            lines.append(f"| {name} | incomplete | incomplete | incomplete |")
    lines += ["", *config["limitations"], "", result["comparison_note"], ""]
    (root / "summary.md").write_text("\n".join(lines))
    if result["complete"]:
        print("Measured experiment complete. Summary:", root / "summary.md", flush=True)


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # Also fence descendants if the parent exited before its workers did.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def package_report(root):
    """Collect analysis evidence without copying training tensors or datasets."""
    paths = (
        list(root.glob("*.json")) + list(root.glob("*.md")) + list(root.glob("*.log"))
    )
    paths += list((root / "runs").glob("*.json"))
    paths += list((root / "runs").glob("*/events.json"))
    temporary = root / "analysis.zip.tmp"
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(paths):
            archive.write(path, path.relative_to(root))
    temporary.replace(root / "analysis.zip")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    location = parser.add_mutually_exclusive_group()
    location.add_argument("--output", type=Path)
    location.add_argument("--resume", type=Path)
    parser.add_argument("--max-minutes", type=float, default=28)
    parser.add_argument("--smoke", action="store_true", default=None)
    parser.add_argument("--native-workers", action="store_true", default=None)
    parser.add_argument("--checkpoint-delay-s", type=float)
    parser.add_argument(
        "--target-seconds",
        type=float,
        help="Fast-worker compute target for trial A; B targets twice this",
    )
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--profile-repeats", type=int)
    parser.add_argument("--stage", choices=STAGES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.stage:
        root = args.output.resolve()
        config = read(root / "config.json")
        {
            "calibrate": calibrate,
            "profile": full_profile,
            "plan": plan_study,
            "replay": replay_study,
            "report": report,
        }[args.stage](root, config)
        return 0
    if os.name != "posix" or not hasattr(os, "sched_getaffinity"):
        parser.error("This CPU-affinity and process-loss experiment requires Linux")
    if len(os.sched_getaffinity(0)) < 3:
        parser.error("Need at least three available CPU cores")
    if not 0 < args.max_minutes <= 28:
        parser.error("--max-minutes must be positive and at most 28")
    if args.resume:
        if any(
            getattr(args, k) is not None
            for k in (
                "smoke",
                "native_workers",
                "checkpoint_delay_s",
                "target_seconds",
                "repeats",
                "profile_repeats",
            )
        ):
            parser.error(
                "Resume uses the saved experiment settings; only --max-minutes may change"
            )
        root = args.resume.resolve()
        config = read(root / "config.json")
        if config["source_hashes"] != source_hashes():
            parser.error(
                "Experiment source changed; use its original commit to resume or start a new output directory"
            )
    else:
        try:
            config = make_config(args)
        except ValueError as exc:
            parser.error(str(exc))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        root = (
            args.output
            or Path("ray_gcp_results/measured-selective") / f"{stamp}-{uuid4().hex[:8]}"
        ).resolve()
        root.mkdir(parents=True, exist_ok=False)
        write_json(root / "config.json", config)
    lock = (root / "run.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error("Another runner is active in this output directory")
    print("Training results:", root, flush=True)
    print(
        "Settings:",
        config["kind"],
        "checkpoint delay",
        config["machines"]["safe"]["execution_by_task"]["trial-a"][
            "checkpoint_delay_s"
        ],
        flush=True,
    )
    outputs = dict(
        calibrate="calibration.json",
        profile="profile.json",
        plan="search.json",
        replay="replay-complete.json",
        report="report-complete.json",
    )
    start = time.monotonic()
    status = dict(
        state="running",
        completed=[],
        invocation_started=datetime.now(timezone.utc).isoformat(),
    )
    try:
        for stage in STAGES:
            if (root / outputs[stage]).exists():
                status["completed"].append(stage)
                print("Reuse stage:", stage, flush=True)
                continue
            remaining = args.max_minutes * 60 - (time.monotonic() - start)
            if remaining <= 0:
                raise subprocess.TimeoutExpired(stage, args.max_minutes * 60)
            status["active"] = stage
            write_json(root / "status.json", status)
            log_path = root / f"{stage}.log"
            print(f"Running {stage}; log: {log_path}", flush=True)
            with log_path.open("a") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-u",
                        str(Path(__file__).resolve()),
                        "--output",
                        str(root),
                        "--stage",
                        stage,
                    ],
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
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    stop_group(process)
                    raise
            if code:
                stop_group(process)
                raise RuntimeError(f"{stage} exited {code}; see {log_path}")
            if stage in ("replay", "report"):
                write_json(root / outputs[stage], dict(complete=True))
            status["completed"].append(stage)
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
        package_report(root)
        lock.close()
    print(json.dumps(status, indent=2), flush=True)
    if status["state"] != "complete":
        print(
            f'Resume: python "{Path(__file__).resolve()}" --resume "{root}"', flush=True
        )
    else:
        print("Upload:", root / "analysis.zip", flush=True)
    return 0 if status["state"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
