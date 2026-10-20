"""Resumable model-only comparison of joint, sequential and alternating planning.

Uses only the standard library and the existing deterministic planner. It never
starts training, Ray, cloud instances or worker processes. See JOINT_PLANNING.md.
"""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import fcntl
import hashlib
import itertools
import json
import math
from pathlib import Path
import sys
import time
from uuid import uuid4
import zipfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import planner  # noqa: E402


SCHEMA = 1
TASKS = ("t1", "t2", "t3")
NODES = ("safe", "spot_a", "spot_b")
DEADLINES = (1.2, 1.5, 2.0)
TARGETS = (0.90, 0.95)
METHODS = (
    "uniform_checkpoint",
    "risk_aware_sequential",
    "sequential_multistart",
    "alternating_multistart",
    "joint_checkpoint",
)
LIMITATIONS = [
    "Synthetic deterministic model, not measured training or cloud performance.",
    "Probabilities are assumed scenario weights, not estimated interruption rates.",
    "At most one loss per node; the correlated scenario loses both spot nodes.",
    "Failure times are absolute and shared by all policies, including idle-node losses.",
    "Controller, task inputs, and shared checkpoints survive every modeled failure.",
    "Each task has one execution; no redundant training, TorchFT, or backup selection.",
    "All methods use the same greedy relocation to the cheapest free surviving node.",
    "No provisioning delay, detection delay, network transfer cost or storage contention.",
    "Leases start on first use and last until node loss or whole-workflow completion.",
    "Final outputs are always saved; intermediate checkpoint intervals are selectable.",
    "Exactness is limited to the finite placement, dispatch-order and checkpoint menu.",
    "Held-out times test temporal sensitivity within this model, not real reliability.",
    "Grid cells are not independent repetitions or samples of production workloads.",
    "These are research controls, not implementations of Dyna, uHEFT or CKPTSOME.",
]


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def read(path):
    return json.loads(path.read_text())


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


def source_hashes():
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (Path(__file__), HERE.parent / "planner.py")
    }


def family_grid(suite):
    """Freeze the complete grid before any result is available."""
    axes = {
        "t1_units": [1.5, 2.0],
        "save_fraction": [0.005, 0.10],
        "spot_price": [0.25, 0.75],
        "failure_mass": [0.10, 0.50],
        "spot_to_safe_risk_ratio": [1.0, 20.0],
        "correlated_share": [0.0, 0.5],
    }
    if suite == "full":
        axes["t1_units"] = [1.0, 1.5, 2.0]
        axes["save_fraction"] = [0.0, 0.005, 0.025, 0.10]
    elif suite == "smoke":
        axes = {key: [values[-1]] for key, values in axes.items()}
    return [
        dict(zip(axes, values)) for values in itertools.product(*axes.values())
    ]


def make_problem(spec):
    # A unit is 60 modeled seconds, not a calibration or an actual training run.
    tasks = {
        t: dict(
            epochs=int(24 * spec["t1_units"]) if t == "t1" else 24,
            checkpoint_every=24,
            depends_on=["t2"] if t == "t3" else [],
        )
        for t in TASKS
    }
    machines = {
        m: dict(
            cost_per_hour=1.0 if m == "safe" else spec["spot_price"],
            accounting={"bill_warm_pool": False},
        )
        for m in NODES
    }
    estimates = {
        t: {
            m: dict(init_s=0.3, epoch_s=2.5, save_s=60 * spec["save_fraction"])
            for m in NODES
        }
        for t in TASKS
    }
    return tasks, machines, estimates


def plan_space(tasks):
    orders = [
        list(order)
        for order in itertools.permutations(TASKS)
        if order.index("t2") < order.index("t3")
    ]
    schedules = [
        dict(copies={t: [m] for t, m in zip(TASKS, nodes)}, order=order)
        for nodes, order in itertools.product(
            itertools.product(NODES, repeat=3), orders
        )
    ]
    # Uniform includes shared work fractions AND shared epoch intervals, with
    # final-only as an explicit option. Selective uses their Cartesian closure.
    uniform = [{t: tasks[t]["epochs"] for t in TASKS}]
    shortest = min(task["epochs"] for task in tasks.values())
    for divisor in (2, 4, 8):
        uniform.append({t: max(1, shortest // divisor) for t in TASKS})
        uniform.append({t: max(1, tasks[t]["epochs"] // divisor) for t in TASKS})
    uniform = list({digest(c): c for c in uniform}.values())
    choices = {t: sorted({c[t] for c in uniform}) for t in TASKS}
    checkpoints = [
        dict(zip(TASKS, values))
        for values in itertools.product(*(choices[t] for t in TASKS))
    ]
    keys = {digest(c) for c in uniform}
    uniform_ids = [i for i, c in enumerate(checkpoints) if digest(c) in keys]
    return schedules, checkpoints, uniform_ids


def scenario_set(spec, reference, fractions):
    mass = spec["failure_mass"]
    correlated = mass * spec["correlated_share"]
    single = mass - correlated
    ratio = spec["spot_to_safe_risk_ratio"]
    safe = single / (1 + 2 * ratio)
    rows = [dict(name="healthy", probability=1 - mass, failures={})]
    for name, nodes, weight in (
        ("safe", ("safe",), safe),
        ("spot_a", ("spot_a",), safe * ratio),
        ("spot_b", ("spot_b",), safe * ratio),
        ("correlated", ("spot_a", "spot_b"), correlated),
    ):
        if weight == 0:
            continue
        for fraction in fractions:
            rows.append(
                dict(
                    name=f"{name}-{fraction:g}",
                    probability=weight / len(fractions),
                    failures=dict.fromkeys(nodes, reference * fraction),
                )
            )
    if not math.isclose(sum(s["probability"] for s in rows), 1, abs_tol=1e-12):
        raise ValueError("Scenario weights must sum to one")
    return rows


def evaluate_plan(plan, problem, scenarios, deadlines, horizon, details=False):
    outcomes = [
        planner.simulate(plan, *problem, s["failures"], horizon) for s in scenarios
    ]
    metrics = {
        "expected_cost": sum(
            s["probability"] * r["cost"] for s, r in zip(scenarios, outcomes)
        ),
        "expected_elapsed_s": sum(
            s["probability"] * r["elapsed_s"] for s, r in zip(scenarios, outcomes)
        ),
        "expected_worker_busy_s": sum(
            s["probability"] * r["worker_busy_s"]
            for s, r in zip(scenarios, outcomes)
        ),
        "success_by_deadline": [
            sum(
                s["probability"]
                for s, r in zip(scenarios, outcomes)
                if r["complete"] and r["elapsed_s"] <= deadline
            )
            for deadline in deadlines
        ],
        "completion_probability": sum(
            s["probability"] for s, r in zip(scenarios, outcomes) if r["complete"]
        ),
    }
    if details:
        metrics["outcomes"] = outcomes
    return metrics


def feasible(row, deadline_index, target):
    return row["success_by_deadline"][deadline_index] + 1e-12 >= target


def score(row, deadline_index, target):
    """One total order for all methods, with no reward for infeasible cheapness."""
    ok = feasible(row, deadline_index, target)
    return (
        0 if ok else 1,
        0 if ok else -round(row["success_by_deadline"][deadline_index], 12),
        round(row["expected_cost"], 12),
        round(row["expected_elapsed_s"], 9),
        round(row["expected_worker_busy_s"], 9),
        row["schedule_id"],
        row["checkpoint_id"],
    )


def solve_methods(rows, uniform_ids, deadline_index, target):
    """Block coordinate descent sees only queried rows, never the joint winner.

    Precomputed rows share expensive simulation work, but every method tracks
    its own unique candidate queries. This is not a runtime speed comparison.
    """
    by_schedule, by_checkpoint = {}, {}
    for i, row in enumerate(rows):
        by_schedule.setdefault(row["schedule_id"], []).append(i)
        by_checkpoint.setdefault(row["checkpoint_id"], []).append(i)
    keys = [score(row, deadline_index, target) for row in rows]
    seed_queries = set()

    def best(indices, queries):
        queries.update(indices)
        return min(indices, key=keys.__getitem__)

    # Optimize EVERY uniform rule, including final-only. Add reliability-first
    # and time-first seeds from the same queried rows to avoid making all starts
    # collapse onto one cost-optimal schedule. None uses the joint winner.
    seeds = []
    for checkpoint_id in uniform_ids:
        indices = by_checkpoint[checkpoint_id]
        seeds.append(best(indices, seed_queries))
        seeds.append(min(indices, key=lambda i: (
            -round(rows[i]["success_by_deadline"][deadline_index], 12), *keys[i]
        )))
        seeds.append(min(indices, key=lambda i: (
            round(rows[i]["expected_elapsed_s"], 9), *keys[i]
        )))
    seeds = list(dict.fromkeys(seeds))
    uniform_best = min(seeds, key=keys.__getitem__)
    result = {}

    def record(name, index, queries, **extra):
        result[name] = dict(
            row_index=index,
            candidate_queries=len(queries),
            **extra,
        )

    record("uniform_checkpoint", uniform_best, seed_queries)
    queries = set(seed_queries)
    sequential = best(by_schedule[rows[uniform_best]["schedule_id"]], queries)
    record("risk_aware_sequential", sequential, queries, seed_row=uniform_best)

    queries = set(seed_queries)
    first_passes = [
        best(by_schedule[rows[seed]["schedule_id"]], queries) for seed in seeds
    ]
    multi = min(first_passes, key=keys.__getitem__)
    record("sequential_multistart", multi, queries, seed_rows=seeds)

    queries = set(seed_queries)
    traces, terminals = [], []
    for seed in seeds:
        current = seed
        trace = [current]
        # Total-order strict descent on a finite space terminates without an
        # arbitrary iteration cap. Both blocks include the incumbent.
        while True:
            previous = current
            current = best(by_schedule[rows[current]["schedule_id"]], queries)
            if current != trace[-1]:
                trace.append(current)
            current = best(by_checkpoint[rows[current]["checkpoint_id"]], queries)
            if current != trace[-1]:
                trace.append(current)
            if current == previous:
                break
            if keys[current] >= keys[previous]:
                raise RuntimeError("Alternating search did not strictly descend")
        terminals.append(current)
        traces.append(trace)
    record(
        "alternating_multistart",
        min(terminals, key=keys.__getitem__),
        queries,
        seed_rows=seeds,
        trajectories=traces,
    )
    record(
        "joint_checkpoint",
        min(range(len(rows)), key=keys.__getitem__),
        set(range(len(rows))),
    )
    return result


def comparison(joint, baseline, index, target):
    j_ok, b_ok = feasible(joint, index, target), feasible(baseline, index, target)
    gain = None
    if j_ok and b_ok:
        gain = 1 - joint["expected_cost"] / baseline["expected_cost"]
        if math.isclose(
            joint["expected_cost"],
            baseline["expected_cost"],
            rel_tol=1e-9,
            abs_tol=1e-12,
        ):
            verdict = "cost_tie"
        else:
            verdict = "joint_cheaper" if gain > 0 else "baseline_cheaper"
    elif j_ok:
        verdict = "only_joint_feasible"
    elif b_ok:
        verdict = "only_baseline_feasible"
    else:
        verdict = "neither_feasible"
    return dict(
        verdict=verdict,
        joint_feasible=j_ok,
        baseline_feasible=b_ok,
        cost_gain_fraction=gain,
        success_difference=(
            joint["success_by_deadline"][index]
            - baseline["success_by_deadline"][index]
        ),
    )


class BudgetExpired(Exception):
    pass


def check_budget(end):
    if time.monotonic() >= end:
        raise BudgetExpired()


def enumerate_family(root, family_id, spec, manifest, end, progress):
    problem = make_problem(spec)
    schedules, checkpoints, uniform_ids = plan_space(problem[0])
    final = {t: problem[0][t]["epochs"] for t in TASKS}
    reference = min(
        planner.simulate(
            dict(**schedule, checkpoint_every=final), *problem, {}, 10000
        )["elapsed_s"]
        for schedule in schedules
    )
    deadlines = [reference * m for m in manifest["deadline_multipliers"]]
    horizon = 8 * reference
    scenarios = scenario_set(spec, reference, manifest["planning_fractions"])
    total = len(schedules) * len(checkpoints)
    cache_path = root / "cache" / f"{family_id}.json"
    identity = digest(dict(manifest=manifest["run_id"], spec=spec))
    cache = (
        read(cache_path) if cache_path.exists() else dict(identity=identity, rows=[])
    )
    if cache["identity"] != identity or len(cache["rows"]) > total:
        raise ValueError(f"Incompatible cache: {cache_path}")
    rows = cache["rows"]
    for i, row in enumerate(rows):
        if (row["schedule_id"], row["checkpoint_id"]) != divmod(
            i, len(checkpoints)
        ):
            raise ValueError(f"Noncontiguous cache: {cache_path}")
    try:
        for i in range(len(rows), total):
            check_budget(end)
            schedule_id, checkpoint_id = divmod(i, len(checkpoints))
            plan = dict(
                **schedules[schedule_id], checkpoint_every=checkpoints[checkpoint_id]
            )
            metrics = evaluate_plan(plan, problem, scenarios, deadlines, horizon)
            rows.append(
                dict(schedule_id=schedule_id, checkpoint_id=checkpoint_id, **metrics)
            )
            if len(rows) % 128 == 0:
                atomic_json(cache_path, cache)
                progress.update(
                    active_family=family_id, plans_done=len(rows), plans_total=total
                )
                atomic_json(root / "status.json", progress)
    finally:
        # A timeout or Ctrl-C retains every fully evaluated candidate.
        atomic_json(cache_path, cache)
        progress.update(
            active_family=family_id, plans_done=len(rows), plans_total=total
        )
        atomic_json(root / "status.json", progress)
    return dict(
        problem=problem,
        schedules=schedules,
        checkpoints=checkpoints,
        uniform_ids=uniform_ids,
        reference_s=reference,
        deadlines_s=deadlines,
        horizon_s=horizon,
        scenarios=scenarios,
        rows=rows,
    )


def finish_family(root, family_id, spec, manifest, space, end):
    path = root / "families" / f"{family_id}.json"
    held_out = scenario_set(
        spec, space["reference_s"], manifest["held_out_fractions"]
    )
    result = read(path) if path.exists() else dict(
        family_id=family_id,
        spec=spec,
        model=dict(
            tasks=space["problem"][0],
            machines=space["problem"][1],
            estimates=space["problem"][2],
            reference_s=space["reference_s"],
            horizon_s=space["horizon_s"],
        ),
        plan_space=dict(
            schedules=space["schedules"],
            checkpoints=space["checkpoints"],
            uniform_checkpoint_ids=space["uniform_ids"],
        ),
        planning_scenarios=space["scenarios"],
        held_out_scenarios=held_out,
        total_candidates=len(space["rows"]),
        cases=[],
    )
    completed = {(c["deadline_index"], c["target"]) for c in result["cases"]}
    detailed = {}
    for index, target in itertools.product(
        range(len(space["deadlines_s"])), manifest["targets"]
    ):
        if (index, target) in completed:
            continue
        check_budget(end)
        selected = solve_methods(space["rows"], space["uniform_ids"], index, target)
        policies = {}
        for method, selection in selected.items():
            row_id = selection["row_index"]
            row = space["rows"][row_id]
            plan = dict(
                **space["schedules"][row["schedule_id"]],
                checkpoint_every=space["checkpoints"][row["checkpoint_id"]],
            )
            if row_id not in detailed:
                detailed[row_id] = {
                    phase: evaluate_plan(
                        plan,
                        space["problem"],
                        scenarios,
                        space["deadlines_s"],
                        space["horizon_s"],
                        details=True,
                    )
                    for phase, scenarios in (
                        ("planning", space["scenarios"]),
                        ("held_out", held_out),
                    )
                }
            trace_ids = {
                r for trace in selection.get("trajectories", []) for r in trace
            }
            policies[method] = dict(
                **selection,
                plan=plan,
                plan_id=digest(plan),
                trajectory_metrics={str(r): space["rows"][r] for r in sorted(trace_ids)},
                **detailed[row_id],
            )
        pairs = {}
        for method in METHODS[:-1]:
            pairs[method] = {
                phase: comparison(
                    policies["joint_checkpoint"][phase],
                    policies[method][phase],
                    index,
                    target,
                )
                for phase in ("planning", "held_out")
            }
            pairs[method]["identical_plan"] = (
                policies[method]["plan_id"] == policies["joint_checkpoint"]["plan_id"]
            )
        # A practical screening threshold, NOT statistical significance. The
        # finite optimum only bounds planning performance, never held-out results.
        strong = ("sequential_multistart", "alternating_multistart")
        candidate = all(
            pairs[method][phase]["joint_feasible"]
            and pairs[method][phase]["baseline_feasible"]
            and (pairs[method][phase]["cost_gain_fraction"] or 0) >= 0.05
            for method in strong
            for phase in ("planning", "held_out")
        )
        result["cases"].append(
            dict(
                case_id=f"{family_id}-d{index}-p{target:g}",
                deadline_index=index,
                deadline_multiplier=manifest["deadline_multipliers"][index],
                deadline_s=space["deadlines_s"][index],
                target=target,
                policies=policies,
                comparisons=pairs,
                practical_candidate=candidate,
            )
        )
        atomic_json(path, result)
    return result


def reports(root, manifest, status):
    flat, candidate_ids = [], []
    for path in sorted((root / "families").glob("*.json")):
        family = read(path)
        for case in family["cases"]:
            if case["practical_candidate"]:
                candidate_ids.append(case["case_id"])
            for method, pair in case["comparisons"].items():
                j = case["policies"]["joint_checkpoint"]
                b = case["policies"][method]
                row = dict(
                    case_id=case["case_id"], family_id=family["family_id"],
                    **family["spec"], deadline_multiplier=case["deadline_multiplier"],
                    target=case["target"], baseline=method,
                    identical_plan=pair["identical_plan"],
                    practical_candidate=case["practical_candidate"],
                    joint_queries=j["candidate_queries"], baseline_queries=b["candidate_queries"],
                )
                for phase in ("planning", "held_out"):
                    row.update({f"{phase}_{k}": v for k, v in pair[phase].items()})
                    for label, policy in (("joint", j), ("baseline", b)):
                        row[f"{phase}_{label}_cost"] = policy[phase]["expected_cost"]
                        row[f"{phase}_{label}_success"] = policy[phase]["success_by_deadline"][case["deadline_index"]]
                flat.append(row)
    completed = len({r["case_id"] for r in flat})
    expected = len(manifest["families"]) * len(manifest["targets"]) * len(manifest["deadline_multipliers"])
    counts = {
        phase: {
            method: dict(Counter(
                r[f"{phase}_verdict"] for r in flat if r["baseline"] == method
            ))
            for method in METHODS[:-1]
        }
        for phase in ("planning", "held_out")
    }
    strata = {}
    if manifest["families"]:
        for axis in manifest["families"][0]:
            strata[axis] = {
                str(value): dict(Counter(
                    r["held_out_verdict"] for r in flat
                    if r[axis] == value and r["baseline"] == "alternating_multistart"
                ))
                for value in sorted({s[axis] for s in manifest["families"]})
            }
    summary = dict(
        state=status["state"], completed_cases=completed, expected_cases=expected,
        counts=counts, held_out_vs_alternating_by_axis=strata,
        practical_candidate_ids=candidate_ids, limitations=LIMITATIONS,
        interpretation="All counts describe a predeclared synthetic grid. No statistical or production-reliability claims.",
    )
    atomic_json(root / "summary.json", summary)
    csv_path = root / "comparisons.csv"
    temporary = csv_path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="") as stream:
        if flat:
            writer = csv.DictWriter(stream, fieldnames=list(flat[0]))
            writer.writeheader()
            writer.writerows(flat)
    temporary.replace(csv_path)
    lines = [
        "# Joint checkpoint planning: offline screen", "",
        f"Status: {status['state']}. Completed cases: {completed}/{expected}.",
        "Synthetic model only; no training, cloud measurements or independent repetitions.", "",
        "Objective: minimum expected lease cost subject to the specified weighted deadline target.",
        "Held-out plans are frozen before evaluation; they are never reselected using held-out results.", "",
    ]
    for phase in ("planning", "held_out"):
        lines += [f"## {phase}", "", "| Baseline | Joint cheaper | Cost tie | Baseline cheaper | Only joint feasible | Only baseline feasible | Neither feasible |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for method, c in counts[phase].items():
            values = [c.get(k, 0) for k in (
                "joint_cheaper", "cost_tie", "baseline_cheaper", "only_joint_feasible",
                "only_baseline_feasible", "neither_feasible",
            )]
            lines.append(f"| {method} | " + " | ".join(map(str, values)) + " |")
        lines.append("")
    lines += [
        f"Cases with at least 5% cost savings over BOTH multistart baselines, with all plans meeting their targets in BOTH evaluations: {len(candidate_ids)}.",
        "This is a practical screening flag, not a significance test or novelty claim.", "",
        "See comparisons.csv for every win, tie, loss and feasibility difference; summary.json also groups results by grid axis.", "",
        "## Limits", "", *[f"- {item}" for item in LIMITATIONS], "",
    ]
    (root / "summary.md").write_text("\n".join(lines))
    temporary_zip = root / "analysis.zip.tmp"
    with zipfile.ZipFile(temporary_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ("manifest.json", "status.json", "summary.json", "summary.md", "comparisons.csv"):
            archive.write(root / name, name)
        for path in sorted((root / "families").glob("*.json")):
            archive.write(path, str(path.relative_to(root)))
    temporary_zip.replace(root / "analysis.zip")
    return completed, expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    location = parser.add_mutually_exclusive_group()
    location.add_argument("--output", type=Path)
    location.add_argument("--resume", type=Path)
    parser.add_argument("--suite", choices=("smoke", "screen", "full"))
    parser.add_argument("--max-minutes", type=float, default=28)
    args = parser.parse_args()
    if not math.isfinite(args.max_minutes) or not 0 < args.max_minutes <= 28:
        parser.error("--max-minutes must be finite, greater than zero and at most 28")
    if args.resume and args.suite is not None:
        parser.error("Resume uses the saved suite; only --max-minutes may change")
    if args.resume:
        root = args.resume.resolve()
        if not (root / "manifest.json").is_file():
            parser.error("Resume directory has no manifest.json")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        root = (args.output or Path("ray_gcp_results/joint-checkpoint-offline") / f"{stamp}-{uuid4().hex[:8]}").resolve()
        root.mkdir(parents=True, exist_ok=False)
    with (root / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error("Another invocation holds this run's lock")
        if args.resume:
            manifest = read(root / "manifest.json")
            if manifest["schema"] != SCHEMA or manifest["source_hashes"] != source_hashes():
                parser.error("Source/schema changed; use the original commit to resume or start a new run")
        else:
            suite = args.suite or "screen"
            manifest = dict(
                schema=SCHEMA, run_id=uuid4().hex, suite=suite,
                created_utc=datetime.now(timezone.utc).isoformat(),
                source_hashes=source_hashes(), families=family_grid(suite),
                deadline_multipliers=list(DEADLINES), targets=list(TARGETS),
                planning_fractions=[0.2, 0.5, 0.8, 1.1],
                held_out_fractions=[0.1, 0.35, 0.65, 0.95, 1.2],
                objective="expected_cost", limitations=LIMITATIONS,
            )
            atomic_json(root / "manifest.json", manifest)
        started = time.monotonic()
        end = started + args.max_minutes * 60
        status = dict(state="running", families_done=0, families_total=len(manifest["families"]))
        atomic_json(root / "status.json", status)
        print(f"Output: {root}", flush=True)
        print(f"Resume: python {Path(__file__).relative_to(HERE.parents[2])} --resume {root}", flush=True)
        exit_code = 0
        try:
            for number, spec in enumerate(manifest["families"]):
                family_id = f"family-{number:03d}"
                path = root / "families" / f"{family_id}.json"
                expected = len(manifest["targets"]) * len(manifest["deadline_multipliers"])
                if not path.exists() or len(read(path)["cases"]) != expected:
                    check_budget(end)
                    print(f"{family_id} ({number + 1}/{len(manifest['families'])}): planning", flush=True)
                    space = enumerate_family(root, family_id, spec, manifest, end, status)
                    finish_family(root, family_id, spec, manifest, space, end)
                status["families_done"] = number + 1
                atomic_json(root / "status.json", status)
                print(f"{family_id}: complete", flush=True)
            status["state"] = "complete"
        except BudgetExpired:
            status["state"] = "paused_budget"
            print("Budget reached. Completed candidates are saved; use the resume command.", flush=True)
        except KeyboardInterrupt:
            status["state"] = "interrupted"
            exit_code = 130
        except Exception as exc:
            status.update(state="failed", error=f"{type(exc).__name__}: {exc}")
            print(status["error"], file=sys.stderr, flush=True)
            exit_code = 1
        finally:
            status["invocation_compute_wall_s"] = time.monotonic() - started
            atomic_json(root / "status.json", status)
            completed, expected = reports(root, manifest, status)
            print(f"Cases complete: {completed}/{expected}. Status: {status['state']}", flush=True)
            print(f"Upload: {root / 'analysis.zip'}", flush=True)
        return exit_code


if __name__ == "__main__":
    sys.exit(main())
