"""Finite policy enumeration, with shared dispatch rules for model and replay.

This is a warm-pool, independent-trial experiment, not a general DAG optimizer.
"""

import itertools
import math


def plans(tasks, machines):
    names = tuple(machines)
    choices = [(m,) for m in names] + list(itertools.permutations(names, 2))
    for assignment in itertools.product(choices, repeat=len(tasks)):
        for order in itertools.permutations(tasks):
            yield {"copies": dict(zip(tasks, assignment)), "order": order}


def plan_key(plan):
    return tuple(plan["order"]), tuple(
        (t, tuple(ms)) for t, ms in sorted(plan["copies"].items())
    )


class Schedule:
    def __init__(self, plan, machines):
        self.machines = machines
        self.done = set()
        self.dead = set()
        self.active = {}
        self.pending = [
            (t, plan["copies"][t][copy])
            for copy in range(2)
            for t in plan["order"]
            if len(plan["copies"][t]) > copy
        ]
        self.order = plan["order"]

    def dispatch(self):
        # Original executions have priority over backups. A busy but surviving
        # assigned machine is waited for; a dead assignment can be relocated.
        launches = []
        for task, assigned in list(self.pending):
            if task in self.done:
                self.pending.remove((task, assigned))
                continue
            free = set(self.machines) - self.dead - set(self.active)
            if assigned in self.dead:
                assigned_live = sorted(
                    free, key=lambda m: (self.machines[m]["cost_per_hour"], m)
                )
                machine = assigned_live[0] if assigned_live else None
            else:
                machine = assigned if assigned in free else None
            if machine is not None:
                self.pending.remove((task, assigned))
                self.active[machine] = task
                launches.append((task, machine))
        return launches

    def fail(self, machine):
        self.dead.add(machine)
        task = self.active.pop(machine, None)
        if task is not None and task not in self.done:
            if task not in self.active.values() and not any(
                t == task for t, _ in self.pending
            ):
                self.pending.append((task, machine))

    def finish(self, task):
        self.done.add(task)
        cancelled = [m for m, t in self.active.items() if t == task]
        for machine in cancelled:
            del self.active[machine]
        self.pending = [(t, m) for t, m in self.pending if t != task]
        return cancelled


def lease_cost(machines, starts, failures, elapsed):
    # Once first used, a machine is billed until workflow completion or loss,
    # including idle periods. Costs are assumptions, never actual cloud invoices.
    return sum(
        max(0, min(elapsed, failures.get(m, elapsed)) - start)
        * machines[m]["cost_per_hour"]
        / 3600
        for m, start in starts.items()
    )


def simulate(plan, tasks, machines, profile, failures, horizon):
    schedule = Schedule(plan, machines)
    checkpoints = dict.fromkeys(tasks, 0)
    active = {}
    starts = {}
    events = sorted((float(t), m) for m, t in failures.items())
    now = 0.0
    attempts = 0
    while now < horizon and len(schedule.done) < len(tasks):
        # Loss wins a tie with a checkpoint/result: conservative failure model.
        while events and events[0][0] <= now:
            _, machine = events.pop(0)
            active.pop(machine, None)
            schedule.fail(machine)
        for task, machine in schedule.dispatch():
            starts.setdefault(machine, now)
            attempts += 1
            p = profile[task][machine]
            step = checkpoints[task]
            end = min(step + tasks[task]["checkpoint_every"], tasks[task]["epochs"])
            duration = p["init_s"] + (end - step) * p["epoch_s"] + p["save_s"]
            active[machine] = (now + duration, task, end)
        if not active:
            now = horizon
            break
        next_time = min(v[0] for v in active.values())
        if events and events[0][0] <= next_time:
            now = min(events[0][0], horizon)
            continue
        now = min(next_time, horizon)
        if next_time > horizon:
            break
        for machine, (when, task, step) in list(active.items()):
            if when > now or machine not in active:
                continue
            checkpoints[task] = max(checkpoints[task], step)
            if step == tasks[task]["epochs"]:
                for cancelled in schedule.finish(task):
                    active.pop(cancelled, None)
            else:
                end = min(step + tasks[task]["checkpoint_every"], tasks[task]["epochs"])
                p = profile[task][machine]
                active[machine] = (
                    now + (end - step) * p["epoch_s"] + p["save_s"],
                    task,
                    end,
                )
    return {
        "complete": len(schedule.done) == len(tasks),
        "elapsed_s": now,
        "cost": lease_cost(machines, starts, failures, now),
        "attempts": attempts,
    }


def evaluate(plan, tasks, machines, profile, scenarios, deadline, horizon):
    rows = [
        simulate(plan, tasks, machines, profile, s["failures"], horizon)
        for s in scenarios
    ]
    return {
        "plan": plan,
        "success_probability": sum(
            s["probability"]
            for s, row in zip(scenarios, rows)
            if row["complete"] and row["elapsed_s"] <= deadline
        ),
        "expected_cost": sum(
            s["probability"] * r["cost"] for s, r in zip(scenarios, rows)
        ),
        "expected_elapsed_s": sum(
            s["probability"] * r["elapsed_s"] for s, r in zip(scenarios, rows)
        ),
        "scenarios": rows,
    }


def choose(rows, target):
    feasible = [r for r in rows if r["success_probability"] + 1e-12 >= target]
    if not feasible:
        return None
    return min(
        feasible,
        key=lambda r: (
            r["expected_cost"],
            r["expected_elapsed_s"],
            plan_key(r["plan"]),
        ),
    )


def search(tasks, machines, profile, scenarios, deadline, target=0.95):
    if not 0 < target <= 1 or deadline <= 0:
        raise ValueError("Invalid target or deadline")
    if (
        not scenarios
        or any(s["probability"] < 0 for s in scenarios)
        or not math.isclose(sum(s["probability"] for s in scenarios), 1.0, abs_tol=1e-9)
    ):
        raise ValueError("Scenario probabilities must be nonnegative and sum to one")
    if any(
        m not in machines or t < 0 for s in scenarios for m, t in s["failures"].items()
    ):
        raise ValueError("Failure times/machine names are invalid")
    if not tasks or len(machines) < 2:
        raise ValueError("Need at least one task and two machines")
    count = len(machines) ** (2 * len(tasks)) * math.factorial(len(tasks))
    if count > 10000:
        raise ValueError(f"{count} candidates exceeds the small-study limit (10000)")
    if any(t["epochs"] <= 0 or t["checkpoint_every"] <= 0 for t in tasks.values()):
        raise ValueError("Epoch and checkpoint counts must be positive")
    if any(
        not math.isfinite(m["cost_per_hour"]) or m["cost_per_hour"] <= 0
        for m in machines.values()
    ):
        raise ValueError("Machine costs must be finite and positive")
    horizon = max(
        deadline * 10,
        max((t for s in scenarios for t in s["failures"].values()), default=0)
        + deadline * 10,
    )
    all_rows = [
        evaluate(p, tasks, machines, profile, scenarios, deadline, horizon)
        for p in plans(tasks, machines)
    ]
    single = [
        r for r in all_rows if all(len(ms) == 1 for ms in r["plan"]["copies"].values())
    ]
    double = [
        r for r in all_rows if all(len(ms) == 2 for ms in r["plan"]["copies"].values())
    ]
    # Strong sequential baseline: optimize healthy placement/order first, then
    # optimize backup additions with the same failure model/objective as joint.
    healthy = [
        {**r, "healthy": simulate(r["plan"], tasks, machines, profile, {}, horizon)}
        for r in single
    ]
    eligible = [r for r in healthy if r["healthy"]["elapsed_s"] <= deadline] or healthy
    fixed = min(
        eligible, key=lambda r: (r["healthy"]["cost"], r["healthy"]["elapsed_s"])
    )["plan"]
    sequential = [
        r
        for r in all_rows
        if tuple(r["plan"]["order"]) == tuple(fixed["order"])
        and all(r["plan"]["copies"][t][0] == fixed["copies"][t][0] for t in tasks)
    ]
    # Simple risk heuristic: protect longest tasks on machines targeted by the
    # supplied scenarios, taking prefixes of this ranking (including no backup).
    exposure = {
        m: sum(s["probability"] for s in scenarios if m in s["failures"])
        for m in machines
    }
    ranked = sorted(
        tasks,
        key=lambda t: -(
            exposure[fixed["copies"][t][0]]
            * tasks[t]["epochs"]
            * profile[t][fixed["copies"][t][0]]["epoch_s"]
        ),
    )
    greedy_keys = set()
    for count in range(len(tasks) + 1):
        copy = {t: tuple(ms) for t, ms in fixed["copies"].items()}
        for t in ranked[:count]:
            others = [m for m in machines if m != copy[t][0]]
            backup = min(
                others,
                key=lambda m: (
                    machines[m]["cost_per_hour"] * profile[t][m]["epoch_s"],
                    m,
                ),
            )
            copy[t] += (backup,)
        greedy_keys.add(plan_key({"copies": copy, "order": fixed["order"]}))
    families = {
        "checkpoint_retry": single,
        "uniform_replication": double,
        "placement_first": sequential,
        "greedy_replication": [
            r for r in all_rows if plan_key(r["plan"]) in greedy_keys
        ],
        "joint_enumeration": all_rows,
    }
    return {
        "candidate_count": len(all_rows),
        "deadline_s": deadline,
        "target": target,
        "scenarios": scenarios,
        "machines": machines,
        "horizon_s": horizon,
        "policies": {name: choose(rows, target) for name, rows in families.items()},
        "maximum_success_probability": {
            name: max(r["success_probability"] for r in rows)
            for name, rows in families.items()
        },
    }
