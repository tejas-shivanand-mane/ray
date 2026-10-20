"""Sensitivity simulator: physical loss precedes detection and retry.

This is an offline model, not a Ray benchmark. With zero delay and no added
startup, it must match the unmodified planner simulator.
"""
import math
import random

from reference import planner


def simulate(
    plan,
    tasks,
    machines,
    profile,
    failures,
    horizon,
    detection_s=0.0,
    epoch_scale=1.0,
    dispatch_s=0.0,
):
    tasks = planner.execution_tasks(tasks, plan)
    schedule = planner.Schedule(plan, machines, tasks)
    checkpoints = dict.fromkeys(tasks, 0)
    active, starts = {}, {}
    lost = set()
    # Physical failure before detection before completion at equal times.
    events = sorted(
        [(float(t), 0, m) for m, t in failures.items()]
        + [(float(t) + detection_s, 1, m) for m, t in failures.items()]
    )
    now, attempts, busy = 0.0, 0, 0.0
    while now < horizon and len(schedule.done) < len(tasks):
        while events and events[0][0] <= now:
            _, kind, machine = events.pop(0)
            if kind == 0:
                lost.add(machine)
                active.pop(machine, None)
            else:
                schedule.fail(machine)
        for task, machine in schedule.dispatch():
            starts.setdefault(machine, now)
            attempts += 1
            # Submission to an undetected dead worker waits until detection.
            if machine in lost:
                continue
            p = profile[task][machine]
            step = checkpoints[task]
            end = min(step + tasks[task]["checkpoint_every"], tasks[task]["epochs"])
            duration = (
                dispatch_s
                + p["init_s"]
                + (end - step) * p["epoch_s"] * epoch_scale
                + p["save_s"]
            )
            active[machine] = (now + duration, task, end)
        next_completion = min((v[0] for v in active.values()), default=math.inf)
        next_event = events[0][0] if events else math.inf
        next_time = min(next_completion, next_event, horizon)
        # Matches measured occupied-worker time, including detection wait.
        busy += len(schedule.active) * (next_time - now)
        now = next_time
        if next_event <= next_completion:
            continue
        if next_completion > horizon:
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
                    now + (end - step) * p["epoch_s"] * epoch_scale + p["save_s"],
                    task,
                    end,
                )
    return dict(
        complete=len(schedule.done) == len(tasks),
        elapsed_s=now,
        worker_busy_s=busy,
        attempts=attempts,
        cost=planner.lease_cost(machines, starts, failures, now),
    )


def evaluate(plan, tasks, machines, profile, scenarios, deadline, horizon, **kwargs):
    rows = [
        simulate(plan, tasks, machines, profile, s["failures"], horizon, **kwargs)
        for s in scenarios
    ]
    return dict(
        plan=plan,
        scenarios=rows,
        success_probability=sum(
            s["probability"]
            for s, r in zip(scenarios, rows)
            if r["complete"] and r["elapsed_s"] <= deadline
        ),
        expected_elapsed_s=sum(
            s["probability"] * r["elapsed_s"] for s, r in zip(scenarios, rows)
        ),
        expected_worker_busy_s=sum(
            s["probability"] * r["worker_busy_s"] for s, r in zip(scenarios, rows)
        ),
        expected_cost=sum(
            s["probability"] * r["cost"] for s, r in zip(scenarios, rows)
        ),
    )


def verify():
    rng = random.Random(18271)
    machines = {m: dict(cost_per_hour=1.0) for m in ["safe", "spot_a", "spot_b"]}
    tasks = {
        "a": dict(epochs=12, checkpoint_every=3),
        "b": dict(epochs=20, checkpoint_every=5),
        "c": dict(epochs=8, checkpoint_every=2, depends_on=["a", "b"]),
    }
    choices = {t: [v["checkpoint_every"], v["epochs"]] for t, v in tasks.items()}
    plans = list(planner.plans(tasks, machines, choices))
    for _ in range(200):
        plan = rng.choice(plans)
        profile = {
            t: {
                m: dict(
                    epoch_s=rng.uniform(0.2, 2), save_s=rng.uniform(0, 1), init_s=0.2
                )
                for m in machines
            }
            for t in tasks
        }
        failures = {
            m: rng.uniform(0, 30) for m in ["spot_a", "spot_b"] if rng.random() < 0.7
        }
        horizon = rng.choice([15, 1000])
        a = planner.simulate(plan, tasks, machines, profile, failures, horizon)
        b = simulate(plan, tasks, machines, profile, failures, horizon)
        assert a["complete"] == b["complete"] and a["attempts"] == b["attempts"], (a, b)
        for k in ["elapsed_s", "worker_busy_s", "cost"]:
            assert math.isclose(a[k], b[k], rel_tol=1e-10, abs_tol=1e-9), (k, a, b)
    # Loss at 2.5 preserves epoch 2, detection at 4.5, retry needs 2 more.
    t = {"a": dict(epochs=4, checkpoint_every=2)}
    p = {"a": {m: dict(epoch_s=1, save_s=0, init_s=0) for m in machines}}
    plan = dict(order=["a"], copies={"a": ["spot_a"]})
    r = simulate(plan, t, machines, p, {"spot_a": 2.5}, 100, detection_s=2)
    assert r["elapsed_s"] == 6.5 and r["worker_busy_s"] == 6.5, r
    # An already running healthy copy finishes without waiting for detection.
    plan["copies"]["a"] = ["spot_a", "safe"]
    r = simulate(plan, t, machines, p, {"spot_a": 2.5}, 100, detection_s=2)
    assert r["elapsed_s"] == 4, r
    print("PASS: 200 upstream-equivalence cases and two delayed-loss checks")


if __name__ == "__main__":
    verify()
