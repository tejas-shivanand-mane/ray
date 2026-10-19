"""Measured execution: independent fault clock, cancellation fencing, post-run validation."""

import os
import signal
import threading
import time
from pathlib import Path

from planner import Schedule, lease_cost, simulate
from workload import Workers, validate_state, write_json


class FaultInjector:
    """Worker-process failures only; this thread lives on the surviving controller."""

    def __init__(self, workers, failures, begin, schedule):
        self.workers, self.failures, self.begin = workers, failures, begin
        self.schedule = schedule
        self.records = {}
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        try:
            for machine, requested in sorted(self.failures.items(), key=lambda p: p[1]):
                if self.stop.wait(max(0, self.begin + requested - time.monotonic())):
                    break
                pid = self.workers.processes[machine].pid
                active_task = self.schedule.active.get(machine)
                before = time.monotonic()
                os.kill(pid, signal.SIGKILL)
                after = time.monotonic()
                with self.lock:
                    self.records[machine] = dict(
                        requested_s=requested,
                        observed_s=after - self.begin,
                        signal_call_s=after - before,
                        pid=pid,
                        active_task=active_task,
                    )
        except Exception as exc:
            self.error = exc

    def snapshot(self):
        if self.error:
            raise RuntimeError("Fault injector failed") from self.error
        with self.lock:
            return dict(self.records)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("Fault injector did not stop")


def replay_once(measured, case, plan, scenario, output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    tasks, machines = measured["tasks"], case["machines"]
    workers = Workers(machines, measured["data_path"])
    schedule = Schedule(plan, machines, tasks)
    checkpoints, committed, starts, tokens = {}, {}, {}, {}
    blocked, cancellations, restorations, events, evidence = {}, [], [], [], {}
    work_started, busy_times = {}, []
    attempts = 0
    complete_at = None
    begin = time.monotonic()
    injector = FaultInjector(workers, scenario["failures"], begin, schedule)

    def log(event):
        events.append({"elapsed_s": time.monotonic() - begin, **event})

    def collect_faults():
        for machine, record in injector.snapshot().items():
            if machine in schedule.dead:
                continue
            workers.processes[machine].join(timeout=0)
            evidence[machine] = dict(record)
            if machine in work_started:
                busy_times.append(
                    max(0, record["observed_s"] - work_started.pop(machine))
                )
            log(dict(kind="process_killed", machine=machine, **evidence[machine]))
            schedule.fail(machine)
            schedule.blocked.discard(machine)
            blocked.pop(machine, None)
            tokens.pop(machine, None)

    try:
        while time.monotonic() - begin <= case["horizon_s"]:
            collect_faults()
            for name, machine in schedule.dispatch():
                attempts += 1
                starts.setdefault(machine, time.monotonic() - begin)
                work_started[machine] = time.monotonic() - begin
                checkpoint = checkpoints.get(name, {}).get("path")
                parent_names = tasks[name].get("depends_on", [])
                parents = [committed[parent]["path"] for parent in parent_names]
                try:
                    tokens[machine] = workers.submit(
                        machine,
                        name,
                        tasks[name],
                        checkpoint,
                        output / f"attempt-{attempts}",
                        parents,
                    )
                except (BrokenPipeError, EOFError):
                    # The injector can signal between dispatch and pipe.send.
                    if machine not in scenario["failures"]:
                        raise
                    continue
                log(
                    dict(
                        kind="dispatch",
                        machine=machine,
                        task=name,
                        checkpoint=checkpoint,
                        parents=parent_names,
                        generation=tokens[machine],
                    )
                )
            for machine in machines:
                if machine in schedule.dead:
                    continue
                connection = workers.connections[machine]
                try:
                    while connection.poll():
                        event = connection.recv()
                        if event["kind"] == "error":
                            raise RuntimeError(event["traceback"])
                        if event["kind"] == "idle":
                            waiting = blocked.get(machine)
                            if waiting and waiting["generation"] == event["generation"]:
                                delay = (
                                    time.monotonic() - begin - waiting["requested_s"]
                                )
                                if machine in work_started:
                                    busy_times.append(
                                        time.monotonic()
                                        - begin
                                        - work_started.pop(machine)
                                    )
                                cancellations.append(
                                    dict(
                                        machine=machine,
                                        acknowledged_s=time.monotonic() - begin,
                                        latency_s=delay,
                                        **waiting,
                                    )
                                )
                                blocked.pop(machine)
                                schedule.blocked.discard(machine)
                            continue
                        if tokens.get(machine) != event.get("generation"):
                            continue
                        name = event["task"]
                        log({**event, "machine": machine})
                        if event["kind"] == "start":
                            restorations.append({**event, "machine": machine})
                        if event["kind"] == "checkpoint":
                            if event["epoch"] > checkpoints.get(name, {}).get(
                                "epoch", 0
                            ):
                                checkpoints[name] = event
                        if event["kind"] == "result":
                            if name in committed:
                                raise AssertionError("Duplicate commit")
                            # Crash-only model: accept result, verify every committed
                            # state after timing. Invalid states invalidate the run.
                            committed[name] = event
                            for cancelled in schedule.finish(name):
                                blocked[cancelled] = dict(
                                    generation=tokens[cancelled],
                                    requested_s=time.monotonic() - begin,
                                    task=name,
                                    loser=cancelled != machine,
                                )
                                schedule.blocked.add(cancelled)
                                workers.cancel(cancelled)
                                tokens.pop(cancelled, None)
                            log(dict(kind="commit", task=name))
                            if len(committed) == len(tasks):
                                complete_at = time.monotonic() - begin
                except (EOFError, BrokenPipeError):
                    # Wait for the independent injector's evidence publication.
                    if (
                        machine not in scenario["failures"]
                        or time.monotonic() - begin < scenario["failures"][machine]
                    ):
                        raise RuntimeError(f"Uninjected worker exit: {machine}")
                if (
                    not workers.processes[machine].is_alive()
                    and machine not in scenario["failures"]
                ):
                    raise RuntimeError(f"Uninjected worker exit: {machine}")
            if complete_at is not None and not blocked:
                break
            if not schedule.active and not schedule.pending and not blocked:
                break
            time.sleep(0.001)
        settled = time.monotonic() - begin
        injector.close()
        collect_faults()
        for machine, record in evidence.items():
            workers.processes[machine].join(timeout=2)
            record["exitcode"] = workers.processes[machine].exitcode
            if record["exitcode"] != -signal.SIGKILL:
                raise AssertionError("Injected process did not exit with SIGKILL")
        elapsed = complete_at if complete_at is not None else settled
        # No tensor validation, file logging, or plotting in the timed loop.
        write_json(output / "events.json", events)
        validation_start = time.monotonic()
        errors = {
            name: validate_state(event["path"], measured["references"][name])
            for name, event in committed.items()
        }
        complete = len(committed) == len(tasks) and not blocked
        selected = (
            max(sorted(committed), key=lambda t: committed[t]["accuracy"])
            if complete
            else None
        )
        reference_accuracy = {
            t: next(iter(measured["samples"][t].values()))[0]["accuracy"] for t in tasks
        }
        if complete and selected != max(
            sorted(tasks), key=lambda t: reference_accuracy[t]
        ):
            raise AssertionError("Trial selection changed")
        final_tasks = measured["config"].get("final_tasks", list(tasks))
        final_accuracy = {
            t: committed[t]["accuracy"] for t in final_tasks if t in committed
        }
        result = dict(
            schema_version=2,
            kind="measured_local_process_replay",
            scenario=scenario,
            plan=plan,
            complete=complete,
            correct=complete,
            met_deadline=complete and elapsed <= case["deadline_s"],
            elapsed_s=elapsed,
            settled_s=settled,
            validation_s=time.monotonic() - validation_start,
            predicted=simulate(
                plan,
                tasks,
                machines,
                measured["estimates"],
                scenario["failures"],
                case["horizon_s"],
            ),
            assumed_cost=lease_cost(
                machines,
                starts,
                {m: e["observed_s"] for m, e in evidence.items()},
                settled,
            ),
            deadline_s=case["deadline_s"],
            injections={m: e["observed_s"] for m, e in evidence.items()},
            fault_evidence=evidence,
            fault_coverage_complete=set(evidence) == set(scenario["failures"]),
            worker_busy_s=sum(busy_times)
            + sum(settled - t for t in work_started.values()),
            attempts=attempts,
            max_abs_error=errors,
            selected_trial=selected,
            final_accuracy=final_accuracy,
            restorations=restorations,
            cancellations=cancellations,
            workers=workers.ready,
        )
        write_json(output / "result.json", result)
        return result
    finally:
        injector.close()
        workers.close()
