"""Evaluation-only observation; never enabled by recovery.enable() itself."""

import time
import uuid

import ray
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.operators.map_operator import MapOperator

NAMESPACE = "ray-recovery-launcher"
MONITOR_KEY = "fixed_r_launcher_monitor"
TRIGGER_KEY = "fixed_r_launcher_trigger"


def snapshot(operators):
    result = []
    for op in operators:
        metrics = op.metrics
        result.append({
            "name": op.name,
            "tasks_submitted": metrics.num_tasks_submitted,
            "tasks_finished": metrics.num_tasks_finished,
            "tasks_failed": metrics.num_tasks_failed,
            "output_rows": metrics.rows_task_outputs_generated,
            "output_blocks": metrics.num_task_outputs_generated,
            **{key: list(value) if isinstance(value, list) else value
               for key, value in metrics.extra_metrics.items() if key.startswith("fixed_r_")},
            "active_enrolled_tasks": [
                task.get_task_id().hex() for task in op.get_active_tasks()
                if getattr(task, "stream", None) is not None
                and task.stream.reader is not None and not task.stream.closed
            ],
        })
    return result


class Monitor:
    def __init__(self):
        self.executions = {}
        self.trigger = None

    def publish(self, key, observation):
        previous = self.executions.get(key)
        if previous and previous["observed_ns"] > observation["observed_ns"]:
            return
        self.executions[key] = observation
        if self.trigger is None and observation.get("trigger"):
            self.trigger = {"execution": key, **observation["trigger"]}

    def read(self):
        return {"executions": self.executions, "trigger": self.trigger}


class Observe(ExecutionCallback):
    _fixed_r_launcher_observer = True

    def before_execution_starts(self, executor):
        self.operators = [op for op in executor._topology if isinstance(op, MapOperator)]
        self.trigger = None
        monitor_name = executor._data_context.get_config(MONITOR_KEY)
        self.trigger_kind = executor._data_context.get_config(TRIGGER_KEY, "output")
        if not self.operators:
            return
        self.key = uuid.uuid4().hex
        self.monitor = ray.get_actor(monitor_name, namespace=NAMESPACE)
        # File listing emits manifests, not application rows. Observe the
        # first replayable application/read stage without requiring counts.
        targets = [i for i, op in enumerate(self.operators)
                   if op.name not in ("ListFiles", "Write")
                   and hasattr(op, "_streaming_recovery_survivor_only")]
        if targets:
            index = targets[0]
            op = self.operators[index]
            submit = op._submit_data_task

            def submit_and_observe(stream, *args, **kwargs):
                submit(stream, *args, **kwargs)
                task = next(task for task in op.get_active_tasks()
                            if getattr(task, "stream", None) is stream)
                if (self.trigger is None and self.trigger_kind == "task-submission"
                        and stream.reader is not None):
                    self.request_failure(index, snapshot(self.operators))
                emit = task._emit_copied_pair

                def emit_and_observe(pair):
                    size = emit(pair)
                    if self.trigger is None and self.trigger_kind == "output":
                        states = snapshot(self.operators)
                        target = states[index]
                        if target["output_blocks"] >= 2 and target["active_enrolled_tasks"]:
                            self.request_failure(index, states)
                    return size

                task._emit_copied_pair = emit_and_observe

            op._submit_data_task = submit_and_observe
        self.publish("running")

    def request_failure(self, index, states):
        self.trigger = {
            "stage": index, "operator": states[index]["name"],
            "kind": self.trigger_kind, "requested_ns": time.monotonic_ns(),
            "output_rows": states[index]["output_rows"],
            "executor_paused_for_failure": False,
        }
        self.publish("running", states)

    def publish(self, state, operators=None, error=None):
        if not getattr(self, "operators", None):
            return
        self.last_report = time.monotonic()
        # Await terminal reports so a normally exiting script cannot drop
        # its final observations before the supervisor receives them.
        ref = self.monitor.publish.remote(self.key, {
            "state": state, "observed_ns": time.monotonic_ns(),
            "application_job_id": ray.get_runtime_context().get_job_id(),
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
            "operators": operators if operators is not None else snapshot(self.operators),
            "trigger": self.trigger, "error": error,
        })
        if state != "running":
            ray.get(ref, timeout=5)

    def on_execution_step(self, executor):
        if getattr(self, "operators", None) and time.monotonic() - self.last_report >= 0.25:
            self.publish("running")

    def after_execution_succeeds(self, executor):
        self.publish("finished")

    def after_execution_fails(self, executor, error):
        self.publish("failed", error=str(error))


def validate(observation, require_replay):
    executions = observation["executions"]
    if not executions:
        raise ValueError("The script did not execute an observed Dataset pipeline")
    for execution in executions.values():
        if execution["state"] != "finished":
            raise ValueError("An observed Dataset pipeline did not finish")
        for op in execution["operators"]:
            if (op["tasks_submitted"] != op["tasks_finished"] or op["tasks_failed"]
                    or op["active_enrolled_tasks"] or op.get("fixed_r_recovery_errors")):
                raise ValueError(f"Incomplete or failed operator: {op}")
            if "fixed_r_closed_streams" in op and op["fixed_r_closed_streams"] != op["tasks_submitted"]:
                raise ValueError(f"Unclosed recovery streams: {op['name']}")
            if op["name"] == "Write" and (
                not op.get("fixed_r_survivor_only") or op.get("fixed_r_enrolled_tasks", 0)
                or op.get("fixed_r_recovered_tasks", 0)
            ):
                raise ValueError("External writes must not use replay")
    if require_replay:
        trigger = observation["trigger"]
        if not trigger:
            raise ValueError("The workload did not reach the requested protected task trigger")
        target = executions[trigger["execution"]]["operators"][trigger["stage"]]
        if target.get("fixed_r_recovered_tasks", 0) < 1:
            raise ValueError("No task replayed in the stage that triggered head failure")
