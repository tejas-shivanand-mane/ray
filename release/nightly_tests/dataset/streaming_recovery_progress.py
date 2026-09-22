"""Shared nonblocking progress trigger for original Dataset recovery workloads."""

import math
import sys
import time
import traceback
from threading import Event, enumerate as enumerate_threads

from ray import cloudpickle


FRACTIONS = {"early": 0.1, "middle": 0.5, "late": 0.9}


def snapshot(control):
    result = []
    for index, op in enumerate(control.operators):
        active = []
        for task in op.get_active_tasks():
            stream = getattr(task, "stream", None)
            if stream is not None and stream.reader is not None and not stream.closed:
                active.append({
                    "task_index": task.task_index(), "task_id": task.get_task_id().hex(),
                    "accepted_returns": stream.next_index,
                    "declared_returns": stream.expected_returns,
                })
        metrics = op.metrics
        result.append({
            "stage_index": index, "name": op.name,
            "tasks_submitted": metrics.num_tasks_submitted,
            "tasks_finished": metrics.num_tasks_finished,
            "tasks_failed": metrics.num_tasks_failed,
            "output_blocks": metrics.num_task_outputs_generated,
            "output_rows": metrics.rows_task_outputs_generated,
            **{key: list(value) if isinstance(value, list) else value
               for key, value in metrics.extra_metrics.items()
               if key.startswith("fixed_r_")},
            "active_enrolled_tasks": active,
        })
    return result


def capture_wait_state(control):
    """Best-effort local observation before cancellation clears active tasks.

    Never poll a stream or call Ray/GCS APIs here. In particular, do not acquire
    consumer locks: recovery can hold one while waiting for a native operation.
    The concurrently running executor can advance during this observation.
    """
    now = time.monotonic()
    names = {thread.ident: thread.name for thread in enumerate_threads()}
    result = {
        "captured_before_shutdown": True,
        "atomic_snapshot": False,
        "thread_stacks": {
            f"{names.get(ident, 'unknown')}:{ident}": "".join(
                traceback.format_stack(frame, limit=40)
            )
            for ident, frame in sys._current_frames().items()
        },
        "operators": snapshot(control),
    }

    def ref_id(ref):
        return None if ref is None or ref.is_nil() else ref.hex()

    topology = getattr(control.executor, "_topology", None) or {}
    for op, record in zip(control.operators, result["operators"]):
        record.update(
            output_backpressured=getattr(op, "_in_task_output_backpressure", None),
            submission_backpressured=getattr(op, "_in_task_submission_backpressure", None),
            output_backpressure_policy=getattr(op, "_task_output_backpressure_policy", None),
            submission_backpressure_policy=getattr(op, "_task_submission_backpressure_policy", None),
        )
        state = topology.get(op)
        if state is not None:
            record["queued_input_blocks"] = state.total_enqueued_input_blocks()
            record["waiting_consumers"] = state.num_waiting_consumers
        tasks = []
        for task in op.get_active_tasks():
            stream = getattr(task, "stream", None)
            if stream is None:
                continue
            reader = stream.reader
            consumer = reader.consumer if reader is not None else None
            last_progress = getattr(task, "_recovery_last_progress_s", now)
            tasks.append({
                "task_id": stream.task_id.hex(), "task_index": task.task_index(),
                "stream_closed": stream.closed, "accepted_returns": stream.next_index,
                "wait_reason": getattr(task, "_recovery_wait_reason", None),
                "seconds_since_progress": now - last_progress,
                "pending_block_ref": ref_id(task._pending_block_ref),
                "pending_metadata_ref": ref_id(task._pending_meta_ref),
                "last_pair_ready_ids": list(getattr(task, "_recovery_pair_ready_ids", [])),
                "copied_return_indices": sorted(task._copied_return_indices),
                "consumer_phase": consumer._phase if consumer is not None else None,
                "consumer_next_index": consumer._next_index if consumer is not None else None,
                "retained_return_indices": sorted(consumer._retained) if consumer is not None else [],
                "pending_owner_read": ref_id(reader._pending_read) if reader is not None else None,
                "recovery_required": reader._recovery_required if reader is not None else False,
            })
        record["task_wait_states"] = tasks
    manager = getattr(control.executor, "_resource_manager", None)
    if manager is not None:
        result["cached_global_resource_usage"] = repr(getattr(manager, "_global_usage", None))
        result["cached_operator_resource_usage"] = {
            op.name: repr(usage) for op, usage in
            list(getattr(manager, "_op_usages", {}).items())
        }
    return result


class ProgressTrigger:
    def __init__(self, total_rows, phase, stage):
        if phase not in FRACTIONS or total_rows <= 0:
            raise ValueError("Use positive row count and early/middle/late progress")
        self.total_rows = total_rows
        self.phase = phase
        self.stage = stage
        self.threshold = math.ceil(total_rows * FRACTIONS[phase])
        self.ready = Event()
        self.observation = None

    def __reduce__(self):
        # Callback classes travel with DataContext to ordinary task workers.
        # Their local copies need no driver event or live task handles.
        return type(self), (self.total_rows, self.phase, self.stage)

    def observe(self, operators, *, event="asynchronous_output_progress"):
        """Called on the executor thread; signal and return without waiting."""
        if self.ready.is_set():
            return
        target = operators[self.stage]
        rows = target["output_rows"]
        if rows < self.threshold or rows >= self.total_rows or not target["active_enrolled_tasks"]:
            return
        self.observation = {
            "observation_at_trigger": operators,
            "head_failure_requested": True,
            "failure_trigger": event,
            "failure_progress_phase": self.phase,
            "failure_progress_stage": self.stage,
            "failure_progress_rows": rows,
            "failure_progress_threshold_rows": self.threshold,
            "failure_progress_total_rows": self.total_rows,
            "executor_paused_for_failure": False,
            "udf_gate_enabled": False,
        }
        self.ready.set()

    def inject(self, future, crash_head, diagnostics, timeout):
        deadline = time.monotonic() + timeout
        while not self.ready.wait(0.01):
            if future.done():
                future.result()
                raise ValueError("Failure not exercised: Dataset finished before the progress trigger")
            if time.monotonic() >= deadline:
                raise TimeoutError("Dataset did not reach the asynchronous failure trigger")
        if future.done():
            future.result()
            raise ValueError("Failure not exercised: Dataset finished before the head failure request")
        diagnostics.update(self.observation)
        started = time.monotonic()
        diagnostics.update(crash_head())
        return started

    def validate(self, operators):
        if not self.ready.is_set():
            raise ValueError("Asynchronous failure was not triggered")
        if not operators[self.stage]["fixed_r_recovered_task_details"]:
            raise ValueError("Failure not exercised: no protected task replayed in the target stage")


def register_for_task_serialization():
    # This file is a benchmark helper, not an installed ray.* module.
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
