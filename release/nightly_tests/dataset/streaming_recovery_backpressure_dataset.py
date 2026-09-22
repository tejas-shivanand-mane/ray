"""Original backpressure Dataset with unknown-count runtime recovery.

Only observation, validation and failure injection are added here. The shared
application builder retains its original UDFs, batching and output shaping.
"""

import argparse
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import numpy as np
import ray
from ray import cloudpickle
from ray.data import DataContext
from ray.data.block import BlockAccessor
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, get_config

from streaming_recovery_head_failure import local_head_failure_cluster
from streaming_recovery_progress import (
    FRACTIONS, ProgressTrigger, capture_wait_state, register_for_task_serialization, snapshot,
)


def run_dataset(args, crash_head, diagnostics, recovery_config=None):
    import backpressure_benchmark as original

    register_for_task_serialization()

    class Control:
        def __init__(self):
            self.executor = None
            self.operators = []
            self.enrolled = Event()
            self.resume = Event()
            self.selected_task_id = None
            self.validated_producer_rows = 0
            self.prefix_returns = None
            self.execution_error_traceback = None

        def __getstate__(self):
            return {"executor": None, "operators": []}

    control = Control()
    failure = args.recovery_mode == "fixed_r_head_failure"
    point = args.runtime_failure_point
    timeout = args.recovery_timeout_s
    timing = getattr(args, "recovery_head_timing", "paused")
    trigger = None
    if failure and timing != "paused":
        trigger = ProgressTrigger(
            args.num_input_blocks * args.output_batches_per_input_batch * args.output_batch_rows,
            timing, 0,
        )

    def pause(stream):
        if not failure or trigger is not None or control.enrolled.is_set():
            return
        if stream.reader is None:
            raise ValueError("Selected task was not protected before head failure")
        control.selected_task_id = stream.task_id.hex()
        control.prefix_returns = stream.next_index
        control.enrolled.set()
        if not control.resume.wait(timeout):
            raise TimeoutError("Head failure controller did not resume Dataset execution")

    class Capture(ExecutionCallback):
        def after_execution_fails(self, executor, error):
            control.execution_error_traceback = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )

        def before_execution_starts(self, executor):
            operators = [op for op in executor._topology if isinstance(op, MapOperator)]
            if len(operators) != 2 or control.executor is not None:
                raise ValueError("Expected one execution with the original two map stages")
            control.executor = executor
            control.operators = operators
            for index, op in enumerate(operators):
                original_submit = op._submit_data_task

                def submit(stream, *positional, _index=index, _op=op,
                           _submit=original_submit, **kwargs):
                    _submit(stream, *positional, **kwargs)
                    task = next(task for task in _op.get_active_tasks()
                                if getattr(task, "stream", None) is stream)
                    if _index == 0:
                        emit = task._emit_copied_pair

                        def validate_and_emit(pair):
                            block = ray.get(pair[0], timeout=timeout)
                            batch = BlockAccessor.for_block(block).to_numpy()
                            values = batch["data"]
                            if (set(batch) != {"data"} or values.dtype != np.uint8
                                    or values.ndim != 2 or values.shape[1] != args.output_row_bytes
                                    or np.any(values)):
                                raise ValueError("Original producer payload changed")
                            control.validated_producer_rows += len(values)
                            size = emit(pair)
                            if trigger is not None and not trigger.ready.is_set():
                                trigger.observe(snapshot(control))
                            elif point == "producer_after_output":
                                pause(stream)
                            return size

                        task._emit_copied_pair = validate_and_emit
                    if ((_index == 0 and point == "producer_before_output") or
                            (_index == 1 and point == "consumer")):
                        pause(stream)

                op._submit_data_task = submit

    context = DataContext.get_current().copy()
    context.enable_fixed_r_task_recovery = True
    context.fixed_r_task_recovery_output_mode = "streaming"
    context.fixed_r_task_recovery_timeout_s = timeout
    context.enable_progress_bars = False
    if recovery_config is not None:
        # A multi-host controller can include the surviving driver's worker in
        # the executor pool without changing the application's Dataset code.
        context.set_config(CONFIG_KEY, recovery_config)
    config = get_config(context)
    if args.recovery_mode == "copy":
        context.set_config(CONFIG_KEY, replace(config, mode="copy"))
    context.custom_execution_callback_classes.append(Capture)
    registered = original.__name__ in cloudpickle.list_registry_pickle_by_value()
    if not registered:
        cloudpickle.register_pickle_by_value(original)

    def consume(dataset):
        rows = blocks = 0
        # Original consumption API. Inspect final blocks without materializing
        # the producer or retaining its payloads in the benchmark harness.
        for bundle in dataset.iter_internal_ref_bundles():
            for ref in bundle.block_refs:
                table = BlockAccessor.for_block(ray.get(ref, timeout=timeout)).to_arrow()
                if table.column_names != ["status"] or any(
                    value != "ok" for value in table["status"].to_pylist()
                ):
                    raise ValueError("Original consumer output changed")
                rows += table.num_rows
                blocks += 1
        return rows, blocks

    try:
        with DataContext.current(context):
            dataset = original.build_fast_producer_slow_consumer(args)
        with ThreadPoolExecutor(max_workers=1) as pool:
            consuming = pool.submit(consume, dataset)
            try:
                if trigger is not None:
                    started = trigger.inject(consuming, crash_head, diagnostics, timeout)
                elif failure:
                    deadline = time.monotonic() + timeout
                    while not control.enrolled.wait(0.01):
                        if consuming.done():
                            consuming.result()
                            raise ValueError("Dataset ended before selected failure point")
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Selected protected task did not reach failure point")
                    diagnostics.update(
                        observation_before_failure=snapshot(control),
                        selected_task_id=control.selected_task_id,
                        consumed_returns_before_failure=control.prefix_returns,
                        head_failure_requested=True,
                        failure_trigger=point,
                        udf_gate_enabled=False,
                        executor_paused_for_failure=True,
                    )
                    started = time.monotonic()
                    diagnostics.update(crash_head())
                    control.resume.set()
                output_rows, output_blocks = consuming.result(timeout=timeout)
                if failure:
                    diagnostics["failure_request_to_completion_s"] = time.monotonic() - started
            except BaseException:
                # The ordinary final snapshot is taken after shutdown, when
                # cancellation has already retired the streams. Preserve the
                # actual waiting states and thread stacks first.
                try:
                    diagnostics["observation_before_shutdown"] = capture_wait_state(control)
                except Exception as diagnostic_error:
                    diagnostics["wait_state_capture_error"] = str(diagnostic_error)
                raise
            finally:
                control.resume.set()
                if control.executor is not None:
                    control.executor.shutdown(force=True)
        operators = snapshot(control)
        expected_rows = (args.num_input_blocks * args.output_batches_per_input_batch
                         * args.output_batch_rows)
        producer, consumer = operators
        if (control.validated_producer_rows != expected_rows
                or producer["output_rows"] != expected_rows
                or producer["tasks_finished"] != args.num_input_blocks
                or consumer["tasks_finished"] != producer["output_blocks"]
                or output_rows != consumer["output_rows"]
                or output_rows != consumer["tasks_finished"]
                or output_blocks != consumer["output_blocks"]):
            raise ValueError(f"Original producer/consumer output accounting mismatch: {operators}")
        for op in operators:
            tasks = op["tasks_submitted"]
            if (tasks != op["tasks_finished"] or op["tasks_failed"]
                    or tasks != op["fixed_r_closed_streams"]
                    or op["fixed_r_copied_blocks"] != op["output_blocks"]
                    or op["active_enrolled_tasks"] or op["fixed_r_recovery_errors"]
                    or tasks != sum(op[key] for key in (
                        "fixed_r_enrolled_tasks", "fixed_r_survivor_tasks", "fixed_r_copy_baseline_tasks"))):
                raise ValueError(f"Incomplete task recovery/retirement: {op}")
            if args.recovery_mode == "copy" and op["fixed_r_copy_baseline_tasks"] != tasks:
                raise ValueError("Copy baseline enrolled protected tasks")
            if args.recovery_mode == "fixed_r" and op["fixed_r_enrolled_tasks"] != tasks:
                raise ValueError("No-failure case bypassed protection")
        recovered = {item["task_id"] for op in operators
                     for item in op["fixed_r_recovered_task_details"]}
        if trigger is not None:
            trigger.validate(operators)
        elif failure and control.selected_task_id not in recovered:
            raise ValueError("Selected protected task did not actually replay")
        if failure and trigger is None and point == "producer_after_output" and control.prefix_returns < 2:
            raise ValueError("Failure did not exercise a consumed streaming prefix")
        return {
            **vars(args), **diagnostics, "operators": operators,
            "validated_producer_rows": control.validated_producer_rows,
            "validated_output_rows": output_rows, "validated_output_blocks": output_blocks,
            "runtime_recovery": "dynamic_count_streaming",
            "original_udfs": True, "user_declared_block_counts": False,
            "calibration_required": False, "whole_task_buffering": False,
            "block_shaping_enabled": True, "fusion_enabled": False,
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
        }
    finally:
        control.resume.set()
        diagnostics["last_observation"] = snapshot(control)
        if control.execution_error_traceback:
            diagnostics["execution_error_traceback"] = control.execution_error_traceback
        if not registered:
            cloudpickle.unregister_pickle_by_value(original)


def run_recovery_cases(args):
    from benchmark import Benchmark

    if args.case == "training-prefetch":
        from streaming_recovery_training_prefetch import run_recovery_cases as run_training

        return run_training(args)
    if args.case != "fast-producer-slow-consumer":
        raise ValueError(f"Unsupported runtime benchmark case: {args.case}")
    if args.recovery_mode not in ("copy", "fixed_r", "fixed_r_head_failure", "suite"):
        raise ValueError("Runtime recovery requires copy, fixed_r, fixed_r_head_failure, or suite")
    for name in ("num_input_blocks", "output_batches_per_input_batch",
                 "output_batch_rows", "output_row_bytes"):
        if getattr(args, name) <= 0:
            raise ValueError(f"{name} must be positive")
    benchmark = Benchmark()
    failures = []
    cases = ([("copy", "none"), ("fixed_r", "none"),
              ("fixed_r_head_failure", "producer_before_output"),
              ("fixed_r_head_failure", "producer_after_output"),
              ("fixed_r_head_failure", "consumer")]
             if args.recovery_mode == "suite"
             else [(args.recovery_mode, getattr(args, "runtime_failure_point", "producer_after_output")
                    if args.recovery_mode == "fixed_r_head_failure" else "none")])
    timing = getattr(args, "recovery_head_timing", "paused")
    if timing != "paused":
        phases = tuple(FRACTIONS) if timing == "suite" else (timing,)
        if args.recovery_mode in ("suite", "fixed_r_head_failure"):
            cases = ([("copy", "none", "paused"), ("fixed_r", "none", "paused")]
                     if args.recovery_mode == "suite" else [])
            cases += [("fixed_r_head_failure", "producer_after_output", phase) for phase in phases]
        else:
            raise ValueError("Asynchronous head timing requires fixed_r_head_failure or suite")
    else:
        cases = [(mode, point, "paused") for mode, point in cases]
    for mode, point, phase in cases:
        selected = argparse.Namespace(**vars(args))
        selected.recovery_mode = mode
        selected.runtime_failure_point = point
        selected.recovery_head_timing = phase
        diagnostics = {}
        key = f"backpressure/original_dataset/{mode}/{point}"
        if phase != "paused":
            key += f"/async_{phase}"
        try:
            with local_head_failure_cluster(selected) as (case_args, crash):
                benchmark.run_fn(key, run_dataset, case_args, crash, diagnostics)
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            failures.append(f"{mode}/{point}")
            traceback.print_exc()
            benchmark.result[key] = {
                **vars(selected), **diagnostics, "validation_status": "failed",
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        finally:
            benchmark.write_result()
    if failures:
        raise RuntimeError(f"Runtime backpressure recovery failed: {failures}; inspect saved JSON")
