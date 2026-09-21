"""Exercise the original worker Dataset pipeline with runtime-owned recovery.

Only configuration, observation, and fault injection live here. The application
uses the shared original range -> map_batches -> materialize implementation.
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

from streaming_recovery_benchmark import _operator_metrics
from streaming_recovery_head_failure import local_head_failure_cluster
from streaming_recovery_worker_scaling import validate_block, validate_recovery_args


def execution_control(args):
    class Control:
        def __init__(self):
            self.executor = None
            self.operators = []
            self.enrolled = Event()
            self.resume = Event()
            self.target = None
            self.execution_error_traceback = None

        def __getstate__(self):
            # Callback classes travel in the DataContext, but the executor,
            # task handles and thread events must stay in the driver.
            return {
                "executor": None, "operators": [], "target": None,
                "execution_error_traceback": None,
            }

    control = Control()
    failure = args.recovery_mode == "fixed_r_head_failure"
    target_index = (
        0 if args.recovery_failure_stage == "read"
        else args.recovery_failure_operator + 1
    )
    operator_count = args.num_operators + 1
    timeout_s = args.recovery_timeout_s

    class Capture(ExecutionCallback):
        def after_execution_fails(self, executor, error):
            # Preserve internal frames before Dataset's public exception
            # decorator removes them. Store text, not live traceback references.
            control.execution_error_traceback = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )

        def before_execution_starts(self, executor):
            ops = [op for op in executor._topology if isinstance(op, MapOperator)]
            if not ops:
                # materialize() may execute an InputData-only cached Dataset.
                return
            if len(ops) != operator_count or not ops[0].name.startswith("Read"):
                raise ValueError(f"Unexpected original Dataset plan: {[op.name for op in ops]}")
            if control.executor is not None:
                raise ValueError("The original workload unexpectedly started a second map execution")
            control.executor = executor
            control.operators = ops
            control.target = target_index
            if failure:
                op = ops[target_index]
                submit = op._submit_data_task

                def submit_then_pause(stream, *positional, **kwargs):
                    submit(stream, *positional, **kwargs)
                    if not control.enrolled.is_set():
                        if stream.reader is None:
                            raise ValueError("Selected task was not protected before failure injection")
                        # Enrollment and task registration are complete. Pause
                        # the scheduling thread before it consumes any output.
                        # The task itself and its original UDF remain unchanged.
                        control.enrolled.set()
                        if not control.resume.wait(timeout_s):
                            raise TimeoutError("Head-failure controller did not release the executor")

                op._submit_data_task = submit_then_pause

    return control, Capture


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
        result.append({
            "stage_index": index, "name": op.name, **_operator_metrics(op),
            "active_enrolled_tasks": active,
        })
    return result


def validate_accounting(args, operators, count, rows):
    if len(operators) != args.num_operators + 1:
        raise ValueError("Read/map operator accounting is incomplete")
    for op in operators:
        expected = {
            "tasks_submitted": count, "tasks_finished": count, "tasks_failed": 0,
            "output_blocks": count, "output_rows": count * rows,
            "fixed_r_closed_streams": count, "fixed_r_copied_blocks": count,
        }
        if any(op.get(key) != value for key, value in expected.items()) or op["active_enrolled_tasks"]:
            raise ValueError(f"Read/map output or retirement mismatch: {op}")
        enrolled, survivor, copied, recovered = [op[key] for key in (
            "fixed_r_enrolled_tasks", "fixed_r_survivor_tasks",
            "fixed_r_copy_baseline_tasks", "fixed_r_recovered_tasks",
        )]
        details = op["fixed_r_recovered_task_details"]
        if (
            enrolled + survivor + copied != count
            or not 0 <= recovered <= enrolled <= count
            or not 0 <= op["fixed_r_pre_submission_failovers"] <= survivor
            or len(details) != recovered
            or len({item["task_id"] for item in details}) != recovered
            or len({item["task_index"] for item in details}) != recovered
            or any(not 0 <= item["task_index"] < count for item in details)
        ):
            raise ValueError(f"Invalid recovery accounting: {op}")
        if args.recovery_mode == "copy":
            valid = copied == count and enrolled == survivor == recovered == 0
        elif args.recovery_mode == "fixed_r":
            valid = enrolled == count and copied == survivor == recovered == 0
        else:
            valid = copied == 0
        if not valid:
            raise ValueError(f"Unexpected task submission ownership: {op}")
    if args.recovery_mode == "fixed_r_head_failure":
        target = 0 if args.recovery_failure_stage == "read" else args.recovery_failure_operator + 1
        if not any(item["task_index"] == 0 for item in
                   operators[target]["fixed_r_recovered_task_details"]):
            raise ValueError("The selected original Dataset task did not replay")


def run_dataset(args, crash_head, diagnostics):
    import worker_scaling_benchmark as original

    context = DataContext.get_current().copy()
    context.enable_fixed_r_task_recovery = True
    context.fixed_r_task_recovery_output_mode = getattr(args, "recovery_output_mode", "streaming")
    context.fixed_r_task_recovery_timeout_s = args.recovery_timeout_s
    context.enable_progress_bars = False
    config = get_config(context)
    if args.recovery_mode == "copy":
        context.set_config(CONFIG_KEY, replace(config, mode="copy"))
    control, callback = execution_control(args)
    context.custom_execution_callback_classes.append(callback)
    failure = args.recovery_mode == "fixed_r_head_failure"
    registered = original.__name__ in cloudpickle.list_registry_pickle_by_value()
    if not registered:
        # Import packaging only; serialize the original class/method by value,
        # as when this benchmark is launched as a __main__ script.
        cloudpickle.register_pickle_by_value(original)
    try:
        with DataContext.current(context):
            dataset = original.build_dataset(args)
        with ThreadPoolExecutor(max_workers=1) as pool:
            materializing = pool.submit(dataset.materialize)
            try:
                if failure:
                    deadline = time.monotonic() + args.recovery_timeout_s
                    while not control.enrolled.wait(0.01):
                        if materializing.done():
                            materializing.result()
                            raise ValueError("Original Dataset ended before the selected enrollment")
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Original Dataset did not enroll the selected task")
                    diagnostics.update(
                        observation_before_failure=snapshot(control),
                        head_failure_requested=True,
                        failure_trigger="executor_paused_after_selected_task_enrollment",
                        udf_gate_enabled=False,
                    )
                    started = time.monotonic()
                    diagnostics.update(crash_head())
                    control.resume.set()
                materialized = materializing.result(timeout=args.recovery_timeout_s)
                if failure:
                    diagnostics["failure_request_to_materialize_s"] = time.monotonic() - started
            finally:
                control.resume.set()
                if control.executor is not None:
                    control.executor.shutdown(force=True)
        # Assertions observe the completed original pipeline; they do not feed
        # block counts, schemas, or a probe invocation into runtime recovery.
        rows = original._rows_per_block(args.num_scalar_cols, args.num_array_cols)
        count = args.num_workers * args.blocks_per_worker
        expected = original.make_realistic_schema_udf(
            args.seed, args.num_scalar_cols, args.num_array_cols,
        )({"id": np.arange(1)})
        schema = BlockAccessor.batch_to_arrow_block(expected).schema
        checked = argparse.Namespace(**vars(args), recovery_rows_per_block=rows)
        output_blocks = 0
        for bundle in materialized.iter_internal_ref_bundles():
            for ref in bundle.block_refs:
                validate_block(ray.get(ref), checked, schema)
                output_blocks += 1
        if output_blocks != count or materialized.count() != count * rows:
            raise ValueError("Original materialized Dataset output count mismatch")
        operators = snapshot(control)
        validate_accounting(args, operators, count, rows)
        return {
            **vars(args), **diagnostics, "operators": operators,
            "validated_output_blocks": output_blocks, "validated_output_rows": count * rows,
            "physical_operator_names": [op["name"] for op in operators],
            "workload_variant": "original_range_map_batches_materialize",
            "runtime_recovery": ("dynamic_count_streaming" if config.dynamic_task_outputs
                                 else "bounded_finite_task_envelope"),
            "max_task_output_bytes": config.max_task_output_bytes,
            "user_declared_block_counts": False, "calibration_required": False,
            "benchmark_input_copies": False, "read_tasks_protected": True,
            "block_shaping_enabled": True, "fusion_enabled": False,
            "output_validation": "exact_schema_all_scalar_and_array_values_and_row_counts",
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
        }
    finally:
        control.resume.set()
        if control.execution_error_traceback is not None:
            diagnostics["execution_error_traceback"] = control.execution_error_traceback
        diagnostics["last_observation"] = snapshot(control)
        if not registered:
            cloudpickle.unregister_pickle_by_value(original)


def run_recovery_cases(args):
    from benchmark import Benchmark

    validate_recovery_args(args)
    benchmark = Benchmark()
    failures = []
    cases = (
        [("copy", "map"), ("fixed_r", "map"),
         ("fixed_r_head_failure", "read"), ("fixed_r_head_failure", "map")]
        if args.recovery_mode == "suite"
        else [(args.recovery_mode, args.recovery_failure_stage)]
    )
    for mode, stage in cases:
        selected = argparse.Namespace(**vars(args))
        selected.recovery_mode = mode
        selected.recovery_failure_stage = stage
        selected.owner_node_id = None
        selected.executor_node_ids = None
        selected.producer_concurrency = args.num_workers // args.num_operators
        diagnostics = {}
        key = f"worker_scaling/original_dataset/{args.num_operators}_operators/{mode}"
        if mode == "fixed_r_head_failure":
            key += f"/{stage}"
        try:
            with local_head_failure_cluster(selected) as (case_args, crash_head):
                benchmark.run_fn(key, run_dataset, case_args, crash_head, diagnostics)
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            failures.append(f"{mode}/{stage}")
            traceback.print_exc()
            benchmark.result[key] = {
                **vars(selected), **diagnostics, "validation_status": "failed",
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        finally:
            benchmark.write_result()
    if failures:
        raise RuntimeError(f"Original Dataset recovery cases failed: {', '.join(failures)}; inspect saved JSON")
