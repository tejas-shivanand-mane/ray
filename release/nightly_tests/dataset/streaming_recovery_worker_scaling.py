"""Controlled recovery coverage for the collaborator's task worker workload.

Uses its actual schema UDF and row-sizing formula. Range is prepared and copied
before timing; final output refs are retained while draining the public Dataset
iterator. This is correctness coverage, not the unchanged scaling benchmark.
"""

import argparse
import math
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import ray
from ray import cloudpickle
from ray.data import DataContext, TaskPoolStrategy
from ray.data.block import BlockAccessor, _apply_batch_format
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import BatchMapTransformFn
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, FixedRDataConfig
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import _generate_transform_fn_for_map_batches

from streaming_recovery_benchmark import (
    _failure_observation,
    _make_execution_capture,
    _make_gate,
    _operator_metrics,
    _OutputProgress,
)
from streaming_recovery_head_failure import local_head_failure_cluster


MODES = ("copy", "fixed_r", "fixed_r_head_failure")


def validate_recovery_args(args):
    if args.recovery_mode not in (*MODES, "suite"):
        raise ValueError("Unknown worker-schema recovery mode")
    if args.worker_type != "tasks":
        raise ValueError("Recovery currently requires --worker-type tasks; actors are unsupported")
    for name in ("num_workers", "num_operators", "blocks_per_worker"):
        if type(getattr(args, name)) is not int or getattr(args, name) < 1:
            raise ValueError(f"{name} must be a positive integer")
    if args.num_workers < args.num_operators:
        raise ValueError("num_workers must be at least num_operators")
    if min(args.num_scalar_cols, args.num_array_cols) < 0 or (
        args.num_scalar_cols + args.num_array_cols <= 0
    ):
        raise ValueError("Use nonnegative column counts with at least one column")
    if not 0 <= args.recovery_failure_operator < args.num_operators:
        raise ValueError("recovery_failure_operator must select an existing map stage")
    if not 2 <= args.local_executor_nodes <= 250 or args.local_object_store_mb < 80:
        raise ValueError("Use 2..250 local executor nodes and object stores of at least 80 MiB")
    if not math.isfinite(args.recovery_timeout_s) or args.recovery_timeout_s <= 0:
        raise ValueError("recovery_timeout_s must be finite and positive")


def original_udf_bytes(args):
    import worker_scaling_benchmark as original

    registered = original.__name__ in cloudpickle.list_registry_pickle_by_value()
    if not registered:
        cloudpickle.register_pickle_by_value(original)
    try:
        return cloudpickle.dumps(original.make_realistic_schema_udf(
            args.seed, args.num_scalar_cols, args.num_array_cols,
        ))
    finally:
        if not registered:
            cloudpickle.unregister_pickle_by_value(original)


def make_udf(payload, stage, gate_name=None, timeout_s=180):
    # All benchmark-specific code is serialized by value. No actor handles or
    # ObjectRefs enter the protected task recipe, including the optional gate.
    def worker_schema(batch):
        ctx = TaskContext.get_current()
        attempt = ray._private.worker.global_worker.core_worker.get_current_task_attempt_number()
        if gate_name is not None and ctx.task_idx == 0 and attempt == 0:
            gate = ray.get_actor(gate_name, namespace="fixed-r-benchmark")
            ray.get(gate.arrive.remote(
                "Produce", ctx.task_idx, ray.get_runtime_context().get_node_id(),
            ), timeout=timeout_s)
            deadline = time.monotonic() + timeout_s
            while not ray.get(gate.is_open.remote(), timeout=timeout_s):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Worker-schema failure gate timed out")
                time.sleep(0.05)
        return cloudpickle.loads(payload)(batch)

    worker_schema.__name__ = f"worker_schema_{stage}"
    return worker_schema


def prepare_workload(args):
    """Retain actual range inputs and calibrate the deterministic map chain."""
    import worker_scaling_benchmark as original

    started = time.monotonic()
    context = DataContext.get_current().copy()
    context.set_config(CONFIG_KEY, None)
    context.enable_progress_bars = False
    context.execution_options.preserve_order = True
    rows = original._rows_per_block(args.num_scalar_cols, args.num_array_cols)
    count = args.num_workers * args.blocks_per_worker
    refs = []
    next_id = 0
    with DataContext.current(context):
        source = ray.data.range(rows * count, override_num_blocks=count)
        for bundle in source.iter_internal_ref_bundles():
            for ref in bundle.block_refs:
                block = ray.get(ref)
                if block.num_rows != rows or not np.array_equal(
                    block["id"].to_numpy(), np.arange(next_id, next_id + rows),
                ):
                    raise ValueError("Range input partitioning differs from the declared workload")
                # The original read task is not inside the protected map chain.
                # Its independent copy stays owned and retained by this driver.
                refs.append(ray.put(block))
                next_id += rows
        if len(refs) != count or next_id != rows * count:
            raise ValueError("Range input count mismatch")
        ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs(refs)
        payload = original_udf_bytes(args)
        sample = pa.table({"id": np.arange(rows)})
        schema = None
        for stage in range(args.num_operators):
            # Calibration runs the original function directly (no Ray UDF gate).
            udf = cloudpickle.loads(payload)
            transformer = BatchMapTransformFn(
                _generate_transform_fn_for_map_batches(udf), batch_size=None,
                batch_format=_apply_batch_format("default"), zero_copy_batch=True,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=context.target_max_block_size,
                ),
            )
            blocks = list(transformer([sample], TaskContext(0, "SchemaProbe")))
            if len(blocks) != 1 or BlockAccessor.for_block(blocks[0]).num_rows() != rows:
                raise ValueError("Recovery requires one calibrated schema block per input per stage")
            sample = blocks[0]
            schema = BlockAccessor.for_block(sample).schema()
    args.recovery_rows_per_block = rows
    args.recovery_input_blocks = count
    args.recovery_target_max_block_size = context.target_max_block_size
    args.recovery_preparation_s = time.monotonic() - started
    return refs, payload, schema


def validate_block(block, args, schema):
    accessor = BlockAccessor.for_block(block)
    if accessor.num_rows() != args.recovery_rows_per_block or accessor.schema() != schema:
        raise ValueError("Worker-schema output has an unexpected row count or schema")
    batch = accessor.to_numpy()
    rng = np.random.default_rng(args.seed)
    scalars = rng.uniform(0.0, 1.0, size=args.num_scalar_cols).astype(np.float32)
    arrays = rng.uniform(0.0, 100.0, size=(args.num_array_cols, 32)).astype(np.float32)
    for index, value in enumerate(scalars):
        column = np.asarray(batch[f"scalar_col_{index}"])
        if column.dtype != np.float32 or not np.all(column == value):
            raise ValueError(f"Incorrect scalar_col_{index} values")
    for index, values in enumerate(arrays):
        column = np.asarray(batch[f"array_col_{index}"])
        # Arrow list columns may convert to a one-dimensional object array.
        if column.ndim == 1:
            column = np.stack(column)
        if (
            column.shape != (args.recovery_rows_per_block, 32)
            or column.dtype != np.float32 or not np.all(column == values)
        ):
            raise ValueError(f"Incorrect array_col_{index} values")


def validate_metrics(metrics, args):
    tasks = args.recovery_input_blocks
    recovered_total = 0
    for name, actual in metrics.items():
        expected = {
            "tasks_submitted": tasks, "tasks_finished": tasks, "tasks_failed": 0,
            "output_blocks": tasks, "output_rows": tasks * args.recovery_rows_per_block,
            "fixed_r_closed_streams": tasks, "fixed_r_copied_blocks": tasks,
        }
        if any(actual.get(key) != value for key, value in expected.items()):
            raise ValueError(f"Task/output/retirement mismatch for {name}: {actual}")
        enrolled = actual["fixed_r_enrolled_tasks"]
        survivor = actual["fixed_r_survivor_tasks"]
        copied = actual["fixed_r_copy_baseline_tasks"]
        recovered = actual["fixed_r_recovered_tasks"]
        details = actual["fixed_r_recovered_task_details"]
        if (
            enrolled + survivor + copied != tasks
            or not 0 <= recovered <= enrolled <= tasks
            or not 0 <= actual["fixed_r_pre_submission_failovers"] <= survivor
            or len(details) != recovered
            or len({item["task_index"] for item in details}) != recovered
            or len({item["task_id"] for item in details}) != recovered
            or any(not 0 <= item["task_index"] < tasks for item in details)
        ):
            raise ValueError(f"Recovery accounting mismatch for {name}: {actual}")
        if args.recovery_mode == "copy":
            valid = copied == tasks and enrolled == survivor == recovered == 0
        elif args.recovery_mode == "fixed_r":
            valid = enrolled == tasks and copied == survivor == recovered == 0
        else:
            valid = copied == 0
        if not valid:
            raise ValueError(f"Unexpected submission mode for {name}: {actual}")
        recovered_total += recovered
    target = f"MapBatches(worker_schema_{args.recovery_failure_operator})"
    if args.recovery_mode == "fixed_r_head_failure" and (
        recovered_total == 0 or not any(
            item["task_index"] == 0
            for item in metrics[target]["fixed_r_recovered_task_details"]
        )
    ):
        raise ValueError("Head replacement did not replay the selected protected task")


def run_workload(args, refs, payload, schema, crash_head, diagnostics):
    failure = args.recovery_mode == "fixed_r_head_failure"
    names = [f"MapBatches(worker_schema_{stage})" for stage in range(args.num_operators)]
    config = FixedRDataConfig(
        args.owner_node_id, tuple(args.executor_node_ids), dict.fromkeys(names, 1),
        mode="copy" if args.recovery_mode == "copy" else "fixed_r",
        timeout_s=args.recovery_timeout_s,
    )
    context = DataContext.get_current().copy()
    context.target_max_block_size = args.recovery_target_max_block_size
    context.eager_free = False
    context.retried_map_errors = False
    context.max_errored_blocks = 0
    context.enable_progress_bars = False
    context.execution_options.preserve_order = True
    context._max_num_blocks_in_streaming_gen_buffer = 1
    context.set_config(CONFIG_KEY, config)
    capture = _make_execution_capture()
    progress = _OutputProgress()
    gate_name = "worker-schema-" + uuid.uuid4().hex if failure else None
    gate = _make_gate(gate_name, args.recovery_timeout_s) if failure else None
    retained_outputs = []

    class CaptureExecution(ExecutionCallback):
        def before_execution_starts(self, executor):
            operators = {op.name: op for op in executor._topology if isinstance(op, MapOperator)}
            if set(operators) != set(names) or len(operators) != len(names):
                raise ValueError(f"Unexpected worker-schema physical stages: {list(operators)}")
            if any(
                op.get_additional_split_factor() != 1
                or op.target_max_block_size_override not in (None, context.target_max_block_size)
                for op in operators.values()
            ):
                raise ValueError("Planned schema block shaping differs from calibration")
            capture.operators = [operators[name] for name in names]
            capture.executor = executor

    context.custom_execution_callback_classes.append(CaptureExecution)
    try:
        with DataContext.current(context):
            dataset = ray.data.from_arrow_refs(refs)
            for stage in range(args.num_operators):
                fn = make_udf(
                    payload, stage,
                    gate_name if stage == args.recovery_failure_operator else None,
                    args.recovery_timeout_s,
                )
                dataset = dataset.map_batches(
                    fn, batch_size=None,
                    compute=TaskPoolStrategy(size=args.num_workers // args.num_operators),
                    num_cpus=0.5, max_retries=1, retry_exceptions=False,
                )

        def drain():
            for bundle in dataset.iter_internal_ref_bundles():
                for ref in bundle.block_refs:
                    validate_block(ray.get(ref), args, schema)
                    # Retain the full final output like the original materialize
                    # sink, while using the iterator for explicit validation.
                    retained_outputs.append(ref)
                    progress.record(len(retained_outputs))
            if len(retained_outputs) != args.recovery_input_blocks:
                raise ValueError("Missing or extra worker-schema final output blocks")
            progress.finished_at = time.monotonic()

        with ThreadPoolExecutor(max_workers=1) as pool:
            drained = pool.submit(drain)
            try:
                if failure:
                    deadline = time.monotonic() + args.recovery_timeout_s
                    target = names[args.recovery_failure_operator]
                    while True:
                        if drained.done():
                            drained.result()
                            raise RuntimeError("Workload ended before the enrolled failure gate")
                        arrivals = ray.get(gate.snapshot.remote(), timeout=args.recovery_timeout_s)
                        observation = _failure_observation(capture, progress)
                        active = observation["operators"].get(target, {}).get("active_enrolled_tasks", [])
                        if 0 in arrivals["Produce"] and any(t["task_index"] == 0 for t in active):
                            break
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Selected protected task did not reach the failure gate")
                        time.sleep(0.05)
                    diagnostics.update(
                        failure_trigger="selected_enrolled_task_gate", failure_gate_enabled=True,
                        observation_before_failure=observation, head_failure_requested=True,
                    )
                    print(f"FIXED_R_WORKER_SCHEMA_HEAD_FAILURE_READY stage={target}", flush=True)
                    started = time.monotonic()
                    diagnostics.update(crash_head())
                    ray.get(gate.open.remote(), timeout=args.recovery_timeout_s)
                drained.result(timeout=args.recovery_timeout_s)
                if failure:
                    diagnostics["failure_request_to_drain_s"] = progress.finished_at - started
            finally:
                # Shut down before joining the drain thread on errors/timeouts.
                try:
                    if gate is not None:
                        ray.get(gate.open.remote(), timeout=args.recovery_timeout_s)
                finally:
                    if capture.executor is not None:
                        capture.executor.shutdown(force=True)
        metrics = {op.name: _operator_metrics(op) for op in capture.operators}
        if set(metrics) != set(names):
            raise ValueError("Missing worker-schema operator accounting")
        validate_metrics(metrics, args)
        diagnostics["last_observation"] = _failure_observation(capture, progress)
        if any(op["active_enrolled_tasks"] for op in
               diagnostics["last_observation"]["operators"].values()):
            raise ValueError("An enrolled worker-schema stream remains active after drain")
        return {
            **vars(args), **diagnostics, "operators": metrics,
            "validated_output_blocks": len(retained_outputs),
            "validated_output_rows": len(retained_outputs) * args.recovery_rows_per_block,
            "physical_operator_names": names, "declared_blocks_per_task": config.expected_blocks,
            "workload_variant": "original_worker_schema_udf_retained_range_inputs",
            "output_validation": "exact_schema_all_scalar_and_array_values_and_row_counts",
            "input_preparation": "range_then_coordinator_owned_copies_before_timing",
            "sink": "public_iterator_with_retained_final_output_refs",
            "workers_per_operator": args.num_workers // args.num_operators,
            "fusion_enabled": False, "block_shaping_enabled": True,
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
        }
    finally:
        diagnostics["last_observation"] = _failure_observation(capture, progress)
        try:
            if capture.executor is not None:
                capture.executor.shutdown(force=True)
        finally:
            if gate is not None:
                ray.kill(gate, no_restart=True)


def run_recovery_cases(args):
    from benchmark import Benchmark

    validate_recovery_args(args)
    benchmark = Benchmark()
    failed = []
    modes = MODES if args.recovery_mode == "suite" else (args.recovery_mode,)
    for mode in modes:
        selected = argparse.Namespace(**vars(args))
        selected.recovery_mode = mode
        selected.owner_node_id = None
        selected.executor_node_ids = None
        selected.producer_concurrency = args.num_workers // args.num_operators
        key = f"worker_scaling/tasks/{args.num_operators}_operators/{mode}"
        diagnostics = {}
        try:
            with local_head_failure_cluster(selected) as (case_args, crash_head):
                refs, payload, schema = prepare_workload(case_args)
                diagnostics.update(vars(case_args))
                benchmark.run_fn(
                    key, run_workload, case_args, refs, payload, schema, crash_head, diagnostics,
                )
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            failed.append(mode)
            traceback.print_exc()
            benchmark.result[key] = {
                **vars(selected), **diagnostics, "validation_status": "failed",
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        finally:
            benchmark.write_result()
    if failed:
        raise RuntimeError(f"Worker-schema recovery cases failed: {', '.join(failed)}; inspect saved JSON")
