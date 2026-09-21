"""Fixed-R variants of fast-producer-slow-consumer.

Uses the real Data streaming executor, with explicit one-block input bundles and
block shaping disabled. This is a workload adaptation, not the unchanged public
Dataset benchmark. The opt-in Dataset variant uses from_blocks/map_batches and
the public streaming iterator, with declared counts and unshaped output batches.
No large output stream is materialized on the driver.
"""

import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import numpy as np
import pyarrow as pa
import ray

from ray.data import DataContext, TaskPoolStrategy
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, FixedRDataConfig
from ray.data.block import BlockAccessor
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


MODES = ("ordinary", "copy", "fixed_r", "fixed_r_failure")
FAILURE_MODES = ("fixed_r_failure", "fixed_r_head_failure")
HEAD_FAILURE_FRACTIONS = {"early": 0.1, "middle": 0.5, "late": 0.9}


def _blocks_per_producer(args):
    return getattr(args, "recovery_blocks_per_producer", args.output_batches_per_input_batch)


def head_failure_target(point, expected):
    if point not in HEAD_FAILURE_FRACTIONS:
        raise ValueError(f"Unknown progress-triggered head failure: {point}")
    if expected < 10:
        raise ValueError("Progress-triggered head failure requires at least 10 outputs")
    return min(expected - 1, math.ceil(expected * HEAD_FAILURE_FRACTIONS[point]))


def recovery_system_config():
    return {
        "enable_recovery_succession": True,
        "enable_recovery_witness_holder_baseline": True,
        "enable_recovery_streaming_fixed_r": True,
        "recovery_succession_target_holder_count": 2,
        "recovery_succession_witness_count": 2,
        "recovery_frontier_group_size": 1,
        "recovery_baseline_perf_protect_every_n": 1,
        "health_check_initial_delay_ms": 1000,
        "health_check_period_ms": 1000,
        "health_check_timeout_ms": 3000,
        "health_check_failure_threshold": 3,
    }


def validate_args(args):
    if args.recovery_mode not in (*MODES, "fixed_r_head_failure"):
        raise ValueError(f"Unknown controlled mode: {args.recovery_mode}")
    for name in (
        "num_input_blocks", "output_batches_per_input_batch",
        "output_batch_rows", "output_row_bytes", "producer_concurrency",
    ):
        value = getattr(args, name)
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    for name in ("consumer_sleep_s", "recovery_timeout_s"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and nonnegative")
    if args.recovery_timeout_s == 0:
        raise ValueError("recovery_timeout_s must be positive")
    config = FixedRDataConfig(
        args.owner_node_id, tuple(args.executor_node_ids),
        {"Produce": args.output_batches_per_input_batch, "Consume": 1},
    )
    config.validate()
    nodes = {node["NodeID"]: node for node in ray.nodes() if node["Alive"]}
    if any(
        node not in nodes for node in (config.owner_node_id, *config.executor_node_ids)
    ):
        raise ValueError("Owner and all executors must be live at benchmark start")
    coordinator = ray.get_runtime_context().get_node_id()
    if config.owner_node_id == coordinator or coordinator in config.executor_node_ids:
        raise ValueError("Use separate coordinator, owner, and task executor nodes")
    if any(
        nodes[node]["Resources"].get("CPU", 0) < 1
        for node in config.executor_node_ids
    ):
        raise ValueError("Each executor needs at least one CPU")
    point = getattr(args, "head_failure_point", "gated")
    if point != "gated":
        if args.recovery_mode != "fixed_r_head_failure":
            raise ValueError("Progress-triggered failure requires fixed_r_head_failure")
        if getattr(args, "recovery_plan", "physical") != "dataset":
            raise ValueError("Progress-triggered failure requires --recovery-plan dataset")
        head_failure_target(
            point, args.num_input_blocks * _blocks_per_producer(args)
        )
    if args.recovery_mode in FAILURE_MODES and point == "gated":
        if args.output_batches_per_input_batch < 2:
            raise ValueError("Owner failure needs at least two producer output blocks")
        # All initial producers wait while the first consumer starts. Ensure a
        # CPU remains on its assigned node, otherwise the gate would deadlock.
        wave = min(args.producer_concurrency, args.num_input_blocks)
        for index, node in enumerate(config.executor_node_ids):
            producers = len(range(index, wave, len(config.executor_node_ids)))
            needed = producers + (index == 0)
            if nodes[node]["Resources"].get("CPU", 0) < needed:
                raise ValueError(
                    "Failure gate needs CPUs for the initial wave plus consumer"
                )
    return config


def _make_gate(name, timeout_s):
    # Local class serializes by value: workers need no benchmark module on PYTHONPATH.
    class Gate:
        def __init__(self):
            self.arrivals = {"Produce": {}, "Consume": {}}
            self.opened = False

        def arrive(self, stage, task_index, node):
            self.arrivals[stage][task_index] = node

        def is_open(self):
            return self.opened

        def snapshot(self):
            return self.arrivals

        def open(self):
            self.opened = True

    gate = ray.remote(num_cpus=0, max_restarts=0)(Gate).options(
        name=name, namespace="fixed-r-benchmark",
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        ),
    ).remote()
    ray.get(gate.__ray_ready__.remote(), timeout=timeout_s)
    return gate


def _transforms(args, gate_name):
    count = args.output_batches_per_input_batch
    rows = args.output_batch_rows
    row_bytes = args.output_row_bytes
    sleep_s = args.consumer_sleep_s
    timeout_s = args.recovery_timeout_s

    def gate_once(stage, ctx):
        if gate_name is None:
            return
        core_worker = ray._private.worker.global_worker.core_worker
        if core_worker.get_current_task_attempt_number() != 0:
            return
        # Capture only the name in the transformer: actor handles would introduce
        # contained references into an otherwise ready coordinator-owned input.
        gate = ray.get_actor(gate_name, namespace="fixed-r-benchmark")
        ray.get(
            gate.arrive.remote(
                stage, ctx.task_idx, ray.get_runtime_context().get_node_id()
            ),
            timeout=timeout_s,
        )
        deadline = time.monotonic() + timeout_s
        while not ray.get(gate.is_open.remote(), timeout=timeout_s):
            if time.monotonic() >= deadline:
                raise TimeoutError("Owner-failure benchmark gate timed out")
            time.sleep(0.05)

    def produce(blocks, ctx):
        inputs = iter(blocks)
        block = next(inputs)
        if next(inputs, None) is not None or block.num_rows != 1:
            raise ValueError("Controlled producer requires one single-row input block")
        input_id = block["id"][0].as_py()
        for index in range(count):
            if index == 1:
                gate_once("Produce", ctx)
            block = BlockAccessor.batch_to_arrow_block({
                "data": np.zeros((rows, row_bytes), dtype=np.uint8),
            })
            metadata = dict(block.schema.metadata or {})
            metadata.update({
                b"fixed_r_input": str(input_id).encode(),
                b"fixed_r_index": str(index).encode(),
                b"fixed_r_producer": ray.get_runtime_context().get_node_id().encode(),
            })
            yield block.replace_schema_metadata(metadata)

    def consume(blocks, ctx):
        inputs = iter(blocks)
        block = next(inputs)
        if next(inputs, None) is not None:
            raise ValueError("Controlled consumer requires exactly one input block")
        if ctx.task_idx == 0:
            gate_once("Consume", ctx)
        # Match map_batches' NumPy conversion as well as its sleep workload.
        batch = BlockAccessor.for_block(block).to_numpy()
        if batch["data"].shape != (rows, row_bytes):
            raise ValueError("Producer payload shape changed")
        time.sleep(sleep_s)
        meta = block.schema.metadata
        yield pa.table({
            "status": ["ok"], "input_id": [int(meta[b"fixed_r_input"])],
            "block_index": [int(meta[b"fixed_r_index"])], "rows": [block.num_rows],
            "producer_node": [meta[b"fixed_r_producer"].decode()],
            "consumer_node": [ray.get_runtime_context().get_node_id()],
        })

    return produce, consume


def _placement(nodes):
    index = 0

    def next_options():
        nonlocal index
        node = nodes[index % len(nodes)]
        index += 1
        return {"scheduling_strategy": NodeAffinitySchedulingStrategy(node, soft=False)}

    return next_options


def _operator_metrics(op):
    metrics = op.metrics
    return {
        "tasks_submitted": metrics.num_tasks_submitted,
        "tasks_finished": metrics.num_tasks_finished,
        "tasks_failed": metrics.num_tasks_failed,
        "output_blocks": metrics.num_task_outputs_generated,
        "output_rows": metrics.rows_task_outputs_generated,
        **{key: list(value) if isinstance(value, list) else value
           for key, value in metrics.extra_metrics.items()
           if key.startswith("fixed_r_")},
    }


class _OutputProgress:
    def __init__(self):
        self._lock = Lock()
        self._outputs = 0
        self.finished_at = None

    def record(self, outputs):
        with self._lock:
            self._outputs = outputs

    def count(self):
        with self._lock:
            return self._outputs


def _failure_observation(capture, progress):
    # Observation only: do not lock/pause the executor or the UDFs. Metrics and
    # active-task lists may move between reads; they are not an atomic cut.
    operators = {}
    for op in capture.operators:
        active = []
        for task in op.get_active_tasks():
            stream = getattr(task, "stream", None)
            if stream is not None and stream.reader is not None and not stream.closed:
                active.append({
                    "task_index": task.task_index(),
                    "task_id": task.get_task_id().hex(),
                    "accepted_returns": stream.next_index,
                    "declared_returns": stream.expected_returns,
                })
        operators[op.name] = {**_operator_metrics(op), "active_enrolled_tasks": active}
    return {"validated_outputs": progress.count(), "operators": operators}


def _drain_with_progress_failure(
    args, capture, progress, drain, crash_owner, failure_metrics,
):
    expected = args.num_input_blocks * _blocks_per_producer(args)
    target = head_failure_target(args.head_failure_point, expected)
    failure_metrics.update(
        failure_trigger="validated_output_progress", failure_gate_enabled=False,
        target_validated_outputs=target,
        target_output_fraction=HEAD_FAILURE_FRACTIONS[args.head_failure_point],
        head_failure_requested=False,
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        drained = pool.submit(drain)
        try:
            deadline = time.monotonic() + args.recovery_timeout_s
            while True:
                if drained.done():
                    drained.result()
                    raise RuntimeError("Execution ended before progress-triggered failure")
                observation = _failure_observation(capture, progress)
                if (
                    target <= observation["validated_outputs"] < expected
                    and any(op["active_enrolled_tasks"]
                            for op in observation["operators"].values())
                ):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("No active enrolled stream reached the failure target")
                time.sleep(0.01)
            failure_metrics["observation_before_failure"] = observation
            failure_metrics["head_failure_requested"] = True
            print(
                "FIXED_R_HEAD_FAILURE_PROGRESS "
                f"point={args.head_failure_point} "
                f"outputs={observation['validated_outputs']}/{expected} "
                f"owner_node_id={args.owner_node_id}", flush=True,
            )
            start = time.monotonic()
            # This runs on the controller thread while the Dataset executor and
            # the drain thread keep running. No gate actor exists in this path.
            replacement_metrics = crash_owner()
            if replacement_metrics is not None:
                failure_metrics.update(replacement_metrics)
            failure_metrics["validated_outputs_at_replacement_ready"] = progress.count()
            result = drained.result(timeout=args.recovery_timeout_s)
            failure_metrics["failure_request_to_drain_s"] = progress.finished_at - start
            return result
        finally:
            failure_metrics["last_observation"] = _failure_observation(capture, progress)
            if capture.executor is not None:
                capture.executor.shutdown(force=True)


def _make_execution_capture():
    # Local class: workers do not need this benchmark module on PYTHONPATH.
    class ExecutionCapture:
        """Observe the driver executor without serializing it into task arguments."""

        def __init__(self):
            self.executor = None
            self.operators = []

        def __getstate__(self):
            # Dataset contexts carry callback classes to workers. Their closure
            # must not carry the executor, locks, actors, or owned ObjectRefs.
            return {"executor": None, "operators": []}

    return ExecutionCapture()


def _dataset(args, context, gate_name, capture):
    produce_blocks, consume_blocks = _transforms(args, gate_name)

    # Local functions serialize by value, including the instrumented workload.
    # Arrow preserves the validation metadata; consume_blocks also performs the
    # original NumPy conversion before sleeping.
    def produce(batch):
        yield from produce_blocks([batch], TaskContext.get_current())

    def consume(batch):
        return next(consume_blocks([batch], TaskContext.get_current()))

    original_workload = getattr(args, "recovery_workload", "instrumented") == "original"
    if original_workload:
        from streaming_recovery_original_workload import original_workload_udfs

        produce, consume = original_workload_udfs(args)

    class CaptureExecution(ExecutionCallback):
        def before_execution_starts(self, executor):
            operators = {op.name: op for op in executor._topology
                         if isinstance(op, MapOperator)}
            names = ("MapBatches(produce)", "MapBatches(consume)")
            if set(operators) != set(names):
                raise ValueError(f"Unexpected Dataset physical stages: {list(operators)}")
            if original_workload and any(
                op.get_additional_split_factor() != 1
                or op.target_max_block_size_override not in (
                    None, args.recovery_target_max_block_size,
                )
                for op in operators.values()
            ):
                raise ValueError("Planned block sizing differs from producer calibration")
            capture.operators = [operators[name] for name in names]
            capture.executor = executor

    context.custom_execution_callback_classes.append(CaptureExecution)
    with DataContext.current(context):
        return (
            ray.data.from_blocks([
                pa.table({"id": [input_id]})
                for input_id in range(args.num_input_blocks)
            ])
            .map_batches(
                produce, batch_size=None,
                batch_format="default" if original_workload else "pyarrow",
                compute=TaskPoolStrategy(size=args.producer_concurrency),
                num_cpus=1, max_retries=1, retry_exceptions=False,
            )
            .map_batches(
                consume, batch_size=None,
                batch_format="default" if original_workload else "pyarrow",
                compute=TaskPoolStrategy(size=1),
                num_cpus=1, max_retries=1, retry_exceptions=False,
            )
        )


def run_controlled(args, crash_owner=None, diagnostics=None):
    config = validate_args(args)
    public_dataset = getattr(args, "recovery_plan", "physical") == "dataset"
    original_workload = getattr(args, "recovery_workload", "instrumented") == "original"
    if original_workload and (
        not public_dataset or not hasattr(args, "recovery_producer_block_rows")
        or args.recovery_mode != "fixed_r_head_failure"
        or getattr(args, "head_failure_point", "gated") == "gated"
    ):
        raise ValueError("Original workload requires calibration and Dataset progress failure")
    if public_dataset and args.recovery_mode == "ordinary":
        raise ValueError(
            "The Dataset recovery plan requires declared counts; use copy for "
            "its no-enrollment baseline, or original for the unchanged benchmark"
        )
    failure = args.recovery_mode in FAILURE_MODES
    progress_failure = failure and getattr(args, "head_failure_point", "gated") != "gated"
    if args.recovery_mode == "fixed_r_head_failure" and crash_owner is None:
        raise ValueError("Head failure requires the head replacement controller")
    context = DataContext.get_current().copy()
    if original_workload:
        context.target_max_block_size = args.recovery_target_max_block_size
    context.eager_free = False
    context.retried_map_errors = False
    context.max_errored_blocks = 0
    context.enable_progress_bars = False
    context.execution_options.preserve_order = True
    context._max_num_blocks_in_streaming_gen_buffer = 1
    recovery_config = None
    if args.recovery_mode != "ordinary":
        counts = (
            {"MapBatches(produce)": _blocks_per_producer(args),
             "MapBatches(consume)": 1}
            if public_dataset else config.expected_blocks
        )
        recovery_config = FixedRDataConfig(
            config.owner_node_id, config.executor_node_ids, counts,
            "copy" if args.recovery_mode == "copy" else "fixed_r",
            args.recovery_timeout_s,
            preserve_batch_output_blocks=public_dataset and not original_workload,
        )
    context.set_config(CONFIG_KEY, recovery_config)
    gate_name = "fixed-r-" + uuid.uuid4().hex if failure and not progress_failure else None
    gate = _make_gate(gate_name, args.recovery_timeout_s) if gate_name else None
    capture = _make_execution_capture()
    progress = _OutputProgress()
    failure_metrics = {}
    try:
        if public_dataset:
            dataset = _dataset(args, context, gate_name, capture)

            def iterate():
                # Start inside drain(): this public call waits for the first
                # output, which the gated variant blocks until head replacement.
                yield from dataset.iter_internal_ref_bundles()

            iterator = iterate()
        else:
            capture.executor = StreamingExecutor(context)
            produce, consume = _transforms(args, gate_name)
            bundles = []
            for input_id in range(args.num_input_blocks):
                block = pa.table({"id": [input_id]})
                accessor = BlockAccessor.for_block(block)
                bundles.append(RefBundle(
                    [BlockEntry(ray.put(block), accessor.get_metadata())],
                    schema=accessor.schema(), owns_blocks=False,
                ))
            source = InputDataBuffer(context, bundles)
            for name, fn, concurrency in (
                ("Produce", produce, args.producer_concurrency), ("Consume", consume, 1)
            ):
                source = MapOperator.create(
                    MapTransformer([BlockMapTransformFn(fn, disable_block_shaping=True)]),
                    source, context, name=name, supports_fusion=False,
                    compute_strategy=TaskPoolStrategy(size=concurrency),
                    ray_remote_args={
                        "num_cpus": 1, "max_retries": 1, "retry_exceptions": False,
                    },
                    ray_remote_args_fn=_placement(config.executor_node_ids),
                )
                capture.operators.append(source)
            iterator = capture.executor.execute(source)
        expected = args.num_input_blocks * _blocks_per_producer(args)

        def drain():
            # Only fetch tiny status blocks; never collect the producer's arrays.
            output_count = 0
            producer_nodes, consumer_nodes = set(), set()
            for bundle in iterator:
                for ref in bundle.block_refs:
                    result = ray.get(ref).to_pydict()
                    if original_workload:
                        expected_rows = args.recovery_producer_block_rows[
                            output_count % _blocks_per_producer(args)
                        ]
                        if (
                            result["status"] != ["ok"]
                            or result["task_index"] != [output_count]
                            or result["rows"] != [expected_rows]
                            or result["consumer_node"] != [
                                config.executor_for_task(output_count)
                            ]
                        ):
                            raise ValueError(f"Original workload output mismatch at {output_count}")
                        consumer_nodes.update(result["consumer_node"])
                        output_count += 1
                        progress.record(output_count)
                        continue
                    input_id, block_index = divmod(
                        output_count, args.output_batches_per_input_batch
                    )
                    if (
                        result["status"] != ["ok"] or result["input_id"] != [input_id]
                        or result["block_index"] != [block_index]
                        or result["rows"] != [args.output_batch_rows]
                    ):
                        raise ValueError(
                            f"Wrong, missing, or duplicate output at {output_count}"
                        )
                    expected_producer = config.executor_for_task(input_id)
                    expected_consumer = config.executor_for_task(output_count)
                    if (
                        result["producer_node"] != [expected_producer]
                        or result["consumer_node"] != [expected_consumer]
                    ):
                        raise ValueError("Task or replay ran outside its selected executor")
                    producer_nodes.update(result["producer_node"])
                    consumer_nodes.update(result["consumer_node"])
                    output_count += 1
                    progress.record(output_count)
            if output_count != expected:
                raise ValueError(f"Got {output_count} outputs, expected {expected}")
            progress.finished_at = time.monotonic()
            return {
                "validated_output_blocks": output_count,
                **({} if original_workload else {
                    "observed_producer_nodes": sorted(producer_nodes),
                }),
                "observed_consumer_nodes": sorted(consumer_nodes),
            }

        if progress_failure:
            result = _drain_with_progress_failure(
                args, capture, progress, drain, crash_owner, failure_metrics,
            )
        elif failure:
            with ThreadPoolExecutor(max_workers=1) as pool:
                drained = pool.submit(drain)
                try:
                    deadline = time.monotonic() + args.recovery_timeout_s
                    wave = min(args.producer_concurrency, args.num_input_blocks)
                    while True:
                        if drained.done():
                            drained.result()
                            raise RuntimeError("Execution ended before the failure gate")
                        arrivals = ray.get(
                            gate.snapshot.remote(), timeout=args.recovery_timeout_s
                        )
                        operators = capture.operators
                        enrolled = [op.metrics.extra_metrics["fixed_r_enrolled_tasks"]
                                    for op in operators]
                        if (
                            len(operators) == 2
                            and len(arrivals["Produce"]) == wave
                            and len(arrivals["Consume"]) == 1
                            and len(operators[0].get_active_tasks()) == wave
                            and len(operators[1].get_active_tasks()) == 1
                            and enrolled == [wave, 1]
                        ):
                            break
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Initial enrolled wave did not reach failure gate")
                        time.sleep(0.05)
                    failure_metrics["enrolled_at_failure"] = wave + 1
                    marker = (
                        "FIXED_R_HEAD_FAILURE_READY" if
                        args.recovery_mode == "fixed_r_head_failure" else
                        "FIXED_R_OWNER_FAILURE_READY"
                    )
                    print(
                        f"{marker} "
                        f"owner_node_id={config.owner_node_id}", flush=True,
                    )
                    failure_start = time.monotonic()
                    if crash_owner is not None:
                        replacement_metrics = crash_owner()
                        if replacement_metrics is not None:
                            failure_metrics.update(replacement_metrics)
                    # On external clusters the operator stops ONLY the printed
                    # owner node now. Never execute a shell kill on a remote host.
                    deadline = time.monotonic() + args.recovery_timeout_s
                    while not any(
                        node["NodeID"] == config.owner_node_id and not node["Alive"]
                        for node in ray.nodes()
                    ):
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Protected owner was not reported dead by GCS")
                        time.sleep(0.05)
                    failure_metrics["failure_request_to_gcs_dead_s"] = (
                        time.monotonic() - failure_start
                    )
                    ray.get(gate.open.remote(), timeout=args.recovery_timeout_s)
                    result = drained.result()
                    failure_metrics["failure_request_to_drain_s"] = (
                        time.monotonic() - failure_start
                    )
                finally:
                    try:
                        ray.get(gate.open.remote(), timeout=args.recovery_timeout_s)
                    finally:
                        if capture.executor is not None:
                            capture.executor.shutdown(force=True)
        else:
            result = drain()
        operators = capture.operators
        metrics = {name: _operator_metrics(op)
                   for name, op in zip(("Produce", "Consume"), operators)}
        for name, tasks, blocks in (
            ("Produce", args.num_input_blocks, expected), ("Consume", expected, expected)
        ):
            actual = metrics[name]
            expected_rows = (
                args.num_input_blocks * args.output_batches_per_input_batch
                * args.output_batch_rows
                if name == "Produce" else expected
            )
            if actual["output_rows"] != expected_rows:
                raise ValueError(f"Output row count mismatch for {name}: {actual}")
            actual_counts = (
                actual["tasks_submitted"], actual["tasks_finished"],
                actual["tasks_failed"], actual["output_blocks"],
            )
            if actual_counts != (tasks, tasks, 0, blocks):
                raise ValueError(f"Physical count mismatch for {name}: {actual}")
            if args.recovery_mode != "ordinary" and (
                actual["fixed_r_closed_streams"] != tasks
                or actual["fixed_r_copied_blocks"] != blocks
            ):
                raise ValueError(f"Copy/retirement count mismatch for {name}: {actual}")
            if progress_failure:
                enrolled = actual["fixed_r_enrolled_tasks"]
                recovered = actual["fixed_r_recovered_tasks"]
                details = actual["fixed_r_recovered_task_details"]
                if (
                    enrolled + actual["fixed_r_survivor_tasks"] != tasks
                    or not 0 <= recovered <= enrolled <= tasks
                    or actual["fixed_r_copy_baseline_tasks"] != 0
                    or not (
                        0 <= actual["fixed_r_pre_submission_failovers"]
                        <= actual["fixed_r_survivor_tasks"]
                    )
                    or len(details) != recovered
                    or len({item["task_index"] for item in details}) != recovered
                    or len({item["task_id"] for item in details}) != recovered
                    or any(not 0 <= item["task_index"] < tasks for item in details)
                ):
                    raise ValueError(f"Submission/recovery accounting mismatch for {name}: {actual}")
                actual["enrolled_completed_without_replay"] = enrolled - recovered
            elif args.recovery_mode != "ordinary":
                protected = (
                    (wave if name == "Produce" else 1) if failure else
                    (tasks if args.recovery_mode == "fixed_r" else 0)
                )
                expected_counts = {
                    "fixed_r_enrolled_tasks": protected,
                    "fixed_r_recovered_tasks": protected if failure else 0,
                    "fixed_r_survivor_tasks": tasks - protected if failure else 0,
                    "fixed_r_copy_baseline_tasks": tasks if args.recovery_mode == "copy" else 0,
                }
                if any(actual[key] != value for key, value in expected_counts.items()):
                    raise ValueError(f"Submission/recovery count mismatch for {name}: {actual}")
        if progress_failure and not any(
            op["fixed_r_recovered_tasks"] for op in metrics.values()
        ):
            raise RuntimeError("Head was replaced but no task replayed; failure timing missed recovery")
        return {
            **vars(args), **result, **failure_metrics, "operators": metrics,
            "workload_variant": (
                "original_udfs_shaped_map_batches" if original_workload else
                "public_dataset_unshaped_map_batches" if public_dataset else
                "unshaped_physical_task_map_chain"
            ),
            "physical_operator_names": [op.name for op in operators],
            "declared_blocks_per_task": (
                recovery_config.expected_blocks if recovery_config else config.expected_blocks
            ),
            "logical_producer_payload_bytes": (
                args.num_input_blocks * args.output_batches_per_input_batch
                * args.output_batch_rows * args.output_row_bytes
            ),
            "block_shaping_enabled": original_workload,
            "fusion_enabled": False,
            "output_validation": (
                "consumer_task_indices_and_calibrated_row_counts" if original_workload
                else "producer_input_and_batch_indices_and_row_counts"
            ),
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
        }
    finally:
        if diagnostics is not None:
            diagnostics.update(failure_metrics)
            diagnostics["last_observation"] = _failure_observation(capture, progress)
        try:
            if capture.executor is not None:
                capture.executor.shutdown(force=True)
        finally:
            if gate is not None:
                ray.kill(gate, no_restart=True)
