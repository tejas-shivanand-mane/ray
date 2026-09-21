"""Controlled Fixed-R variant of fast-producer-slow-consumer.

Uses the real Data streaming executor, with explicit one-block input bundles and
block shaping disabled. This is a workload adaptation, not the unchanged public
Dataset benchmark. No large output stream is materialized on the driver.
"""

import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import ray

from ray.data import DataContext, TaskPoolStrategy
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
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
    if args.recovery_mode not in MODES:
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
    if args.recovery_mode == "fixed_r_failure":
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
        **{key: value for key, value in metrics.extra_metrics.items()
           if key.startswith("fixed_r_")},
    }


def run_controlled(args, crash_owner=None):
    config = validate_args(args)
    failure = args.recovery_mode == "fixed_r_failure"
    context = DataContext.get_current().copy()
    context.eager_free = False
    context.retried_map_errors = False
    context.max_errored_blocks = 0
    context.enable_progress_bars = False
    context.execution_options.preserve_order = True
    context._max_num_blocks_in_streaming_gen_buffer = 1
    recovery_config = None
    if args.recovery_mode != "ordinary":
        recovery_config = FixedRDataConfig(
            config.owner_node_id, config.executor_node_ids, config.expected_blocks,
            "copy" if args.recovery_mode == "copy" else "fixed_r",
            args.recovery_timeout_s,
        )
    context.set_config(CONFIG_KEY, recovery_config)
    gate_name = "fixed-r-" + uuid.uuid4().hex if failure else None
    gate = _make_gate(gate_name, args.recovery_timeout_s) if failure else None
    executor = StreamingExecutor(context)
    try:
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
        operators = []
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
            operators.append(source)
        iterator = executor.execute(source)
        expected = args.num_input_blocks * args.output_batches_per_input_batch

        def drain():
            # Only fetch tiny status blocks; never collect the producer's arrays.
            output_count = 0
            producer_nodes, consumer_nodes = set(), set()
            for bundle in iterator:
                for ref in bundle.block_refs:
                    result = ray.get(ref).to_pydict()
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
            if output_count != expected:
                raise ValueError(f"Got {output_count} outputs, expected {expected}")
            return {
                "validated_output_blocks": output_count,
                "observed_producer_nodes": sorted(producer_nodes),
                "observed_consumer_nodes": sorted(consumer_nodes),
            }

        failure_metrics = {}
        if failure:
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
                        enrolled = [op.metrics.extra_metrics["fixed_r_enrolled_tasks"]
                                    for op in operators]
                        if (
                            len(arrivals["Produce"]) == wave
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
                    print(
                        "FIXED_R_OWNER_FAILURE_READY "
                        f"owner_node_id={config.owner_node_id}", flush=True,
                    )
                    failure_start = time.monotonic()
                    if crash_owner is not None:
                        crash_owner()
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
                        executor.shutdown(force=True)
        else:
            result = drain()
        metrics = {op.name: _operator_metrics(op) for op in operators}
        for name, tasks, blocks in (
            ("Produce", args.num_input_blocks, expected), ("Consume", expected, expected)
        ):
            actual = metrics[name]
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
            if args.recovery_mode != "ordinary":
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
        return {
            **vars(args), **result, **failure_metrics, "operators": metrics,
            "workload_variant": "unshaped_physical_task_map_chain",
            "declared_blocks_per_task": config.expected_blocks,
            "logical_producer_payload_bytes": (
                expected * args.output_batch_rows * args.output_row_bytes
            ),
            "coordinator_node_id": ray.get_runtime_context().get_node_id(),
        }
    finally:
        try:
            executor.shutdown(force=True)
        finally:
            if gate is not None:
                ray.kill(gate, no_restart=True)
