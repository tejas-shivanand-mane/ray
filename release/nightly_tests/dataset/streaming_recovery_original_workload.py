"""Original backpressure UDFs with normal block shaping and finite-count recovery.

The producer's data is unchanged. The consumer adds small validation fields to
its status result. Fusion, placement, output copying, and driver survival still
follow the Fixed-R adapter; this is not an unchanged benchmark deployment.
"""

import time

import ray
from ray import cloudpickle
from ray.data import DataContext
from ray.data.block import BlockAccessor, _apply_batch_format
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import BatchMapTransformFn
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_map_batches,
)


def original_workload_udfs(args):
    import backpressure_benchmark as original

    # Workers need not have the release benchmark directory on PYTHONPATH.
    # Freeze the actual original functions by value, not a reimplementation.
    registered = original.__name__ in cloudpickle.list_registry_pickle_by_value()
    if not registered:
        cloudpickle.register_pickle_by_value(original)
    try:
        producer_bytes = cloudpickle.dumps(original.produce)
        consumer_bytes = cloudpickle.dumps(original.consume_slow)
    finally:
        if not registered:
            cloudpickle.unregister_pickle_by_value(original)
    producer_kwargs = dict(
        output_batches_per_input_batch=args.output_batches_per_input_batch,
        output_batch_rows=args.output_batch_rows,
        output_row_bytes=args.output_row_bytes,
    )
    sleep_s = args.consumer_sleep_s

    def produce(batch):
        yield from cloudpickle.loads(producer_bytes)(batch, **producer_kwargs)

    def consume(batch):
        result = cloudpickle.loads(consumer_bytes)(batch, sleep_s=sleep_s)
        if result != {"status": ["ok"]}:
            raise ValueError("Original consumer status changed")
        # Only the tiny final status is instrumented. Producer payloads pass
        # through normal NumPy batching and block coalescing/splitting unchanged.
        return {
            **result,
            "task_index": [TaskContext.get_current().task_idx],
            "rows": [len(batch["data"])],
            "consumer_node": [ray.get_runtime_context().get_node_id()],
        }

    return produce, consume


def prepare_original_workload(args):
    """Measure one input-independent producer task before timed execution.

    Use Ray's actual batch transformer/output buffer with the original defaults.
    Discard payload blocks incrementally and retain only their row counts. A
    mismatch during real execution fails the native declared-count checks or
    consumer row checks; calibration never authorizes a partial success.
    """
    import backpressure_benchmark as original

    if args.recovery_plan != "dataset" or args.head_failure_point == "gated":
        raise ValueError("Original workload requires Dataset progress-triggered failure")
    for name in (
        "num_input_blocks", "output_batches_per_input_batch",
        "output_batch_rows", "output_row_bytes",
    ):
        if type(getattr(args, name)) is not int or getattr(args, name) <= 0:
            raise ValueError(f"{name} must be a positive integer")
    started = time.monotonic()
    context = DataContext.get_current().copy()
    target = context.target_max_block_size
    produce, _ = original_workload_udfs(args)
    transformer = BatchMapTransformFn(
        _generate_transform_fn_for_map_batches(produce),
        batch_size=None, batch_format=_apply_batch_format("default"), zero_copy_batch=True,
        output_block_size_option=OutputBlockSizeOption.of(target_max_block_size=target),
    )
    rows = []
    with DataContext.current(context):
        for block in transformer(original.make_inputs(1), TaskContext(0, "ProduceProbe")):
            rows.append(BlockAccessor.for_block(block).num_rows())
            del block
    if not rows or any(count <= 0 for count in rows) or sum(rows) != (
        args.output_batches_per_input_batch * args.output_batch_rows
    ):
        raise ValueError("Original producer calibration lost or added rows")
    if args.num_input_blocks * len(rows) < 10:
        raise ValueError("Progress-triggered failure requires at least 10 shaped outputs")
    args.recovery_blocks_per_producer = len(rows)
    args.recovery_producer_block_rows = rows
    args.recovery_target_max_block_size = target
    args.recovery_calibration_s = time.monotonic() - started
    print(
        f"FIXED_R_ORIGINAL_WORKLOAD batches={args.output_batches_per_input_batch} "
        f"physical_blocks_per_producer={len(rows)} rows={rows} "
        f"expected_outputs={args.num_input_blocks * len(rows)}", flush=True,
    )
