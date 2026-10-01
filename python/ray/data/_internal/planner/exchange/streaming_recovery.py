"""Fixed-R adapters for finite, non-keyed exchange tasks.

Keep the existing shuffle/split algorithms, but submit their outputs as streams.
Only coordinator-owned copies cross a phase boundary. At most one task per
configured executor is enrolled at a time; the blocking shuffle still retains
its intermediate partitions in the object store, just like pull-based shuffle.
"""

import time

from ray import cloudpickle
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.execution.streaming_recovery import (
    StreamingRecoveryDataOpTask,
    submit_stream,
)
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskScheduler
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import BlockMetadataWithSchema, to_stats


def _map_outputs(spec, index, block, output_count):
    outputs = spec.map(index, block, output_count, *spec._map_args)
    # The ordinary map task reports whole-input statistics. Each streamed
    # partition needs its own size/schema for copying and downstream accounting.
    for index, block in enumerate(outputs[:-1]):
        yield block
        yield cloudpickle.dumps(BlockMetadataWithSchema.from_block(
            block, block_exec_stats=outputs[-1].exec_stats if index == 0 else None
        ))


def _reduce_outputs(spec, *blocks):
    block, metadata = spec.reduce(*spec._reduce_args, *blocks)
    yield block
    yield cloudpickle.dumps(metadata)


def _split_outputs(block_id, block, metadata, indices):
    from ray.data._internal.split import _split_single_block

    # The original helper appends an end index. Never mutate a replay recipe.
    (_, metas), *blocks = _split_single_block(
        block_id, block, metadata, list(indices)
    )
    for block, meta in zip(blocks, metas):
        yield block
        yield cloudpickle.dumps(BlockMetadataWithSchema.from_metadata(
            meta, schema=BlockMetadataWithSchema.from_block(block).schema
        ))


class _ExchangeTask(StreamingRecoveryDataOpTask):
    """Use Data's copy/release/replay protocol without its output queue."""

    def __init__(self, index, stream, output):
        super().__init__(index, stream, None, "exchange")
        self.output = output

    def produce_block(self, block_ref, meta_bytes, *, owns_blocks=True):
        metadata = cloudpickle.loads(meta_bytes)
        self.output.append((block_ref, metadata))
        self._last_block_meta = metadata.metadata
        return metadata.metadata.size_bytes or 0


def run_exchange_tasks(config, producer_fn, calls, options, metrics):
    """Return copied (block, metadata) pairs in task order, never finish order.

    ``calls`` yields (top-level arguments, expected block count). ObjectRef inputs
    must stay top-level so submit_stream can retain and validate independent copies.
    The normal Data task adapter provides the native reference-release barriers.
    """
    if not config.dynamic_task_outputs:
        raise ValueError("Fixed-R exchanges require streaming output mode")
    producer = cached_remote_fn(producer_fn)
    options = dict(options or {})
    if "num_returns" in options:
        raise ValueError("Fixed-R exchanges control their own streaming returns")
    options["_generator_backpressure_num_objects"] = 2
    pending = iter(enumerate(calls))
    active = {}
    outputs = []
    exhausted = False
    try:
        while active or not exhausted:
            while not exhausted and len(active) < len(config.executor_node_ids):
                try:
                    index, (args, count) = next(pending)
                except StopIteration:
                    exhausted = True
                    break
                output = []
                outputs.append(output)
                stream = submit_stream(
                    config, producer, args, {}, options, count, metrics,
                    task_index=index,
                )
                active[index] = (_ExchangeTask(index, stream, output), count)
            progressed = False
            for index, (task, count) in list(active.items()):
                before = len(task.output)
                task.on_data_ready(float("inf"), None)
                progressed |= len(task.output) != before
                if task.has_finished:
                    if len(task.output) != count:
                        raise ValueError("Fixed-R exchange returned an unexpected count")
                    del active[index]
                    progressed = True
            if active and not progressed:
                time.sleep(0.01)
    finally:
        # Try every retirement even if one close fails. Do not silently leave an
        # enrolled task alive when another producer or the consumer fails.
        close_error = None
        for task, _ in active.values():
            try:
                task.stream.close()
            except Exception as exc:
                close_error = exc
        if close_error is not None:
            raise close_error
    return outputs


def split_blocks(
    config, metrics, blocks_with_metadata, indices, owned, label_selector=None
):
    from ray.data._internal.split import _drop_empty_block_split

    # Preserve the original split algorithm, including no-op and empty splits.
    results = [None] * len(blocks_with_metadata)
    calls, task_blocks = [], []
    for index, ((block, meta), cuts) in enumerate(zip(blocks_with_metadata, indices)):
        cuts = _drop_empty_block_split(cuts, meta.num_rows)
        if cuts:
            calls.append(((index, block, meta, cuts), len(cuts) + 1))
            task_blocks.append(index)
        else:
            results[index] = [(block, meta)]
    options = {"label_selector": label_selector} if label_selector else {}
    outputs = run_exchange_tasks(config, _split_outputs, calls, options, metrics)
    for index, output in zip(task_blocks, outputs):
        results[index] = [(block, meta.metadata) for block, meta in output]
    return (pair for result in results for pair in result)


class FixedRShuffleTaskScheduler(ExchangeTaskScheduler):
    """Protected pull-based map/reduce for random shuffle and repartition."""

    def __init__(self, spec, config):
        super().__init__(spec)
        self.config = config

    def execute(
        self, refs, output_num_blocks, task_ctx, map_ray_remote_args=None,
        reduce_ray_remote_args=None, _debug_limit_execution_to_num_blocks=None,
    ):
        if _debug_limit_execution_to_num_blocks is not None:
            raise ValueError("Fixed-R exchanges do not support debug truncation")
        metrics = task_ctx.kwargs["fixed_r_metrics"]
        inputs = [block for bundle in refs for block in bundle.block_refs]
        maps = run_exchange_tasks(
            self.config, _map_outputs,
            (((self._exchange_spec, i, block, output_num_blocks), output_num_blocks)
             for i, block in enumerate(inputs)),
            map_ray_remote_args, metrics,
        )
        reduces = run_exchange_tasks(
            self.config, _reduce_outputs,
            (((self._exchange_spec, *(output[j][0] for output in maps)), 1)
             for j in range(output_num_blocks)),
            reduce_ray_remote_args, metrics,
        ) if maps else []
        output = [
            RefBundle([BlockEntry(block, meta.metadata)], owns_blocks=False,
                      schema=meta.schema)
            for result in reduces for block, meta in result
        ]
        return output, {
            "map": to_stats([meta for result in maps for _, meta in result]),
            "reduce": to_stats([meta for result in reduces for _, meta in result]),
        }
