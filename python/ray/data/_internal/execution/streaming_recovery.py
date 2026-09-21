"""Experimental Fixed-R Data execution with coordinator-owned output copies.

Only finite task-map chains with declared physical output counts are supported.
No original protected output is exported: copy one block/metadata pair before
emitting it, then release the original refs. This deliberately adds a data copy.
"""

import math
from dataclasses import dataclass
from typing import Dict, Tuple, Union

import ray
from ray._private.streaming_recovery import (
    StreamingRecoveryCountError,
    StreamingRecoveryOwnerActor,
    StreamingRecoveryReader,
    StreamingRecoveryRequired,
)
from ray._raylet import _inspect_recovery_stream_descriptor
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    TaskGeneratorState,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


CONFIG_KEY = "streaming_recovery_fixed_r_copy"


@dataclass(frozen=True)
class FixedRDataConfig:
    """Private, explicit opt-in to the copied-output experiment.

    Counts name every physical task-map operator and declare its final blocks
    per task, after shaping. Fusion is disabled in both modes. ``copy`` is the
    comparison path: identical counts, task placement and copying, with ordinary
    coordinator-owned task submission. ``fixed_r`` enrolls tasks on a separate
    owner until that node is authoritatively dead, then submits subsequent tasks
    from the surviving coordinator. This covers only one owner-node failure.

    ``executor_node_id`` accepts a single node ID or an ordered tuple of IDs.
    Each operator assigns task i to tuple[i % len(tuple)] with hard affinity;
    replay retains that assignment. Every listed executor must survive.

    ``preserve_batch_output_blocks`` opts public ``map_batches`` into one block
    per UDF output batch, without output coalescing or splitting. It requires
    synchronous task UDFs and ``batch_size=None`` (one input block per task).
    Counts then include empty output batches; an empty input block bypasses the
    UDF under normal Dataset semantics, so callers must account for that too.
    """

    owner_node_id: str
    executor_node_id: Union[str, Tuple[str, ...]]
    expected_blocks: Dict[str, int]
    mode: str = "fixed_r"
    timeout_s: float = 60
    preserve_batch_output_blocks: bool = False

    @property
    def executor_node_ids(self):
        """Ordered surviving executors; a string preserves the single-node API."""
        if isinstance(self.executor_node_id, str):
            return (self.executor_node_id,)
        return self.executor_node_id

    def executor_for_task(self, task_index):
        # Selection happens once at submission. The immutable recipe keeps the
        # same hard affinity during replay, even as other tasks are submitted.
        nodes = self.executor_node_ids
        return nodes[task_index % len(nodes)]

    def validate(self):
        if type(self.preserve_batch_output_blocks) is not bool:
            raise ValueError("preserve_batch_output_blocks must be a bool")
        if self.mode not in ("fixed_r", "copy"):
            raise ValueError("Fixed-R Data mode must be 'fixed_r' or 'copy'")
        if (
            not isinstance(self.timeout_s, (int, float))
            or not math.isfinite(self.timeout_s)
            or self.timeout_s <= 0
        ):
            raise ValueError("Fixed-R Data timeout must be finite and positive")
        if not isinstance(self.expected_blocks, dict) or not self.expected_blocks:
            raise ValueError("Declare expected_blocks for every task-map operator")
        for name, count in self.expected_blocks.items():
            if (
                not isinstance(name, str)
                or not name
                or type(count) is not int
                or not 0 <= count < 2**62
            ):
                raise ValueError(
                    "Expected blocks must map operator names to nonnegative ints"
                )
        executors = self.executor_node_ids
        if not isinstance(executors, tuple) or not executors:
            raise ValueError("Executors must be a node ID or a nonempty tuple of node IDs")
        for node_id in (self.owner_node_id, *executors):
            if not isinstance(node_id, str):
                raise ValueError("Node IDs must be hexadecimal strings")
            try:
                parsed = ray.NodeID.from_hex(node_id)
            except (ValueError, TypeError) as exc:
                raise ValueError("Invalid Fixed-R Data node ID") from exc
            if parsed.is_nil():
                raise ValueError("Fixed-R Data node IDs must be non-nil")
        if len(executors) != len(set(executors)):
            raise ValueError("Fixed-R Data executor node IDs must be unique")
        if self.owner_node_id in executors:
            raise ValueError("Protected owner and task executor must be separate nodes")


def get_config(context):
    config = context.get_config(CONFIG_KEY)
    if config is not None:
        if not isinstance(config, FixedRDataConfig):
            raise ValueError(f"{CONFIG_KEY} must contain FixedRDataConfig")
        config.validate()
        if context.eager_free:
            raise ValueError("Fixed-R Data requires eager_free=False for retained inputs")
        if context.retried_map_errors or context.max_errored_blocks != 0:
            raise ValueError("Fixed-R Data requires no UDF retries or ignored block errors")
    return config


def validate_execution(dag, context):
    """Reject unsupported plans and misspelled declarations before dispatch."""
    config = get_config(context)
    if config is None:
        return
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.operators.task_pool_map_operator import (
        TaskPoolMapOperator,
    )

    names = []
    op = dag
    while isinstance(op, TaskPoolMapOperator):
        if (
            len(op.input_dependencies) != 1
            or get_config(op.data_context) != config
            or op._streaming_recovery_config != config
            or op.supports_fusion()
        ):
            raise ValueError("Fixed-R Data requires one consistently configured map chain")
        names.append(op.name)
        op = op.input_dependencies[0]
    if not isinstance(op, InputDataBuffer):
        raise ValueError("Fixed-R Data supports only InputDataBuffer -> task-map chains")
    if len(names) != len(set(names)) or set(names) != set(config.expected_blocks):
        raise ValueError(
            f"Declare each physical operator exactly once: actual={names}, "
            f"declared={list(config.expected_blocks)}"
        )
    alive = _owner_alive(config)
    if config.mode == "fixed_r" and not alive:
        raise ValueError("The protected owner must be alive when Dataset execution starts")


def _owner_alive(config):
    nodes = {node["NodeID"]: node for node in ray.nodes()}
    if any(node_id not in nodes for node_id in (
        config.owner_node_id, *config.executor_node_ids
    )):
        raise ValueError("Fixed-R Data owner/executor node is unknown to GCS")
    if any(not nodes[node_id]["Alive"] for node_id in config.executor_node_ids):
        raise ValueError("Fixed-R Data requires all configured task executors to survive")
    if ray.get_runtime_context().get_node_id() == config.owner_node_id:
        raise ValueError("The Dataset coordinator must survive on a different node")
    return nodes[config.owner_node_id]["Alive"]


class _DataStream:
    """One enrolled reader or ordinary surviving-coordinator generator."""

    def __init__(self, expected_returns, stats, reader=None, generator=None, owner=None):
        self.reader = reader
        self.generator = generator
        self.owner = owner
        self.expected_returns = expected_returns
        self.next_index = 0
        self.stats = stats
        self.closed = False
        if reader is not None:
            info = _inspect_recovery_stream_descriptor(reader.descriptor)
            self.task_id = ray.TaskID(info["task_id"])
        else:
            self.task_id = generator.completed().task_id()

    def waitable(self):
        if self.reader is not None:
            return self.reader.get_waitable()
        if self.generator._stream_exhausted():
            return self.generator.completed()
        return self.generator

    def poll(self):
        if self.reader is not None:
            ref = self.reader.poll_next()
        else:
            if self.generator._stream_exhausted():
                ready, _ = ray.wait(
                    [self.generator.completed()], timeout=0, fetch_local=True
                )
                if not ready:
                    return None
            try:
                ref = self.generator._next_sync(timeout_s=0)
            except StopIteration:
                if self.next_index != self.expected_returns:
                    raise StreamingRecoveryCountError("Map task ended before declared count")
                raise
            if ref.is_nil():
                return None
            if ref == self.generator.completed():
                ray.get(ref)
            if self.next_index >= self.expected_returns:
                raise StreamingRecoveryCountError("Map task exceeded declared count")
        if ref is not None:
            self.next_index += 1
        return ref

    def recover(self):
        if self.reader is None:
            raise RuntimeError("A survivor-owned Data task cannot use owner-loss replay")
        self.reader.recover()
        self.stats["fixed_r_recovered_tasks"] += 1

    def release_pair(self, first_index):
        if self.reader is not None:
            self.reader.release(first_index)
            self.reader.release(first_index + 1)

    def close(self):
        if self.closed:
            return
        if self.reader is not None:
            # Do not kill the helper until the durable tombstone barrier succeeds.
            self.reader.close()
            ray.kill(self.owner, no_restart=True)
        elif self.generator is not None:
            ray.cancel(self.generator, force=False, recursive=True)
            self.generator = None
        self.closed = True
        self.stats["fixed_r_closed_streams"] += 1


def submit_stream(
    config, producer, args, kwargs, options, expected_blocks, stats, *, task_index=0
):
    owner_alive = _owner_alive(config)
    inputs = tuple(
        value for value in (*args, *kwargs.values())
        if isinstance(value, ray.ObjectRef)
    )
    ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs(inputs)
    options = dict(options)
    options.update(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            config.executor_for_task(task_index), soft=False
        ),
        max_retries=1,
        retry_exceptions=False,
    )
    count = 2 * expected_blocks
    if config.mode == "copy" or not owner_alive:
        generator = producer.options(**options).remote(*args, **kwargs)
        key = (
            "fixed_r_copy_baseline_tasks" if config.mode == "copy"
            else "fixed_r_survivor_tasks"
        )
        stats[key] += 1
        return _DataStream(count, stats, generator=generator)

    owner = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(
        StreamingRecoveryOwnerActor
    ).options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            config.owner_node_id, soft=False
        )
    ).remote()
    try:
        reader = StreamingRecoveryReader.submit(
            owner, producer, expected_returns=count, args=args, kwargs=kwargs,
            timeout_s=config.timeout_s, **options,
        )
    except BaseException:
        # Reader.submit already queues close for an abandoned offer. Give it a
        # bounded opportunity to execute before retiring the private helper.
        try:
            ray.get(owner.close.remote(), timeout=config.timeout_s)
        finally:
            ray.kill(owner, no_restart=True)
        raise
    stats["fixed_r_enrolled_tasks"] += 1
    return _DataStream(count, stats, reader=reader, owner=owner)


def new_metrics():
    return dict.fromkeys((
        "fixed_r_enrolled_tasks", "fixed_r_survivor_tasks", "fixed_r_copy_baseline_tasks",
        "fixed_r_recovered_tasks", "fixed_r_copied_blocks", "fixed_r_closed_streams",
    ), 0)


class StreamingRecoveryDataOpTask(DataOpTask):
    # The executor checks this before get_waitable(), which can return native
    # backpressure credit by starting an owner read.
    requires_output_budget_before_wait = True

    def __init__(self, task_index, stream, *args, **kwargs):
        super().__init__(task_index, stream, *args, **kwargs)
        self.stream = stream
        self._pair_start = 0
        self._cancelled_ref = None

    def get_task_id(self):
        return self.stream.task_id

    def get_waitable(self):
        if self._cancelled_ref is not None:
            return self._cancelled_ref
        if not self._pending_meta_ref.is_nil():
            return self._pending_meta_ref
        return self.stream.waitable()

    def _cancel(self, force):
        self.stream.close()
        self._clear_pair()
        # PhysicalOperator's force-shutdown loop waits on this after _cancel.
        self._cancelled_ref = ray.put(None)

    def _clear_pair(self):
        self._pending_block_ref = ray.ObjectRef.nil()
        self._pending_meta_ref = ray.ObjectRef.nil()

    def _copy_pair_if_ready(self):
        # This method returns only the independent copy and metadata bytes. Its
        # temporary original refs/values must leave scope before release/recovery.
        refs = [self._pending_block_ref, self._pending_meta_ref]
        ready, _ = ray.wait(refs, num_returns=2, timeout=0, fetch_local=True)
        if len(ready) != 2:
            return None
        block, metadata = ray.get(refs, timeout=0)
        copied = ray.put(block)
        # Reject contained refs/tensor transport before any export to Data.
        ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs(
            [copied]
        )
        return copied, metadata

    def _finish(self, error=None):
        self.stream.close()
        self._clear_pair()
        self._task_error = error
        self._state = TaskGeneratorState.DRAINED
        # Original refs never enter the threaded metadata fetcher. All emitted
        # copies are independent, so completion can fire synchronously.
        self.mark_done()

    def on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        self._track_task_output_backpressure(max_bytes_to_read)
        if self.has_finished or max_bytes_to_read == 0:
            return 0
        try:
            if self._pending_block_ref.is_nil():
                self._pair_start = self.stream.next_index
                try:
                    ref = self.stream.poll()
                except StopIteration:
                    self._finish()
                    return 0
                if ref is None:
                    return 0
                self._pending_block_ref = ref
                del ref
            if self._pending_meta_ref.is_nil():
                try:
                    ref = self.stream.poll()
                except StopIteration as exc:
                    raise StreamingRecoveryCountError(
                        "Map stream ended between block and metadata"
                    ) from exc
                if ref is None:
                    return 0
                self._pending_meta_ref = ref
                del ref
            try:
                copied_pair = self._copy_pair_if_ready()
            except ray.exceptions.GetTimeoutError:
                return 0
            if copied_pair is None:
                return 0
            self._clear_pair()
            self.stream.release_pair(self._pair_start)
            copied_ref, metadata = copied_pair
            # Let distributed reference counting retire copies. Data's
            # owns_blocks=True permits explicit free even if an exported alias
            # remains live; it is distinct from native coordinator ownership.
            size = self.produce_block(copied_ref, metadata, owns_blocks=False)
            self.stream.stats["fixed_r_copied_blocks"] += 1
            return size  # At most one complete block per scheduling pass.
        except (StreamingRecoveryRequired, ray.exceptions.OwnerDiedError):
            # No original output was exported, no metadata background get was
            # submitted, and synchronous gets have settled before reaching here.
            # Previously emitted copies and their downstream users can continue.
            self.stream.recover()
            return 0
        except Exception as exc:
            self._finish(error=exc)
            raise
