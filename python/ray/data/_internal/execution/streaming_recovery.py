"""Experimental Fixed-R Data execution with coordinator-owned output copies.

Finite task-map chains support dynamic-count streaming, declared physical output
counts, or bounded task envelopes. DataContext.enable_fixed_r_task_recovery uses
dynamic-count streaming by default. No original protected output is exported:
copies remain owned by the surviving coordinator.
"""

import math
import sys
import time
import traceback
from collections import deque
from dataclasses import dataclass
from typing import Dict, Tuple, Union

import ray
from ray._private.streaming_recovery import (
    StreamingRecoveryCountError,
    StreamingRecoveryOwnerActor,
    StreamingRecoveryReader,
    StreamingRecoveryRequired,
)
from ray._raylet import (
    _inspect_recovery_stream_descriptor,
    _recovery_stream_return_id,
)
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

    ``buffered_task_outputs`` instead protects each finite task as one bounded
    envelope, including read tasks and zero/multiple physical blocks. It uses
    no operator-name/count declarations and snapshots output before advancing
    the UDF. This mode trades within-task streaming for bounded buffering.

    ``dynamic_task_outputs`` runs normal streaming map/read tasks with unknown
    counts. Blocks are copied and delivered incrementally; EOF supplies the final
    count. Deterministic finite tasks and a surviving coordinator are required.
    """

    owner_node_id: str
    executor_node_id: Union[str, Tuple[str, ...]]
    expected_blocks: Dict[str, int]
    mode: str = "fixed_r"
    timeout_s: float = 60
    preserve_batch_output_blocks: bool = False
    buffered_task_outputs: bool = False
    dynamic_task_outputs: bool = False
    max_task_output_bytes: int = 256 * 1024**2

    @property
    def automatic_outputs(self):
        return self.buffered_task_outputs or self.dynamic_task_outputs

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
        if type(self.buffered_task_outputs) is not bool:
            raise ValueError("buffered_task_outputs must be a bool")
        if type(self.dynamic_task_outputs) is not bool:
            raise ValueError("dynamic_task_outputs must be a bool")
        if self.dynamic_task_outputs and (
            self.buffered_task_outputs or self.preserve_batch_output_blocks or self.expected_blocks
        ):
            raise ValueError("Dynamic recovery requires normal shaping and no block declarations")
        if self.buffered_task_outputs and (
            self.preserve_batch_output_blocks or self.expected_blocks
            or type(self.max_task_output_bytes) is not int or self.max_task_output_bytes <= 0
        ):
            raise ValueError("Buffered recovery requires a positive byte limit and no block declarations")
        if not isinstance(self.expected_blocks, dict) or (
            not self.expected_blocks and not self.automatic_outputs
        ):
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
    if context.enable_fixed_r_task_recovery:
        if context.fixed_r_task_recovery_output_mode not in ("streaming", "buffered"):
            raise ValueError("Fixed-R output mode must be streaming or buffered")
        if config is None:
            from ray._common.constants import HEAD_NODE_RESOURCE_NAME

            nodes = [node for node in ray.nodes() if node["Alive"]]
            heads = [node["NodeID"] for node in nodes
                     if HEAD_NODE_RESOURCE_NAME in node["Resources"]]
            coordinator = ray.get_runtime_context().get_node_id()
            if len(heads) != 1 or coordinator == heads[0]:
                raise ValueError("Fixed-R task recovery requires a surviving non-head Dataset coordinator")
            executors = tuple(sorted(
                node["NodeID"] for node in nodes
                if node["NodeID"] not in (heads[0], coordinator)
                and node["Resources"].get("CPU", 0) > 0
            ))
            if len(executors) < 2:
                raise ValueError("Fixed-R task recovery requires at least two surviving CPU executor nodes")
            config = FixedRDataConfig(
                heads[0], executors, {}, timeout_s=context.fixed_r_task_recovery_timeout_s,
                buffered_task_outputs=context.fixed_r_task_recovery_output_mode == "buffered",
                dynamic_task_outputs=context.fixed_r_task_recovery_output_mode == "streaming",
                max_task_output_bytes=context.fixed_r_task_recovery_max_output_bytes,
            )
            context.set_config(CONFIG_KEY, config)
        if not isinstance(config, FixedRDataConfig) or not config.automatic_outputs:
            raise ValueError("Automatic task recovery cannot use declared-count recovery configuration")
        if config.dynamic_task_outputs != (context.fixed_r_task_recovery_output_mode == "streaming"):
            raise ValueError("Fixed-R output mode conflicts with cached recovery configuration")
        # Retention is a runtime responsibility in this mode, not a UDF change.
        context.eager_free = False
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
    if config.automatic_outputs and not names:
        # materialize() creates a new InputData-only Dataset from independent
        # coordinator-owned copies. It remains readable after owner-head loss.
        return
    if not config.automatic_outputs and (
        len(names) != len(set(names)) or set(names) != set(config.expected_blocks)
    ):
        raise ValueError(
            f"Declare each physical operator exactly once: actual={names}, "
            f"declared={list(config.expected_blocks)}"
        )
    alive = _owner_alive(config)
    if config.mode == "fixed_r" and not alive and not config.automatic_outputs:
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
                if self.expected_returns >= 0 and self.next_index != self.expected_returns:
                    raise StreamingRecoveryCountError("Map task ended before declared count")
                raise
            if ref.is_nil():
                return None
            if ref == self.generator.completed():
                ray.get(ref)
            if self.expected_returns >= 0 and self.next_index >= self.expected_returns:
                raise StreamingRecoveryCountError("Map task exceeded declared count")
        if ref is not None:
            self.next_index += 1
        return ref

    def recover(self):
        if self.reader is None:
            raise RuntimeError("A survivor-owned Data task cannot use owner-loss replay")
        try:
            self.reader.recover()
        except Exception as exc:
            # Record strings/counts before Dataset strips the traceback or
            # shutdown clears the reader. Never mint new ObjectRefs here.
            failure = {
                "task_id": self.task_id.hex(),
                "next_index": self.reader.consumer.next_index,
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
                "retained_returns": {
                    str(index): ref.hex()
                    for index, ref in self.reader.consumer._retained.items()
                },
            }
            try:
                descriptor = self.reader.descriptor
                info = _inspect_recovery_stream_descriptor(descriptor)
                ids = {info["generator_id"].hex()}
                ids.update(_recovery_stream_return_id(descriptor, index).hex()
                           for index in range(self.next_index))
                counts = ray._private.worker.global_worker.core_worker.get_all_reference_counts()
                failure["native_reference_counts"] = {
                    key: value for key, value in counts.items() if key in ids
                }
            except Exception as diagnostic_error:
                failure["diagnostic_error"] = str(diagnostic_error)
            self.stats.setdefault("fixed_r_recovery_errors", []).append(failure)
            raise
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
    if config.automatic_outputs:
        # Retain independent local inputs, including read recipes and inputs
        # originating outside this executor. Native validation below still
        # rejects contained ObjectRefs and other unsupported dependencies.
        def retain(value):
            return (ray.put(ray.get(value, timeout=config.timeout_s))
                    if isinstance(value, ray.ObjectRef) else value)

        args = tuple(retain(value) for value in args)
        kwargs = {key: retain(value) for key, value in kwargs.items()}
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
    count = -1 if config.dynamic_task_outputs else 2 * expected_blocks

    def submit_from_coordinator():
        generator = producer.options(**options).remote(*args, **kwargs)
        key = (
            "fixed_r_copy_baseline_tasks" if config.mode == "copy"
            else "fixed_r_survivor_tasks"
        )
        stats[key] += 1
        return _DataStream(count, stats, generator=generator)

    if config.mode == "copy" or not owner_alive:
        return submit_from_coordinator()

    owner = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(
        StreamingRecoveryOwnerActor
    ).options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            config.owner_node_id, soft=False
        )
    ).remote()
    # Do not ask this helper to create a protected producer until startup has
    # succeeded. A failure here is unambiguously before begin/descriptor/receipt,
    # so there is no protected task or witness offer to abandon or duplicate.
    try:
        ray.get(owner.__ray_ready__.remote(), timeout=config.timeout_s)
        owner_alive = _owner_alive(config)
    except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
        try:
            deadline = time.monotonic() + config.timeout_s
            while _owner_alive(config):
                # An actor error or timeout alone is not node-death authority.
                # Keep the original error if GCS never confirms head loss.
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)
        finally:
            ray.kill(owner, no_restart=True)
        stream = submit_from_coordinator()
        stats["fixed_r_pre_submission_failovers"] += 1
        return stream
    except BaseException:
        ray.kill(owner, no_restart=True)
        raise

    # Head status can change while the helper starts. This is still before any
    # begin call, so switching submission ownership remains safe here.
    if not owner_alive:
        ray.kill(owner, no_restart=True)
        stream = submit_from_coordinator()
        stats["fixed_r_pre_submission_failovers"] += 1
        return stream
    try:
        reader = StreamingRecoveryReader.submit(
            owner, producer, expected_returns=count, args=args, kwargs=kwargs,
            timeout_s=config.timeout_s, **options,
        )
    except BaseException:
        # Reader.submit already queues close for an abandoned offer. Give it a
        # bounded opportunity to execute before retiring the private helper.
        try:
            try:
                ray.get(owner.close.remote(), timeout=config.timeout_s)
            except ray.exceptions.RayActorError:
                # Do not mask the original enrollment/retirement exception with
                # a second RPC to the same dead helper. No fresh submission is
                # allowed after begin may have run.
                pass
        finally:
            ray.kill(owner, no_restart=True)
        raise
    stats["fixed_r_enrolled_tasks"] += 1
    return _DataStream(count, stats, reader=reader, owner=owner)


def new_metrics():
    metrics = dict.fromkeys((
        "fixed_r_enrolled_tasks", "fixed_r_survivor_tasks", "fixed_r_copy_baseline_tasks",
        "fixed_r_recovered_tasks", "fixed_r_copied_blocks", "fixed_r_closed_streams",
        "fixed_r_pre_submission_failovers",
    ), 0)
    # One owner failure: only the tasks still live at that loss can replay.
    # Record identities, not every successful task in the Dataset.
    metrics["fixed_r_recovered_task_details"] = []
    metrics["fixed_r_recovery_errors"] = []
    return metrics


class StreamingRecoveryDataOpTask(DataOpTask):
    # The executor checks this before get_waitable(), which can return native
    # backpressure credit by starting an owner read.
    requires_output_budget_before_wait = True

    def __init__(self, task_index, stream, *args, **kwargs):
        super().__init__(task_index, stream, *args, **kwargs)
        self.stream = stream
        self._pair_start = 0
        self._cancelled_ref = None
        self._copied_return_indices = set()

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

    def _emit_copied_pair(self, copied_pair):
        copied_ref, metadata = copied_pair
        size = self.produce_block(copied_ref, metadata, owns_blocks=False)
        self.stream.stats["fixed_r_copied_blocks"] += 1
        return size

    def _release_copied_pair(self):
        if self.stream.expected_returns == -1 and self.stream.reader is not None:
            self._copied_return_indices.update((self._pair_start, self._pair_start + 1))
            self._release_unused_copies()
        else:
            self.stream.release_pair(self._pair_start)

    def _release_unused_copies(self):
        if not self._copied_return_indices:
            return
        consumer = self.stream.reader.consumer
        core = ray._private.worker.global_worker.core_worker
        for index in tuple(self._copied_return_indices):
            # Same-instance Python aliases do not increment native refcounts.
            # With dict ownership + the getrefcount argument, exactly two means
            # no such alias. Native eligibility additionally checks RPC-envelope
            # nesting, separate ObjectRef instances and distributed borrowers.
            # Keep the GIL through the check/release; original outputs are never
            # exported by this adapter. Lingering aliases stay listed on replay.
            if sys.getrefcount(consumer._retained[index]) != 2:
                continue
            if (consumer.phase == "replaying" or
                    core.try_release_streaming_recovery_return(consumer._retained[index])):
                consumer.release(index)
                self._copied_return_indices.remove(index)

    def on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        self._track_task_output_backpressure(max_bytes_to_read)
        if self.has_finished or max_bytes_to_read == 0:
            return 0
        try:
            self._release_unused_copies()
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
            self._release_copied_pair()
            # Let distributed reference counting retire copies. Data's
            # owns_blocks=True permits explicit free even if an exported alias
            # remains live; it is distinct from native coordinator ownership.
            return self._emit_copied_pair(copied_pair)
        except (StreamingRecoveryRequired, ray.exceptions.OwnerDiedError):
            # No original output was exported, no metadata background get was
            # submitted, and synchronous gets have settled before reaching here.
            # Previously emitted copies and their downstream users can continue.
            self.stream.recover()
            self.stream.stats["fixed_r_recovered_task_details"].append({
                "task_index": self.task_index(),
                "task_id": self.stream.task_id.hex(),
            })
            return 0
        except Exception as exc:
            self._finish(error=exc)
            raise


_BUFFERED_TASK_MARKER = b"ray-data-fixed-r-buffered-task-v1"


def buffered_map_task(map_transformer, data_context, ctx, *blocks, **kwargs):
    """Publish a finite task atomically without a user-declared block count.

    This deliberately buffers output, not an implementation of unbounded
    streaming recovery. The normal map/read transformer and block shaping run
    unchanged. The native protocol sees exactly two returns for every task,
    including a task producing no blocks. Errors publish no successful result.
    """
    from ray.data.block import BlockAccessor
    from ray import cloudpickle
    from ray._raylet import StreamingGeneratorStats
    from ray.data._internal.execution.operators.map_operator import _map_task

    config = get_config(data_context)
    limit = config.max_task_output_bytes
    outputs = []
    size = 0
    produced = _map_task(map_transformer, data_context, ctx, *blocks, **kwargs)
    try:
        while True:
            try:
                block = next(produced)
            except StopIteration:
                break
            size += BlockAccessor.for_block(block).size_bytes() + 64
            if size > limit:
                raise ValueError(
                    "Fixed-R task output exceeds fixed_r_task_recovery_max_output_bytes; "
                    "reduce task size or explicitly raise the buffer limit"
                )
            # Snapshot before resuming the generator, like normal Ray yield
            # serialization. A UDF may reuse/mutate its batch buffer on its next
            # iteration. Keep real objects in the envelope (not opaque pickled
            # bytes), so contained references remain visible to Ray validation.
            serialization_started = time.perf_counter()
            serialized_block = cloudpickle.dumps(block)
            serialization_time_s = time.perf_counter() - serialization_started
            block = cloudpickle.loads(serialized_block)
            del serialized_block
            try:
                # _map_task delegates to yield_block_with_stats, which expects
                # Ray's generator runner to send serialization feedback. We
                # drive it locally, so report this block's actual snapshot
                # serialization time. Advancing with next() would leave required
                # BlockExecStats.block_ser_time_s unset and break output metrics.
                metadata = produced.send(StreamingGeneratorStats(
                    object_creation_dur_s=serialization_time_s,
                ))
            except StopIteration as exc:
                raise StreamingRecoveryCountError("Task ended between block and metadata") from exc
            # Account for metadata and empty blocks too. This bounds retained
            # payload, not the UDF's own allocations or total process RSS.
            size += len(metadata)
            if size > limit:
                raise ValueError(
                    "Fixed-R task output exceeds fixed_r_task_recovery_max_output_bytes; "
                    "reduce task size or explicitly raise the buffer limit"
                )
            outputs.append((block, metadata))
    finally:
        produced.close()
    yield outputs
    yield _BUFFERED_TASK_MARKER


class BufferedRecoveryDataOpTask(StreamingRecoveryDataOpTask):
    """Unpack a protected finite result into normal coordinator-owned blocks."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._copied_outputs = deque()

    def get_waitable(self):
        if self._copied_outputs:
            return self._copied_outputs[0][0]
        return super().get_waitable()

    def _release_copied_pair(self):
        # This finite task has only two native returns. Keep both registered
        # with the consumer until close, even after independently owned copies
        # are emitted. Copying alone does not prove that all runtime aliases
        # (waitables, RPC envelopes, deserialization buffers) have disappeared.
        # Recovery before EOF must list every still-live consumed return; the
        # native adoption check must continue rejecting omitted references.
        # close() releases these holds after the tombstone barrier.
        pass

    def _copy_pair_if_ready(self):
        refs = [self._pending_block_ref, self._pending_meta_ref]
        ready, _ = ray.wait(refs, num_returns=2, timeout=0, fetch_local=True)
        if len(ready) != 2:
            return None
        outputs, marker = ray.get(refs, timeout=0)
        if marker != _BUFFERED_TASK_MARKER or not isinstance(outputs, list):
            raise StreamingRecoveryCountError("Invalid protected task envelope")
        copies = []
        for item in outputs:
            if not isinstance(item, tuple) or len(item) != 2 or not isinstance(item[1], bytes):
                raise StreamingRecoveryCountError("Invalid protected block/metadata pair")
            ref = ray.put(item[0])
            ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs([ref])
            copies.append((ref, item[1]))
        return copies

    def _emit_copied_pair(self, copies):
        self._copied_outputs.extend(copies)
        if not self._copied_outputs:
            return 0
        return super()._emit_copied_pair(self._copied_outputs.popleft())

    def on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        if self._copied_outputs:
            self._track_task_output_backpressure(max_bytes_to_read)
            if max_bytes_to_read == 0:
                return 0
            try:
                return super()._emit_copied_pair(self._copied_outputs.popleft())
            except Exception as exc:
                self._copied_outputs.clear()
                self._finish(error=exc)
                raise
        return super().on_data_ready(max_bytes_to_read, metadata_fetcher)

    def _cancel(self, force):
        try:
            super()._cancel(force)
        finally:
            self._copied_outputs.clear()
