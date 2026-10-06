"""Experimental deterministic input replay after split-coordinator process loss.

This is NOT Fixed-R: it reconstructs input from serialized dataset lineage.
Consumer processes, the actor owner and input storage must survive. Node relocation
is opt-in and requires initial placement away from the owner. Delivered payloads
and progress belong to consumers, never the coordinator.
"""

import hashlib
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


CONFIG_KEY = "experimental_resumable_split"
EMPTY_DIGEST = hashlib.sha256(b"ray-resumable-split-v1").digest()
# Scheduling reservation only, not a cap on the coordinator's actual memory.
RELOCATION_MEMORY_RESERVATION = 1024**2


@dataclass(frozen=True)
class ResumeConfig:
    # Explicit application assertion: immutable source, deterministic pure UDFs.
    deterministic: bool = False
    rows_per_chunk: int = 32
    max_restarts: int = 1
    timeout_s: float = 120.0
    max_round_bytes: int = 64 * 1024**2
    preferred_node_id: Optional[str] = None
    allow_node_relocation: bool = False

    def __post_init__(self):
        if self.deterministic is not True:
            raise ValueError("Input replay requires an explicit deterministic=True contract")
        for name in ("rows_per_chunk", "max_round_bytes"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.max_restarts) is not int or not 0 <= self.max_restarts <= 3:
            raise ValueError("Use a bounded restart budget from 0 to 3")
        if not math.isfinite(self.timeout_s) or self.timeout_s <= 0:
            raise ValueError("timeout_s must be finite and positive")
        if type(self.allow_node_relocation) is not bool:
            raise ValueError("allow_node_relocation must be a boolean")
        if self.preferred_node_id is not None:
            node = self.preferred_node_id
            if (not isinstance(node, str) or len(node) != 56
                    or any(c not in "0123456789abcdef" for c in node)
                    or ray.NodeID.from_hex(node).is_nil()):
                raise ValueError("preferred_node_id must be a non-nil Ray node ID")
        if self.allow_node_relocation and self.preferred_node_id is None:
            raise ValueError("Node relocation requires explicit placement away from the actor owner")


def coordinator_placement(config, owner_node_id):
    target = config.preferred_node_id or owner_node_id
    if config.allow_node_relocation and target == owner_node_id:
        raise ValueError("Node relocation requires a preferred node different from the actor owner")
    # Soft affinity lets Ray restart on an available node after the preferred
    # node disappears. The surviving owner retains actor creation arguments.
    return NodeAffinitySchedulingStrategy(target, soft=config.allow_node_relocation)


def _encode(table):
    # Coalesce chunks so source block boundaries do not affect the digest.
    table = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _advance(digest, payload):
    return hashlib.sha256(digest + hashlib.sha256(payload).digest()).digest()


class ReplayRounds:
    """Single-threaded, bounded deterministic sharding and prefix verification.

    The coordinator serializes access. Two rounds cover a lost reply while a
    peer has already requested the next round. No model state is stored here.
    """

    def __init__(self, batches, n, max_bytes):
        self.batches = iter(batches)
        self.n = n
        self.max_bytes = max_bytes
        self.index = -1
        self.digests = [EMPTY_DIGEST] * n
        self.cache = {}
        self.eof = False

    def produce(self):
        if self.eof:
            raise ValueError("Request beyond end of input")
        table = next(self.batches, None)
        if table is not None and table.nbytes > self.max_bytes:
            raise ValueError("Input replay round exceeds max_round_bytes")
        rows = 0 if table is None else table.num_rows // self.n
        # Like equal=True, drop fewer than n leftover rows, not a full batch.
        self.eof = rows == 0
        replies = []
        for rank in range(self.n):
            before = self.digests[rank]
            payload = None if self.eof else _encode(table.slice(rank * rows, rows))
            after = before if payload is None else _advance(before, payload)
            replies.append((before, payload, after))
            self.digests[rank] = after
        if sum(len(p) for _, p, _ in replies if p is not None) > self.max_bytes:
            raise ValueError("Serialized input replay round exceeds max_round_bytes")
        self.index += 1
        self.cache[self.index] = replies
        self.cache.pop(self.index - 2, None)

    def reply(self, rank, sequence, prefix):
        if sequence not in self.cache:
            raise ValueError("Stale or out-of-order input request")
        before, payload, after = self.cache[sequence][rank]
        if prefix != before:
            raise ValueError("Input replay prefix mismatch; refusing to change training input")
        return payload, after


class ReplayCoordinator:
    """Recreated by Ray after process death; consumers carry epoch/cursor/digest."""

    def __init__(self, lineage, n, config, owner_node_id=None):
        from ray.data import Dataset

        self.dataset = Dataset.deserialize_lineage(lineage)
        DataContext._set_current(self.dataset.context)
        self.n, self.config = n, config
        self._owner_node_id = owner_node_id
        self.cv = threading.Condition(threading.RLock())
        self.epoch = None
        self.rounds = None
        self.batches = None
        self.seen = [-1] * n
        self.arrivals = set()
        self.stopped = False
        self.error = None

    def _close(self):
        if self.batches is not None:
            self.batches.close()
            self.batches = None

    def _begin(self, epoch):
        self._close()
        self.epoch = epoch
        self.batches = iter(self.dataset.iter_batches(
            batch_size=self.n * self.config.rows_per_chunk,
            batch_format="pyarrow", prefetch_batches=0,
        ))
        self.rounds = ReplayRounds(self.batches, self.n, self.config.max_round_bytes)
        self.seen = [-1] * self.n
        self.arrivals.clear()

    def _wait(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Input replay timed out waiting for all surviving consumers")
        self.cv.wait(min(remaining, 1.0))
        if self.error is not None:
            raise RuntimeError(f"Input replay failed: {self.error}")
        if self.stopped:
            raise RuntimeError("Input replay execution was shut down")

    def get(self, epoch, rank, sequence, prefix):
        with self.cv:
            if self.error is not None:
                raise RuntimeError(f"Input replay failed: {self.error}")
            try:
                return self._get(epoch, rank, sequence, prefix)
            except Exception as exc:
                # Poison this execution after a contract/data error. Peers must
                # not consume a suffix after another rank rejected its prefix.
                self.error = str(exc)
                self.cv.notify_all()
                raise

    def _get(self, epoch, rank, sequence, prefix):
        if (type(epoch) is not int or epoch < 0 or type(rank) is not int
                or not 0 <= rank < self.n or type(sequence) is not int or sequence < 0
                or not isinstance(prefix, bytes) or len(prefix) != 32):
            raise ValueError("Invalid input replay cursor")
        deadline = time.monotonic() + self.config.timeout_s
        with self.cv:
            if self.stopped:
                raise RuntimeError("Input replay execution was shut down")
            if self.epoch is None:
                self._begin(epoch)
            if epoch == self.epoch + 1:
                self.arrivals.add(rank)
                if len(self.arrivals) == self.n:
                    self._begin(epoch)
                    self.cv.notify_all()
                while epoch != self.epoch:
                    self._wait(deadline)
            if epoch != self.epoch:
                raise ValueError("Stale epoch or unsupported cross-epoch recovery")
            if self.seen[rank] >= 0 and sequence > self.seen[rank] + 1:
                raise ValueError("Consumer skipped an input delivery cursor")

            # A reconstructed coordinator must recompute historical input. Its
            # first request may be far into an epoch; retain the preceding round
            # too, since a peer's reply may have been lost at the failure.
            bootstrap = self.rounds.index == -1
            while self.rounds.index < sequence:
                if not bootstrap and min(self.seen) < self.rounds.index:
                    self._wait(deadline)
                    # Another consumer may have produced our round while this
                    # call released the lock. Recheck the target before waiting
                    # on (or advancing) that newer round.
                    continue
                self.rounds.produce()
            reply = self.rounds.reply(rank, sequence, prefix)
            self.seen[rank] = max(self.seen[rank], sequence)
            self.cv.notify_all()
            return reply

    def shutdown_executor(self):
        with self.cv:
            self.stopped = True
            self._close()
            self.cv.notify_all()

    def notify_split_finished(self, epoch, rank, exhausted=True):
        # Normal epoch completion needs no progress RPC. Early consumer exit is
        # unsupported; terminate rather than leave its peers blocked forever.
        if not exhausted:
            self.shutdown_executor()

    def identity(self):
        context = ray.get_runtime_context()
        return {"pid": os.getpid(), "node_id": context.get_node_id(),
                "worker_id": context.get_worker_id()}


class ResumableSplitIterator(StreamSplitDataIterator):
    def __init__(self, coordinator, rank, n, config, context, schema, dataset_tag):
        super().__init__(coordinator, rank, n)
        self._resume_config = config
        self._context = context
        self._schema = schema
        self._dataset_tag = dataset_tag
        self._next_epoch = 0
        self._exhausted = False
        self._in_iteration = False

    def _iter_batches(self, **kwargs):
        if kwargs.get("local_shuffle_buffer_size") is not None:
            raise ValueError("Local batch shuffling is outside the input replay contract")
        return super()._iter_batches(**kwargs)

    def _to_ref_bundle_iterator(self):
        if self._in_iteration:
            raise ValueError("Only one active iterator per resumable split is supported")
        self._in_iteration = True
        epoch = self._next_epoch
        self._next_epoch += 1
        self._active_epoch = epoch
        self._exhausted = False

        def blocks():
            sequence, prefix = 0, EMPTY_DIGEST
            while True:
                # The actor returns values, NOT coordinator-owned ObjectRefs.
                # A completed response belongs to this surviving consumer. Ray
                # may retry an ambiguous actor call with this same cursor.
                payload, after = ray.get(self._coord_actor.get.remote(
                    epoch, self._output_split_idx, sequence, prefix,
                ), timeout=self._resume_config.timeout_s)
                if payload is None:
                    if after != prefix:
                        raise ValueError("Invalid EOF digest")
                    self._exhausted = True
                    return
                if _advance(prefix, payload) != after:
                    raise ValueError("Invalid input replay response digest")
                table = pa.ipc.open_stream(payload).read_all()
                accessor = BlockAccessor.for_block(table)
                # Move the delivery cursor only after creating a local owned
                # copy. Prefetched batches survive coordinator death as well.
                ref = ray.put(table)
                prefix, sequence = after, sequence + 1
                yield RefBundle(
                    blocks=(BlockEntry(ref, accessor.get_metadata()),),
                    schema=accessor.schema(), owns_blocks=True,
                )

        return blocks(), self._iter_stats, False, None

    def _on_iteration_end(self, executor):
        self._in_iteration = False
        epoch, self._active_epoch = self._active_epoch, None
        if epoch is not None and not self._exhausted:
            self._coord_actor.notify_split_finished.remote(
                epoch, self._output_split_idx, False,
            )

    def get_context(self):
        return self._context

    def schema(self):
        return self._schema

    def stats(self):
        return self._iter_stats.to_summary().to_string()

    def _get_dataset_tag(self):
        return {"dataset": self._dataset_tag, "split_index": self._output_split_idx}


def create_resumable_split(dataset, n, equal, options):
    config = ResumeConfig(**options)
    if type(n) is not int or n < 1 or equal is not True:
        raise ValueError("Resumable splitting requires n >= 1 and equal=True")
    if not dataset.context.execution_options.preserve_order:
        raise ValueError("Resumable splitting requires preserve_order=True")
    if dataset.context.enable_fixed_r_task_recovery:
        raise ValueError("Coordinator input replay is separate from Fixed-R; disable Fixed-R")
    allowed = {"Read", "ReadFiles", "ListFiles", "MapBatches", "MapRows", "Project", "Filter"}
    for op in dataset._logical_plan.dag.post_order_iter():
        if type(op).__name__ not in allowed or isinstance(getattr(op, "compute", None), ActorPoolStrategy):
            raise ValueError("Resumable splitting supports only persistent reads and pure task maps/filters")
    lineage = dataset.serialize_lineage()
    owner_node_id = ray.get_runtime_context().get_node_id()
    coordinator = ray.remote(ReplayCoordinator).options(
        num_cpus=0, max_concurrency=n + 2,
        # Ray randomly places resource-free actors even with soft affinity.
        # A nonempty request makes the scheduler honor the preferred node while
        # retaining relocation after its loss, without consuming training CPUs.
        **({"memory": RELOCATION_MEMORY_RESERVATION} if config.allow_node_relocation else {}),
        max_restarts=config.max_restarts, max_task_retries=config.max_restarts,
        scheduling_strategy=coordinator_placement(config, owner_node_id),
    ).remote(lineage, n, config, owner_node_id)
    # Metadata access must not depend on a coordinator RPC during restart.
    schema = dataset.schema(fetch_if_missing=False)
    return [ResumableSplitIterator(coordinator, i, n, config, dataset.context,
                                  schema, dataset._get_uuid()) for i in range(n)]
