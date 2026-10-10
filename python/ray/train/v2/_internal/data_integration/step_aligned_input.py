"""Experimental input retention for fixed-membership, synchronous TorchFT.

This is not automatic DataIterator checkpointing. The training loop must request
input AFTER synchronous quorum/healing, using the committed model step. One
step consumes one equal batch per rank; all ranks must participate. The dataset
manager, coordinator, head and source storage must survive. No durable writes,
coordinator restart, changing membership, gradient accumulation, or epoch reset
are supported by this prototype.
"""

import copy
import io

import pyarrow as pa

import ray
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.train import DataConfig
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class StepInputRounds:
    """Retain the complete current round, including replies lost with a worker.

    A caller's next committed model step authorizes retiring the prior round.
    Merely delivering a batch never retires it. This relies on the explicit
    synchronous full-membership training contract, not on delivery counters as
    proof that an optimizer update committed.
    """

    def __init__(self, batches, world_size, batch_size, max_round_bytes):
        if any(type(value) is not int or value <= 0
               for value in (world_size, batch_size, max_round_bytes)):
            raise ValueError("World size, batch size and byte limit must be positive integers")
        self.batches = iter(batches)
        self.n = world_size
        self.batch_size = batch_size
        self.max_bytes = max_round_bytes
        self.step = -1
        self.generations = [0] * world_size
        self.delivered = set()
        self.payloads = None
        self.error = None

    def fence(self, rank):
        if type(rank) is not int or not 0 <= rank < self.n:
            raise ValueError("Invalid input rank")
        self.generations[rank] += 1
        return self.generations[rank]

    def get(self, rank, generation, committed_step):
        if (type(rank) is not int or not 0 <= rank < self.n
                or type(generation) is not int or generation != self.generations[rank]):
            raise ValueError("Stale input consumer generation")
        if type(committed_step) is not int or committed_step < 0:
            raise ValueError("Invalid committed model step")
        if self.error is not None:
            raise RuntimeError(self.error)
        if committed_step < self.step or committed_step > self.step + 1:
            raise ValueError("Input step is stale or skipped; complete peer model healing before requesting input")
        if committed_step == self.step + 1:
            if self.step >= 0 and len(self.delivered) != self.n:
                raise ValueError("Cannot advance input before all fixed-membership ranks received the prior round")
            # A healed next-step request authorizes retirement. Release the
            # previous payload before allocating the next serialized round.
            self.payloads = None
            try:
                table = next(self.batches, None)
                if table is None:
                    raise ValueError("Step-aligned input exhausted; provision one full round per model step")
                if table.num_rows != self.n * self.batch_size:
                    raise ValueError("Step-aligned input requires full equal batches; partial final round rejected")
                if table.nbytes > self.max_bytes:
                    raise ValueError("Retained input round exceeds max_round_bytes")
                payloads = []
                for r in range(self.n):
                    part = table.slice(r * self.batch_size, self.batch_size).combine_chunks()
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, part.schema) as writer:
                        writer.write_table(part)
                    payloads.append(sink.getvalue().to_pybytes())
                if sum(map(len, payloads)) > self.max_bytes:
                    raise ValueError("Serialized input round exceeds max_round_bytes")
            except Exception as exc:
                # The source may already have advanced. Never retry it as if
                # the failed round had not happened.
                self.error = f"Step-aligned input failed: {exc}"
                raise
            self.payloads = payloads
            self.step = committed_step
            self.delivered.clear()
        self.delivered.add(rank)
        return self.payloads[rank]


@ray.remote(num_cpus=0, max_restarts=0)
class StepInputCoordinator:
    def __init__(self, dataset, world_size, batch_size, max_round_bytes):
        self.dataset = dataset
        DataContext._set_current(dataset.context.copy())
        self.batches = iter(dataset.iter_batches(
            batch_size=world_size * batch_size, batch_format="pyarrow", prefetch_batches=0,
        ))
        self.rounds = StepInputRounds(self.batches, world_size, batch_size, max_round_bytes)
        self.stopped = False

    def get(self, rank, generation, committed_step):
        if self.stopped:
            raise RuntimeError("Step-aligned input coordinator was stopped")
        return self.rounds.get(rank, generation, committed_step)

    def fence(self, rank):
        return self.rounds.fence(rank)

    def shutdown_executor(self):
        self.stopped = True
        self.batches.close()
        self.rounds.payloads = None


class StepAlignedIterator(StreamSplitDataIterator):
    """Explicit batch requests indexed by the healed model's committed step."""

    def __init__(self, coordinator, rank, world_size, context):
        super().__init__(coordinator, rank, world_size)
        self._generation = 0
        self._context = context

    def batch_for_step(self, committed_step, timeout_s=60):
        """Return NumPy columns; repeat a step to retry an uncommitted update.

        Call after TorchFT optimizer.zero_grad() with use_async_quorum=False.
        Do not cache this result across another quorum/healing operation.
        No cursor advances here: the next request must come from the model's
        actual committed step, never a local batch counter or delivered count.
        """
        payload = ray.get(self._coord_actor.get.remote(
            self._output_split_idx, self._generation, committed_step), timeout=timeout_s)
        table = pa.ipc.open_stream(io.BytesIO(payload)).read_all()
        return BlockAccessor.for_block(table).to_numpy()

    def for_replacement(self):
        replacement = copy.copy(self)
        replacement._generation = ray.get(
            self._coord_actor.fence.remote(self._output_split_idx), timeout=60
        )
        return replacement

    def _to_ref_bundle_iterator(self):
        raise TypeError("Use batch_for_step(committed_step); ordinary iteration cannot track optimizer commit")

    def get_context(self):
        return self._context

    def stats(self):
        return "Experimental step-aligned input: one retained round; no durable checkpoints"

    def schema(self):
        raise NotImplementedError("Inspect the source Dataset schema before training")


class StepAlignedDataConfig(DataConfig):
    """Opt-in coordinated input for synchronous TorchFT with a fixed world size.

    Dataset names are configured independently on first access, including when
    a nonzero rank is first to request them. All input names use this protocol.
    One serialized round is retained centrally; Arrow conversion, RPC replies,
    worker copies and source execution require additional transient memory.
    Batch assignment differs from default streaming_split. This is experimental
    and deliberately rejects unsupported recovery rather than silently replaying.
    """

    def __init__(self, batch_size, *, synchronous_full_membership=False,
                 max_round_bytes=64 * 1024**2):
        if synchronous_full_membership is not True:
            raise ValueError("Explicit synchronous_full_membership=True contract required")
        if (type(batch_size) is not int or batch_size <= 0
                or type(max_round_bytes) is not int or max_round_bytes <= 0):
            raise ValueError("Batch size and retained round byte limit must be positive integers")
        # This custom configure method splits by rounds, without the default
        # rank-zero initialization barrier or streaming_split cursor semantics.
        super().__init__(datasets_to_split=[])
        self.batch_size = batch_size
        self.max_round_bytes = max_round_bytes

    def configure(self, datasets, world_size, worker_handles, worker_node_ids, **kwargs):
        result = [{} for _ in range(world_size)]
        for name, dataset in datasets.items():
            ds = dataset.copy(dataset)
            ds.context.execution_options = self._resolve_execution_options(name)
            ds.context.execution_options.preserve_order = True
            coordinator = StepInputCoordinator.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(), soft=False),
            ).remote(ds, world_size, self.batch_size, self.max_round_bytes)
            for rank in range(world_size):
                result[rank][name] = StepAlignedIterator(coordinator, rank, world_size, ds.context.copy())
        return result
