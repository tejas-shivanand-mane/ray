# Experimental coordinator-process recovery without model rollback

Status: first Python prototype, source-reviewed only; local validation pending.
This extends Ray Data input delivery. It is **not Fixed-R**, selective Train
retry, physical-machine recovery, or a measured performance improvement.

## What it attempts

When only the input coordinator process dies, surviving training workers keep
their current model, optimizer, RNG and prefetched input in memory. Ray restarts
the coordinator on the same surviving node. It reconstructs the dataset from
lineage, recomputes the already-delivered input prefix, verifies its digest and
continues supplying the next input. It does not repeat optimizer updates.

For example, with a checkpoint at epoch 1 and a coordinator failure after
update 4 of epoch 2, the intended outcome is to continue with update 5 using
the same worker actors. If the coordinator restart budget is exhausted or
replay detects changed input, the error propagates to Train. Ordinary full-group
checkpoint retry remains available and may restore epoch 1. No new model
checkpointing code is required in the application.

This trades recomputation after failure for avoiding a replicated coordinator
journal. It still incurs steady-state hashing, serialization, consumer-owned
copies and deterministic sharding overhead. Neither low overhead nor a net
benefit has been measured. Recovery delay grows with the input prefix, and can
exceed the iterator or distributed-communication timeout.

## Why a delivered batch survives

The ordinary split coordinator returns bundles of references whose owners can
die with it. The prototype instead returns an Arrow-encoded value; the actor
method's result belongs to the caller. The worker decodes it, stores its own
local Ray object and only then advances its delivery cursor. Both buffered data
and the cursor therefore survive a coordinator-process crash.

The cursor is `(epoch, per-rank chunk sequence, prefix digest)`. It records
delivery to the surviving iterator, not an optimizer commit. This distinction
is safe only because the same worker, iterator, prefetch queue and training
invocation continue. It does not let a replacement training worker resume an
arbitrary optimizer step.

An ambiguous actor call can be retried with the same cursor. The coordinator
retains two output rounds, and prevents a fast rank from discarding a round its
peer might still request. After reconstruction, it deterministically recreates
the prefix and checks the requesting rank's cumulative digest before returning
new data. Duplicate requests do not advance a worker's iterator twice.

## Deliberately narrow contract

- Opt-in; default `streaming_split` and normal Train behavior are unchanged.
- Finite immutable external input with serializable lineage and deterministic,
  side-effect-free task maps/filters. The application must explicitly assert
  this contract. The implementation cannot prove that arbitrary Python UDFs
  are pure. A prefix check does not detect every possible future source change.
- Ordered reads with `preserve_order=True`; no random augmentation, local batch
  shuffling, actor maps, in-memory/materialized input sources, joins or exchanges.
- Deterministic contiguous chunks assigned round-robin by rank. This differs
  from the ordinary locality-aware splitter, whose assignment is not stable.
  `equal=True` is required; fewer than `world_size` trailing rows are dropped.
- One active iterator per rank, fully consumed each epoch. All consumers, the
  actor owner (normally the dataset manager), source storage and coordinator
  node must survive. Worker, driver and node failure are outside this mechanism.
- One process restart by default, configurable from zero to three. Zero is a
  same-sharding ablation, not an unmodified ordinary-Ray baseline.
- Mid-epoch recovery is the first validation target. An ambiguous failure
  crossing epoch boundaries can fail closed to Train checkpoint retry.
- Fixed-R must be OFF in this first prototype. It cannot restore copies owned
  by the dead coordinator, and its enrollment overhead should not be charged
  to this independent recovery mechanism.
- Two rounds of serialized replies are retained. `max_round_bytes` limits each
  logical round after reading/encoding; it is not a hard process or object-store
  memory cap. Upstream execution and per-worker prefetch use additional memory.

The implementation is in
`python/ray/data/_internal/iterator/resumable_split.py`, reached by an opt-in
branch in `Dataset.streaming_split`. It adds no C++ or model-state snapshotting.

## Configuration

Set the context before constructing datasets and the Trainer:

```python
from ray.data import DataContext

context = DataContext.get_current()
context.enable_fixed_r_task_recovery = False
context.execution_options.preserve_order = True
context.set_config("experimental_resumable_split", {
    "deterministic": True,
    "rows_per_chunk": 32,
    "max_restarts": 1,
    "timeout_s": 120,
})
```

Use the usual `TorchTrainer`, `DataConfig(datasets_to_split=["train"])`, and
`iter_torch_batches`. Keep application checkpoints and Train failure retries
enabled. The unsplit validation dataset is unchanged and does not receive this
new recovery capability. This configuration is experimental, not a promise that
arbitrary existing dataset pipelines satisfy the contract.

## Local correctness gate

```bash
conda activate ray-dev
git fetch origin
git switch streaming-coordinator-resume
git pull --ff-only
bash gossip_benchmarks/validate_coordinator_resume.sh
```

The focused suite checks prefix mismatch, duplicate replies, bounded buffering,
equal tails, unsupported sources, process restart and exhausted budgets. Its
two-worker CPU/Gloo learning test runs two epochs of a small linear model with
Adam and real optimizer updates. It kills only the data coordinator after
update 4 of epoch 2. For restart budgets zero and one, it compares fault-free
and faulted executions with identical deterministic sharding.

The assertions require unchanged worker PIDs, exactly one execution of each
optimizer step, exact per-rank sample order and matching final model/Adam
digests in the resume arm. With coordinator restarts disabled, they require
full-group Train checkpoint retry, replacement worker PIDs, checkpoint epoch 1
and repeated epoch-2 work. Baseline Train retries are never disabled.

This small model is a mechanism test, **not** the realistic-workload benchmark.
After it passes, the next experiment is the existing CIFAR/ResNet workload,
with one middle-training coordinator-process failure, before extending a timing
matrix. Include ordinary Ray plus checkpoint retry, the deterministic splitter
without coordinator restart, and the resumable splitter; measure each arm's
control overhead as well as fault completion and repeated optimizer work.
Keep Fixed-R OFF to isolate this mechanism. Record results in JSON and plot them
with a separate command. Do not claim a benefit before those results exist.
