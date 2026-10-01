# Experimental coordinator-process recovery without model rollback

Status: the user reports that the complete focused correctness suite passed
locally at `25dab161` on 2026-10-01. The agent did not run the suite. This covers
the small CPU/Gloo mechanism test described below. A subsequent uploaded
CIFAR/ResNet report passed all four observations at `bf0806eb`; see the preliminary
results below. Repeated timing measurements remain open.
This extends Ray Data input delivery. It is **not Fixed-R**, selective Train
retry, physical-machine recovery, or evidence of a general performance advantage.

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
git switch main
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


## Short CIFAR/ResNet comparison

The user ran the benchmark locally and uploaded a passing report at `bf0806eb`.
The agent inspected its process, checkpoint and optimizer evidence, and did not
run tests, training or rendering. Plot rendering remains user-validated separately.

```bash
conda activate ray-dev
git fetch origin
git switch coordinator-learning-comparison
git pull --ff-only
bash gossip_benchmarks/validate_coordinator_training.sh \
  --data-directory ~/ray-coverage/cifar-streaming
```

Reuse the already prepared CIFAR input. If its `manifest.json` is missing,
prepare it before the benchmark:

```bash
python gossip_benchmarks/workloads/cifar_streaming.py \
  --prepare-data --data-directory ~/ray-coverage/cifar-streaming
```

The default is four observations: two no-failure controls followed by two
coordinator-process failures, using ordinary Ray and input resume. Each trains
for two epochs. With the default 2,048 training images, batch size 32 and two
workers, the fault follows update 16 of epoch 2, after epoch 1 is checkpointed.
The existing CIFAR application, ResNet model and data preparation are unchanged.
All arms use Fixed-R OFF, selective retry OFF and one ordinary full-group Train
retry. No owner placement is forced, and the head is not killed.

Each observation has a 300-second cap including startup and final verification;
four observations therefore have a 20-minute observation budget, plus the small
evidence test suite and up to 15 seconds of forced cleanup per observation.
Actual timing for this new splitter is unmeasured. `--controls-only` runs two
observations first. `--include-deterministic-baseline` adds a third arm with the
same deterministic splitter but zero coordinator restarts (six observations),
to distinguish sharding overhead from the effect of restarting its coordinator.
`--repeats 3` provides repetitions; start with the default short run.

The JSON retains actual worker identities, coordinator replacement evidence,
checkpoint delivery, per-update/sample telemetry and completed decode work.
It measures observed repeated optimizer updates, failure-to-next-update and
failure-to-progress-beyond-the-pre-fault-model delays on both ranks. The optional
same-sharding arm is compared both with ordinary Ray and with input resume.
A killed training process can lose its final telemetry write, so baseline
repeated-update counts are observed work and can undercount that last update.
Only successful completed observations enter timing comparisons. If an ordinary
baseline naturally continues without restoring a checkpoint, that is recorded;
no exception or forced Train retry is injected to manufacture a difference.

A resume success requires the same two training invocations, no checkpoint
restoration, no repeated optimizer work, and a completed data execution in the
replacement coordinator process. Its committed model/Adam/RNG checkpoint hashes
and per-rank sample order must match its own no-failure control. Ordinary Ray's
batch assignment can differ, so cross-arm identical weights are not assumed.
A failed resume attempt remains failed even if fallback checkpoint retry finishes
training; the JSON still preserves that workload completion and its evidence.

Plot separately from the self-contained JSON (no result folders needed):

```bash
python gossip_benchmarks/plot_coordinator_training.py \
  ~/ray-coverage/coordinator-training-comparison.json \
  --output ~/ray-coverage/coordinator-training-comparison.png
```

This creates PNG and PDF panels for the no-failure and coordinator-failure cases.
The curves show the minimum current optimizer position across ranks; a rollback
is drawn only at an observed checkpoint-restoring invocation. Gate timestamps
place the completed pre-fault update before the failure, even though application
telemetry is emitted after the gate releases. Failed/timeout endpoints remain
marked as such. Workload time includes instrumentation, injection waiting,
application validation and checkpoint writes, but excludes cluster startup and
the harness's final checkpoint verification. Total observation time is also
saved. This is coordinator-process coverage, not head-node or physical-machine
recovery and not evidence that Fixed-R improves ML performance.

## First measured CIFAR result (2026-10-01)

Source: user-uploaded `coordinator-training-comparison.json`, clean checkout
`bf0806ebef632190ed95470a9b0ff407b5822624`. All four observations passed with
matching source and loaded native-extension fingerprints. This is one repetition,
2,048 training images, 512 validation images, two epochs and 32 updates per rank
per epoch. The coordinator fault follows epoch-2 update 16 on both ranks.

| Observation / metric | Ordinary Ray | Coordinator input resume |
| --- | ---: | ---: |
| No-failure workload | 88.9156 s | 85.8888 s |
| Faulted workload | 110.5169 s | 89.0767 s |
| Fault versus own control | +24.29% | +3.71% |
| Observed repeated optimizer updates per rank | 18 | 0 |
| Fault to next update on both recovered/continuing ranks | 8.3973 s | 1.9933 s |
| Fault to both ranks exceeding injection-point progress | 23.9283 s | 1.9933 s |

Ordinary Ray replaced both training actors, restored the epoch-1 checkpoint,
and executed 50 initial plus 32 restored updates per rank (82 versus the 64
required). The initial workers made two further buffered updates after injection
before retry. Input resume retained both training actors and executed exactly
64 updates per rank in one invocation, with no checkpoint restoration. Its
coordinator process identity changed on the same surviving node, and a completed
data execution was recorded in the replacement process. All logical nodes stayed
alive in both fault cases.

Both faulted runs' checkpoint file hashes matched their respective controls.
The resume arm also passed exact per-rank sample-order comparison. The faulted
resume workload was 21.4402 seconds (19.40%) shorter than ordinary Ray in this
pair. Its control was 3.40% shorter, which does not establish negative overhead:
run variation and the different splitter are confounded. The optional
same-sharding, zero-restart baseline was not run.

The two splitter configurations produced different model trajectories: final
accuracy was 36.328125% for ordinary Ray and 34.9609375% for input resume, with
no fault/control difference within either arm. Cross-arm equal weights or
convergence quality are not claimed. Training-image decode counts were 4,096
in each control, 5,484 in the ordinary fault case and 5,628 in the resume fault
case. Input replay still recomputes data; the demonstrated benefit is preserving
model/optimizer work in surviving training processes. Fixed-R and selective
Train retry were OFF throughout. This result does not demonstrate node, driver,
storage or physical-machine recovery, or generalize the timing benefit beyond
this preliminary workload/configuration.
