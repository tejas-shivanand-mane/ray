# Fixed-R: surviving Ray Data inputs

Starting checkpoint: `8b0b283d65ccab0224f9bddf9fe37a51aef0c1dd`, fetched from
`main` before editing. The user reported the focused native streaming cases and
all **48 Python tests passing in 247.13s** at that checkpoint.

The priority is now the collaborator benchmarks using Fixed-R. Succession
streaming is deferred. README and the benchmark workloads remain unchanged.

## Why this step is needed

`TaskPoolMapOperator._try_schedule_task` calls Ray Data's `_map_task` with
ObjectRefs for the map transformer, DataContext, and input blocks. The previous
streaming contract rejected every by-reference argument. Copying these blocks
into a recipe would inflate replication and change the benchmark's data path.

The bounded input extension accepts direct ObjectRefs owned by the designated
surviving consumer. It does not replicate input data or recover its owner:

- Recipe validation checks input identity and exact owner address. Inputs from
  the protected owner, third-party owners, or the replay task itself are rejected.
- The consumer checks native ownership, a positive local ref count, creation
  completion, and absence of nested refs/tensor transport before enrollment.
  Missing, explicitly freed, pending, or borrowed inputs fail before dispatch.
- `StreamingRecoveryReader` retains the direct positional/keyword input refs
  through original EOF and replay until acknowledged close. The application may
  drop its aliases but must not explicitly free or mutate these inputs.
- Before adopting any stream refs, the TaskManager validates the dependency
  contract and live consumer ownership again. Normal pending-task dependency
  accounting keeps the inputs referenced during replay and releases submitted
  references at completion. No dependency is silently promoted or recomputed.

Nested input refs, recovery sidecars, tensor transport, unknown output counts,
and additional failures remain outside this contract. Low-level users of
`StreamingRecoveryOwner` / native replay must retain and validate the consumer's
inputs themselves; the reader performs this lifecycle automatically.

## Ray Data integration coverage

The crash suite now runs Ray Data's real `_map_task` through a thin wrapper that
records attempt numbers and delegates with `yield from`. The transformer,
DataContext, and a Plasma-sized Arrow block are consumer-owned ObjectRefs.
An identity block transform preserves one known output block; the real map task
emits that block and its pickled metadata as two separate stream objects.

The tests crash before reading either object, between block and metadata, and
after original EOF. They drop application input-ref aliases before failure and
check stable retained output IDs, Arrow values, schema/row metadata, and exactly
attempts 0 and 1. Timing/memory metrics inside metadata are attempt-specific and
are not asserted byte-identical. This is map-task integration coverage, not a
claim of full Dataset executor recovery or benchmark performance.

Native tests cover valid/invalid dependency recipes, unchanged input references
in replay, rejection before mutation when an input is absent, pending-input
rejection, and dependency reference accounting through completion. A Python
negative case checks nested ObjectRefs inside a consumer-owned input payload.
The old by-reference rejection case now uses an owner-owned input; consumer-owned
direct inputs are intentionally supported.

## Fastest benchmark path

1. Target `backpressure_benchmark.py --case fast-producer-slow-consumer` first.
   Its operators use normal tasks. Integrate the surviving executor's stream
   state with protected submissions, output-pair handling, input lifetime, and
   recovery while downstream work is quiescent. Resolve the actual output count
   contract at the map-task boundary; UDF yield counts alone are insufficient
   because block shaping and operator fusion can change the stream count.
2. Use `worker_scaling_benchmark.py --worker-type tasks` for the initial scaling
   comparison. The script defaults to actors; its actor variant and the
   training-prefetch case are later targets. Keep schema/worker-count/workload
   parameters equal across comparisons and report the chosen worker type.

Unmodified scripts can already run ordinary Ray Data, but simply enabling flags
does not enroll their streams in this protocol. Do not report such runs as
protected Fixed-R benchmark results. The next integration must demonstrate
enrollment coverage and recovery at the full execution boundary.

## One local validation batch

No builds, tests, lint, or benchmarks were run by the agent. Rebuild Ray for the
new native validation binding, then run:

```bash
bazel test //src/ray/common/streaming_recovery:streaming_recovery_test \
  //src/ray/core_worker/tests:task_manager_test \
  --test_output=errors

python -m pytest -q \
  python/ray/tests/test_streaming_recovery_consumer.py \
  python/ray/tests/test_streaming_recovery_submission.py \
  python/ray/tests/test_streaming_recovery_owner_loss.py
```

The Python batch now has 52 cases and requires the Ray Data dependencies
(including PyArrow). Passing the previous 48 cases does not validate this patch.
