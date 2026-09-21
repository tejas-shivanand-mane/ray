# Move recovery into the Dataset runtime

Goal: run the worker-schema task workload through its actual
`range -> map_batches -> materialize` path without benchmark input copies,
physical operator declarations, count calibration, UDF wrappers, or a replacement
sink. This change is implementation, not just a new test for the old adapter.

## Application configuration

Before constructing the Dataset, enable:

```python
ray.data.DataContext.get_current().enable_fixed_r_task_recovery = True
```

The cluster must already run the compiled native Fixed-R implementation with its
system flags enabled. The driver must run on a surviving non-head node, and at
least two additional CPU executor nodes must survive. Ray automatically discovers
the live head and executors. Node placement, retained input copies, disabling eager
free and fusion, and task recovery are managed inside Ray, not in the UDF.
The existing declared-count streaming mode remains available separately.

## Implementation and boundaries

The currently compiled streaming protocol needs a known number of returns. This
mode runs each finite read/map task normally but buffers its resulting block and
metadata pairs. It publishes one envelope plus a marker: exactly two native
returns, irrespective of the number of physical blocks, including zero outputs.
The coordinator copies the result into independently owned blocks and emits them
through the normal executor. Root/read recipes and task input refs are retained
inside Ray. Repeated physical operator names are accepted. InputData-only cached
plans remain usable after head replacement, including `materialize()` output.

This is bounded finite-task recovery, not unbounded/dynamic streaming recovery.
Within-task output streaming is replaced by buffering until that task finishes.
Default output payload limit is 256 MiB per task, configurable through
`fixed_r_task_recovery_max_output_bytes`. It counts block data, metadata, and a
per-block allowance, not total process RSS or temporary UDF allocations. Oversize
results fail explicitly; no partial successful envelope is emitted. Small batches
may be copied/snapshotted multiple times. Performance optimization comes later.
Task replay assumes deterministic, side-effect-free computation; arbitrary read
sources, external side effects, actors, shuffles and `streaming_split` are not
established by this change. Contained references remain subject to native input
validation. The single-owner failure model and pre-enrollment-only startup
failover rule are unchanged. After owner loss, new tasks are coordinator-owned;
this does not renew protection against another owner/coordinator failure.

## Original benchmark path

`build_dataset(args)` is extracted from the original worker benchmark so the normal
entrypoint and recovery harness use the same range and map construction, including
the original UDF, single-stage uncapped task execution, and multi-stage concurrency
caps. Both use actual `Dataset.materialize()`. Importing the original UDF module
by value is packaging only. No probe/calibration is used to decide output counts.
Validation inspects the materialized Dataset after the workload finishes.

The controller pauses the executor after a selected task is enrolled and registered,
before its output is consumed, then kills/replaces the head and resumes execution.
The original UDF has no gate. This is deterministic fault injection, not a naturally
occurring failure or a latency comparison. A pass requires that selected task to
replay, plus correct values, task accounting and stream retirement for ReadRange
and both map stages. The task may have completed computing before head loss;
its unconsumed protected result still needs recovery.

## Run in ray-dev

These are Python runtime changes; the existing native Fixed-R build is reused.
No native rebuild is required. Restart the benchmark process after pulling.

```bash
cd /home/tejas/Downloads/ray && git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-original-dataset-recovery-suite.json \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  --worker-type tasks --num-workers 4 --blocks-per-worker 4 \
  --num-scalar-cols 128 --num-array-cols 32 --num-operators 2 \
  --recovery-plan dataset --recovery-mode suite \
  --recovery-failure-stage map --recovery-failure-operator 1 \
  --local-executor-nodes 2 --local-object-store-mb 512 \
  --recovery-timeout-s 180
```

The suite uses four fresh clusters: copy baseline, protected no-failure,
head-process failure at read-task enrollment, and head-process failure at the
selected map-task enrollment. JSON preserves diagnostics per case and continues
after failures. To run read-task recovery alone, select
`--recovery-mode fixed_r_head_failure --recovery-failure-stage read` and a separate
output filename. This remains head-process loss with surviving RocksDB storage,
driver and worker nodes; it does not establish physical host/disk-loss recovery.

Regression command (includes variable block counts, repeated operator names,
materialization, input retention, read/map head failure, reused batch buffers,
limits/errors, and actor rejection):

```bash
python -m pytest -q --tb=long \
  python/ray/tests/test_fixed_r_automatic_data.py \
  python/ray/tests/test_streaming_recovery_data.py
```

No builds, tests, lint, or benchmarks were run by the assistant. Source inspection
only; this new runtime path awaits user validation. README is unchanged.

## First suite result and serialization metadata fix

The user's first run failed in all four cases when emitting the first ReadRange
block. Output accounting incremented before the metrics assertion failed; no
copied block was recorded. The read-head-failure case did replay one protected
ReadRange task, but it then hit the same assertion and did not pass validation.
The map-head-failure case did not reach its selected enrollment.

Source inspection found that the envelope wrapper advanced `_map_task` with
`next()` after snapshotting each block. The normal generator runner instead sends
`StreamingGeneratorStats` into `yield_block_with_stats`. Without that feedback,
`BlockExecStats.block_ser_time_s` was None, violating the output metrics contract.
The wrapper now measures each block's snapshot serialization and sends that
duration through the existing protocol. This measures local snapshot serialization,
not subsequent envelope publication or coordinator copying. The metrics assertion
remains intact. Regression code exercises the real metadata protocol for multiple
blocks and checks that each measured duration survives serialization.

The harness also saves `execution_error_traceback` from the executor callback
before the Dataset API strips internal frames. Rerun the same suite command above;
no native rebuild is needed. This fix and the new regression code have not been
executed by the assistant and await user validation.

## Second suite result and bounded-reference retention

The user's next suite passed copy (4.676 s), protected no-failure (27.822 s),
and read-task head failure (6.847 s). Each passed case validated 16 final blocks,
58,240 rows, and all 48 tasks/stream closures, with zero reported spilling.
Read-task head failure replayed ReadRange task 0, replaced the head in 2.282 s,
and completed materialization 5.932 s after the failure request. The remaining
47 tasks in that case were coordinator-owned submissions after owner loss.

Map-stage head failure still failed: the native adoption guard reported
`Streaming task has conflicting or unlisted references`. Four tasks had replayed
before the error (two reads and one task in each map stage); this is not a passing
run. The JSON did not identify the rejected task or reference, so it does not
establish the exact alias responsible.

The bounded adapter previously removed both consumed return refs from recovery
tracking immediately after copying, before EOF/closure. Independent output copies
do not establish that all scheduler/RPC/buffer aliases of the originals have gone
out of scope. The bounded path now retains its two native returns in the consumer
through closure. If recovery occurs in this interval, its snapshot includes both
consumed returns, while downstream still receives only coordinator-owned copies.
The native whole-task reference validation and single-replay rule are unchanged;
declared-count streaming keeps its existing incremental release behavior.
This can retain a task envelope longer, within the existing per-task payload limit;
it does not impose a cluster-wide memory bound.

New regression code injects head failure after copying an empty or multi-block
envelope and before EOF, with local original-ref aliases deliberately retained.
It checks replay, no duplicate output, value validation, and eventual release.
Recovery failures now record the task ID, cursor, retained return IDs, available
native reference counts, and the internal traceback directly in operator metrics.
This avoids relying solely on callbacks that shutdown may fail to reach.

No builds, tests, lint, or benchmarks were run by the assistant. Rerun the map case
first with `--recovery-mode fixed_r_head_failure --recovery-failure-stage map` and
a separate JSON filename; the full suite remains the follow-up validation gate.
These are Python changes and reuse the existing native build. The retention fix
remains unvalidated until that rerun.
