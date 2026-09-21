# Fixed-R Dataset recovery without declared counts or whole-task buffering

Status: implemented and source-reviewed; **not built or executed by the assistant**.
The earlier buffered worker Dataset suite passed all four cases at `f3ad80f`:
copy, protected/no failure, head failure during read, and head failure during map.
Those results do not validate this new native streaming extension.

## Application-facing change

On a cluster configured for Fixed-R R=2/W=2, with a surviving non-head Dataset
coordinator and two surviving CPU executor nodes:

```python
ray.data.DataContext.get_current().enable_fixed_r_task_recovery = True
```

The default output mode is now `streaming`. The application keeps its normal
Dataset construction, UDFs, batching, block shaping, and consumption APIs. Ray
runs the normal `_map_task` generator and copies each block to coordinator
ownership as it arrives, subject to Dataset output budgets and the generator's
normal backpressure credit. No whole-task envelope, calibration, or physical
output-count declaration is used in this mode. The old envelope byte limit does
not apply to streaming; this is not a total memory bound. Retained inputs,
independent output copies, lingering runtime aliases, and in-flight buffers still
consume memory. Fusion and eager freeing remain disabled. This is still an
experimental recovery execution mode, not a performance-equivalent baseline.

The previous buffered implementation remains selectable:

```python
ctx = ray.data.DataContext.get_current()
ctx.enable_fixed_r_task_recovery = True
ctx.fixed_r_task_recovery_output_mode = "buffered"
```

Choose the mode before constructing/executing the Dataset; changing it on a
context with cached recovery placement fails explicitly. The earlier separately
configured declared-count streaming adapter remains available and unchanged.

## Protocol change

Version 1 descriptors retain their exact declared-count contract. Version 2 uses
`expected_returns=-1` for deterministic finite streams whose output count is
unknown at enrollment. It retains the same all-holder installation and consumer
receipt barrier, authoritative GCS owner-death check, witness claim, immutable
recipe, original ObjectIDs, adoption checks, and explicit tombstone barrier.
The version number rejects unsupported descriptors on an older native runtime.

Replay restores the consumed cursor and suppresses that prefix. A successful
replay must produce at least the already consumed count. Its first successful
completion establishes the exact count, including zero, for later reconstruction.
A shorter replay fails with STREAMING_GENERATOR_REPLAY_INCONSISTENT. The locally
registered spec retains the prefix lower bound so an early replay failure also
settles live consumed refs that have not yet been reported by the replay.
Original EOF, when observed by the surviving reader, pins its exact count too.

This requires deterministic UDFs and stable input data: count validation does not
prove byte-for-byte equality of a recomputed prefix. No exactly-once side-effect
claim is made. There is still only one owner-node recovery transition.

Copied originals must not disappear from recovery tracking while native/RPC
aliases remain. The streaming adapter defers their release until there are no
other aliases to the Python handle and an atomic native check/release confirms
there are no remaining local, nested, submitted, borrower, or lineage holds.
Still-live consumed refs remain in the adoption snapshot. This does not weaken
the native rejection of omitted/conflicting references and does not retain every
original payload until task completion.

## Benchmark integration and validation

`backpressure_benchmark.py` extracts the existing fast-producer/slow-consumer
Dataset expression into one shared builder. Ordinary execution and the new
`--recovery-plan runtime` harness use that builder and the original UDFs. The
harness only adds recovery configuration, observers, assertions, and head-failure
injection. The existing `physical`/`dataset` recovery plans remain intact.

The runtime backpressure suite starts five fresh local clusters:

1. Copy baseline.
2. Fixed-R with no failure.
3. Head failure after producer enrollment, before output consumption.
4. Head failure after the producer's first block/metadata pair was delivered.
5. Head failure after consumer-task enrollment.

Failure injection pauses the executor thread, not the UDF. Each failure case
requires replay of the selected protected task, correct payload/schema/status
values and row accounting, all submitted tasks finished, all streams closed,
no active enrolled streams, and no recovery errors. The producer observer checks
all output values are zero, dtype/shape, and total rows; the original UDF does
not carry input identity, so this is not independent per-input provenance proof.
The after-output case requires a nonzero consumed prefix. A separate gated
native-reader regression proves delivery before producer completion.

The worker-scaling task-based Dataset suite now uses streaming by default and
accepts `--recovery-output-mode buffered` for the prior path. It retains exact
schema/value/row checks and read/map failure coverage.

## One local validation batch

This patch changes C++ and Cython: **rebuild this fork's native components and
Python extension in the existing `ray-dev` environment before running it**.
Use the same full-Ray source-build command you used for the existing compiled
fork; do not install a released Ray wheel. The assistant ran no builds, tests,
lint, or benchmarks.

After `git pull --ff-only` and your source rebuild, run from the repository:

```bash
bash gossip_benchmarks/validate_fixed_r_streaming_datasets.sh
```

The script runs the native protocol/task-manager targets, relevant Python
regressions, the four-case original worker Dataset suite, and the five-case
original backpressure suite. It stops if a stage fails. JSON results are written
to `/tmp/fixed-r-streaming-datasets/worker-scaling.json` and `backpressure.json`.
Override the destination using `RAY_RECOVERY_OUTPUT_DIR` if needed. The benchmark
size remains small: 16 input blocks, 4 GiB logical producer payload per
backpressure case, with ordinary block shaping. No scaling study is included.

## Remaining boundaries

Head testing kills all managed head processes and replaces the head at the same
GCS endpoint using surviving RocksDB storage. The same driver/job and executors
survive. This covers head-process loss, not loss of the physical host/disk,
driver restart, or an arbitrary multi-machine deployment. Later tasks become
coordinator-owned and are not enrolled for another coordinator/owner failure.
Enrollment failure after `begin` may have run still fails closed.

Actor-based worker scaling, training-prefetch/streaming_split, shuffles,
stateful/side-effecting tasks, arbitrary read-source recovery, and Succession
streaming recovery are not implemented or established by this change.

## Reported validation and test setup fixes

The user reported **100 passed, 2 failed** in the Python regression batch at
`e607e49`. Source inspection identified two test setup defects:

- The actor rejection test passed `1` positionally to the keyword-only
  `ActorPoolStrategy` constructor. It now uses `size=1` so execution reaches the
  intended unsupported-plan check.
- The copied-return lifetime test used a `Mock` core worker. `ObjectRef`
  construction calls `add_object_ref_reference(self)`, whose recorded mock call
  retained an unintended strong alias. A plain stub now handles registration,
  removal, and release checks without retaining the ObjectRef. Assertions still
  require retaining real Python aliases and native holds before releasing.

These corrections change tests only; no recovery runtime or native changes and
no rebuild are required. The assistant did not execute tests. After pulling,
rerun the two failed tests, then resume the benchmark suites:

```bash
python -m pytest -q --tb=long \
  python/ray/tests/test_fixed_r_automatic_data.py::test_actor_map_is_rejected_before_execution \
  python/ray/tests/test_fixed_r_streaming_data.py::test_copied_return_keeps_python_and_native_aliases && \
bash gossip_benchmarks/validate_fixed_r_streaming_datasets.sh --benchmarks-only
```

`--benchmarks-only` skips native and Python regression stages; the default script
still runs the full batch. No standalone benchmark-suite results have been
reported yet for this streaming update.
