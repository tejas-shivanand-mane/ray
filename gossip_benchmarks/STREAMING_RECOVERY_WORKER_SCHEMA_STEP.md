# Worker-schema task workload: coverage before scaling

The original-UDF backpressure suite passed early, middle, and late head-process
failure on two local executor nodes. The next target is a different collaborator
workload: `worker_scaling_benchmark.py --worker-type tasks`. This step is not a
scaling experiment and does not require eight workers or 5,000 workers.

## What the opt-in recovery path exercises

- The actual `make_realistic_schema_udf` / `RealisticSchemaUDF` implementation,
  serialized by value, including scalar float32 and float32[32] columns.
- The original 16 MiB row-sizing formula and normal Ray Data block shaping.
- One or multiple distinct `map_batches` task operators with fusion disabled.
- Copy baseline, Fixed-R without failure, and Fixed-R head-process failure, each
  in a fresh cluster. `suite` preserves each result and continues after a failure.
- Head failure gates task index 0 of `--recovery-failure-operator` (zero based)
  until the controller observes it as an active enrolled stream. Selecting stage
  1 in a two-stage chain exercises replay with copied upstream map outputs.
- Exact schema, every scalar/array value, per-block rows, total output blocks,
  submitted/finished tasks, stream retirement, and submission/replay accounting.
  A head-failure pass requires replay of the selected task, not just a replacement
  head or replay of a task in some other stage.

## Explicit adaptations and limits

`range` executes before timing; each checked range block is copied into surviving
driver ownership and retained. Read-task recovery is not covered. The real batch
transformer calibrates one deterministic block through each map stage before
execution, requiring one physical output block of the same row count per stage.
This does not support arbitrary data-dependent output counts.

The single-stage recovery path explicitly caps tasks at `--num-workers`; the
original single-stage task path is uncapped. Chained task pools use the original
`num_workers // num_operators` cap. Placement is controlled across local executors.
The sink drains the public Dataset iterator, validates values, and retains all
final refs, instead of invoking `Dataset.materialize()`. Identical schema outputs
have no per-input IDs; full value checks plus task/block accounting are used, not
an assertion of unique application-level record identities. Timing includes
validation and is not comparable to the unchanged scaling benchmark.

Only Fixed-R R=2/W=2 is covered. The driver, coordinator, executor nodes and GCS
RocksDB storage survive. All managed head processes are killed and a replacement
uses the same database and endpoint. This does not simulate host/disk loss or
recover the driver. The actor worker variant and `training-prefetch` are still
unsupported. In particular, `training-prefetch` needs the `streaming_split`
execution/coordinator path, which the current task-map-only plan validator rejects.
No fallback after ambiguous enrollment was added or changed.

## Run in the existing ray-dev environment

No native rebuild is needed for these Python-only additions. This covers two map
stages and targets downstream task replay, using 16 original-sized input/output
blocks per case (approximately 256 MiB of schema payload per stage).

```bash
cd /home/tejas/Downloads/ray && git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-worker-schema-recovery-suite.json \
python release/nightly_tests/dataset/worker_scaling_benchmark.py \
  --worker-type tasks --num-workers 4 --blocks-per-worker 4 \
  --num-scalar-cols 128 --num-array-cols 32 --num-operators 2 \
  --recovery-mode suite --recovery-failure-operator 1 \
  --local-executor-nodes 2 --local-object-store-mb 512 \
  --recovery-timeout-s 180
```

For a single-stage run use `--num-operators 1 --recovery-failure-operator 0`.
Regression code, including both single-stage and downstream failure, is provided:

```bash
python -m pytest -q --tb=long python/ray/tests/test_streaming_recovery_worker_scaling.py
```

The assistant inspected source but did not execute builds, tests, lint, or
benchmarks. The new recovery cases are unvalidated until the user runs them.
