# Fixed-R Data: multiple executors and controlled backpressure benchmark

Base: `fbc7d5c12ffd5b364ca911f7e8322f748a8ad6ab`, fetched from `main` before
editing. The user reported **13 passed in 29.19s** for the preceding Data
integration. The agent has inspected source only: no builds, tests, lint,
benchmarks, rendering, or Actions were run for this change. No performance
result is claimed. This is Python-only; keep the existing compiled fork.

## Placement and failure scope

`FixedRDataConfig.executor_node_id` now accepts either the original single ID
or an ordered tuple of distinct surviving executor IDs. Per physical operator,
task `i` uses executor `i % N` with hard node affinity. The immutable recovery
recipe retains that task's chosen affinity. New tasks after the owner is GCS-dead
continue the same sequence with ordinary coordinator ownership and copying.
All configured executors must be live, even if a particular submission targets
another executor. This does not add executor failure recovery, task migration,
weighted placement, load-aware scheduling, or a second owner failure.

The benchmark requires disjoint coordinator, owner, and executor nodes. Existing
Data API callers may still place their coordinator on a surviving executor.
The head/GCS, driver/job, coordinator, and all task executors must survive.
R=W=2 needs two surviving witness/holder raylets outside the protected owner.
The local harness starts every comparison cluster with identical native flags.

## Benchmark variants and actual counts

`release/nightly_tests/dataset/backpressure_benchmark.py` keeps its default
`--recovery-mode original` public Dataset workload. Opt-in modes delegate to
`streaming_recovery_benchmark.py`, using the actual Data `StreamingExecutor` and
`TaskPoolMapOperator` with an explicit physical plan:

| Mode | Submission and outputs | Intended comparison |
| --- | --- | --- |
| `original` | Existing public Dataset construction and scheduling | Original workload reference |
| `ordinary` | Coordinator submits ordinary streams, no output copying | Controlled workload baseline |
| `copy` | Coordinator submits ordinary streams, copies every output | Cost of copied-output integration |
| `fixed_r` | Separate owner enrolls streams, copies every output | Additional Fixed-R enrollment/read/retirement cost |
| `fixed_r_failure` | Same protection, one gated owner-node loss | Recovery correctness and end-to-end failure run |

The controlled path deliberately disables shaping and fusion, uses one block
per input bundle, preserves order, sets the native stream buffer to one block,
and assigns the same round-robin affinities in every mode. The producer pool
defaults to the executor count; `--producer-concurrency` overrides it. The
consumer pool has size one. Eager free and UDF retries are disabled. Producer
payloads retain the original zero-filled uint8 arrays and the consumer retains
NumPy conversion and per-block sleep. These restrictions change the workload
relative to the original benchmark; **do not present the controlled path as
measurements of the unchanged original workload**.

Each single-row input invokes one producer task. Its generator emits exactly
`B = output_batches_per_input_batch` Arrow tensor blocks; no shaper buffers,
merges, or splits them. Every consumer task receives exactly one such block and
emits one small status block. With `I` inputs:

- Producer: `I` tasks, `B` blocks per task, `I*B` output blocks.
- Consumer: `I*B` tasks, one block per task, `I*B` output blocks.
- Native protected streams declare `2*B` returns for a producer task and two
  for a consumer task, including alternating block metadata returns.

Adapters reject bundling changes, and the harness checks final physical task,
block, copy, and successful-close totals. Schema metadata carries input/block
identity and the producer node without adding a column to the large payload.
The small consumer statuses report those identities, row counts, and executor
IDs. Streaming validation checks every output in order and rejects duplicates,
missing outputs, wrong shapes, and unexpected task placement. It does not scan
every payload byte for a checksum or retain the large output stream. Copy mode
still transfers each large block through coordinator memory/object storage.

## Deterministic owner loss

Only the failure mode creates a small named gate actor pinned to the surviving
coordinator. Transformer closures capture its name, not its handle, preserving
the no-contained-references input contract. Initial producer attempts pause
after yielding their first block, before yielding their second. The first
consumer pauses with its copied input. The driver waits for all producers in
the initial concurrency wave and that consumer to finish enrollment and reach
the gates. None can finish and start replacement work during this interval.

The local harness then removes only the owner node ungracefully. It waits for
GCS-known death before opening the gates. Enrolled tasks must all recover;
subsequent tasks are counted as survivor-owned. Successful completion also
requires all streams to close and all logical output counts to match. At least
two producer outputs are required, and the gate preflight requires enough CPUs
for the whole initial wave plus the first consumer on executor zero. Object
store pressure can still delay the wave; a timeout is surfaced as failure,
never silently changed into a no-failure result.

On an external cluster, the script prints:

```text
FIXED_R_OWNER_FAILURE_READY owner_node_id=<explicit protected owner ID>
```

Stop that owner node through your cluster controls after the marker. The script
does not issue remote shell kills. Stop the node, not just one helper actor;
native recovery requires authoritative node death. The driver waits up to
`--recovery-timeout-s` for detection. The timeout bounds gate coordination and
protocol operations, not the full drain of a potentially long workload.

The JSON includes standard benchmark elapsed time, aggregate sampled peak
object-store usage and spill bytes, workload parameters, physical per-operator
counts, Fixed-R counters, observed executor IDs, and failure-phase durations.
`failure_request_to_gcs_dead_s` and `failure_request_to_drain_s` start at the
marker; external runs include human/cluster-control delay. The latter also
includes the remaining consumer work and is **not isolated replay latency**.
Gate overhead applies only to the failure run. `logical_producer_payload_bytes`
excludes replay, metadata, serialization overhead, and copying; it is a workload
size, not a measurement of network traffic. No-failure modes have no gate actor.

## One local validation and smoke batch

The ordinary-mode hang reported after this step exposed a native task-spec
aliasing regression. The supplied session logs show all four producer tasks
finishing with 12 blocks, while the first consumer remains pending and its
raylet reports an object with no in-memory or external-storage location.
`TaskSpecification::GetMutableMessage()` detached shared protobufs, so dependency
resolution inlined the small streamed block into its private copy. The submitter
still dispatched the original by-reference dependency, requesting a Plasma copy
that did not exist. This also affected actor submission's queued task copies.

The correction restores shared mutation for live task specifications and takes
an independent replay-recipe snapshot at Recovery Frontier registration. That
snapshot remains shared within Frontier; registration now pays for a snapshot
copy to isolate it from live task rewrites. Native regression coverage checks
both already-ready and asynchronously resolved inline arguments through queued
and callback-captured task copies, plus isolation of the registered recipe from
later live mutations. No builds, tests, lint, or benchmarks were run by the agent;
the diagnosis and patch were reviewed from source and the supplied logs.

**Rebuild the native fork with your usual full-Ray build after pulling this
correction.** A Python-only refresh is insufficient. Then run the validation
batch below. The ordinary benchmark still uses its original block references;
no copying workaround or larger payload was added to hide the inline path.

Run in `/home/tejas/Downloads/ray` with the existing `ray-dev` environment active:

```bash
git pull --ff-only && \
python -m pytest -q --tb=long python/ray/tests/test_streaming_recovery_data.py && \
TEST_OUTPUT_JSON=/tmp/fixed-r-backpressure-smoke.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer --recovery-mode suite \
  --local-executor-nodes 2 --num-input-blocks 4 \
  --output-batches-per-input-batch 3 --output-batch-rows 4 \
  --output-row-bytes 64 --consumer-sleep-s 0.01
```

The integration file includes the previous 13 cases, executor configuration and
survival checks, and small runs of all four controlled modes across two
surviving executor nodes. The CLI suite then uses four fresh local clusters
(head/coordinator + two workers + owner), writes the shared JSON after each
successful case, and shuts each cluster down. The failure case performs its
owner-node removal automatically. Small inputs validate the runner and counters;
they are not representative throughput measurements. Local object stores
default to 256 MiB per raylet and can be changed with `--local-object-store-mb`.

## External multi-worker measurements

Use explicit IDs from `ray.nodes()` and the same survivor list and producer
concurrency for the three controlled no-failure runs. For example, after the
existing fork and native flags have been deployed:

```bash
TEST_OUTPUT_JSON=/tmp/fixed-r-backpressure-copy.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer --recovery-mode copy \
  --owner-node-id "$OWNER_NODE_ID" --executor-node-ids "$EXECUTOR_NODE_IDS" \
  --producer-concurrency 7
```

`EXECUTOR_NODE_IDS` is a comma-separated list of live worker node IDs excluding
the protected owner and coordinator. Repeat with `ordinary` and `fixed_r`,
changing the result filename; run `fixed_r_failure` last and remove its owner
after the marker. Keep native flags, topology, block sizes, pool sizes, and
hardware fixed when comparing the controlled modes. For head + eight workers,
dedicating one worker to ownership leaves seven task executors; eight surviving
task executors require an additional owner node. All modes reserve that owner
node so the no-failure comparisons use the same executor resources.

The default payload is 128 inputs * 8 blocks * 128 rows * 1 MiB = 128 GiB of
logical producer output. Size object stores, coordinator memory/network, and
concurrency for that workload before using defaults. There is no whole-stream
materialization, but copies and active tasks increase live memory. These runs
still measure the explicit copied-output prototype, including synchronous
enrollment, recovery, copy creation, and close. The 5,000-worker scaling
benchmark, actor pipelines, arbitrary output counts, and head loss remain
future work. README is unchanged.
