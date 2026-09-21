# Public Dataset backpressure across head replacement

Base: `38ed8a07e9633b46b8a0d88ba42b6ed01dd0f9ae`. The user has validated
both the small and 1 GiB physical-plan head replacement runs. The 1 GiB run
completed 128 outputs with two producers and one consumer recovered, all
144 streams closed, and the original driver and executor nodes surviving.

This step adds `--recovery-plan dataset` to the same benchmark entry point.
It actually constructs `ray.data.from_blocks(...).map_batches(...).map_batches(...)`
and drains `Dataset.iter_internal_ref_bundles()`. The normal logical planner,
physical optimizer, execution callbacks, and streaming iterator all participate.
The harness observes the executor through a callback; it does not construct or
execute a physical plan in the Dataset path. The callback's capture serializes
without driver executors, locks, actors, or ObjectRefs.

## Explicit batch boundary contract

`FixedRDataConfig(..., preserve_batch_output_blocks=True)` opts synchronous
task-based `map_batches` into one physical block per output batch. Each stage
uses `batch_size=None` and one input block per task. Fusion was already disabled
for recovery task operators; this change also bypasses output coalescing and
splitting. A generator yielding nothing produces zero blocks, and an empty
output batch counts as one block. Normal input batching can omit empty input
blocks; declarations must reflect that behavior. Counts remain fixed per task
and are checked at runtime. Non-default batching, actor/async UDFs, and per-block
limits are rejected for this preservation option.

The benchmark declares `MapBatches(produce): B` and `MapBatches(consume): 1`.
It reuses the existing NumPy payload generation, sleep, identity checks, and
failure gates through synchronous batch UDFs. Arrow batches preserve validation
metadata; the consumer converts the payload to NumPy before sleeping. This is
still an instrumented adaptation with declared counts, placement constraints,
and disabled shaping/fusion, not the unchanged original benchmark or support
for arbitrary Dataset plans. The existing `original` and `physical` paths retain
their behavior. Dataset runs support `copy`, `fixed_r`, `fixed_r_failure`, and
`fixed_r_head_failure`; use `copy` as the Dataset no-enrollment comparison.

## One validation command

In the existing `ray-dev` environment, using the already compiled fork:

```bash
cd /home/tejas/Downloads/ray && git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-dataset-head-failure.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer \
  --recovery-mode fixed_r_head_failure --recovery-plan dataset \
  --local-executor-nodes 2 --num-input-blocks 16 \
  --output-batches-per-input-batch 8 --output-batch-rows 32 \
  --output-row-bytes 262144 --consumer-sleep-s 0.1
```

No native rebuild is needed. The run generates 1 GiB of logical producer
payload, drains 128 tiny status blocks, and automatically replaces the head
after two producers and one consumer enroll. Success requires 128 ordered,
nonduplicated outputs, all 144 streams closed, three recovered tasks, and later
tasks submitted by the surviving coordinator. JSON additionally identifies
`workload_variant=public_dataset_unshaped_map_batches` and the two actual
`MapBatches(...)` operator names.

The head harness is unchanged: it kills all original managed head processes,
restores GCS at the same endpoint from surviving RocksDB storage, and checks
the cluster/session/job identity plus surviving coordinator/executor IDs.
This tests head-process loss with surviving storage, not loss of the physical
machine and its disk, driver recovery, or a second owner failure.

Regression code covers generator yields at small/large target block sizes,
zero/empty yields, rejection of input rebatching, and Dataset head replacement.
Source inspection only: no builds, tests, lint, benchmarks, rendering, or Actions
were run by the agent. The new Dataset head-failure path awaits the user's run.
