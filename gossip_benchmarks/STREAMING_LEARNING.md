# Checkpoint-frequency limitation study

Use the existing streaming runner with `--comparison checkpoints` to investigate
worker-node recovery using **ordinary Ray in both arms**. Fixed-R, selective
retry and coordinator resume are disabled. This is a baseline limitation study,
not a new fault-tolerance contribution.

Both policies use the same ResNet-18, Adam, data, batches and deterministic
per-rank file stripes via ordinary unsharded Ray Data iterators. This explicit
application sharding differs from the default `streaming_split` assignment.
It is needed to test exact input/model alignment without quietly changing batch
assignment on restart. PNG decoding remains lazy and repeats each epoch.

The epoch policy saves after each epoch. The mid-epoch policy additionally
saves every eight updates. Each checkpoint stores **each rank's** model buffers,
Adam state, CPU RNG and consumed sample IDs/fingerprints. Recovery reconstructs
the iterator, verifies the consumed prefix and skips it before restoring the
training RNG and executing the next update. The skipped prefix still incurs
input reconstruction and decoding work. Only two checkpoints are retained.

## Local validation

From the repository root in `ray-dev`, use your existing 2,048/512 prepared CIFAR
subset. If that directory has no manifest, prepare it once:

```bash
python gossip_benchmarks/workloads/cifar_streaming.py --prepare-data \
  --data-directory ~/ray-coverage/cifar-streaming \
  --train-rows 2048 --validation-rows 512
```

Run four observations (two healthy controls, two node failures):

```bash
bash gossip_benchmarks/validate_streaming_learning.sh \
  --comparison checkpoints \
  --data-directory ~/ray-coverage/cifar-streaming \
  --epochs 2 --checkpoint-every-steps 8 --fault-after-step 18 \
  --repeats 1 --timeout-s 420 \
  --output ~/ray-coverage/worker-checkpoint-study.json
```

A different prepared subset must have two equal file stripes divisible by the
batch size. Set the checkpoint interval and fault step accordingly. The fault
must fall strictly between checkpoint boundaries. This study selects one
worker-node loss in epoch 2; `--failure-kind` and `--failure-point` are not used.
Each observation has its own timeout; startup/verification can extend the total
suite beyond the measured workload time. Controls must match before faults run.

Plot only after inspecting the saved report:

```bash
python gossip_benchmarks/plot_streaming_learning.py \
  ~/ray-coverage/worker-checkpoint-study.json \
  --output ~/ray-coverage/worker-checkpoint-study.png
```

## Interpretation

For 2,048 rows and batch size 32, each rank has 32 steps per epoch. Failure after
step 18 should repeat 18 updates with epoch checkpoints versus two with
mid-epoch checkpoints. Mid-epoch recovery must also verify/skip 16 batches per
rank. These are assertions to validate, not measured results yet.

The report includes healthy/faulted workload times; time to restored model/optimizer
state readiness and first optimizer updates; executed/repeated updates; verified
skipped batches; completed decoded rows; checkpoint bytes; serialization time;
and time inside `train.report`. Checkpoint times are summed rank-seconds and
can overlap, so do not add them to wall time. They include synchronization and
report/upload costs, not just disk throughput. Per-sample correctness telemetry
and selected-checkpoint hashing add measurement overhead in both policies.

Final loaded model, optimizer, rank-local buffers and RNG state must match
exactly across controls and fault runs. A mismatch fails the observation rather
than treating changed training as a speedup. Failed/time-limited samples remain
in the JSON and do not become timing bars. Completed decode counts exclude
killed in-flight calls and include prefetch; they do not measure exact physical
storage reads or bytes. Compare them with the matched control, not just the
minimum dataset size.

The entire logical worker node (raylet, object store and descendants) is killed
at a synchronized post-update boundary. Head, driver, shared storage and spare
capacity survive. This does not test actual VM termination, node-local disk
loss, provisioning delays, GPU/NCCL or arbitrary in-flight collective loss.

No tests, benchmarks, builds, lint or rendering were run by the agent. Run the
wrapper locally; send the resulting JSON before expanding the experiment.

---

## Existing Fixed-R study (unchanged default mode)

The following older instructions apply to `--comparison fixed-r`, the default.

# CPU learning with streaming input

This workload trains a ResNet-18 classifier on actual CIFAR-10 images. Encoded
PNG images are read, decoded and normalized lazily through Ray Data every epoch.
There is no pre-training materialization or feature cache. The 32x32 model uses
a 3x3 stride-1 input convolution and no initial max pool; all ResNet layers are
trained from scratch with Adam. No pretrained weights are downloaded.

This follows the image-classification pattern of Ray's existing
`release/train_tests/benchmark` (Ray Data transforms, Torch batches, DDP and
application checkpoints), using a local CPU application and an accessible small
dataset. It does not reproduce that benchmark's ImageNet scale or accuracy.
Model/data APIs: [torchvision ResNet-18](https://docs.pytorch.org/vision/stable/models/generated/torchvision.models.resnet18.html)
and [CIFAR-10](https://docs.pytorch.org/vision/0.18/generated/torchvision.datasets.CIFAR10.html).

## Comparison and scope

The [ownership audit](STREAMING_LEARNING_OWNERSHIP_AUDIT.md) at `73910a15`
found no natural owner-loss advantage for current Fixed-R in this workload.
The latest control pair passed but still measured 72.1% overhead. Further
optimization and the failure timing matrix are paused pending evidence of a
useful recovery case; the commands below remain available for regression work.

Both arms enable one standard full-group Ray Train retry and save application
model, optimizer, epoch and per-rank CPU RNG checkpoints every epoch. Fixed-R is
OFF versus ON; selective retry is not part of this comparison. The external
harness operates failures and configures recovery. No new runtime recovery
capability is implemented by this workload.

The first run uses 2,048 training images and 512 held-out images, sampled without
replacement using a recorded seed from the separate official splits, four
epochs and two CPU training workers. At batch size 32 that is 32 updates per rank
per epoch. The default failure is after update 16 in epoch 3, with epoch 2 as the
last checkpoint. Early/middle/late at four epochs mean epochs 2/3/4, respectively.
This is a short correctness/recovery experiment, not evidence of convergence,
production performance, or full-dataset accuracy.

- **Head-node:** replaces logical head processes, including GCS, using surviving
  local RocksDB. The driver/controller, training workers, executors and storage
  survive. Ordinary Ray may continue successfully. ON replay is reported only
  when recovery counters actually show it.
- **Worker-node:** kills a training worker's logical node, including raylet and
  object store. The standard Train checkpoint-retry path remains enabled in both
  arms. This can expose Fixed-R's unsupported executor/coordinator-loss cases;
  success is not promised and failure is not relabeled as success.
- Both ranks pause after real optimizer updates for injection. It is not an
  arbitrary in-flight collective/kernel failure or physical-machine loss.
- Data ownership remains the ordinary ownership of the workload in the OFF arm.
  The ON adapter still introduces its existing helper-owner topology. We do not
  force OFF owners onto the head.

## Local commands

Use the existing compiled fork and compatible CPU torch/torchvision in `ray-dev`.
These changes do not require a new native build relative to the working main.
The branch is unvalidated until these commands are run locally.

```bash
git fetch origin
git switch fixed-r-streaming-learning
git pull --ff-only
conda activate ray-dev

python gossip_benchmarks/workloads/cifar_streaming.py \
  --prepare-data --data-directory ~/ray-coverage/cifar-streaming

bash gossip_benchmarks/validate_streaming_learning.sh \
  --data-directory ~/ray-coverage/cifar-streaming
```

Preparation downloads the CIFAR-10 archive through torchvision and writes the
recorded subset. Download/preparation is outside the timed experiment. Reuse the
directory on subsequent runs; preparation refuses to overwrite an existing
manifest. Use a new directory and `--train-rows`/`--validation-rows` to select a
larger dataset, keeping the training count divisible by two worker batches.

The validation command first runs focused tests, then four observations:
OFF/ON no-failure controls and OFF/ON middle-training head loss. Each observation
has a 300-second cap, including cluster startup, workload, final model check and
cleanup. Four caps sum to 20 minutes, plus tests and timeout-cleanup overhead;
this is a budget, not a predicted duration.
If controls fail or do not demonstrate streaming overlap, fault cases are
skipped and the partial report is retained.

For only the initial two controls, add `--controls-only`. For both node kinds
and all failure points, explicitly add:

```bash
--failure-kind head-node --failure-kind worker-node \
--failure-point early --failure-point middle --failure-point late
```

That expanded matrix has 14 observations per repetition. Do not start it until
the short case is checked. `--repeats N` retains every trial and alternates arm
order between repetitions; `--timeout-s` changes the per-observation cap.

Generate plots separately, without running training or needing original result
directories:

```bash
python gossip_benchmarks/plot_streaming_learning.py \
  ~/ray-coverage/streaming-learning-comparison.json \
  --output ~/ray-coverage/streaming-learning.png
```

Both PNG and PDF are written. The top row shows committed epochs; the bottom
shows measured decode completions and batch deliveries, including repeated
work. Delivery is recorded before computation and is not a completed optimizer
update. The active-fault evidence separately records completed and repeated
optimizer updates. Failed, timed-out and unrun observations remain visible.
Timeout stop markers use the parent-observed stop and include cleanup; they are
not completed workload timings.

## Evidence and interpretation

The full report embeds producer events, batch/update events, committed epoch
reports, dataset execution metrics, source/native identities and fault evidence.
No original result directories are needed to replot it.

- Each committed epoch must consume every selected training sample exactly once
  across the two ranks, with the expected rows and optimizer updates per rank.
  Fingerprints of the actual normalized image tensors and labels must match
  the prepared inputs, including after replay/retry.
- A retry must receive the exact committed checkpoint on both ranks. The shared
  active-step harness checks rollback/recomputation and actual worker identities.
- The final persisted model is independently evaluated on the selected held-out
  images and checked against its reported accuracy.
- Producer completion must occur between optimizer updates in every epoch.
  Otherwise validation fails: a lazy plan alone is not proof of overlap.
  Timestamps are comparable because all logical nodes use one physical machine.
- `streaming_overlap.decode_calls_started_after_fault_in_interrupted_epoch`
  distinguishes additional data work in the interrupted epoch from production
  only in later epochs. A zero count must not be described as an in-flight data
  recovery demonstration. The gate does not guarantee an active protected task.
- ON must show task enrollment. `fixed_r_recovered_tasks` reports actual replay;
  zero replay is allowed and means completion did not demonstrate that mechanism.
- Ray's streaming splitter does not guarantee identical batch assignment/order.
  Identical final weights or control predictions are not claimed. The report
  retains training losses, held-out accuracy and between-arm/control accuracy
  differences; short-run accuracy is not a convergence equivalence test.

Transforms are deterministic decode/normalization with no random augmentation.
Telemetry is written separately from task outputs, so timestamps are not part
of replayed data. Both arms use the same two-data-CPU budget, 4 MiB object-store
scheduling budget, block sizing and prefetch settings. The memory budget is a
scheduler target, not a hard physical memory limit. Local telemetry costs are
included in both workloads; this instrumented run is not a minimal-overhead
measurement. The initial workload change did not optimize streaming recovery;
the subsequent helper-reuse change below targets a measured runtime cost.

## Diagnose the no-failure slowdown

The first uploaded four-epoch control run completed OFF in 169.39 seconds of
workload time. ON reached epoch 3, update 22/32 on both ranks before the
300-second observation timeout. It was still making progress. Epoch 2's
checkpoint was committed at 87.95 seconds OFF and 218.65 seconds ON from workload
start (2.49x for the same completed prefix). This is not a completed-run overhead
measurement. The first two model/optimizer checkpoint hashes matched; failure
cases were skipped. The report did not identify which runtime costs dominated.

Use the same prepared data, model, batch size and resource budgets for a shorter
diagnostic pair. The two-epoch exception applies only to controls; fault matrices
still require at least four epochs. This command preserves the original JSON:

```bash
bash gossip_benchmarks/validate_streaming_learning.sh \
  --data-directory ~/ray-coverage/cifar-streaming \
  --controls-only --epochs 2 --profile-fixed-r --timeout-s 300 \
  --output ~/ray-coverage/streaming-learning-profile.json
```

Based on the observed two-epoch prefixes, allow roughly 5–6 minutes for the pair
plus tests; this is an estimate, with two 300-second caps plus cleanup. No failure
injection or plotting runs. This remains an instrumented correctness/diagnostic
run, not a performance optimization or a recovery result.

`--profile-fixed-r` enables local runtime counters through the private
`fixed_r_profile_timing` DataContext configuration before planning. It changes
neither protection coverage nor retry/placement/block/memory settings. It adds
clock/counter overhead to ON and periodic evidence snapshots to both arms.
No additional helper RPCs, barriers, copies or background threads are introduced.

Each operator in `samples[].data_executions[].operators[]` includes
`fixed_r_timing_<phase>_s`, `_count` and `_max_s`. Counts include calls that raise;
absent keys mean no completed timing scope was recorded, not a measured zero.

| Phase | Measured boundary |
| --- | --- |
| `submission` | Whole synchronous Data task submission; includes the setup phases below |
| `owner_liveness` | Existing GCS owner/executor checks on the normal submission path |
| `input_get`, `input_put`, `input_validate` | Fetch/deserialize retained input, make coordinator-owned copy, validate dependencies |
| `helper_create_request`, `helper_ready` | Issue owner-helper creation, then wait for its ready reply |
| `enrollment` | Whole reader enrollment, including the following handshake phases |
| `owner_begin`, `consumer_register`, `owner_confirm`, `enrollment_offer`, `enrollment_poll_sleep`, `consumer_mark_ready` | Existing submission/registration/acknowledgement calls and readiness polling |
| `owner_pull_result`, `owner_pull_pending`, `owner_pull_failed`, `owner_pull_abandoned` | Issue-to-settlement time of each owner read, separated by outcome |
| `consumer_accept_item`, `consumer_accept_eof` | Local/native acceptance of the read result or EOF |
| `output_get`, `output_put`, `output_validate` | Fetch/deserialize a ready streaming output pair, copy its block, validate the copy |
| `release_scan` | Scan previously copied returns and release eligible native references |
| `stream_close` | Whole stream close, including owner close, tombstone barrier and helper kill request |
| `owner_close`, `tombstone_barrier`, `consumer_close`, `helper_kill_request` | Individual closure calls; kill measures the request, not process exit |
| `data_ready_callback` | Whole Data callback, including polling, copying, release, close or recovery |
| `recovery`, `survivor_submit` | Existing replay/fallback paths, only if entered |

These are inclusive **wall times**, not CPU profiles. Nested phases must not be
added together. Tasks/operators/executions can overlap. Owner-read latency also
includes producer readiness and delays before the scheduler polls its response;
it is not pure network time or blocked-thread time. The native handshake timers
do not separate individual holder/witness RPCs. Output copy phases cover the
dynamic streaming path used here, not buffered-envelope unpacking.

`runtime_metrics` alongside these timings records ordinary Ray task counts,
task completion/backpressure durations and object-store spill/free bytes in both
arms. Backpressure and task durations also overlap; they are not additive to the
phase timings. Spill/free counters are not peak memory measurements.

With profiling enabled, the harness atomically replaces each execution's snapshot
approximately every five seconds when its scheduling loop can make progress.
Terminal snapshots have `state=completed` or `failed`; a timeout may leave
`state=running`. The parent embeds the latest snapshots in JSON, without summing
earlier snapshots again. An in-flight timing scope is absent until it exits, so
timeout evidence can be partial. Success still requires the usual input,
checkpoint and streaming checks, plus the requested ON timing evidence.

## Reuse retired owner helpers

The validated two-epoch profile at `7a5a437` completed OFF in 85.93 seconds and ON
in 213.21 seconds (+148.11%). Both committed checkpoints matched. Across ON's
244 tasks, accumulated helper-ready time was 120.17 seconds, owner-begin time
56.33 seconds, input/output get-and-put time 2.21 seconds, and stream-close time
0.58 seconds. These inclusive phase sums are not additive components of the
127.28-second workload difference. No failures were injected in that profile.

Dynamic read/map operators now cache up to **two idle owner helpers per physical
operator execution**. If none is idle, a fresh helper is created; active task
concurrency is unchanged. A helper still owns only one live protected stream.
The cache is local to the existing surviving Data coordinator, is not detached,
does not cross executions/epochs, and does not migrate ownership. Declared-count,
buffered-envelope and exchange submissions retain the fresh-helper path.

Reuse happens only after the prior reader's close, including native durable
tombstone acknowledgement, succeeds. Every checkout has a strictly increasing
generation. Old pull/offer/confirm calls fail; an old close cannot cancel the
current generation. A failed or ambiguous submission discards its helper rather
than caching it or submitting a replacement task. Pool shutdown kills idle
helpers only; failed retirement must still be resolved by the task's close path.
Owner-node loss retains the existing replay and authoritative-death rules.
This does not add helper-actor-process recovery on a surviving owner node.

There is no new C++ code or native build requirement. The workload, producer
placement, holder count, copies, retry policy and checkpoint responsibility stay
the same. The change can only amortize setup when a helper becomes idle before
later task submissions. A burst of concurrent tasks still needs one helper per
active task; the idle limit is not a cap on peak helper processes.

Run the focused validation and the same two-epoch control pair after pulling
`fixed-r-owner-helper-reuse`:

```bash
bash gossip_benchmarks/validate_streaming_learning.sh \
  --data-directory ~/ray-coverage/cifar-streaming \
  --controls-only --epochs 2 --profile-fixed-r --timeout-s 300 \
  --output ~/ray-coverage/streaming-learning-helper-reuse.json
```

The validation now also includes local-raylet tests for dynamic-map reuse and
owner-node loss after a helper has served a previous task, as well as unit cases
for generation fencing, failed barriers and ambiguous submissions. Those tests
run before the learning observations. Neither tests nor benchmarks were run by
the agent; this change remains unvalidated until local results are checked.

The report includes `fixed_r_helper_creations`, `fixed_r_helper_reuses` and
`fixed_r_helper_kill_requests`, both per operator and per sample. Creations and
reuses count checkout attempts, including failures before enrollment. Kill counts
mean requests, not confirmation of process exit. The no-failure ON run requires
actual reuse and matching creation/cleanup-request counts. Existing correctness
checks remain required. Do not infer a speedup until completed timings are read.

For a same-source implementation ablation, add `--fresh-owner-helpers` and use a
different output filename. That disables the cache in ON; OFF remains ordinary
Ray. It preserves all other benchmark settings. Profiling remains opt-in.
Plotting is still separate and reads either report with `plot_streaming_learning.py`.
