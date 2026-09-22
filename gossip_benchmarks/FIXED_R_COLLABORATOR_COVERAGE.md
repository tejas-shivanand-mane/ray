# One combined step toward collaborator benchmark coverage

Current next step: the shared API/launcher acceptance command, `--entrypoints-only`,
documented in `FIXED_R_USER_INTERFACE.md`. The specialized local backpressure,
actor survival, single-worker and two-worker XGBoost cases have passed. The shared
interface is Python-only and uses the existing native build. Earlier commands
below record previous steps; do not repeat them by default.

The goal is head-process failure recovery with minimal application changes.
This step adds one workload and removes the controlled coordinator pause from
the failure runs. It is a finite coverage gate, not a repeated timing campaign.
Stay on the user's local 16-thread / 32-GiB machine; no cloud provisioning is
needed. No performance or physical-machine resilience claim follows from it.

## Evidence before this change

The uploaded eight-logical-worker `backpressure(1).json` passed all five cases:

| Case | Runtime seconds | Replayed producers | Replayed consumers |
| --- | ---: | ---: | ---: |
| Copy baseline | 68.4199 | 0 | 0 |
| Fixed-R without failure | 82.1689 | 0 | 0 |
| Head failure before output | 44.4033 | 1 | 0 |
| Head failure after output | 57.3695 | 16 | 0 |
| Head failure at consumer submission | 61.7793 | 16 | 1 |

Every case completed 48 tasks, validated 4096 producer rows and 32 final output
blocks, and reported no recovery errors or active protected streams. Head
replacement took 1.34, 1.78, and 2.35 seconds in the failure cases. Those runs
paused the Dataset coordinator during replacement. The original intermittent
consumer timeout remains unexplained; later successful runs do not establish
that a particular runtime fix resolved it.

## New implementation

Automatic Fixed-R task recovery now accepts a final `OutputSplitter` following
the supported linear task-map/read chain. The existing split coordinator is
pinned to the driver's node. Its actor, the driver, and the consuming trainers
must survive; this is not general actor recovery. Copied producer outputs and
ordinary split/slicing tasks are owned by that surviving coordinator.

The original `training-prefetch` function now shares a builder with the recovery
harness, as the other benchmark paths already do. `produce`, `Trainer.train`,
its `iter_batches` call, prefetch settings, SPREAD trainer placement and
`streaming_split(equal=True)` behavior are unchanged. The harness validates
each consumed batch through an iterator wrapper, counts rows per trainer, and
checks the split's intentional remainder truncation. A zero-CPU coordinator
keeps CPU-consuming trainers on executor nodes without rewriting Trainer.
Each two-CPU worker retains CPU capacity for producer tasks in the local profile.

Both task benchmark families accept `--recovery-head-timing` with early,
middle, late, or suite. A callback signals after 10%, 50%, or 90% of the selected
stage's expected rows have been delivered. The executor does not wait for the
controller. The controller kills/replaces the head while scheduling, delivery,
and task submission are free to continue. Ordinary GCS/recovery waits may still
block runtime operations; “asynchronous” does not mean zero interruption.
These row counts schedule the fault only; they are not streaming return-count
declarations or recovery prerequisites. Normal block shaping can overshoot the
requested percentage, so the JSON records the actual trigger row count.

For backpressure and training-prefetch, progress means **producer output rows**.
For worker scaling, it means output rows from the selected read or map stage.
The observation is taken when the executor signals, not an atomic snapshot of
the later head-process death. A stage that finishes before a fault is exercised
does not pass: the target stage must show actual protected task replay. Such a
miss is reported explicitly instead of being counted as successful recovery.

The previous paused cases remain available with the default `paused` value.
Worker-scaling failure diagnostics capture local wait states before shutdown.
Training-prefetch also has a separate observational thread in the surviving
split coordinator. It publishes pending references, per-task wait reasons,
backpressure state and thread stacks every five seconds, even if the executor
callback is blocked. Trainer progress is reported at most once per second plus
EOF. These reports are best-effort observations, not atomic snapshots.

## Single local command

Use the existing source-built `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh
```

No native rebuild is required by this Python-only change. The script runs 11
distinct cases, once each, and writes one combined report:

`/home/tejas/ray-coverage/coverage.json`

| Workload | Cases |
| --- | --- |
| Training-prefetch | Copy, protected no-failure, asynchronous middle failure |
| Worker scaling: read plus two map stages | Copy, protected no-failure, asynchronous early read failure, asynchronous early final-map failure |
| Worker scaling: single map with original unbounded task pool | Asynchronous middle map failure |
| Fast producer / slow consumer | Asynchronous early, middle, late failures |

The existing paused backpressure cases and its baselines are not repeated.
Independent workload groups continue even if another group fails so one report
shows all coverage gaps. Any failed/missing case makes the script exit nonzero.
Each invocation uses a fresh result subdirectory, preventing stale case files
from becoming a false pass. The combined report requires exactly 11 passes.

Backpressure and training-prefetch each use 4 GiB of logical producer data.
Training uses eight trainers and two prefetched batches per trainer. Worker
scaling uses eight task workers, four blocks per worker, and the original
16-MiB row-sizing formula with 128 scalar and 32 array columns. These are local
correctness sizes, not the collaborator's original 5000-worker configuration.

Both reports and Ray temporary/spill files must be on disk because the user's
`/tmp` is tmpfs. Defaults are `$HOME/ray-coverage` and `$HOME/raytmp` respectively;
override with `RAY_RECOVERY_OUTPUT_DIR` and `RAY_RECOVERY_TEMP_DIR`. Spill paths
are kept separate and short to avoid Unix socket path-length limits.

## What remains after this gate

| Collaborator variant | Status / boundary |
| --- | --- |
| Fast producer / slow consumer | Paused local head failure passed; asynchronous coverage added here |
| Worker scaling, task-based | Original read/map/materialize chain supported; asynchronous single/chained coverage added here |
| Training-prefetch / streaming_split | Copy and protected no-failure passed; asynchronous head-failure stalled and remains unresolved |
| Worker scaling, actor-based (the benchmark's default) | Still rejected; actor state/lifecycle recovery requires separate implementation |
| Multi-machine and 5000-worker scale | Not established on one local machine |

If this gate passes, do not add more repetitions or phase permutations as the
default next step. Prioritize the actor-based worker-scaling gap. In particular,
silently rewriting the actor benchmark as tasks would not establish actor
recovery or preserve the benchmark's purpose. The new training-prefetch path
does not justify a claim of general actor failure recovery.

Failure scope remains all managed head processes with surviving GCS RocksDB,
driver, and workers. Replacement is harness-managed. Subsequent task ownership
moves to the surviving coordinator; repeated owner failure is not covered.
Fusion/eager freeing remain disabled, and coordinator-owned output copying and
payload validation remain part of these correctness runs.

## Verification performed here

Source review only. No build, tests, lint, or benchmarks were run by the
assistant. Added regressions cover nonblocking trigger signaling, rejection of
unexercised faults, and exact equal-split/trainer row accounting with a remainder.
The first combined user run is recorded below. The follow-up changes have not
been executed by the assistant.


## Follow-up to the uploaded coverage.json

The first combined run reported six passes and five failures, with no missing
case results:

| Cases | Observed result |
| --- | --- |
| Training-prefetch copy and protected no-failure | Passed: 16 producer tasks, 4096 validated rows, eight trainers consuming 512 rows each |
| Two-map worker chain copy and protected no-failure | Passed: all 96 read/map tasks finished and output validated |
| Two-map worker chain asynchronous read failure | Passed: 13 protected read tasks replayed |
| Single-map asynchronous middle failure | Passed: nine protected map tasks and one read task replayed |
| Backpressure asynchronous early/middle/late | All failed at helper readiness with ActorUnschedulableError |
| Two-map chain asynchronous early final-map failure | Output completed and validated, but no task replayed; recovery coverage was not established |
| Training-prefetch asynchronous middle failure | Timed out after 600 seconds; 11 tasks replayed, ten finished, 3328/4096 producer rows delivered |

All three backpressure failures have the same confirmed startup error. A helper
with hard affinity can become unschedulable when the head dies between the
liveness check and helper startup. The runtime now handles
`ActorUnschedulableError` in its existing **pre-begin** failover path, alongside
actor death and timeout. GCS must still confirm owner-node death; otherwise the
original error propagates. An error after begin may have run never authorizes
fresh submission. A source regression covers these three boundaries.

The bounded worker-map chain now signals at the first protected submission
**after** the selected output-progress threshold, rather than at the last
output of a wave that can finish before the controller kills the head. Read and
single-map triggers retain output-progress signaling. No UDF or executor pause
is added. The JSON identifies the trigger type, and actual replay in the
selected stage remains mandatory. This reduces the missed-fault window but
does not guarantee a particular thread schedule.

The training timeout is **not fixed or diagnosed** by these results. Its six
remaining active tasks had accepted four returns each, but the report lacked
pending-payload and scheduler wait states. The new remote observations preserve
that evidence if the stall recurs. That follow-up retained the 600-second timeout; the subsequent change below
reduces it at the user's request.

Run only the five gaps, once each, using the existing source-built environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --failed-only
```

The result is `/home/tejas/ray-coverage/coverage-retry.json`; the original
`coverage.json` remains intact. This is a fixed follow-up profile for the five
gaps above, not an automatic parser of arbitrary failure reports. It skips all
six passing cases, requires five passing results, and does not relabel old
results or merge them into a claim about the new revision. No native rebuild
is required. The existing no-argument command still runs the full 11 cases.


## Results from coverage-retry.json and the remaining stall

Four of five follow-up cases passed on the user's local machine:

| Case | Seconds | Replayed tasks |
| --- | ---: | --- |
| Chained worker maps, early final-map failure | 40.62 | Four first-map tasks, one final-map task |
| Backpressure early failure | 62.74 | 16 producers |
| Backpressure middle failure | 76.04 | Nine producers |
| Backpressure late failure | 79.22 | Three producers |

Each passing case completed all expected tasks, validated output, closed its
streams, and reported no recovery errors. Each backpressure case also exercised
one pre-submission failover. Do not repeat these four cases by default.

Training-prefetch still timed out at 600 seconds. It attached 11 replays, finished
eight tasks, and delivered 22 blocks / 2816 rows. Eight tasks remained in
`waiting_for_pair_payload`, all with ready metadata but missing local block
payloads. Six had accepted four returns and two had accepted two. The executor
was polling payload readiness; output backpressure was false. All trainer get
calls were waiting for split output, with no trainer progress reported.

This establishes the location of the stall, not its native cause. The report
does not show whether those replays are executing or which payload locations /
object transfers are pending. Do not claim a fix or change the benchmark's
splitting, locality, or prefetch semantics to hide the failure.

Before another benchmark run, collect existing native logs:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
python gossip_benchmarks/collect_fixed_r_training_logs.py
```

Upload `/home/tejas/ray-coverage/training-prefetch-native-logs.json`. This command
uses only Python's standard library, starts no Ray processes, and runs no
benchmark. It matches the failed report's node/cluster identifiers or GCS PIDs
to existing session logs, avoiding `session_latest` from the later successful
cases. It includes bounded excerpts of raylet, debug-state and core-worker logs;
missing files and truncation are explicit. `--report`, `--temp-dir`, and `--output`
are available for non-default paths. The original benchmark report is unchanged.

The local coverage script now supplies `--recovery-timeout-s 120` instead of 600
for both workload families, as requested. This bounds the configured recovery /
benchmark waits; cluster startup and cleanup are additional time. No rerun is
requested with this log-collection step. The timeout and collector changes were
reviewed as source only; no tests, builds, lint or benchmarks were run here.

## Native log evidence and executor return cleanup

The uploaded `training-prefetch-native-logs.json` matches the failed training
session. The split coordinator completed all 11 replay dispatches and worker
lease returns and received 44 generator-item reports. Replay execution therefore
occurred. Its remaining pulls included a block advertised on an executor where
the object manager repeatedly logged `Invalid Push request` for that same ID.
The consumer had room for that pull. This points to an absent advertised payload,
not merely a replay waiting for a CPU. The excerpts omit parts of the failure
window, so they do not prove the exact deletion interleaving.

Source review found a compatible race: replay uses deterministic return IDs,
and `CreateExisting` can reuse an old Plasma copy. Pinning an existing ID under a
different owner leaves the original owner bookkeeping in place. A pending free
or spill completion from that owner can then remove the replay's advertised
copy. The consumer-side ownership barrier did not drain executor-side cleanup.

The native path now prepares each replayed Plasma yield on its executor before
allocation. Its raylet must have observed the original owner node's death in
GCS, flushed old frees, drained freed spill bookkeeping, and observed removal of
any local copy. Freed spilling objects remain pending even after the free batch
is flushed. Replay then creates a fresh copy with the surviving consumer as
owner. A copy that reappears between this barrier and allocation is rejected
instead of silently reused. This preparation has a 30-second deadline per
return. Ordinary execution and inline returns do not use this barrier.

This fixes the identified cleanup gap; successful training recovery remains
unverified. It is not a claim that every possible stale transfer race is solved.
A native regression source covers the freed-but-still-spilling state after a
free-batch flush. No tests, build, lint, or benchmark were run by the assistant.

In the existing `ray-dev` environment, rebuild the source fork (both native
binaries and generated protocol bindings changed), then run only the remaining
training-prefetch head-failure case:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
SKIP_BAZEL_BUILD=0 RAY_BUILD_CORE=1 python -m pip install -e ./python --no-build-isolation --no-deps &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --training-only
```

The report is `/home/tejas/ray-coverage/coverage-training.json`. The profile
requires exactly one passing case, uses a fresh result directory, and retains
the existing workload, eight trainers, and prefetch settings. Its configured
benchmark/recovery timeout is 120 seconds; native compilation, cluster startup,
and cleanup are additional time. The already-passing cases are skipped. If this
case passes, the next implementation gap is actor-based worker scaling, not
more repetitions of these task-based cases.

## Training-prefetch passed; next cover surviving actor maps

The uploaded `coverage-training.json` passed its one asynchronous middle-failure
case: 24.117 seconds total, 9.981 seconds from failure request to completion,
13 producer replays, all 16 tasks finished and streams closed, and no recovery
errors. All 4096 rows were validated; each of eight trainers consumed 512 rows.
The executor and UDF were not paused. This is a Ray Data workload, not the
collaborator's XGBoost Ray Train benchmark. Do not rerun it by default.

The next implementation permits actor-map stages in the existing dynamic
streaming Fixed-R Data chain. Ray Data's surviving coordinator creates the
ordinary actors and owns their method calls and outputs. Fixed-R still protects
task-based reads and any task-map stages; actor calls are not enrolled or
replayed. The runtime selects hard affinity on the surviving executor nodes,
disables fusion, enforces non-detached coordinator ownership, and disables
actor/method retries. Initial actor placement continues across map stages to
avoid concentrating each pool on the first few nodes. Incompatible placement,
detached/reused actors, and observed actor death/restart are rejected. Actor
inputs must be ready coordinator-owned values without nested ObjectRefs, checked
by the existing native input validator. General actor-state recovery is outside
this path; application references to unrelated head-owned objects remain outside
the supported dependency contract.

The benchmark keeps the original `RealisticSchemaUDF`, constructor, fixed actor
pools, half-CPU actor resource request, normal streaming output and
range/map_batches/materialize chain. No task replacement or UDF wrapper is used.
The harness observes initialized actor/worker IDs, node IDs and PIDs before
execution and after materialization, retaining handles until validation to avoid
normal end-of-job actor collection. The same processes must remain on surviving
workers. It also requires exact output schema, values and row counts, no failed
tasks, unchanged pool counts, and actual protected-read replay. Actor maps have
separate survival accounting; they are never reported as Fixed-R task replays.

One local case uses eight actors split across two operators on four logical
executor nodes, four blocks per worker, and the collaborator's original 200
scalar / 400 array column schema. All actors are initialized before execution;
head failure is triggered asynchronously after early read-output progress, with
no pause in the executor or UDF. This checks survival of initialized actors and
the mixed task/actor pipeline; it does not guarantee a particular actor method
is in flight at the instant of head death. A missed read replay is a failure,
not a successful recovery result.

Run in the existing source-built `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --actors-only
```

The combined report is `/home/tejas/ray-coverage/coverage-actors.json`. It requires
one passing case and uses a fresh result directory. Existing profiles and
reports remain separate. The recovery/benchmark timeout is 120 seconds; actor
identity probes are bounded at 30 seconds, and cluster startup/cleanup take
additional time. No native rebuild is needed beyond the previous cleanup fix.

Implementation and regression sources were reviewed, but no build, tests, lint
or benchmark were run by the assistant. Regressions cover forbidden actor
placement/ownership, disabled retries, actor loss, changed process identities,
and unsupported benchmark modes. Actor recovery coverage remains unverified
until the user runs this case. The 5000-actor/15-operator cloud configuration and
both XGBoost Train sizes remain unvalidated; this does not claim Train support.

## Actor case passed; next integrate the original XGBoost Train pipeline

The uploaded `coverage-actors.json` passed: 9.822 seconds total, 3.414 seconds
from failure request to materialization, one protected read replay, and all
96 read/map tasks finished. All 10304 output rows were validated. Eight actors
kept the same actor IDs, worker IDs, node IDs and PIDs through head replacement.
At the trigger snapshot actors were initialized but no map calls were submitted,
so this establishes initialized-actor survival and subsequent pipeline completion,
not failure during an active actor method. Do not repeat this case by default.

The next step uses Ray Train v2's original XGBoost benchmark functions:
`train`, `xgboost_train_loop_function`, `RayTrainReportCallback`, `predict`, and
`XGBoostPredictor`. The ten boosting rounds, training parameters, Dataset shard
materialization, checkpoint loading, prediction batch size and actor concurrency
formula are unchanged. Optional function arguments supply a local storage path,
RunConfig and read block count. LightGBM is optional for an XGBoost invocation.

The local harness uses one one-CPU Train worker, 32768 synthetic binary-label
rows with 16 float features in 32 Parquet files, and four surviving logical CPU
nodes. These are local integration data, not the collaborator's 10-GB S3 data or
cloud machine sizes. A zero-CPU coordinator keeps Train workers off the driver;
the head also has zero CPUs. Existing Train v2 placement keeps the controller,
Rabit tracker, data manager and split coordinator with the surviving driver.
The harness runs the original pipeline inside a separately owned job actor so
an expired case can be terminated without waiting indefinitely in `Trainer.fit`.

Head failure is requested asynchronously after early Parquet read progress
during **training data ingestion**. The executor and UDF do not wait for failure
injection. Validation requires actual Fixed-R read replay, successful completion
of all Data stages and closed task streams, the same Train controller and worker
processes, a loadable ten-round checkpoint, and actor-based inference followed
by Parquet output. Persisted predictions must have the expected count/schema and
match direct inference from the saved model as an unordered multiset of values.
This does not independently establish per-row ordering or model equivalence to
a separate no-failure training run. Train worker/controller retries are disabled.

The runtime also distinguishes external writes from replayable computation.
Write tasks remain owned by the surviving Dataset coordinator and execute on
surviving nodes with retries disabled, even while the original head is alive.
They are never enrolled in Fixed-R or replayed on head loss. This does not add
transactional/exactly-once writes or recovery of a lost writer. Actor outputs
remain coordinator-owned as in the previous actor-map integration.

In `ray-dev`, install the Train dependencies from this source fork plus the
repository's pinned XGBoost version, with native compilation explicitly skipped,
then run the single new case:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
SKIP_BAZEL_BUILD=1 python -m pip install -e './python[train]' 'xgboost==2.1.0' --no-build-isolation &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --xgboost-only
```

If these dependencies are already installed, the pip line can be omitted. This
does not install a released Ray package. The existing compiled fork must include
the earlier native streaming cleanup change. No new native rebuild is required.

The combined result is `/home/tejas/ray-coverage/coverage-xgboost.json`; generated
input, checkpoint and predictions stay in its fresh result subdirectory. The
job uses one 120-second processing deadline across training and inference;
worker startup/health/collective settings are 30 seconds, identity probes at most
15 seconds, and the head-replacement observation wait uses at most 30 seconds
or the remaining case budget. Cluster startup/process replacement/cleanup and
initial dependency installation add overhead. No earlier cases are repeated.

Source review only: no build, tests, lint or benchmark were run by the assistant.
Regression sources reject missing replay, changed worker processes, enrolled
writes and missing prediction rows; an additional regression checks that writes
are submitted from the coordinator without retries even when the head is alive.
Train coverage remains unverified until this case passes. It does not claim
recovery of lost model/actor state, failure during boosting, head-disk loss,
multi-worker collective recovery, or either exact 10-GB/100-GB cloud configuration.

The first executable XGBoost report (`run.fG6pcu`) failed in the observation
callback before head injection: the V2 Parquet plan starts with `ListFiles`,
followed by `ReadFilesParquetV2`. The callback and final validator now select the
actual Parquet reader in either the V1 or V2 plan; listing replay cannot satisfy
the training-read recovery requirement. The local context also sets the minimum
block size to zero so the V2 partitioner can retain the requested 32 small read
buckets. Training and prediction functions remain unchanged. Regression sources
cover the V2 stage selection and reject listing-only replay; none were executed
by the assistant. Rerun only `--xgboost-only`, without reinstalling or rebuilding.

## Single-worker XGBoost passed; next run two training workers

The uploaded `coverage-xgboost(1).json` (`run.RX1KYA`) passed in 39.623 seconds.
All original head processes exited; the replacement was ready after 2.329
seconds. Fixed-R replayed two Parquet-read tasks and one listing task. The same
Train controller and worker processes completed training, a ten-round checkpoint
was loaded, and all 32768 persisted predictions matched direct inference from
that checkpoint. Failure request to full completion was 28.110 seconds, including
the remaining training and inference; it is not a recovery-only latency.

The next profile runs only the missing distributed-training case, with two
one-CPU Train workers, the same 32768 rows and four two-CPU logical executor
nodes. An optional `placement_strategy` argument to the original benchmark's
`train()` selects `STRICT_SPREAD` for this local profile. This leaves one CPU
available on each training node for the read tasks that its worker is waiting
on. The default benchmark placement remains `PACK`. The training loop, ten
boosting rounds, XGBoost collective backend, checkpoint callback and predictor
are unchanged.

The lifecycle probe records both ranks' world size, rank and process identities
before training and before worker shutdown. Validation requires ranks 0 and 1
in a world of size two, two distinct worker processes on distinct surviving
executor nodes, and unchanged identities across the failure. Existing checks
still require actual Parquet-read replay, completed Data stages, the same Train
controller, a loadable checkpoint and correct persisted predictions. The head
is still killed during shared data ingestion; successful distributed boosting
after that fault is required, but failure during an active boosting collective
and recovery of lost model state are not claimed.

Run in the existing `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --xgboost-multi-only
```

No reinstall, native rebuild or repeat of the single-worker case is needed.
Output: `/home/tejas/ray-coverage/coverage-xgboost-multi.json`. The new profile
preserves the earlier `coverage-xgboost.json`. The case retains a 120-second
processing deadline plus cluster startup/cleanup overhead, with the same shorter
internal waits. No builds, tests, lint or benchmarks were run by the assistant.
Source regressions reject missing/restarted workers, duplicate ranks, incorrect
world size, packed placement and workers placed on the coordinator. The two-worker
case remains unverified until the user runs it; the exact ten-worker/100G cloud
configuration remains unvalidated even if the local case passes.

## Two-worker XGBoost passed; simplify application enablement

The uploaded `coverage-xgboost-multi.json` (`run.RXclvg`) passed in 39.602 seconds.
Both ranks 0 and 1 reported world size two, occupied separate surviving executor
nodes, and retained their actor/worker/node IDs and PIDs. All head processes
exited; replacement took 2.329 seconds. Two Parquet reads and one listing task
replayed. A ten-round checkpoint and all 32768 persisted predictions validated.
Failure request to full completion was 28.050 seconds. This establishes the
local two-worker ingestion-failure case, not an active boosting failure or the
ten-worker/100G cloud configuration.

The next change consolidates configuration, local cluster supervision and generic
failure observation under `ray.experimental.recovery`. Benchmarks can run through
their normal entry points with the common launcher, or add `recovery.enable()`
after initialization. Recovery-specific legacy CLI dispatch moved out of the
two Data benchmark bodies. See `FIXED_R_USER_INTERFACE.md` for API, cluster setup,
disable semantics, the single combined acceptance command and remaining scope.
