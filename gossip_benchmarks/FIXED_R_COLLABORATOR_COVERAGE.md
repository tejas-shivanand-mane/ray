# One combined step toward collaborator benchmark coverage

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
that evidence if the stall recurs. The 600-second timeout is unchanged.

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
