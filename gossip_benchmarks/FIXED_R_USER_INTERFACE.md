# Enable recovery without rewriting the workload

This source fork now provides `ray.experimental.recovery`. The native runtime
still supplies Fixed-R protection/replay; the Python interface configures it
for supported Dataset pipelines and Ray Train ingestion. This is experimental,
not a general actor/model-state recovery feature or an upstream Ray release.

## Existing scripts: use the launcher

On a surviving worker host, with an already configured cluster:

```bash
python -m ray.experimental.recovery --address auto --timeout-s 120 -- benchmark.py <normal-arguments>
```

The script keeps its normal `ray.init()` call (including runtime environments)
or Ray's automatic initialization. The launcher runs its normal `__main__` with
the original arguments and sibling import path. An opt-in initialization hook
enables recovery before Dataset/Train work. No benchmark-specific callbacks,
task counts, node IDs or recovery dispatch branches are required. The script
must use a native Ray connection; Ray Client is not supported here.

The external launcher does not provision machines or restart their head. Start
the cluster with the settings returned by `recovery.system_config()`, persistent
GCS storage, and a deployment supervisor that restores the same cluster/endpoint
after head process loss. The application driver and required workers must be on
surviving hosts. Enabling the API checks native algorithm settings and discovers
the head and surviving CPU executors; it cannot verify that a storage volume
will physically survive or that a deployment supervisor is correctly configured.

The external launcher reports script exit status, not proof of fault recovery.
Its timeout is configurable for real workloads. Existing script choices still
apply: recovery does not rewrite resource requirements, datasets, actor methods,
user retries, XGBoost worker counts or placement groups.

## Application code: two added lines

```python
import ray
from ray.experimental import recovery

ray.init(address="auto")
recovery.enable()

# Existing Dataset / Train code follows unchanged.
```

Enable before constructing Datasets/Trainers, which snapshot their DataContext.
Data and Train propagate that context through their existing execution paths.
An explicit `context=` parameter is available for code using DataContext.current.
The API defaults to streaming recovery and a 120-second operation timeout.

For cluster provisioning, merge this dictionary into the head's startup
`--system-config` JSON (or `_system_config` for `ray.cluster_utils.Cluster`):

```python
from ray.experimental import recovery
native_settings = recovery.system_config()
```

Storage/supervisor settings are separate. These native settings must be applied
at cluster startup; a Python call cannot enable missing native support on an
already running cluster or a released Ray wheel.

## Local head-failure evaluation

The reusable launcher also contains the existing Linux process-cluster fixture:

```bash
python -m ray.experimental.recovery \
  --local --inject-head-failure --timeout-s 120 --report /home/tejas/ray-coverage/example.json \
  -- release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer --num-input-blocks 16 \
  --output-batches-per-input-batch 8 --output-batch-rows 16 \
  --output-row-bytes 1048576 --consumer-sleep-s 0.1
```

Use a disk-backed `TMPDIR`/`RAY_TMPDIR` for this machine. The combined command
below configures those automatically. Local mode creates a zero-CPU head, a
zero-CPU coordinator and four two-CPU executor nodes. The application runs in
its own driver process attached to the coordinator. Two training workers use
`STRICT_SPREAD` and one CPU each, leaving capacity for reads on each worker node.

An evaluation-only observer chooses the first task read/map stage (skipping
file listing and writes), requests failure after at least two output blocks
while a protected task remains active, and does not pause the executor/UDF.
For the short actor benchmark input reads, the acceptance harness selects
`RAY_RECOVERY_FAILURE_TRIGGER=task-submission` instead: request failure as soon
as the first protected read is admitted, before requiring output. The report
records the trigger kind, row count and supervisor response delay. This is
read recovery/actor survival coverage, not failure during actor method execution.
The supervisor kills all head processes and restores the head from the same
surviving RocksDB directory and endpoint. This does not simulate host/disk loss.

Passing requires normal script completion, successful observed Data stages,
closed task streams, and actual replay in the stage that triggered failure.
Writes remain coordinator-owned with no replay. Generic runtime observations
do not replace application-specific correctness checks. The XGBoost acceptance
case additionally validates its saved checkpoint and prediction values outside
the benchmark. Earlier specialized reports remain the evidence for worker/actor
process continuity; the generic observer does not repeat all of those probes.

## One acceptance command for this refactor

In the existing source-built `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --entrypoints-only
```

No reinstall or native rebuild is needed. This runs the normal main entry points
for backpressure, actor worker-scaling and two-worker XGBoost, once each. These
are integration checks of the new interface, not repetitions of the old test
suites. Each case has a 120-second processing deadline plus cluster startup and
cleanup. Three cases therefore have up to six minutes of processing in total;
execution stops at the first failure. No runtime estimate is claimed before a run.

Result: `/home/tejas/ray-coverage/coverage-entrypoints.json`. Each fresh run
subdirectory also holds the normal benchmark JSON files and separate launcher
reports. Older `coverage-xgboost*.json` and actor reports are preserved.

To continue after a failed acceptance case without repeating earlier passes:

```bash
bash gossip_benchmarks/run_fixed_r_collaborator_coverage.sh --entrypoints-resume
```

This preserves a copy of the existing combined report, retains passed cases
with matching workload arguments, and runs the failed/unrun cases in fresh
clusters. Retained cases are explicitly historical evidence, not executions of
the latest revision. The combined output stays at `coverage-entrypoints.json`.
Each new launcher report includes `application_log` (complete stdout/stderr)
and `application_output_tail` (last 32 KiB), including the child traceback on
failure. Application output is written to the printed log path during execution.

The first uploaded entry-point report (`run.skCNwx`) passed backpressure with
six producer replays. The actor pipeline finished all 32 reads and 64 actor
calls, but its script exited with status 1 and its read stage recorded zero
replays. XGBoost was not run. That report omitted the child traceback, so it
does not identify the exact exception. Source review found two blockers in
normal post-execution reporting: the observer was a local class that standard
schema pickle cannot serialize, and detailed scheduling statistics query the
dashboard State API although the local fixture disables that dashboard.

The observer is now module-level with plain settings carried in DataContext.
For recovery-enabled Datasets, unavailable optional scheduling telemetry logs
a warning and returns no overhead samples; the query uses a five-second HTTP
timeout (API address resolution has its own existing retry behavior). Normal
disabled behavior and execution failure handling remain unchanged. Actor-case
injection is earlier and supervisor polling is faster, but a missed replay
still fails validation. No benchmark workload change was needed for these fixes.

The original UDFs, training loop, predictor and boosting parameters remain
unchanged. Worker-scaling gains ordinary output-directory/skip-upload options;
XGBoost gains ordinary local data, storage, block sizing and resource options.
Old recovery CLI options remain available through `streaming_recovery_legacy_cli`
for reproducibility, but are not used by this acceptance command.

Source review only: the assistant did not run builds, tests, lint or benchmarks.
The shared interface remains unverified until this acceptance pass succeeds.

## Disable behavior and scope

Without launcher opt-in or `recovery.enable()`, fresh Data contexts use ordinary
execution. `recovery.disable()` clears cached/explicit recovery configuration,
removes launcher observers from the current context and restores its previous
eager-free setting. Turning the DataContext flag off also clears a configuration
cached while the flag was enabled. Legacy explicitly installed private experiment
configs remain explicit opt-ins until cleared; the public disable API clears them.

Disabling affects future Datasets/Trainers only. Previously created snapshots and
running work are unchanged. Native cluster flags are fixed at startup; measuring
fully disabled native overhead requires a fresh cluster with those flags off.
Imports, flag checks and some existing bookkeeping still execute when disabled.

Supported recovery remains finite deterministic task read/map chains, supported
splitting, and coordinator-owned surviving actor maps/writes. The driver, workers
and GCS storage must survive head replacement. Unsupported plans fail before
dispatch. This does not establish general actor/model-state recovery, lost-writer
recovery, exactly-once external effects, failure during XGBoost boosting, or the
collaborators' exact cloud-scale configurations.
