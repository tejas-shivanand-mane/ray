# Head loss during uninterrupted Dataset execution

Base: `3a82b7f230b39f64699cc1f461151bab9f3708ad`. The user validated the
public Dataset path with 1 GiB of producer payload: 128 ordered outputs,
two producers and one consumer recovered, and all 144 streams closed.
That run deliberately blocked UDFs at the initial enrollment gate.

This step adds `--head-failure-point early|middle|late|suite`. The suite runs
all three points on separate fresh local clusters and RocksDB directories.
Each point uses the public Dataset pipeline. The existing `gated` default and
physical-plan modes remain available.

## Failure injection and checks

The controller observes the number of final status blocks whose identities,
ordering, row counts, and executor placement have been validated. It requests
head failure after at least 10%, 50%, or 90% of those outputs, while an enrolled
stream is observed active. For 128 outputs the targets are 13, 64, and 116.
It creates no gate actor, adds no UDF wait, and does not pause the Dataset
executor to take a snapshot or replace the head. Normal backpressure still
applies. Actual observed progress is recorded because execution continues
between observation and process termination; percentages are triggers, not
an atomic cut or a claim to deterministic instruction-level fault injection.

An observation includes active enrolled task IDs, task indices, accepted return
cursors, declared counts, and operator counters. Replay records the actual
task ID and task index after successful native recovery. Completed protected
tasks need no replay, so the old exactly-three-recovered assertion is not used.
Success still requires:

- Every expected status block, in order, with no missing or duplicate output.
- Every logical task finished, no task errors, and all returned streams closed.
- Per-stage enrolled plus survivor submissions equal the expected task count;
  replay counts and unique recovered task indices agree.
- At least one successful replay. Killing the head after all relevant streams
  have finished does not count as a passing recovery experiment.
- All original head processes exited, replacement restored the cluster/session
  and GCS endpoint, and the original driver and task executor nodes survived.

The JSON records `validation_status` separately for each point and is written
after each case. A failed case includes its error, traceback, and last observed
progress/counters; later cases still run. The command exits nonzero after the
suite if any case fails. A partial or missed-failure run is never marked passed.

## Enrollment races are part of this validation

The controller can now interrupt helper creation, enrollment, delivery, or
retirement while other streams remain active. The previous gate avoided those
timings. Native replay already waits for authoritative GCS node death.
Enrollment errors still fail closed: this change does not reinterpret a missing
receipt as completed enrollment or blindly resubmit an unknown protected task.
Consequently, the suite may expose an enrollment race requiring a follow-up
protocol fix; its diagnostics are preserved instead of hiding it or changing
failure timing to avoid it. This step adds failure coverage and replay identity
reporting, not a claim that every ungated timing has already passed.

## One command

In the existing `ray-dev` environment with the compiled fork:

```bash
cd /home/tejas/Downloads/ray && git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-dataset-head-timing-suite.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer \
  --recovery-mode fixed_r_head_failure --recovery-plan dataset \
  --head-failure-point suite \
  --local-executor-nodes 2 --num-input-blocks 16 \
  --output-batches-per-input-batch 8 --output-batch-rows 32 \
  --output-row-bytes 262144 --consumer-sleep-s 0.1 \
  --recovery-timeout-s 180
```

This produces 1 GiB per case, 3 GiB across the suite. Later failure points spend
more time on the protected path before replacement and can take longer than the
previous initially gated head-failure run. `--head-failure-point middle`, for
example, selects just one case. No native rebuild is required.

The failure model remains all managed head processes lost with surviving
RocksDB storage and surviving coordinator/executors, not physical host/disk
loss or driver recovery. The Dataset still declares finite per-task counts
and disables fusion/output shaping. Optimization remains deferred.

Regression code covers all three ungated points, forbids gate creation in those
integration cases, checks replay accounting, and checks that the suite persists
an error and continues through the remaining cases. Source inspection only:
no builds, tests, lint, benchmarks, rendering, or Actions were run by the agent.

## Reporting correction after the first user run

The first suite run reported `Object of type method is not JSON serializable`.
Both the active-task observation and the replay identity record mistakenly
stored `task_index` instead of calling `task_index()`. Both now store integers.
The suite also prints the original exception before writing its diagnostics,
and the benchmark writer encodes before opening/truncating an existing report.
Regression code uses a real DataOpTask to JSON-round-trip both records and
checks preservation of an earlier report on encoding failure. These tests
remain unrun by the agent. This fixes reporting; the underlying Dataset failure
is not diagnosed from the reporting traceback alone. Rerun the same suite to
obtain its actual recovery results or preserved failure tracebacks.

## Owner-helper startup failure from the saved suite report

The next user report showed all three head replacements succeeded in about
2.3 seconds, with the original coordinator and executors surviving. Each
Dataset stopped at its trigger (13, 64, or 116 validated outputs) because the
next consumer's owner helper died before it started. Its failed `begin` RPC
aborted submission before the executor could recover active producers. A
second `ActorDiedError` from cleanup obscured the original enrollment failure.

The Data adapter now waits for helper readiness before requesting `begin`.
If startup fails or times out, it waits for authoritative GCS owner-node death
while requiring all configured executors to survive. Only then can this still
unsubmitted task use the existing coordinator-owned submission path, preserving
arguments, task index, executor affinity, and output count. No producer,
descriptor, receipt, or witness offer exists before `begin`, so this transition
cannot duplicate an enrolled producer. If the owner remains alive, the original
startup error is raised. A successful readiness RPC followed by confirmed head
loss uses the same safe transition.

This is deliberately limited to the phase before any `begin` request. Once
enrollment may have started, failures still stop execution; this change does
not retry an unknown protected submission. A dead-helper cleanup RPC no longer
masks the original enrollment or retirement exception. Failure during later
enrollment or retirement may still require a separate protocol fix.

`fixed_r_pre_submission_failovers` counts these startup transitions as a subset
of `fixed_r_survivor_tasks`, separately from actual protected-task replays.
The suite still requires at least one successful replay and complete output,
task, placement, and stream-close accounting. Failure diagnostics now retain
the resolved owner and executor IDs. Failure timing and UDF behavior are unchanged.

Added regression code covers startup actor death, startup timeout, head loss
after readiness, refusal to resubmit without confirmed node death, and refusal
to resubmit after enrollment errors while preserving the original exception.
Source inspection only: no builds, tests, lint, benchmarks, rendering, or
Actions were run by the agent. No native rebuild is needed. Pull and rerun the
same command above in the existing compiled `ray-dev` environment; passing
early/middle/late recovery is still awaiting that run.
