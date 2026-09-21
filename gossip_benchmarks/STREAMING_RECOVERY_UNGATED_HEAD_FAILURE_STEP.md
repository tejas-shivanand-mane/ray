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
