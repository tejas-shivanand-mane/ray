# Original backpressure UDFs with normal block shaping

Base: `efc351de02b975c9be86fab0893352e46f068ae8`.

The user validated the instrumented, unshaped public Dataset workload at early,
middle, and late head failure: all three passed with 128 final outputs, 144
finished/closed streams, two producer replays, one consumer startup failover,
and zero spilling per case. Each case generated 1 GiB of logical producer data.
Total runtimes were 27.78, 57.41, and 84.54 seconds; head replacement took about
2.3 seconds. These results do not validate the new mode below.

## Compatibility change

`--recovery-workload original` calls the actual `produce` and `consume_slow`
functions in `backpressure_benchmark.py`, serialized by value so executor
workers do not need the release directory on their import paths. The producer
returns its original NumPy batches with no extra columns or Arrow metadata.
The consumer calls the original sleep/status function, then adds task index,
input row count, and executor ID to its tiny status output for validation.

Both public `map_batches` stages use the original default batch format and
batch size. Normal block coalescing and splitting are enabled; the earlier
one-yield/one-block override is off. The original entry point and UDF bodies
are unchanged. The new option requires the Dataset head-failure path and an
ungated early, middle, late, or suite trigger.

Fixed-R still needs a finite physical return count before task submission.
Before the timed run, calibration executes one original producer task through
Ray's real batch transformer and output buffer, discards blocks incrementally,
and retains their row counts. This is valid for this particular producer,
whose output size and number of batches are independent of input values.
It is not a solution for arbitrary input-dependent or nondeterministic UDFs.
Calibration uses the configured target block size, which is also applied to
the real Dataset. Extra planner splitting or a changed target is rejected.
`recovery_calibration_s` reports this extra preparation time separately.

Failure targets now use the calibrated number of physical consumer outputs,
which may differ from the number of producer UDF yields. Native declared-count
checks remain strict. The drain validates consecutive consumer task indices,
the calibrated input row sequence, status values, and consumer placement.
Operator checks verify total producer rows, task counts, output block counts,
copy counts, stream retirement, unique replay records, and at least one replay.
Producer identity metadata is intentionally absent from this variant, and the
JSON reports its validation scope rather than claiming per-input payload
identity checks or observed producer node IDs.

## One next run

In the existing compiled `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray && git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-original-workload-head-failure.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer \
  --recovery-mode fixed_r_head_failure --recovery-plan dataset \
  --recovery-workload original --head-failure-point middle \
  --local-executor-nodes 2 --local-object-store-mb 1024 \
  --num-input-blocks 16 --output-batches-per-input-batch 8 \
  --output-batch-rows 128 --output-row-bytes 262144 \
  --consumer-sleep-s 0.1 --recovery-timeout-s 180
```

This processes 4 GiB of logical producer data in the timed workload, plus one
256 MiB producer calibration outside the timer. Object stores are 1 GiB per
logical node. The startup calibration line reports the actual shaped counts;
do not assume 128 final outputs as in the earlier unshaped run.

The driver/coordinator and two executors still survive on separate logical
nodes from the failed head. GCS uses surviving RocksDB storage, fusion remains
disabled, output blocks are copied into coordinator ownership, producer
concurrency is fixed, and task placement is controlled. The producer payload
size, sleep duration, and node count remain smaller than the collaborator's
original full-scale settings. This adds original UDF and normal block-shaping
coverage; it does not establish an unchanged eight-worker deployment, driver
recovery, training-prefetch support, or worker-scaling support.

Added regression code covers real coalescing and splitting during calibration,
the consumer's shaped input size, and a head replacement integration case using
the original UDFs. Source inspection only: no builds, tests, lint, benchmarks,
rendering, or Actions were run by the agent. No native rebuild is required;
the new recovery mode is awaiting user validation.
