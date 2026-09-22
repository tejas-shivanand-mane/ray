# No-failure performance comparison

Use the existing source-built `ray-dev` environment:

```bash
cd /home/tejas/Downloads/ray &&
git pull --ff-only &&
bash gossip_benchmarks/run_fixed_r_overhead.sh
```

No reinstall or native rebuild is needed. This is a performance measurement,
not another failure/recovery acceptance suite. The assistant has only reviewed
the measurement implementation; no builds, tests, lint or benchmarks were run.

## What runs

| Local workload | Size | Default observations |
| --- | --- | --- |
| Fast producer / slow consumer | 16 inputs, 8 batches/input, 16 rows/batch, 1 MiB/row, 0.1 s consumer sleep | OFF + ON |
| Actor worker-scaling | 8 actors, 2 map stages, 32 blocks, 200 scalar + 400 array columns; 10,304 rows | ON + OFF |
| XGBoost single worker | 32,768 rows, 16 features, 10 rounds, 1 training worker with 1 CPU | OFF + ON |
| XGBoost multiple workers | Same input/parameters, 2 training workers with 1 CPU each | ON + OFF |

The default runs eight observations total: one pair per case. Treat this as a
preliminary estimate; one sample per mode cannot establish variance/significance.
Each process has a 120-second limit, with cluster startup/cleanup extra. It stops
on the first error/timeout and saves partial results; no automatic retry loop.
The worst-case processing budget is 16 minutes for the default, not a prediction
of runtime. Prior failure-run times are not valid no-failure estimates.

Each observation starts a fresh cluster and fresh application process. Both
modes have the same zero-CPU head and driver node, four two-CPU executor nodes,
512 MiB object stores per node, surviving RocksDB storage, and an enabled
dashboard for the original actor benchmark's State API statistics. This requires
the full dashboard dependencies in the existing environment; a missing dependency
is reported as a startup error, not silently worked around in only one mode.
Data, seeds, workload arguments, thread limits, and storage medium match. Each
observation writes to fresh output/checkpoint directories. XGBoost input is
generated once and shared by all observations. No page-cache eviction is done.

OFF explicitly disables the three native recovery flags and leaves Data recovery
off. ON uses `recovery.system_config()` and the standard launcher opt-in. The
driver is attached to the same logical node in both modes. A post-script audit
checks native settings, Data opt-in, driver placement, absence of the launcher
observer, and unchanged live-node membership. No head process is killed and no
failure observer/polling actor is installed. The ordinary native/Data baseline
is from this fork, not a separately built pristine upstream Ray.

## Metrics and interpretation

Primary runtime comes from the existing benchmark's own timer, excluding cluster
startup and cleanup. The JSON separately records launcher process time, cluster
startup time, and total observation wall time. Data timers include work performed
inside the original benchmark function, including actor-benchmark statistics
collection. XGBoost reports training and prediction separately, plus their sum.
They include cold-job worker initialization and ordinary training coordination;
these are not warmed steady-state throughput measurements.

Runtime overhead is `100 * (ON time / OFF time - 1)`; positive means slower.
Throughput change is `100 * (OFF time / ON time - 1)`; negative means lower
throughput. Rates are logical rows divided by the corresponding runtime:
producer rows for backpressure, output rows for actor scaling, input rows for
XGBoost (not boosting-iteration samples or cluster-wide physical row transfers).
For XGBoost total, the rate uses one input dataset divided by training + prediction
time. The full Data benchmark metrics, including its object-store/spill metrics,
are retained in each sample. XGBoost checkpoint/prediction validation runs after
the measured process and cluster teardown.

This measures the complete cost of enabling the current recovery implementation,
including copying, retention/eager-free, placement, fusion changes and helper
processes. The OFF path retains its ordinary optimizer and scheduling behavior.
An algorithm-only comparison with matched internal execution policies would be
a different experiment. All nodes share one physical machine; these results do
not establish overhead on the collaborators' original cloud configurations.

## Outputs and optional repetitions

- Latest report: `$HOME/ray-coverage/overhead.json`
- Latest summary: `$HOME/ray-coverage/overhead.csv`
- Archived report, raw metrics, settings and application logs:
  `$HOME/ray-coverage/overhead.XXXXXX/`

Set `RAY_RECOVERY_OUTPUT_DIR` and `RAY_RECOVERY_TEMP_DIR` before invoking the shell
wrapper to override disk locations. Default temporary storage is `$HOME/raytmp`.

If repeated measurements are wanted, request the desired number of pairs:

```bash
bash gossip_benchmarks/run_fixed_r_overhead.sh --repeats 3
```

Order reverses on successive pairs; reports retain every observation and compute
means, sample standard deviations, and mean paired percentage changes. One
pair has null standard deviations. There are no confidence/significance claims
from a single pair. No unreported warmup runs are added.

To measure only selected workloads:

```bash
bash gossip_benchmarks/run_fixed_r_overhead.sh --case backpressure --case worker-scaling-actors
```

Original workload bodies and entry points are unchanged by this measurement
runner. The fault-coverage results remain in their separate coverage reports.
