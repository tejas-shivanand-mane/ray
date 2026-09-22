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
Plotting uses Matplotlib. If it is absent, the script stops before creating any
cluster and gives the one-time `python -m pip install matplotlib` command.

## What runs

| Local workload | Size | Default observations |
| --- | --- | --- |
| Fast producer / slow consumer | 16 inputs, 8 batches/input, 16 rows/batch, 1 MiB/row, 0.1 s consumer sleep | OFF + ON |
| Actor worker-scaling | 8 actors, 2 map stages, 32 blocks, 200 scalar + 400 array columns; 10,304 rows | ON + OFF |
| XGBoost single worker | 32,768 rows, 16 features, 10 rounds, 1 training worker with 1 CPU | OFF + ON |
| XGBoost multiple workers | Same input/parameters, 2 training workers with 1 CPU each | ON + OFF |

The default runs eight observations total: one pair per case. Treat this as a
preliminary estimate; one sample per mode cannot establish variance/significance.
Each process defaults to a 120-second limit, with cluster startup/cleanup extra. It stops
on the first error/timeout and saves partial results; no automatic retry loop.
The worst-case processing budget is 16 minutes for the default, not a prediction
of runtime. Prior failure-run times are not valid no-failure estimates.

Each observation starts a fresh cluster and fresh application process. Both
modes have the same zero-CPU head and driver node, four two-CPU executor nodes,
512 MiB object stores per node and surviving RocksDB storage. The dashboard is
disabled in both modes; no frontend build is required. The actor benchmark uses
the ordinary `--skip-state-api-stats` option in BOTH modes, retaining local
Dataset execution statistics while omitting dashboard scheduling and runtime-env
queries. Its UDF, actor pool, pipeline and normal entry point are unchanged.
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
inside the original benchmark function, including local actor-benchmark
statistics collection. Optional State API queries are excluded symmetrically.
XGBoost reports training and prediction separately, plus their sum.
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
- Latest plots: `$HOME/ray-coverage/overhead.png` and `overhead.pdf`
- Archived report, raw metrics, settings and application logs:
  `$HOME/ray-coverage/overhead.XXXXXX/`

Plots show runtime overhead for Data totals and XGBoost training, prediction and
total. Positive bars indicate a slowdown and negative bars a measured speedup.
With repetitions, whiskers show ±1 sample standard deviation of the paired
percentage changes (not confidence intervals). Single pairs are explicitly
marked preliminary. Incomplete pairs are never plotted as zero overhead;
partial/failed runs are labeled, with unmeasured cases listed. Plot generation
runs outside all workload timing intervals and plots are archived with their
source JSON. A startup failure has no measured bars.

Regenerate plots from an existing report without rerunning benchmarks:

```bash
python gossip_benchmarks/plot_fixed_r_overhead.py "$HOME/ray-coverage/overhead.json"
```

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

## Longer training on the same data

XGBoost uses boosting rounds rather than epochs. The benchmark's ordinary
`--num-boost-round` option defaults to 10; the overhead runner forwards the same
value to both modes, records it in JSON and the plot, and checks the saved model's
round count. Existing recovery coverage still uses 10 rounds by default.

Start with one OFF/ON pair using two training workers and 1,000 rounds:

```bash
bash gossip_benchmarks/run_fixed_r_overhead.sh \
  --case xgboost-multi --num-boost-round 1000 \
  --output "$HOME/ray-coverage/overhead-1000-rounds.json"
```

This retains the 120-second per-process deadline and saves JSON, CSV, PNG and PDF
under the separate `overhead-1000-rounds` name. There are only two observations;
the other workloads are not rerun. Use `--case xgboost-single` for one worker.
No rebuild is required. The assistant has not executed this measurement.

Round count does not guarantee a particular runtime. For an intentionally longer
experiment, explicitly set `--timeout-s`, allowing for training plus prediction
and application startup in each mode. The default remains 120 seconds. A
900-second limit permits up to 30 minutes across two application processes,
plus cluster startup/cleanup and artifact validation, and still stops on the
first failure. No failures are injected.

Compare the **training** timings and both the absolute ON-minus-OFF difference
and percentage overhead against the existing 10-round report. The data is
materialized once before boosting, so more rounds increase computation without
adding ingestion passes. A nearly constant time difference with a falling
percentage would support the fixed-ingestion-cost explanation. More rounds also
increase model/checkpoint size and can affect prediction time; the total timer
therefore does not isolate training. This is not an experiment with neural-network
epochs that reread the dataset each epoch.
