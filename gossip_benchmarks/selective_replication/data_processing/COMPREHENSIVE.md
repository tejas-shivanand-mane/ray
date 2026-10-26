# Real-data selective checkpointing with storage constraints

This benchmark asks two separate questions:

1. Can selectively saving intermediate outputs improve completion time compared
   with reconstructing lost data or checkpointing every output?
2. Does a constraint solver find better **feasible** checkpoint sets than strong
   greedy methods, and do those modeled improvements survive actual execution?

It executes application-pattern pipelines on **real public records**, rather
than generated sentences or imposed task durations. It is still a controlled
single-host benchmark, not a production deployment or a claim of novelty.
No result is presumed: a complete run can legitimately report no solver wins.

`test_budget_planner.py` supplies optional owner-run solver checks, including a
small constructed one-for-two exchange counterexample. It is a software unit
test, not a real-data workload or evidence counted in the benchmark. It is not
executed automatically by the benchmark.

## Run and share

From your repository root, in your existing Python environment:

```bash
git pull --ff-only origin main
python -m pip install -r gossip_benchmarks/selective_replication/data_processing/requirements-comprehensive.txt
python gossip_benchmarks/selective_replication/data_processing/comprehensive.py
```

The assistant prepares code only; the project owner executes it. No benchmark,
test, import check, compilation, or linter execution by the assistant is implied.
Python 3.10+, Linux/macOS, DuckDB, PyArrow, scikit-learn and Z3 are required.
Dependency versions are recorded. There is no cloud deployment or Ray build.

The benchmark has a **28-minute controller budget per invocation**, including
data downloads, profiling, planning, correctness checks, and measured replays.
The supervisor terminates the controller and workers on expiry and writes the
final small report. It stops earlier if all work completes. Package installation
is outside this timer. The total matrix is not guaranteed to finish on every
machine; an individual worker is limited to 180 seconds.

It prints a unique `ray_gcp_results/data-processing-comprehensive/...` directory.
Read `summary.md` and share **`analysis.zip`**. That archive includes all modeled
cases, the discovery scan, SMT encodings, profiles, raw replay events, selected
plans, correctness results, software versions, input fingerprints, and source
snapshots. Raw public datasets/checkpoint payloads are not bundled.

```bash
# Continue after timeout/interruption, retaining profiles, plans and finished runs:
python gossip_benchmarks/selective_replication/data_processing/comprehensive.py \
  --resume ray_gcp_results/data-processing-comprehensive/YOUR_RUN_DIRECTORY

# A smaller fresh experiment, if needed (do not mix its results with the default):
python gossip_benchmarks/selective_replication/data_processing/comprehensive.py \
  --taxi-rows 100000 --news-documents 750 --repeats 3
```

Resume uses the original settings; only `--max-minutes` changes its time budget.
Source or dependency changes require a fresh run. A lock prevents simultaneous
controllers. Normal exits, timeouts, and Ctrl-C remove it. After a hard machine
crash, verify the old controller has stopped before manually removing `run.lock`.
An interrupted final JSONL append is preserved separately and repaired on resume;
earlier corruption or failed output validation is not silently ignored.

## Data and real-world operator patterns

### Taxi dashboard

Source: January 2024 yellow taxi trip records from the NYC Taxi and Limousine
Commission. The default takes 200,000 valid records in a deterministic ordering.
It excludes invalid dates, nonpositive distances/fares, implausibly large
distances/fares, missing location/payment fields, and negative tips. It is a
bounded sample, **not a representative random sample of the entire month**.

The pipeline has ten optional checkpoint outputs and one final report:

- Read cleaned trip records; derive hour/day and trip duration.
- Compute hourly demand, route totals and payment totals in separate branches.
- Calculate rolling demand and per-origin route rankings.
- Identify high-fare outliers and summarize them by pickup zone.
- Compute daily totals and combine the branch reports.

These are dashboard ETL operations: filtering, projections, grouped aggregation,
window functions and sorting. SQL executes through DuckDB; intermediate tables
are Arrow tables. Monetary fields are integer cents and results are sorted for
reproducible output checks.

### Taxi zone enrichment and analytics

The same trip sample is joined twice with the official TLC taxi-zone lookup
table, for origin and destination. Nine optional checkpoints cover trip/zone
reads, the enriched table, borough and corridor totals, airport traffic, rankings,
rolling borough demand and payment summaries. A final report joins their results
logically. This tests reusable joins feeding several analytics branches, with
large detailed tables and small aggregates competing for storage.

### Document search index and duplicate candidates

Source: the real **20 Newsgroups corpus**, downloaded through scikit-learn's
dataset loader. Headers, footers and quoted replies are removed. The default
uses 1,500 nonempty documents from a seeded shuffle of the corpus; each document
is capped at its first 512 normalized tokens to bound computation.

Ten optional outputs cover document reads, normalized text, tokens, per-document
term frequencies, an inverted index, 32-component word-shingle MinHash features,
LSH-style duplicate-candidate buckets, corpus frequencies, a TF-IDF inverted
index and ranked results for four fixed text queries. A final report fingerprints
the outputs. This is indexing/search preprocessing, **not model training**.
Duplicate buckets are candidates, not verified near-duplicate decisions.

The dataset sources are real; the application DAGs are our explicit benchmark
implementations of these operator patterns. They are not copied production DAGs,
complete commercial search systems, standard TPC scores, or Spark/Ray benchmarks.

Downloads occur only when you run the command, use public HTTPS endpoints, and
are cached inside the output directory. No credentials are needed. For restricted
networks, `--taxi-file /path/to/yellow_tripdata_2024-01.parquet` and
`--zones-file /path/to/taxi_zone_lookup.csv` use local source files. Newsgroups
uses scikit-learn's cache under that run's inputs directory. Download failures
stop with a saved log; there is no silent synthetic-data substitution.

## Why a storage budget can expose a greedy limitation

The first experiment had only five optional outputs and no storage limit.
Here a plan must fit a **maximum total size of intermediate checkpoints**. Saving
one large intermediate can rule out saving several smaller intermediates; shared
ancestors and branch reuse also affect the value of combinations. A locally
attractive choice can therefore prevent a globally better collection of saves.

The budgets are experimental parameters, not a claim about a particular cloud
service's quota. Both constrained and unconstrained cases are retained. No task
duration, output size, or failure outcome is changed to manufacture a solver win.
No checkpoint eviction is modeled: selected outputs survive through job end.
The durable original input and mandatory final report are outside this budget.
The measured worker verifies actual committed intermediate bytes against its
budget; the profile uses the maximum serialized size across two runs.

## Five policies

| Policy | Decision rule |
|---|---|
| `recompute` | Save no intermediate results; reconstruct lost dependencies |
| `checkpoint_all` | Save every intermediate; explicitly infeasible when it exceeds the case budget |
| `greedy` | Try gain-first and gain-per-byte addition, no checkpoints, the best feasible singleton, and all checkpoints when feasible; improve each seed with one-bit additions/removals |
| `greedy_swap` | Same seeds, with additional one-for-one exchanges of selected and unselected checkpoints |
| `z3` | Solve the Boolean checkpoint-selection problem with dependency-aware recovery costs and the same byte budget |

These are transparent benchmark algorithms, not reproductions of uHEFT, Dyna,
SpotOn or Han's full workflow mapper. Greedy is deliberately stronger than a
single gain-per-byte pass. Z3's advantage must be assessed against `greedy_swap`,
not just the weakest baseline. A swap method can still miss a beneficial move
requiring one-for-two or two-for-three changes; the measurements determine whether
such interactions actually matter here.

## Planning and optimality

Two complete checkpoint-all runs measure each task's compute, serialization/fsync,
reload time and output size. Median times are quantized to integer microseconds.
All algorithms use these same coefficients. Worker startup/restart constants are
omitted from selection because every candidate uses the same number of processes
under each prescribed failure condition; actual replay elapsed times include them.

For a selected set, the model evaluates the healthy path and recovery after a
single interruption. Recovery loads a saved output when available, otherwise
recursively recomputes its missing dependencies. Shared ancestors are computed
once per surviving worker cache, not once for every dependent task. It accounts
for checkpoint writes on the original and recovery executions and for partial
computation lost in the interrupted task.

Z3 receives Boolean checkpoint choices, a sum-of-bytes constraint, and symbolic
dependency/recompute/restore expressions for each failure scenario. It minimizes
weighted expected completion time, then checkpoint bytes, then a deterministic
mask tie-break. **It does not receive a lookup table of the optimal answer.**
It exports an `.smt2` encoding per case. A result is called proven only if the
solver's lower and upper objective bounds coincide. Unknown/timeout results are
recorded, not relabeled as optimal or replaced with an oracle solution.

An independent imperative replay enumerates all 512 or 1,024 subsets for these
small graphs and checks the proven Z3 objective. A disagreement aborts the run.
This is a correctness audit, not a scalability result. Greedy's timed execution
uses lazy scoring of only its visited candidates, not a free precomputed oracle.
The oracle and discovery scan are experimental overhead, separate from the
reported per-plan selection time.

## Fixed cases, discovery cases, and actual measurements

The **predefined model grid has 63 cases**:

- 3 workloads;
- 7 budgets: 0%, 0.1%, 1%, 3%, 10%, 30%, and 100% of all optional output bytes;
- 3 assumed per-job failure probabilities: 1%, 10%, and 30%.

Zero-budget and unrestricted cases provide useful ties/controls. These are not
63 independent workloads or empirical repetitions.

A separate discovery scan considers budgets equal to each actual checkpoint's
size and each pair's combined size. It records **every scanned case**, including
ties. At each workload/probability it retains at most one greedy-gap candidate,
prioritizing a gap against the swap baseline. This adds at most nine modeled
cases, clearly labeled `discovered_gap`; it does not alter the predefined grid.
If no greedy gap exists, none is invented.

Measured evaluation is bounded to:

- Six predefined cases: all three workloads at 3% and 100% budgets, with 10%
  failure probability;
- Up to three additional cases: the strongest discovered gap per workload,
  frozen before replay. These are exploratory candidate validations and must not
  be used to estimate how commonly a solver wins.

Each uses healthy execution and three failure conditions, with **five repeated
executions by default**. Identical checkpoint sets across policies/cases share the
same replay observations; they are exact plan ties, not noisy separate rankings.
The command reports the actual number of unique replays after planning. Results
from shared observations must not be counted as independent samples.

Inputs are fixed across calibration and repetitions to preserve comparable byte
budgets. **Execution timings and failure locations are evaluated separately;
the input records are not held out.** Repetition/workload blocks and plan/scenario
orders are seeded and shuffled. DuckDB and common native thread pools use one
thread; plans are evaluated one at a time without mutual contention.

## Failure injection and correctness

All tasks run serially in one ephemeral worker, including independent DAG
branches. The volatile cache retains computed values; checkpoint files, source
inputs and controller survive. No whole-VM, disk, controller or correlated
multi-worker failure is claimed.

Planning failure points correspond to 15%, 35%, 55%, 75% and 95% of the profiled
useful computation. Evaluation uses 25%, 50% and 80%. Each position becomes a
task plus fractional computation within that task. This mapping is fixed across
policies, not recomputed from their checkpoint-inflated wall times.

An armed timer abruptly exits the original worker during the designated task.
If a task finishes before the timer can be scheduled, it exits immediately after
computation and **before saving that task**; the result explicitly records a
short-task fallback. It never pads tasks with sleep to make a failure happen.
Actual lost-compute duration and fallback counts are saved. Checkpoint-write
interruptions are not specifically targeted in this experiment.

The controller starts a fresh worker from the last acknowledged completed task.
Missing dependencies are recomputed; committed checkpoints are loaded. Final
results must match the independently executed profile references, which include
fingerprints of full terminal branch results, not just row counts. The two profile
runs and all checkpoint round trips must agree too. Any mismatch aborts.

The probability mixture assumes at most one failure, with equal conditional
weights for each modeled/evaluated position. A 10% failure probability for these
short local jobs is a sensitivity assumption, **not an observed real-world rate**.
This is not a constant-per-second hazard model, and longer plans do not get an
automatically higher chance of failing. Claims must be conditioned on these
assumptions and later checked under realistic job lengths and interruption traces.

## How to decide whether it helped

The report separates healthy completion, completion with failure, weighted
expected time, checkpoint bytes, and planning time. It also reports expected
time plus planning time and, in JSON, the one-off cost including profiling.
Profiling can be amortized over recurring jobs; it is not free for a one-off job.
An amortization count estimates how many repeated jobs would cover a solver's
additional planning cost if its measured expected-time saving persisted.

Only repetitions complete for all feasible policies in a case enter its paired
comparison. Partial results are preserved and labeled. Five repetitions give
exploratory paired-t 95% intervals, with no multiple-comparison correction;
they do not establish broad statistical superiority. Three and seven repetitions
are also supported. Timing uncertainty does not cover cost-model error, failure
probability uncertainty, or choice of dataset/sample.

Three different conclusions are possible:

- Selective checkpointing improves recovery/overhead, but greedy already finds
  the same choices: useful mechanism, no demonstrated solver benefit.
- Z3 proves a better modeled plan, but measured gains vanish or planning costs
  exceed them: an optimization gap, not yet a practical speedup.
- A solver plan improves measured expected time against all **feasible** controls
  and repays planning overhead: evidence for that workload/budget/risk setting,
  requiring distributed and literature comparisons before a novelty claim.

Checkpoint-all exceeding a storage budget is a feasibility result, not a timing
win. Unconstrained runs retain checkpoint-all as a direct performance comparator.
Do not call an adaptively selected gap a representative result, and do not omit
fixed-grid ties, losses, unresolved solver cases, or negative controls.

The storage and processor are local, OS caching is not disabled, and the graphs
are small. There is no data placement search, network contention, elastic worker
pool, monetary billing model, or runtime-native Ray/Spark recovery comparison.
SMT is used to check optimal checkpoint sets in this scope; using SMT alone is
not a research contribution.

## Sources

- NYC TLC data and field documentation: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Taxi source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
- Zone lookup: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- 20 Newsgroups loader: https://scikit-learn.org/stable/modules/generated/sklearn.datasets.fetch_20newsgroups.html
- DuckDB Python API: https://duckdb.org/docs/stable/clients/python/reference/
- Z3 optimization API: https://z3prover.github.io/api/html/classz3py_1_1_optimize.html
