# Selective replication feasibility

For the first measured **data-processing selective recovery** experiment, see
[data_processing/README.md](data_processing/README.md). It compares recomputation,
checkpoint-all, greedy selective checkpoints, and exhaustive subset selection
on three local pipelines, with real worker-process loss and three repetitions.
It uses a 28-minute controller budget and requires only Python's standard library.

The [comprehensive real-data benchmark](data_processing/COMPREHENSIVE.md) adds NYC
taxi analytics and 20 Newsgroups indexing, storage constraints, strong greedy and
Z3 policies, a 63-case predefined model grid, separately labeled gap discovery,
and five-repeat measured validation with resume support. Downloads and execution
share a 28-minute controller budget; no solver benefit is presumed.

For the separate experiment that selectively saves **training state** on
`t1 || (t2; t3)`, see [STATE_PROTECTION_EXPERIMENT.md](STATE_PROTECTION_EXPERIMENT.md).
It compares optimized uniform checkpoints, selective checkpoints, and
placement-first versus joint planning without duplicating task execution.
The newer [asymmetric workload experiment](ASYMMETRIC_STATE_EXPERIMENT.md)
tests whether protecting a shorter risky branch can avoid checkpoint delays
on a longer reliable branch. Code is prepared for the project owner to run;
do not execute experiments, benchmarks, tests, or automatic validation unless
the owner explicitly requests it.

The [literature benchmark](offline_study/LITERATURE_BENCHMARK.md) compares joint
placement/checkpoint selection with sequential/alternating controls and published
interval-policy adaptations, plus a separate Han checkpoint-DP panel. Its default
has 1,152 state-checkpoint cases and 48 analytical Han-DP cases, with a 29-minute
total process limit. It does not execute training or use cloud resources.

Open `gossip_benchmarks/Selective_Replication_Audit.ipynb` with your `ray-dev`
kernel (torch, numpy, scikit-learn, matplotlib). Run cells in order. It prints the
Python path and keeps settings and helper code visible. Results get a new folder;
old notebooks and saved measurements are preserved. Revised runs require a fresh
profile; don't compare version 1 and 2 timings as identical measurements.

First, constructed positive/negative controls check that a surviving duplicate
can beat checkpoint retry after a late loss, and that healthy duplication uses
more worker time. These deliberately favorable controls validate the mechanism,
not a practical benefit. A failed control means investigate before interpretation.

The workload is a small dependent local-SGD example: two identical MLPs train on
disjoint digits shards, then a task averages their weights, resets momentum, and
fine-tunes on the full training split. All policies use the same data and durable
checkpoints. Parent results are retained on shared storage. This is CPU process
loss on one host, not Ray/TorchFT, Fixed-R, SMT, or head-VM recovery.

| Policy | Placement and protection |
|---|---|
| `round_robin_retry` | Fixed round-robin primaries; checkpoint retry |
| `fixed_placement_replication` | Same primaries/order; optimize replica additions |
| `checkpoint_retry` | Exhaustively optimized placement/order; one copy |
| `uniform_replication` | Optimized placement/order; two copies per task |
| `placement_first` | Healthy-cost placement first, then optimize replicas |
| `greedy_replication` | Protect risk-duration-ranked tasks on that placement |
| `joint_enumeration` | Optimize placement/order and one/two copies together |

The 4,374-policy search uses fixed priorities, one task per worker, fixed checkpoint
intervals, and supplied failure scenarios. It does not optimize arbitrary start
times, checkpoint protocols, adaptive schedules, or pool size. Equal modeled costs
(to 12 decimal places) prefer fewer replicas. Model-infeasible baselines are still
replayed and labeled (using maximum modeled success, then cost, if infeasible). An SMT implementation would be a separate scalability and
modeling question, not automatically an improvement over this enumeration.

A controller thread injects faults independently of dispatch; actual signal times
and exit codes are recorded. Result checking and disk log writes happen after the
timed interval. Dependencies consume first-completed results under a crash-only
model; every committed model/optimizer/input history is subsequently checked at
absolute tolerance 1e-5. An invalid result invalidates the run. Cancellation must
be acknowledged before worker reuse; completion time and cancellation settlement
are separate, and billing continues through settlement. Worker-busy seconds,
restore/initialization times, and injection delays are reported. CPU sharing,
checkpoint I/O, and cancellation delays are not accurately predicted by the simple
simulator; measured replays expose these limitations.

Default accounting charges all available warm workers (including idle reserves)
and an assumed shared-storage service hourly rate. Checkpoint writes/restores are
executed and timed. Startup, provisioning, per-byte network charges, and physical
storage failure are outside this experiment. First-use billing is an explicit
alternative that excludes standby costs; report that assumption. No free spare or
storage claim should be inferred. Failure scenarios include both workers and
correlated loss at multiple times; holdout times are excluded from optimization.
Report active versus idle fault exposure and uninjected faults when a job ends
first. Weighted replay costs use all planning scenarios; observed deadline rates
are not statistical reliability guarantees.

Tests: `python -m pytest -q -o addopts='' gossip_benchmarks/selective_replication/test_study.py`.
Share `controls/controls.json`, `workflow/{profile,search,replay}.json`, and
`workflow/summary.md`. Negative findings apply only to this workload/model.

GCP interruption recovery: keep the same notebook kernel and rerun `run_suite`
with the identical job matrix. Completed remote observations are collected before
any reset; their unavailable notebook wall time is recorded as null. Interrupted
observations without a final status are archived after stopping workers and use
unique retry output directories. Explicit remote training failures still stop
with their traceback. Lock operations tolerate lost SSH acknowledgements and
recognize the same supervisor's token; another notebook's or an older unowned
lock is never silently removed. Persistent GCP outages can still stop the run;
no live GCP validation is implied by the local harness tests.

Offline workload discovery is available in [offline_study](offline_study/README.md).
From the repository root, run
`python gossip_benchmarks/selective_replication/offline_study/run_offline.py`
to reproduce the synthetic selective-protection candidate and its stronger
sequential baseline. Add `--full-sweep` for all 1,968 discovery configurations.
The runner uses no cloud resources and has a 28-minute total execution budget.

For measured two-trial training with local worker-process failures, see
[TRAINING_EXPERIMENT.md](TRAINING_EXPERIMENT.md). Run
`python gossip_benchmarks/selective_replication/training_experiment.py` from the
repository root. Worker pacing is explicit; checkpoint delay defaults to zero.
The runner saves completed observations and supports `--resume`.
