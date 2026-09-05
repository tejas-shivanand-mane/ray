# Recovery benchmark suite

Six public commands replace the old numbered/phase experiments. Run from the
repository root using your rebuilt Ray Python environment. Plotting requires
`matplotlib`. No command builds Ray, changes kernel settings, or invokes sudo.

| Entry point | Purpose |
| --- | --- |
| `01_frontier_performance.py` | Disabled vs Fixed-R and Succession at K=1,2,4,8,16,32; throughput and overhead plots |
| `02_object_size_performance.py` | Returned object-size sweep: disabled plus both methods at K=1 and K=32; throughput and overhead plots |
| `03_profile.py` | Both methods: native service/control counters, process/thread CPU, optional perf stacks |
| `04_owner_failure_throughput.py` | Owner-worker failure: time-binned successful reads and fraction of unfinished objects recovered, against disabled |
| `05_succession_correctness.py` | Succession admission, owner failure, commit gap, provisional confirmation, appends, failover, concurrency, retries, late borrowers |
| `06_fixed_r_correctness.py` | Fixed-R owner/node failure, grouping/rollover, lifecycle/cleanup, concurrent claims, witness failure/stall, acting-owner handoff |

`_support/` contains shared workload/plot code and isolated correctness fixtures;
these are implementation modules, not additional experiments to choose from.
The old standalone experiments, patch utility scripts, phase directory, and
tracked historical result snapshots have been removed. Git history retains them.
New results are ignored by Git; this change does not run a cleanup of your local
untracked results.

## 1. Full K comparison

```bash
python gossip_benchmarks/01_frontier_performance.py \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 20 --overwrite
```

Default: 39 fresh-cluster cases. Output:
`gossip_benchmarks/results/frontier_performance/`.

The workload and raw CSV columns are retained from Benchmark 59. Both methods
use R=2/W=2, two independent borrowers, burst size 32, and 128 in-flight tasks.
K=1 disables Frontier; K>1 uses the same K for both methods. Native profiling is
OFF. Producer objects are 1 KiB by default; TaskSpec padding is a separate 1 KiB.

PNG and PDF plots show:
- application throughput, including the shared disabled reference;
- overhead vs the paired disabled run at each K;
- pointwise 95% Student-t intervals across repetitions.

These measure application completion, not completion of every holder admission.
Admission may overlap application work. The paired CSV also reports Succession
vs Fixed-R at equal K. A confidence interval crossing zero does not establish
which method is faster. Three repetitions are a screen, not a guarantee of
precision; 13 gives complete cyclic position balance if a later study needs it.

Replot your existing B59 results without another Ray run:

```bash
python gossip_benchmarks/01_frontier_performance.py plot \
    --output-dir gossip_benchmarks/results/59_recovery_frontier_fixed_vs_succession_performance
```

For new results, the same `plot` command without `--output-dir` uses the new default.

## 2. Object-size effect

```bash
python gossip_benchmarks/02_object_size_performance.py \
    --object-sizes 1KiB 16KiB 256KiB 1MiB 4MiB \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 20 --overwrite
```

Default: 75 fresh-cluster cases. Output:
`gossip_benchmarks/results/object_sizes/`, including
`object_size_comparison.png` and `.pdf`, raw/summary/paired CSVs, settings,
source/build provenance, and case logs.

Five curves: disabled, Fixed-R K=1, Fixed-R K=32, Succession K=1, Succession K=32.
Only returned producer-object size changes; recipe padding and other workload
parameters stay fixed. Both borrowers read each object. Overhead is relative
to disabled at the **same size and repetition**. Resume requires identical
settings and provenance.

```bash
python gossip_benchmarks/02_object_size_performance.py plot
```

## 3. Profiling

```bash
python gossip_benchmarks/03_profile.py
```

Default: service snapshots and system CPU at all six K values, both methods.
System profiling uses three repetitions per method/K. Use
`--ks 1 32` or `--modes service` to narrow an investigation.

Service output includes every exported native counter per owner/borrower in
JSON and `all_counters.csv`, plus interpreted stage timings and count/coverage
checks: registration/export, recipe encoding/materialization, copies/bytes,
candidate/admission work, RPC counts, witness queue/CQ/main-loop timing, H2
readiness, and available lifecycle/current/peak counters. Unexercised paths
remain zero; this does not claim to exercise every recovery path.

System output includes process-class and thread-group CPU, gRPC/CQ,
CoreWorker I/O, context switches and endpoint churn. Ended processes/threads
may be missed, so affected estimates are explicitly reported as lower bounds.
Service elapsed time includes preemption/locks; it is not process CPU.
Nested timings and inclusive categories overlap and must not be summed.

Optional native stacks, through the same entry point:

```bash
python gossip_benchmarks/03_profile.py --modes native --ks 1 32
```

Native mode requires Linux perf and user-space perf permission. Symbol coverage
and exited-thread samples are retained. Profiling throughput is diagnostic;
use experiment 01 or 02 for performance conclusions. Logs and raw files use
a fresh timestamped directory under `results/profile/`.

## 4. Owner failure and resumption

```bash
python gossip_benchmarks/04_owner_failure_throughput.py
```

Default: one fresh-cluster case for disabled and each method, K=1, R=2/W=2.
Use `--k 32` for grouped recovery and `--trials 3` for repetitions.

The actual owner creates 32 distinct objects, retains their references and
exports them directly to two independent borrowers. Original executions are
gated. Eight objects are released/read before the owner worker is killed;
the remaining original gates stay closed. Successful post-failure reads must
retain the ObjectID and have an observed recovery replay. No replacement owner
submits a new workload. The executor node survives, isolating owner failure.

This is **paced read throughput over a finite backlog**, not steady-state task
production throughput. Both borrowers must succeed for an object to count.
Plots end at observation/backlog completion; failures and unattempted objects
are reported separately. Disabled is expected to lose unfinished objects,
but the benchmark records observed behavior rather than drawing a forced zero.
Protection counters are enabled for setup evidence, so this is diagnostic.

Output: timestamped `results/owner_failure/`, raw events/protection/replay
evidence in JSON, bucket/summary CSVs, `owner_failure.png` and `.pdf`.

```bash
python gossip_benchmarks/04_owner_failure_throughput.py plot --output-dir PATH_TO_SAVED_RUN
```

## 5–6. Correctness

```bash
python gossip_benchmarks/05_succession_correctness.py
python gossip_benchmarks/06_fixed_r_correctness.py
```

Each scenario has its own subprocess, cluster, timeout, log and assertions.
Suites stop on the first failure. Do not use Python `-O`.
Use `--list` to inspect coverage and `--cases NAME ...` for selected cases.

Succession has 23 scenarios by default: owner-node loss, witness-ACK/local-commit
gap, and blocked provisional confirmation at each K=1,2,4,8,16,32; partial
groups; dynamic append; failed-append atomicity; and a three-case ordinary K=1
failover/concurrent-recovery/retry-budget fixture with late owner exports.
That last fixture reports each constituent result and exits nonzero on failure.
The commit-gap fixture deliberately stops after the first provisional holder's
witness publication: configured R stays 2, but it does not claim two completed
admissions in that fault window.

Fixed-R has 10 scenarios: K=1 regression, nonleader owner-node failure,
K=4 full group plus rollover, lifecycle, terminal cleanup, concurrent claims,
authoritative witness loss, live-witness stall without unsafe promotion,
acting borrower death, and an in-flight acting-owner handoff. Adversarial
witness cases now use R=2/W=2; they are not an R=3 performance baseline.

These are broad regression suites, not a proof or an exhaustive fault-state
enumeration. The new arrangement and fixture adaptations still need execution
on your system. No local build, test, benchmark or plot execution was performed
when this consolidation was prepared.
