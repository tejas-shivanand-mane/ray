# Recovery benchmark suite

Seven public commands replace the old numbered/phase experiments. Run from the
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
| `07_borrower_count_performance.py` | Disabled vs Fixed-R K=32 and Succession K=32 across application borrower counts, including B=1 < R/W |

`_support/` contains shared workload/plot code and isolated correctness fixtures;
these are implementation modules, not additional experiments to choose from.
The old standalone experiments, patch utility scripts, phase directory, and
tracked historical result snapshots have been removed. Git history retains them.
New results are ignored by Git; this change does not run a cleanup of your local
untracked results.

## Editing plots without rerunning benchmarks

Start with **`_support/plot_settings.py`**. It contains four clearly named
sections: `FRONTIER` (01), `OBJECT_SIZES` (02), `OWNER_FAILURE` (04), and
`BORROWER_COUNTS` (07).
Each has figure size, title/footer, output filename/formats/DPI, layout spacing,
and a settings dictionary for each panel. The `axis()` function at the top
lists all available axis settings and their shared defaults.

| Change | Setting |
| --- | --- |
| Axis labels and panel title | `xlabel`, `ylabel`, `title` |
| Tick positions and displayed text | `xticks` / `xticklabels`, `yticks` / `yticklabels` |
| Tick rotation | `xrotation`, `yrotation` |
| Axis limits | `xlim`, `ylim`, e.g. `(0, 3000)` |
| Linear/log scales | `xscale`, `yscale`; use `xscale_kwargs` for the log base |
| Font sizes | `label_fontsize`, `title_fontsize`, `tick_fontsize` |
| Legend position, columns, font, frame | `legend={"loc": "upper right", "ncol": 1, "fontsize": 9, "frameon": False}` |
| Hide legend or grid | `legend=None` or `grid=None` |
| Colors, names, markers, line styles | The corresponding `*_SERIES` dictionary |
| Error-bar cap size and line width | `ERROR_BARS`; individual series can override these |
| Figure size, resolution, file type | `figsize` (inches), `dpi`, `formats` |
| Figure spacing | `tight_layout`, e.g. `{"rect": (0, 0.12, 1, 0.94), "w_pad": 3}` |
| Global font family and other Matplotlib defaults | `RC_PARAMS` |

Set ticks/labels/limits to `None` to use the automatic or data-derived defaults.
An explicit tick list replaces the default positions; labels must have the same
length as the positions. Set ticks to `[]` to hide them. Explicit limits apply
after ticks, so tick changes cannot silently widen a requested axis range.
Changing ticks or limits changes only the display, not which runs are analyzed.

For example, add these arguments to **both** `axis(...)` calls in
`OBJECT_SIZES` to customize the x axis:

```python
xlabel="Object size",
xticks=[1024, 16384, 262144, 1048576],
xticklabels=["1 KiB", "16 KiB", "256 KiB", "1 MiB"],
xrotation=0,
label_fontsize=12,
tick_fontsize=11,
```

Keep the existing `xscale="log", xscale_kwargs={"base": 2}` if you want
spacing proportional to log object size. For linear spacing, set
`xscale="linear", xscale_kwargs={}` (the log-only `base` argument must be removed).
Tick labels alone do not change the scale.

Tick coordinates differ between plots:

| Panel | Coordinates to put in `xticks` |
| --- | --- |
| 01, both panels | Positions `0, 1, 2, 3, 4, 5` for the usual K values `1, 2, 4, 8, 16, 32` |
| 02, both panels | Object sizes in **bytes**, e.g. `1024`, `1048576` |
| 04, throughput | Seconds relative to owner kill, e.g. `[-5, 0, 5, 10, 20]` |
| 04, recovery bars | `0, 1, 2` for disabled, Fixed-R, Succession |
| 07, both panels | Actual borrower counts, e.g. `1, 2, 4, 8, 16` |

For example, in 01 use `xticks=[0, 2, 5]` and
`xticklabels=["1", "4", "32"]` to show only those three tick labels.
All six measured K values still contribute plotted points.

For larger layout changes, each benchmark has a short drawing module:

| File under `_support/` | Responsibility |
| --- | --- |
| `plot_frontier.py` | 01: K comparison layout |
| `plot_object_sizes.py` | 02: object-size layout |
| `plot_owner_failure.py` | 04: failure timeline and recovery bars |
| `plot_borrowers.py` | 07: borrower-count throughput and overhead |
| `plots.py` | Shared validation, axis formatting, error bars, and saving |

Benchmark 03 currently produces profiling JSON/CSV/logs and native stack data;
it has no Matplotlib figures to customize. Correctness benchmarks 05–06 have
no plot settings.

After editing settings, regenerate figures from saved results:

```bash
python gossip_benchmarks/01_frontier_performance.py plot \
    --output-dir gossip_benchmarks/results/59_recovery_frontier_fixed_vs_succession_performance

python gossip_benchmarks/02_object_size_performance.py plot \
    --output-dir gossip_benchmarks/results/object_sizes_upto_1mib

python gossip_benchmarks/04_owner_failure_throughput.py plot \
    --output-dir PATH_TO_SAVED_OWNER_FAILURE_RUN
```

These commands do not execute benchmark cases. The existing figures are replaced;
choose a different `filename` in the settings to keep multiple visual versions.
01 filenames support `{padding}`; titles/footers in 01 support
`{payload_label}`, `{padding_label}`, and `{repetitions}`; 02 supports
`{padding_label}` and `{repetitions}`. 07 supports `{payload_label}`,
`{padding_label}`, and `{repetitions}`. Set a title/footer to `""` to hide it.
If you change the displayed statistics or remove CIs in a layout module, update
the annotation accordingly.

02's `--exclude-object-sizes` remains available and does not alter saved raw,
configuration, or summary/paired CSV files. 04's existing replot command also
regenerates derived bucket/summary CSVs; its trial JSON stays intact.
`--bucket-seconds` changes 04's aggregation, not just its tick spacing; use the
same bucket width when making appearance-only comparisons.

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

To omit 4 MiB from the saved plots without rerunning any cases:

```bash
python gossip_benchmarks/02_object_size_performance.py plot \
    --exclude-object-sizes 4MiB
```

This regenerates the PNG/PDF and prints the comparison for the remaining sizes.
Raw results, configuration, and summary/paired CSVs retain every measured size.
Use `plot` without exclusions to restore the full-size plots.

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

## 7. Borrower-count effect, including fewer borrowers than R/W

Compare **disabled**, **Fixed-R K=32**, and **Succession K=32** with a configurable
number B of application borrowers per producer object. Default counts are
**1, 2, 4, 8, 16**; R=2 and W=2 remain fixed, including at B=1.

```bash
python gossip_benchmarks/07_borrower_count_performance.py \
    --borrower-counts 1 2 4 8 16 \
    --repetitions 3 --warmup-seconds 5 --duration-seconds 30
```

Default: **45 fresh-cluster cases**, profiling OFF, object payload 1 KiB, separate
TaskSpec padding 1 KiB, burst size 32, and 128 in-flight producer pipelines.
All three variants use the same application workload at a given B.
Three repetitions balance each variant across the three execution positions
within each borrower count. Use six repetitions for two full cycles; this
does not guarantee narrow confidence intervals.

**What B=1 means:** it is one real downstream application borrower, not two
borrowers disguised as one, and neither R nor W is lowered. The executor is not
automatically admitted as a Succession holder: the current implementation admits
downstream borrower candidates. With fewer than R borrowers, Succession can
remain below its target R while Fixed-R retains its witness-holder replication
policy. This measures the application cost of each policy under low fan-out;
it must not be presented as a comparison at equal achieved durability.
No extra holder admissions or dummy application consumers are forced.

The owner exports every object directly to each borrower. A pipeline counts as
complete only after **all B borrowers return the correct object value**. The
denominator is producer pipelines, not individual borrower reads: one pipeline
at B=16 includes sixteen consumes. In-flight producer count stays fixed, so
outstanding borrower calls grow with B. Completion can overlap holder admission;
this benchmark does not measure durable coverage or recovery.

Topology is one head/owner node, one producer/executor node, B distinct borrower
nodes, and two additional witness nodes (**B+4 logical Ray nodes**, for every
variant). All are created locally by Ray's Cluster helper. R/W do not grow with B,
but local process count, configured CPU resources, and memory footprint do.
Node-distinct placement is not a claim of distinct physical machines. Higher B
therefore measures fan-out under this topology, not pure borrower metadata cost
on a fixed-size cluster. Existing 01/02 defaults remain two borrowers.

Output directory: `results/borrower_counts/` under `gossip_benchmarks/`:

- `borrower_count_runs.csv`: every completed case, completion/latency counts,
  actual borrower count, target holders/witnesses, and variant execution position.
- `borrower_count_summary.csv`: throughput and latency statistics by B/variant.
- `borrower_count_paired.csv`: overhead relative to disabled at the **same B and
  repetition**, plus Succession's percentage speedup over Fixed-R K=32.
- `borrower_count_comparison.png/.pdf`: throughput and paired overhead with
  pointwise 95% Student-t intervals.
- `run_config.json` and per-case logs: settings, source/native provenance.
  The native extension SHA-256 identifies the binary even when Ray's build
  commit is a placeholder; it does not recover the missing build commit.

Completed cases are journaled after each success. Repeat the identical run
command to resume. Settings, source commit/content, and native binary must match.
Use a fresh `--output-dir` to retain another run; `--overwrite` starts again
by removing only this benchmark's named outputs in the selected directory.

To run only the below-R case:

```bash
python gossip_benchmarks/07_borrower_count_performance.py \
    --borrower-counts 1 \
    --output-dir gossip_benchmarks/results/borrowers_one
```

For plot edits, change `BORROWER_COUNTS` / `BORROWER_COUNT_SERIES` in
`_support/plot_settings.py`, then:

```bash
python gossip_benchmarks/07_borrower_count_performance.py plot \
    --output-dir gossip_benchmarks/results/borrower_counts
```

Replotting reads the saved journal/configuration, requires all configured cases
to be complete, and replaces only the figures. It does not run Ray cases or
rewrite raw/summary/paired CSVs. Tick positions are actual B values; default
x spacing is log base 2. Larger layout changes belong in
`_support/plot_borrowers.py`.

Implementation was manually source/diff-reviewed; no build, test, benchmark,
lint, or plot rendering was run while preparing this benchmark.
