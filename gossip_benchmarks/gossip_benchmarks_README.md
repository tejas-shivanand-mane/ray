# Recovery Succession Benchmarks — Experimental Section Only

This directory contains only the benchmark programs that correspond to the current paper's **Experimental Results** section. Each numbered Python file maps one-to-one to a paper subsection and contains its runner and plotting code (`run`, `plot`, or `run-and-plot`).

The benchmark suite compares three configurations where the comparison is meaningful:

- `Disabled`: recovery succession is off.
- `Succession-RN`: the proposed dynamic recovery-succession method with `N` non-owner recovery holders.
- `WitnessBaseline-RN`: the witness-as-holder baseline with `N` full-lineage witness holders.

The proposed method and baseline use the **same redundancy knob**:

```text
recovery_succession_target_holder_count = R
```

The baseline is selected only by:

```text
enable_recovery_succession = true
enable_recovery_witness_holder_baseline = true
```

The proposed method uses:

```text
enable_recovery_succession = true
enable_recovery_witness_holder_baseline = false
```

The disabled configuration uses:

```text
enable_recovery_succession = false
```

> The baseline runner requires a Ray build containing the current `enable_recovery_witness_holder_baseline` implementation. These files were syntax-checked here, but the modified Ray runtime itself was not compiled or executed in this environment.

---

## Paper subsection -> benchmark file

| # | Paper subsection | Benchmark file | Compared methods | Main figure output |
|---|---|---|---|---|
| 1 | Simple Recovery | `01_simple_recovery.py` | Disabled, Succession-R1, WitnessBaseline-R1 | `avail_thput.png`, `avail_lat.png` |
| 2 | No-failure steady-state recovery overhead | `02_no_failure_performance.py` | Disabled, Succession-R1..R4, WitnessBaseline-R1..R4 | `throughput_all_payloads.png`, `p95_latency_all_payloads.png` |
| 3 | Recovery latency | `03_recovery_latency.py` | Succession-R1..R4, WitnessBaseline-R1..R4 | `recovery_total_time.png`, `recovery_detection_time.png` |
| 4 | Succession fallback under holder failures | `04_holder_witness_fallback.py` | Succession-R4 only | `accepted_rank_vs_predead_holders.png` |
| 5 | Recovery across failure types | `05_failure_type_recovery.py` | Succession-R2, WitnessBaseline-R2 by default | `failure_type_recovery_latency.png` |
| 6 | Recovery under correlated recovery storms | `06_recovery_storm.py` | Disabled, Succession-R2, WitnessBaseline-R2 by default | `recovery_storm_success_rate.png`, `recovery_storm_latency_scaling.png` |
| 7 | Recovery-succession formation scaling | `07_formation_scaling.py` | Succession-R1..R4, WitnessBaseline-R1..R4 | `recovery_formation_scaling.png` |
| 8 | Recursive dependency-chain recovery | `08_recursive_dependency_chain.py` | Disabled, Succession-R2, WitnessBaseline-R2 by default | `recovery_chain_dag_latency.png` |

`_benchmark_common.py` is only shared support code; it is not a ninth benchmark.

### Why benchmark 4 does not include the baseline

The holder-fallback subsection validates the **ordered dynamically admitted succession list** `H1 -> H2 -> H3 -> H4`. The witness-as-holder baseline does not construct that same dynamic holder list, so inserting it into this benchmark would change the research question. Baseline failure behavior is instead compared in the general recovery, failure-type, storm, latency, formation, and recursive-chain experiments.

---

## Recommended directory layout

```text
gossip_benchmarks/
├── _benchmark_common.py
├── 01_simple_recovery.py
├── 02_no_failure_performance.py
├── 03_recovery_latency.py
├── 04_holder_witness_fallback.py
├── 05_failure_type_recovery.py
├── 06_recovery_storm.py
├── 07_formation_scaling.py
├── 08_recursive_dependency_chain.py
└── gossip_benchmarks_README.md
```

No legacy benchmark is required for the current paper experimental section.

In particular, the following old files should **not** remain part of the active paper benchmark suite after you have verified the replacements:

```text
steady_state_holder_benchmark.py
plot_holder_benchmark.py
plot_recovery_benchmark.py
recovery_availability_benchmark.py
plot_recovery_availability.py
recovery_steady_state_benchmark.py
plot_recovery_steady_state_combined.py
local_recovery_time_benchmark_fixed_v3.py
plot_local_recovery_time_fixed_v3.py
recovery_holder_failure_benchmark.py
plot_recovery_holder_failure.py
recovery_failure_type_benchmark.py
recovery_failure_type_benchmark_diagnostic.py
plot_recovery_failure_type.py
recovery_storm_benchmark.py
plot_recovery_storm.py
recovery_formation_scaling_benchmark.py
plot_recovery_formation_scaling.py
recovery_chain_dag_benchmark.py
plot_recovery_chain_dag.py
```

Do not delete old files until the corresponding numbered replacement has been run successfully on the modified Ray build and its output has been checked.

`phase_benchmarks/` is unrelated to this cleanup and should remain untouched.

---

# Running the benchmarks

Every benchmark accepts the same first positional command:

```bash
python gossip_benchmarks/NN_name.py run
python gossip_benchmarks/NN_name.py plot
python gossip_benchmarks/NN_name.py run-and-plot
```

`run-and-plot` is the default if the command is omitted.

## 1. Simple Recovery

```bash
python gossip_benchmarks/01_simple_recovery.py run-and-plot \
  --trials 3 \
  --duration-seconds 45 \
  --failure-at-seconds 15 \
  --bucket-seconds 1
```

Default methods:

```text
Disabled
Succession-R1
WitnessBaseline-R1
```

The same persistent borrower holds the ObjectRef before failure and performs reads both before and after failure.

---

## 2. No-failure steady-state recovery overhead

Short/debug run:

```bash
python gossip_benchmarks/02_no_failure_performance.py run-and-plot \
  --warmup-seconds 5 \
  --duration-seconds 30 \
  --bucket-seconds 5 \
  --inflight 64 \
  --repetitions 3 \
  --payloads 1KiB:1024 64KiB:65536 256KiB:262144 2MiB:2097152
```

Stronger paper run:

```bash
python gossip_benchmarks/02_no_failure_performance.py run-and-plot \
  --warmup-seconds 15 \
  --duration-seconds 60 \
  --bucket-seconds 5 \
  --inflight 64 \
  --repetitions 7 \
  --payloads 1KiB:1024 64KiB:65536 256KiB:262144 2MiB:2097152
```

All nine configurations execute the identical six-logical-node pipeline:

```text
head/driver -> producer -> consumer1 -> consumer2 -> consumer3 -> consumer4
```

The producer executor is not counted as a recovery holder. The four consumer nodes therefore provide four independent dynamic-holder candidates for `R=4`.

Outputs:

```text
benchmark_runs.csv
benchmark_timeseries.csv
benchmark_summary.csv
plots/throughput_all_payloads.png
plots/p95_latency_all_payloads.png
```

---

## 3. Recovery latency

```bash
python gossip_benchmarks/03_recovery_latency.py run-and-plot \
  --trials 3 \
  --task-durations 5 10 20 30 \
  --payload-bytes 2097152 \
  --fixed-order
```

For each task duration the runner compares:

```text
Succession-R1 .. Succession-R4
WitnessBaseline-R1 .. WitnessBaseline-R4
```

Metrics:

- `failure_to_replay_start_s`: failure injection -> first replay execution start.
- `failure_to_result_s`: failure injection -> recovered value becomes available.
- `post_failure_start_count`: basic duplicate-replay diagnostic.

---

## 4. Succession fallback under holder failures

```bash
python gossip_benchmarks/04_holder_witness_fallback.py run-and-plot \
  --trials 3 \
  --task-duration-seconds 20 \
  --payload-bytes 2097152
```

The benchmark forms `R=4`, then pre-fails the first `K` holders for:

```text
K = 0, 1, 2, 3, 4
```

Expected successful recovery rank:

```text
K=0 -> H1
K=1 -> H2
K=2 -> H3
K=3 -> H4
K=4 -> no surviving holder; recovery should fail
```

---

## 5. Recovery across failure types

```bash
python gossip_benchmarks/05_failure_type_recovery.py run-and-plot \
  --trials 3 \
  --holders 2 \
  --task-duration-seconds 20 \
  --payload-bytes 2097152
```

Failure modes:

```text
owner_worker
owner_node
owner_and_executor_node
```

For each mode, the runner compares the proposed succession method against the witness-as-holder baseline at the same `R`.

---

## 6. Recovery under correlated recovery storms

```bash
python gossip_benchmarks/06_recovery_storm.py run-and-plot \
  --trials 3 \
  --storm-sizes 1 4 8 16 32 \
  --holders 2 \
  --task-duration-seconds 20 \
  --payload-bytes 2097152
```

Methods:

```text
Disabled
Succession-R2
WitnessBaseline-R2
```

The runner now waits for the cumulative expected number of protection confirmations before injecting failure, so an earlier task's commit log cannot incorrectly satisfy a later task's protection check.

---

## 7. Formation scaling

```bash
python gossip_benchmarks/07_formation_scaling.py run-and-plot \
  --trials 3 \
  --protected-outputs 1 4 8 16 32 64 \
  --payload-bytes 1024
```

Two timing fields are deliberately recorded because the methods establish protection at different points:

- `native_formation_time_s`
  - proposed succession: holder-admission start -> all requested succession ranks committed;
  - witness baseline: submission start -> all full TaskSpecs installed on witness-holders.
- `protection_ready_time_s`
  - both methods: task-submission start -> requested protection ready.

The generated comparison plot uses **`protection_ready_time_s`**, because that is the method-neutral metric.

If this baseline comparison is included in the paper, update the formation subsection to describe the plotted metric as **submission-to-protection-ready latency**, rather than only "beginning of holder admission."

---

## 8. Recursive dependency-chain recovery

```bash
python gossip_benchmarks/08_recursive_dependency_chain.py run-and-plot \
  --trials 3 \
  --chain-lengths 2 4 8 16 \
  --holders 2 \
  --stage-duration-seconds 2 \
  --payload-bytes 1048576
```

Methods:

```text
Disabled
Succession-R2
WitnessBaseline-R2
```

The workload is a **serial dependency chain**, not an arbitrary DAG. Every stage output is protected before it is used to submit the next stage. The runner also requires cumulative per-stage protection confirmation so the owner node cannot be failed while later stages are still unprotected.

The plot filename remains `recovery_chain_dag_latency.png` only to avoid breaking the current LaTeX figure path. The experiment itself should continue to be described as a recursive dependency chain.

---

# Baseline interpretation

The witness-as-holder baseline stores a full `TaskSpec` on `R` witness nodes and disables dynamic holder admission. Therefore:

- it is expected to have a different steady-state cost profile;
- its formation occurs on the task-submission path;
- it should be compared at the same `R` as the proposed method;
- it should not be described as simply another `Enabled-R` point belonging to the proposed algorithm.

Use distinct labels in the paper figures, for example:

```text
Disabled
Succession R=1
Succession R=2
...
Witness baseline R=1
Witness baseline R=2
...
```

---

# Before collecting final paper numbers

1. Rebuild the modified Ray branch containing both the proposed method and the baseline flag.
2. Run correctness/failure tests first, especially concurrent recovery claim behavior and the case where the acting recovery owner dies during recovery.
3. Run one short/debug trial of each numbered benchmark.
4. Inspect CSV rows for `success`, replay counts, and expected holder ranks before trusting plots.
5. Run the stronger repeated configurations for paper figures.
6. Only after parity is confirmed, archive/remove the corresponding legacy benchmark and plotting files.

The benchmark scripts themselves do not modify the Ray source tree or `phase_benchmarks/`.
