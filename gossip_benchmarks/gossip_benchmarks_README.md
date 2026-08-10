# Ray Recovery-Succession Prototype Benchmarks

This directory contains local prototype benchmarks for the experimental Ray
recovery-succession implementation.

The benchmarks are intended for development, debugging, and early evaluation.
They are **not yet final paper methodology**. Most experiments use
`ray.cluster_utils.Cluster` to create multiple logical Ray nodes on one physical
machine.

This README documents the top-level benchmark and plotting scripts in
`gossip_benchmarks/`. It intentionally does **not** document the older
`phase_benchmarks/` validation tests.

## General notes

Run commands from the Ray repository root unless noted otherwise.

The benchmarks assume that the Python environment is using the custom Ray build
that contains the recovery-succession implementation.

Useful recovery configuration parameters include:

- `enable_recovery_succession`
- `recovery_succession_target_holder_count`
- `recovery_succession_witness_count`
- `object_timeout_milliseconds`

The local-cluster experiments are useful for prototype comparison and debugging,
but final paper measurements should eventually be repeated on physically
separate machines.

---

# Current prototype benchmark suite

## 1. `recovery_availability_benchmark.py`

### Question

Does recovery succession restore application availability after the owner /
producer node fails?

### Experiment

The benchmark compares:

- `Disabled`: recovery succession is disabled.
- `Enabled`: recovery succession is enabled with one non-owner holder.

A persistent borrower repeatedly accesses a recoverable dependency. The failure
node is removed during the experiment.

Before failure detection, both configurations should continue serving requests.
After Ray detects the failed node:

- the disabled case should eventually stop making useful progress;
- the enabled case should reconstruct the dependency and resume throughput.

### Main metrics

The CSV contains time-bucketed measurements including:

- successful-request throughput;
- P95 successful-request latency;
- failure time;
- recovery-enabled / disabled configuration.

### Example

```bash
python gossip_benchmarks/recovery_availability_benchmark.py \
  --output gossip_benchmarks/recovery_availability_results.csv \
  --trials 3 \
  --duration-seconds 50 \
  --failure-at-seconds 15 \
  --bucket-seconds 1 \
  --payload-bytes 2097152 \
  --fixed-order
```

### Plotting

```bash
python gossip_benchmarks/plot_recovery_availability.py \
  gossip_benchmarks/recovery_availability_results.csv \
  --output-dir gossip_benchmarks/recovery_availability_plots
```

The current plotting script produces:

- `throughput_vs_time.png`
- `p95_latency_vs_time.png`

This benchmark corresponds to the current **Simple Recovery / Availability**
prototype result.

---

## 2. `local_recovery_time_benchmark_fixed_v3.py`

### Question

How long does recovery take when an in-flight stateless task loses its original
owner / producer node?

### Experiment

The benchmark creates separate logical nodes for:

- driver/head;
- original owner + producer;
- recovery holders;
- borrower.

The original task is still executing when the failure node is removed. Recovery
succession then reconstructs the lineage and replays the task.

Cases are:

- Disabled
- Enabled-1-holder
- Enabled-2-holders
- Enabled-3-holders
- Enabled-4-holders

Task execution time can be swept independently.

### Main metrics

Important CSV fields include:

- `formation_success`
- `success`
- `replayed`
- `replay_finished`
- `executions_observed`
- `failure_to_replay_start_s`
- `failure_to_replay_finish_s`
- `failure_to_result_s`

`failure_to_replay_start_s` includes Ray failure detection plus recovery
control-plane work.

`failure_to_result_s` additionally includes re-execution and result delivery.

### Example

```bash
python gossip_benchmarks/local_recovery_time_benchmark_fixed_v3.py \
  --output gossip_benchmarks/recovery_time_results.csv \
  --trials 2 \
  --task-durations 5 10 20 30 \
  --payload-bytes 2097152 \
  --enabled-only \
  --fixed-order
```

Remove `--enabled-only` if the disabled case is also needed.

### Plotting

```bash
python gossip_benchmarks/plot_local_recovery_time_fixed_v3.py \
  gossip_benchmarks/recovery_time_results.csv \
  --output-dir gossip_benchmarks/recovery_time_plots
```

The plotting script produces:

- `recovery_detection_time.png`
- `recovery_total_time.png`

This benchmark corresponds to the current **Recovery Latency** prototype result.

---

## 3. `recovery_steady_state_benchmark.py`

### Question

What steady-state throughput and latency overhead is introduced by enabling
recovery succession when no failures occur?

### Experiment

Every request follows:

```text
driver/owner -> producer -> consumer 1 -> consumer 2 -> consumer 3
```

The cluster always contains five logical Ray nodes.

Cases are:

- Disabled
- Enabled-1-holder
- Enabled-2-holders
- Enabled-3-holders
- Enabled-4-holders

The benchmark uses a fixed number of pipelines in flight and separately measures
different payload sizes.

### Main metrics

Run-level output includes:

- completed pipelines/s;
- payload throughput in MiB/s;
- mean latency;
- P50 latency;
- P95 latency;
- P99 latency.

It writes:

```text
<output-dir>/benchmark_runs.csv
<output-dir>/benchmark_timeseries.csv
```

### Example

```bash
python gossip_benchmarks/recovery_steady_state_benchmark.py \
  --output-dir gossip_benchmarks/recovery_benchmark_results \
  --warmup-seconds 5 \
  --duration-seconds 30 \
  --bucket-seconds 1 \
  --inflight 64 \
  --cpus-per-node 1 \
  --repetitions 3 \
  --payloads small:1024 big:2097152 \
  --fixed-order
```

### Aggregate plotting

```bash
python gossip_benchmarks/plot_recovery_benchmark.py \
  gossip_benchmarks/recovery_benchmark_results/benchmark_runs.csv \
  --output-dir gossip_benchmarks/recovery_benchmark_plots
```

This produces aggregate plots for:

- throughput;
- payload throughput;
- mean latency;
- P95 latency;

and also:

```text
benchmark_summary.csv
```

This is the preferred general steady-state benchmark when comparing multiple
payload sizes.

---

## 4. `steady_state_holder_benchmark.py`

### Question

What does throughput and P95 latency look like over time as the number of
recovery holders changes?

### Experiment

This is the simpler time-series steady-state benchmark used for the current
holder-count plots.

It compares:

- Disabled
- Enabled-1-holder
- Enabled-2-holders
- Enabled-3-holders
- Enabled-4-holders

There are no injected failures.

### Example

```bash
python gossip_benchmarks/steady_state_holder_benchmark.py \
  --output gossip_benchmarks/holder_benchmark.csv \
  --warmup-seconds 5 \
  --duration-seconds 30 \
  --bucket-seconds 1 \
  --inflight 64 \
  --payload-bytes 1024 \
  --cpus-per-node 1 \
  --repetitions 3 \
  --fixed-order
```

### Plotting

```bash
python gossip_benchmarks/plot_holder_benchmark.py \
  gossip_benchmarks/holder_benchmark.csv \
  --output-dir gossip_benchmarks/holder_benchmark_plots
```

This produces:

- `throughput_vs_time.png`
- `p95_latency_vs_time.png`

### Relationship to `recovery_steady_state_benchmark.py`

`steady_state_holder_benchmark.py` is useful when a simple time-series plot is
wanted.

`recovery_steady_state_benchmark.py` is the more complete benchmark for
steady-state evaluation because it supports multiple named payload sizes and
writes both run-level and time-series results.

---

# Additional recovery stress benchmarks

## 5. `recovery_holder_failure_benchmark.py`

### Question

How does recovery behave when early succession ranks are already dead before the
owner fails?

### Experiment

The benchmark:

1. forms a configured number of non-owner holders;
2. kills the first `K` holder nodes;
3. kills the owner / original-producer node;
4. asks a persistent borrower to retrieve the object.

For four holders, a useful sweep is:

```text
K = 0, 1, 2, 3, 4
```

### Main metrics

- `success`
- `accepted_rank`
- `executions_observed`
- `failure_to_result_s`

### Example

```bash
python gossip_benchmarks/recovery_holder_failure_benchmark.py \
  --output gossip_benchmarks/holder_failure_results.csv \
  --holders 4 \
  --predead-holders 0 1 2 3 4 \
  --task-duration 30 \
  --payload-bytes 2097152 \
  --trials 2
```

### Expected qualitative result

If holder fallback works correctly:

- `K=0` should normally recover from rank 1;
- `K=1` should normally recover from rank 2;
- `K=2` should normally recover from rank 3;
- `K=3` should normally recover from rank 4;
- `K=4` should fail because no recovery holder survives.

This benchmark directly tests whether additional succession ranks provide useful
fault tolerance.

---

## 6. `recovery_storm_benchmark.py`

### Question

What happens when one owner failure causes many objects to require recovery at
approximately the same time?

### Experiment

One owner creates many independent in-flight tasks. After holder formation, the
owner node is removed and a persistent borrower concurrently requests all
objects.

The benchmark can compare:

- `disabled`
- `enabled`

### Main metrics

Summary CSV:

- successful-object count / success rate;
- failure-to-first-success;
- P50/P95/P99 failure-to-result latency;
- replayed task count;
- tasks with more than two START events;
- maximum starts observed for a single task.

It also creates a per-object CSV:

```text
<output-stem>_objects.csv
```

### Example

```bash
python gossip_benchmarks/recovery_storm_benchmark.py \
  --output gossip_benchmarks/recovery_storm_results.csv \
  --systems disabled enabled \
  --tasks 16 \
  --holders 2 \
  --task-duration 30 \
  --payload-bytes 1048576 \
  --cpus-per-node 2 \
  --trials 2
```

### Useful prototype sweeps

Start with:

```text
tasks   = 1, 4, 16, 64
holders = 1, 2, 4
```

For a local machine, increase the storm size gradually.

This benchmark is particularly useful for finding:

- thundering-herd behavior;
- duplicate replay;
- recovery RPC bottlenecks;
- recovery serialization;
- poor scaling when many objects lose the same owner.

---

## 7. `recovery_failure_type_benchmark.py`

### Question

Does recovery behave differently depending on what actually fails?

### Failure modes

#### `owner_worker`

The owner actor process dies, but its node and the original producer node survive.

#### `owner_node`

The owner's node dies, while the original producer executes on a different node
and survives.

#### `owner_plus_producer_node`

The owner and original producer are co-located and the entire node dies.

### Why this matters

The first two cases primarily test **ownership loss**.

The third tests ownership loss together with loss of the original computation /
object location and therefore requires re-execution.

### Main metrics

- `success`
- `replayed`
- `executions_observed`
- `failure_to_replay_start_s`
- `failure_to_result_s`
- `original_task_finished_after_owner_failure`

### Example

```bash
python gossip_benchmarks/recovery_failure_type_benchmark.py \
  --output gossip_benchmarks/failure_type_results.csv \
  --modes owner_worker owner_node owner_plus_producer_node \
  --holders 2 \
  --task-duration 20 \
  --payload-bytes 2097152 \
  --trials 2
```

A useful observation is whether recovery causes a replay even when the original
producer survives and eventually finishes after ownership is lost.

---

## 8. `recovery_formation_scaling_benchmark.py`

### Question

How expensive is recovery-holder formation before any failure occurs?

### Experiment

No failure is injected.

The benchmark varies:

- number of recoverable tasks;
- number of holders;
- inline TaskSpec argument size.

The inline argument provides a simple way to vary lineage / TaskSpec metadata
size independently of the returned object size.

### Main metrics

- `formation_time_s`
- `admissions_per_s`
- per-rank formation time
- owner RSS delta
- sum of holder RSS deltas

### Example

```bash
python gossip_benchmarks/recovery_formation_scaling_benchmark.py \
  --output gossip_benchmarks/formation_scaling_results.csv \
  --tasks 1 10 50 \
  --holders 1 2 4 \
  --inline-arg-bytes 0 4096 32768 \
  --payload-bytes 1024 \
  --trials 2
```

### Interpretation

This benchmark helps separate **formation / metadata replication overhead** from
the application throughput effects measured by the steady-state benchmarks.

RSS measurements on a local Ray cluster are noisy and should be treated only as
prototype evidence. Final evaluation should use direct instrumentation for
TaskSpec, manifest, RPC, and retained-lineage bytes.

---

## 9. `recovery_chain_dag_benchmark.py`

### Question

Can recovery handle a dependency chain rather than a single independent task?

### Experiment

The owner submits:

```text
source -> stage1 -> stage2 -> ... -> stageN
```

The final ObjectRef is retained by recovery holders and a persistent borrower.
The owner / compute node is removed while the chain is still in flight.

### Main metrics

- `success`
- `failure_to_result_s`
- `stages_with_replay`
- `stages_with_gt2_starts`
- `max_starts_for_one_stage`

### Example

```bash
python gossip_benchmarks/recovery_chain_dag_benchmark.py \
  --output gossip_benchmarks/chain_recovery_results.csv \
  --systems disabled enabled \
  --chain-length 20 \
  --delay-ms 500 \
  --holders 2 \
  --payload-bytes 1048576 \
  --trials 2
```

### Purpose

This is intentionally an aggressive correctness / capability stress test.

It can expose problems involving:

- recovery metadata embedded in TaskSpec dependencies;
- ownerless intermediate objects;
- stale manifests;
- recursive reconstruction;
- duplicate replay across DAG stages.

Do not assume this benchmark must pass with the current prototype. A failure can
identify a real algorithmic limitation that needs to be fixed.

---

# Plotting utilities

## `plot_recovery_availability.py`

Input:

```text
recovery_availability_results.csv
```

Example:

```bash
python gossip_benchmarks/plot_recovery_availability.py \
  gossip_benchmarks/recovery_availability_results.csv \
  --output-dir gossip_benchmarks/recovery_availability_plots
```

Outputs:

- `throughput_vs_time.png`
- `p95_latency_vs_time.png`

---

## `plot_local_recovery_time_fixed_v3.py`

Input:

```text
recovery_time_results.csv
```

Example:

```bash
python gossip_benchmarks/plot_local_recovery_time_fixed_v3.py \
  gossip_benchmarks/recovery_time_results.csv \
  --output-dir gossip_benchmarks/recovery_time_plots
```

Outputs:

- `recovery_detection_time.png`
- `recovery_total_time.png`

---

## `plot_holder_benchmark.py`

Input:

```text
holder_benchmark.csv
```

Example:

```bash
python gossip_benchmarks/plot_holder_benchmark.py \
  gossip_benchmarks/holder_benchmark.csv \
  --output-dir gossip_benchmarks/holder_benchmark_plots
```

Outputs:

- `throughput_vs_time.png`
- `p95_latency_vs_time.png`

---

## `plot_recovery_benchmark.py`

Input:

```text
recovery_benchmark_results/benchmark_runs.csv
```

Example:

```bash
python gossip_benchmarks/plot_recovery_benchmark.py \
  gossip_benchmarks/recovery_benchmark_results/benchmark_runs.csv \
  --output-dir gossip_benchmarks/recovery_benchmark_plots
```

Outputs include:

- `average_throughput_rps.png`
- `average_data_throughput_mib_s.png`
- `average_latency_mean_ms.png`
- `average_latency_p95_ms.png`
- `benchmark_summary.csv`

---

# Suggested prototype execution order

A useful order for the current stage of the project is:

1. `recovery_availability_benchmark.py`
2. `local_recovery_time_benchmark_fixed_v3.py`
3. `recovery_steady_state_benchmark.py`
4. `recovery_holder_failure_benchmark.py`
5. `recovery_failure_type_benchmark.py`
6. `recovery_storm_benchmark.py`
7. `recovery_formation_scaling_benchmark.py`
8. `recovery_chain_dag_benchmark.py`

The first three reproduce the current basic prototype story:

1. recovery restores availability;
2. recovery latency is dominated by failure detection plus task re-execution;
3. recovery metadata / holder formation introduces measurable steady-state overhead.

The additional benchmarks then test whether the mechanism remains useful and
correct under more demanding conditions:

- failed earlier succession ranks;
- different kinds of owner failure;
- many simultaneous recoveries;
- increasing holder / lineage metadata;
- multi-stage dependency chains.

---

# Suggested prototype result groups

For early internal results, the benchmarks can be organized into the following
groups.

## Availability

Use:

```text
recovery_availability_benchmark.py
```

Shows whether useful work resumes after failure.

## Recovery latency

Use:

```text
local_recovery_time_benchmark_fixed_v3.py
```

Shows failure-to-replay and failure-to-result latency as task duration and holder
count change.

## Failure-free overhead

Use:

```text
recovery_steady_state_benchmark.py
steady_state_holder_benchmark.py
```

Shows throughput and latency cost when no failures occur.

## Redundancy effectiveness

Use:

```text
recovery_holder_failure_benchmark.py
```

Shows whether later succession ranks actually tolerate earlier holder failures.

## Failure semantics

Use:

```text
recovery_failure_type_benchmark.py
```

Separates owner-worker loss, owner-node loss, and simultaneous owner / producer
loss.

## Recovery scalability

Use:

```text
recovery_storm_benchmark.py
recovery_formation_scaling_benchmark.py
```

Tests burst recovery and metadata / holder-formation scaling.

## Dependency-graph stress test

Use:

```text
recovery_chain_dag_benchmark.py
```

Tests whether the mechanism extends beyond a single recoverable task.

---

# Important limitations of these prototype benchmarks

These experiments should not yet be interpreted as final production or paper
performance numbers.

In particular:

1. Most tests use several logical Ray nodes on one physical host.
2. Ray's node-failure detection delay can dominate measured recovery latency.
3. Log-derived replay timestamps are useful for debugging but should eventually
   be replaced with structured internal instrumentation.
4. Process RSS is only an approximate proxy for protocol metadata cost.
5. Actor / pipeline overhead can confound measurements of the recovery protocol
   itself.
6. Recovery success does not by itself prove exactly-once task execution.
7. The DAG benchmark may expose unsupported cases in the current algorithm.

For final evaluation, the most important experiments should eventually be
repeated on multiple physical machines with direct measurements of:

- recovery protocol RPC count and bytes;
- retained TaskSpec / lineage bytes;
- exact failure-detection time;
- recovery claim time;
- replay submission time;
- replay start / finish time;
- duplicate execution count;
- CPU and memory overhead.
