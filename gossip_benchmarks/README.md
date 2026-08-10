# Additional Ray Recovery-Succession Prototype Benchmarks

These scripts are intended to be copied into `gossip_benchmarks/` and run against
the current experimental Ray build. They use `ray.cluster_utils.Cluster`, so they
create several logical Ray nodes on one physical machine, like the current local
recovery benchmarks.

They are prototype/debugging experiments, not final paper methodology.

## 1. recovery_holder_failure_benchmark.py

Question:
How does recovery behave when early succession ranks are already dead?

Default experiment:
- form 4 non-owner holders;
- pre-kill 0, 1, 2, 3, then 4 earliest holder nodes;
- kill the owner/original-producer node;
- have one persistent borrower read the object.

Important output:
- `success`
- `accepted_rank`
- `executions_observed`
- `failure_to_result_s`

Example:

```bash
python gossip_benchmarks/recovery_holder_failure_benchmark.py \
  --output gossip_benchmarks/holder_failure_results.csv \
  --holders 4 \
  --predead-holders 0 1 2 3 4 \
  --task-duration 30 \
  --payload-bytes 2097152 \
  --trials 2
```

Expected qualitative result if the current succession list works:
- K=0 -> rank 1 should normally recover;
- K=1 -> rank 2 should normally recover;
- ...
- K=holders -> recovery should fail because no holder survives.

This is a stronger redundancy result than simply showing that adding holders does
not increase latency.

## 2. recovery_storm_benchmark.py

Question:
What happens when one owner failure makes many objects recover at the same time?

The owner creates many independent in-flight tasks. After formation, the owner node
is removed and a persistent borrower launches concurrent reads for every ObjectRef.

It compares disabled vs enabled.

Important output:
- success rate
- failure-to-first/p50/p95/p99 successful result
- replayed task count
- tasks with >2 START events (possible duplicate-replay symptom)
- per-object detail CSV

Example:

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

After the run, the script also creates:
`recovery_storm_results_objects.csv`.

Good prototype sweeps:
- tasks: 1, 4, 16, 64
- holders: 1, 2, 4
- cpus-per-node: 1, 2, 4

For a local machine, start small. A 64-task storm with long tasks can be expensive.

## 3. recovery_failure_type_benchmark.py

Question:
Does recovery behave differently for owner-worker failure, owner-node failure, and
loss of both owner and original producer?

Modes:
- `owner_worker`: owner actor process dies; producer node survives.
- `owner_node`: owner node dies; producer is on another node and survives.
- `owner_plus_producer_node`: owner and producer are co-located and the node dies.

The first two isolate ownership loss. The third includes compute/object loss and
therefore requires re-execution.

Example:

```bash
python gossip_benchmarks/recovery_failure_type_benchmark.py \
  --output gossip_benchmarks/failure_type_results.csv \
  --modes owner_worker owner_node owner_plus_producer_node \
  --holders 2 \
  --task-duration 20 \
  --payload-bytes 2097152 \
  --trials 2
```

Important output:
- `failure_to_replay_start_s`
- `failure_to_result_s`
- `executions_observed`
- `original_task_finished_after_owner_failure`

A particularly interesting result is whether the system replays even when the
original producer survives and finishes after the owner disappears.

## 4. recovery_formation_scaling_benchmark.py

Question:
How expensive is holder formation itself?

No failures are injected. The owner creates N recoverable tasks and their ObjectRefs
are passed through 1..R holders. The benchmark waits for committed manifests and
records rank-by-rank formation time and process RSS.

It also varies a small inline argument attached to each TaskSpec. This is useful for
testing whether TaskSpec replication cost grows with lineage metadata size.

Example:

```bash
python gossip_benchmarks/recovery_formation_scaling_benchmark.py \
  --output gossip_benchmarks/formation_scaling_results.csv \
  --tasks 1 10 50 \
  --holders 1 2 4 \
  --inline-arg-bytes 0 4096 32768 \
  --payload-bytes 1024 \
  --trials 2
```

Important output:
- `formation_time_s`
- `admissions_per_s`
- `rank1_time_s` ... `rank4_time_s`
- `owner_rss_delta_bytes`
- `holder_rss_sum_delta_bytes`

RSS is noisy on a local Ray process, so use it only as prototype evidence. For final
paper numbers, add direct byte counters inside the recovery manager.

## 5. recovery_chain_dag_benchmark.py

Question:
Can recovery handle a dependency chain rather than one independent task?

This is intentionally experimental. The owner submits:

`source -> stage1 -> stage2 -> ... -> stageN`

The final ObjectRef is retained by holders and a persistent borrower. The owner /
compute node is removed while the chain is in flight.

This stresses:
- recovery metadata inside TaskSpec arguments;
- stale dependency manifests;
- recursive recovery;
- duplicate replay across DAG stages.

Example:

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

Important output:
- `success`
- `failure_to_result_s`
- `stages_with_replay`
- `stages_with_gt2_starts`
- `max_starts_for_one_stage`

Do not be surprised if this benchmark exposes bugs in the current implementation.
That is part of why it is useful at the prototype stage.

## Suggested order

Run these first:

1. holder failure/rank fallback
2. worker vs node failure
3. recovery storm
4. formation scaling
5. chain DAG

The first four should be treated as direct extensions of the current single-object
prototype evaluation. The DAG benchmark is more aggressive and may identify algorithm
changes needed before it becomes a stable result.


