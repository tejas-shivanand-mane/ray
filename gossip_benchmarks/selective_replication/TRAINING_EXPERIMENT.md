# Real training experiment

This runs two independent PyTorch MLP trials on sklearn digits, profiles them,
chooses protection plans, and executes healthy and injected worker-process-loss
runs. It uses three local CPU cores and needs Linux, Python 3.10+, PyTorch, NumPy,
and scikit-learn. Use your existing `ray-dev` environment. No GCP resources are
created and the GCP notebook is unchanged.

From the repository root:

```bash
python gossip_benchmarks/selective_replication/training_experiment.py
```

Trial A has width 128; trial B has width 256. Calibration chooses epoch counts
for approximately 4 and 8 seconds of fast-worker compute. Default profiling
repeats twice; each selected plan/scenario executes once. This is exploratory,
not enough runs for statistical reliability claims. Use `--repeats 3` for more
observations, potentially requiring resume after the per-invocation time limit.

## Explicit experimental controls

The default uses real training with **artificial worker-speed pacing**. On the
`safe` and `spot_b` workers, each training batch receives a cancellable delay
proportional to its measured compute time: approximately 3x total compute time
for A and 1.5x for B. `spot_a` is unpaced. This deliberately instantiates the
heterogeneous timing pattern found offline. These are not measurements of
heterogeneous hardware or real spot-instance failure rates. Worker busy time
includes these waits, and must not be called CPU time, energy, or cloud savings.

Checkpoint writes, flushes, and restoration are real. **There is no added
checkpoint delay by default.** If cheap checkpoints make replication unnecessary,
the report retains the tie or loss. To test a controlled storage-cost hypothesis:

```bash
python gossip_benchmarks/selective_replication/training_experiment.py --checkpoint-delay-s 0.4
```

That adds 0.4 seconds to every checkpoint on every worker, including profiling;
results explicitly record this artificial delay. To disable worker pacing:

```bash
python gossip_benchmarks/selective_replication/training_experiment.py --native-workers
```

Use a new output directory for each setting. `--target-seconds 8` doubles the
fast-worker compute targets; `--smoke` uses small models and short training to
check correctness, not performance.

## Comparisons and faults

All existing planner families are retained, plus risk-aware sequential planning.
Checkpoint intervals use the same measured-cost menu for all methods. Three
predeclared deadline multipliers are searched; every candidate is saved. The
runner selects the largest modeled advantage over the best feasible checkpoint,
fixed-placement, or uniform-replication baseline **before any replay**. A tie or
loss does not stop execution. The fixed-placement baseline may select no backup;
it is the existing stronger baseline, not forced double execution.

Fault times are frozen from profiling before choosing any plan. The early-time
reference is trial A's predicted final-only duration on the fast worker, capped
at half the reliable-worker reference duration to separate early and late times.
Planning uses 20%, 40%, 60%, and 80% of that early reference, plus 60% and 80% of
the reliable-worker reference. Replay uses unseen 30% and 70% of the early
reference, plus 70% of the reliable-worker reference. For the first uploaded
profile, this moves replay faults to approximately **0.97, 2.27, and 9.33 seconds**,
covering trial A before its observed 3.31-second completion while retaining late
losses. Other machines get times from their own profile.

Every policy receives the exact same wall-clock times. Each time tests loss of
either unreliable worker, loss of the usually reliable worker, and correlated
loss of both unreliable workers. Including healthy control, this is 13 scenarios
per distinct plan per repeat. Healthy runs execute first; subsequent order is
seeded and shuffled. Exact duplicate plans share observations, identified in the
report. All tasks retain checkpoint retry, including tasks without replicas.

The assumed scenario weights match the offline experiment: healthy 50%, each
individual unreliable-worker loss 24.5%, reliable-worker loss 0.5%, and correlated
loss 0.5%. Within each failure type, two thirds of the weight belongs to early
losses and one third to late losses, equally split among that band's times.
This changes the timing distribution from the previous all-late experiment;
weighted results across those versions are not directly comparable.

Weighted observed deadline fractions are not reliability estimates. The summary
separates requested, injected, active-task, idle-worker, and unreached faults.
Its **Early faults interrupting A** column and per-policy `fault_exposure` fields
show whether trial A was actually interrupted. Zero is explicitly marked
`not_exercised`; merely injecting a fault is not evidence of recovery. A policy
may legitimately finish or place A elsewhere before the loss; we retain that
result without retiming failures per policy. Compare deadline attainment together
with resource usage.

The controller and shared storage survive. Failures are SIGKILL of local workers,
not machine loss. Every completed trial's model, optimizer, input history, and
identity must match the healthy reference; incorrect results fail the run.

## Outputs and resume

The command prints a directory under `ray_gcp_results/measured-selective/`.
It stops after 28 minutes per invocation, preserving completed observations.
Resume with the **same code and output directory**:

```bash
python gossip_benchmarks/selective_replication/training_experiment.py --resume /absolute/path/to/run
```

Completed profiles, plans, and replay observations are reused. An interrupted
observation gets a new attempt directory; old attempts are retained. A local
file lock prevents two runners using the same directory. `--max-minutes 5` lowers
the budget for an invocation. The printed stage logs show detailed progress.
Start a fresh run after pulling this timing change; the source check prevents
mixing the earlier fault schedule with new observations in a resumed directory.

Upload **`analysis.zip`** from the output directory. It contains the configuration,
profiles, all searches, summary, observation records, fault events, and logs,
without large checkpoint tensors. It is created on both normal completion and
handled interruption. `summary.json` marks incomplete comparisons explicitly.
