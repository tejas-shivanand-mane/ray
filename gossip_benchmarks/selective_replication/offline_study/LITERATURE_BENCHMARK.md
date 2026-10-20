# Bounded comparison with published checkpoint algorithms

Run from the repository root in `ray-dev`:

```bash
python gossip_benchmarks/selective_replication/offline_study/joint_checkpoint_sweep.py
```

The default is now `--suite literature`: **192 workload/platform families,
1,152 deadline/target cases, eight policies**, plus **48 separate Han-DP cases**.
The old 12-case run remains available with `--suite short`.

The owner measured about 5.5 seconds of computation for the old run. This suite
does substantially more work, but its runtime has not been measured. It has a
25-minute cooperative computation budget and an independent **29-minute process
watchdog**, including report generation. It exits early if finished; it does not
pad the runtime to 30 minutes. Timeout is exit code 124, not success. The owner
runs all experiments and validation; none was executed when preparing this code.

## Exactly what is implemented

| Report name | Source and scope |
| --- | --- |
| `young_interval_adapted` | Young's first-order useful-work interval, discretized onto the shared menu; placement optimized with the common deadline/cost objective. |
| `daly_interval_adapted` | Daly's first-order interval including recovery time, with the same discretization and placement wrapper. Not the higher-order Daly model. |
| `checkfreq_bound_adapted` | CheckFreq Algorithm 1 specialized to synchronous saves and constant known profiles; tune an overhead bound on planning scenarios only. |
| `han_dp_after_healthy_mapping` | Han Algorithm 2/Eq. 2 checkpoint kernel on chains, after our failure-blind mapping. Separate analytical panel. |
| `han_dp_best_fixed_node` | Same checkpoint kernel independently on every node, retaining the best. Our strengthened placement wrapper. |

The five existing controls remain: optimized uniform, risk-aware sequential,
multistart sequential, multistart alternating, and exhaustive joint planning.
They are our controls. **This benchmark does not reproduce full CheckFreq,
CKPTSOME's M-SPG mapping algorithm, Dyna, or uHEFT.** A win here is an algorithm
comparison within the documented models, not superiority over those systems.

Sources checked when implementing:

1. [Aupy et al., Checkpointing algorithms and fault prediction](https://arxiv.org/pdf/1302.3752),
   introduction and Eq. (9), documenting Young/Daly's first-order formulas.
   Useful compute intervals are `sqrt(2*C*mu)` and `sqrt(2*C*(mu+R))` for zero downtime.
2. [Mohan et al., CheckFreq, FAST 2021](https://www.usenix.org/system/files/fast21-mohan.pdf),
   Algorithm 1 and Section 4.3.2. `checkfreq_algorithm1` implements the CPU/GPU
   branch and persistence/overhead bounds, rounding to whole iterations. The
   main benchmark uses its synchronous specialization `ceil(C/(p*iteration_s))`.
3. [Han et al., Checkpointing Workflows for Fail-Stop Errors](https://henricasanova.github.io/files/papers/han_tc2018.pdf),
   Section 5.2, Eq. (2) and Algorithm 2. A segment's expected time is
   `(1/lambda + downtime) * expm1(lambda*(read + work + save))`; the DP chooses
   segment boundaries to minimize their sum. All incoming/outgoing data crossing
   a segment are charged, including skip dependencies. The zero-rate limit is
   `read + work + save`. Backtracking includes the mandatory final boundary.

## Main panel: state checkpoints and deadline success

All eight policies use the same task work, simulator, checkpoint costs, node
prices, placement/order choices, recovery rule and failure scenarios. Only
planning scenarios select policies; selected plans are frozen for held-out times.

The 192 families are the Cartesian product of:

- Four workloads: `t1 || (t2; t3)` with t1 lasting 30 or 240 nominal seconds;
  a three-task chain lasting 120/60/60 seconds; and a four-task fork-join
  `t1; (t2 || t3); t4` with 60 seconds per task.
- Save costs: 0.3 or 6 seconds per checkpoint.
- Spot prices: 0.25 or 0.75, relative to safe-node price 1 per hour.
- Spot compute speeds: 0.75 or 1.5 times safe-node speed.
- Loss scenario mass: 0.10 or 0.50.
- Three risk conditions: equal single-node risk; spot-to-safe ratio 20;
  and ratio 20 with half the loss mass assigned to simultaneous spot losses.

Each family uses deadlines 1.2/1.5/2.0 times its best healthy final-only runtime,
and success thresholds 0.90/0.95. There are eight planning failure times from
0.1 to 1.5 times the reference (spacing 0.2), and sixteen distinct held-out times
from 0.05 to 1.55 (spacing 0.1). These are assumed scenario weights, not measured
cloud failure probabilities or independent experimental repetitions.

Uniform rules include final-only and approximately 2/4 chunks per task, plus
common iteration intervals. Every selective method has their Cartesian closure
as its menu. All legal dispatch orders and placements on three nodes are allowed.

**Adaptations that affect interpretation:**

- Young/Daly need an MTBF. For node marginal loss probability `q` by the reference
  time `H`, use equivalent rate `-log(1-q)/H`. This matches one marginal probability;
  it does not turn the finite, correlated scenario model into a Poisson process.
  The discrete interval minimizes `C/W + W/(2*mu_effective)` over the shared menu.
- CheckFreq's CPU/GPU kernel is present, but evaluation uses synchronous saves,
  without its pipelined persistence, data loader, profiling warmup or runtime
  interference. Bound candidates are 1%, 3.5%, 5%, 10%. The planning objective
  selects among them and all placements. A final-only interval may still exceed
  the requested bound for a short task; this is recorded as `bound_satisfied=false`.
- All interval choices remain frozen after relocation, as for the other policies.
  Each policy's original interval, selected interval, node and rule parameters
  are saved. No bound or equivalent rate is tuned on held-out results.
- The joint reference is exact only over this finite menu. It is not an SMT solver
  or a scalable algorithm. Shared simulation caches do not measure standalone
  planner speed; candidate-query counts are only diagnostics.

## Separate panel: Han's output-checkpoint DP

Han checkpoints outputs between tasks, whereas the main panel always saves final
task outputs and selects intermediate training-state intervals. Mixing those
semantics would be misleading. `han_comparison.json` therefore uses a separate
analytical model and the native **expected makespan** objective, with repeated
exponential failures and replacement after one second of downtime. No prices,
success thresholds, or held-out scenario probabilities are attached to it.

Its 48 chain cases vary task count (4/8/12), task-to-node speed affinity, output
save cost, equal/mixed node failure rates, and base failure rate. They compare
the two Han-DP wrappers above with checkpoint-all, checkpoint-none (mandatory
final output retained), and our joint segment-placement DP. A placement change
requires a durable boundary. Failure-blind mapping also accounts for boundary
I/O, so a benefit from ordinary healthy scheduling is not silently credited to
failure-aware planning. Equal-speed/equal-risk cases provide an expected tie
control. The heterogeneous-node extension is ours; the kernel is the published
recurrence. This is a restricted chain comparison, not the full paper evaluation.

## Outputs and interpretation

Upload **`analysis.zip`** from the printed output directory. It includes:

- `manifest.json` and `status.json`: frozen settings, sources, source hashes,
  completion status and elapsed computation time.
- `summary.md`, `summary.json`, `comparisons.csv`: every main-panel win, tie,
  loss and feasibility difference, separated by planning/held-out evaluation.
- `families/*.json`: selected plans, scenario outcomes, interval-rule diagnostics
  and alternating-search trajectories.
- `han_comparison.json`: all 48 analytical cases, inputs, decisions and expected
  times, reported separately from the main-panel counts.

Only `state=complete` means the main benchmark finished. A partial archive exists
before enumeration, after the Han panel, every eight families, and at cooperative
shutdown. A watchdog kill leaves the last archive, potentially marked `running`;
that is incomplete. Family order is fixed by input hashes, never by observed wins.
No conclusion across the full range should be based on an unfinished prefix.

The practical flag now requires at least 5% savings over both multistart controls
and all three interval adaptations, with every compared plan meeting the target
in both evaluations. Report feasibility-only gains separately. Even passing this
flag does not demonstrate novelty or measured training improvement.

Optional correctness checks, prepared for the owner to run separately:

```bash
python gossip_benchmarks/selective_replication/offline_study/test_literature_baselines.py
python gossip_benchmarks/selective_replication/offline_study/test_joint_checkpoint_sweep.py
```

The checks cover the CheckFreq paper's simple example, GPU selection and
persistence limits, Han I/O across skip edges, repeated-failure accounting,
DP versus independently enumerated checkpoint subsets, and four-task ordering.
The benchmark does not automatically invoke them. Source/schema changes require
a new output directory; old runs remain readable with their original commit.
