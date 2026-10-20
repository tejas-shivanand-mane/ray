# Does joint placement and checkpoint planning beat simpler optimization?

This is a new, unexecuted, model-only screen. It does not run training, start
Ray, kill processes, contact a cloud provider, or use a GPU. It uses Python's
standard library and the existing deterministic `planner.py` simulator.

The project owner runs experiments and validation. Do not run experiments,
benchmarks, tests, linters or automatic validation on the owner's behalf unless
explicitly requested. The code and the hand-constructed checks were prepared
without executing them.

## Run and resume

From the repository root in the existing `ray-dev` environment:

```bash
python gossip_benchmarks/selective_replication/offline_study/joint_checkpoint_sweep.py
```

The default screen has **64 model families**, each evaluated at three deadline
multipliers and two success targets: **384 comparison cases**. Each case compares
five methods. There is no requirement to rerun the earlier training experiment.

Each invocation has a 28-minute computation budget. No runtime estimate has been
measured. It prints its output directory and an exact resume command. If it pauses:

```bash
python gossip_benchmarks/selective_replication/offline_study/joint_checkpoint_sweep.py --resume /absolute/path/to/run
```

Resume preserves all completely evaluated candidates, including an unfinished
family. The cache is written every 128 candidates and on a caught timeout or
Ctrl-C. An abrupt process/machine loss may require redoing the latest unsaved
candidates. A run lock prevents concurrent writers. The saved suite and source
hashes must match; only `--max-minutes` may change when resuming. A budget pause
returns exit code 0, but **only `status.json: state=complete` means completion**.
Final report generation/ZIP writing occurs after the computation budget.

Upload **`analysis.zip`** from the printed directory. It contains the manifest,
status, all completed comparison cases, selected plans, scenario outcomes,
alternating trajectories, `comparisons.csv`, and summaries. Both favorable and
unfavorable results are included. Large enumeration caches stay in the run folder
and are excluded from this analysis archive; retain that folder for resumption.

Optional smaller execution for the owner to check the harness:

```bash
python gossip_benchmarks/selective_replication/offline_study/joint_checkpoint_sweep.py --suite smoke
```

That is one family / six cases, not a substitute for the screen. A separate
`--suite full` run uses 192 families / 1,152 cases, adding a shorter t1 branch and
zero/intermediate checkpoint costs. Do not start it merely to seek a win after
the screen ties. The full range is specified now to keep that distinction clear.

Optional hand-constructed checks, also for the owner to run:

```bash
python gossip_benchmarks/selective_replication/offline_study/test_joint_checkpoint_sweep.py
```

## The question and common objective

Given the same information and available actions, does choosing placement and
checkpoint intervals together reduce **expected monetary cost**, subject to
the same required probability of completing by a deadline?

All methods use exactly the same scenario probabilities, deadlines, machine
prices, checkpoint costs, task dependencies and recovery rules. A feasible plan
always ranks ahead of an infeasible one. Among feasible plans, minimize cost,
then elapsed time and worker busy time, with a deterministic plan tie-break.
If no queried plan is feasible, retain maximum deadline success, then minimum
cost, but label it infeasible. Numerical tie-breaking rounds costs to 12 decimal
places and times to 9; this is the finite reference's optimization objective.

| Method | How it chooses a plan |
| --- | --- |
| `uniform_checkpoint` | Optimize placement, dispatch order and a shared checkpoint rule, accounting for failures and checkpoint overhead. Both common epoch intervals and common work fractions are allowed, including final-only. |
| `risk_aware_sequential` | Start from the best uniform plan, freeze placement and dispatch order, then optimize each task's checkpoint interval. |
| `sequential_multistart` | For **every** uniform rule, retain the cost-optimal feasible schedule, a reliability-first schedule and a time-first schedule. Optimize per-task intervals at each fixed schedule, and keep the best. |
| `alternating_multistart` | From the same seeds, optimize the entire checkpoint vector with schedule fixed, then optimize the entire placement/order with checkpoints fixed. Repeat until neither block improves; keep the best terminal plan across starts. |
| `joint_checkpoint` | Enumerate the complete Cartesian product and choose the best plan. This is an exact reference within the finite menu, not a new scalable solver. |

Both alternating blocks search all choices in that block, not just single-task
neighbors. Strict descent in a common deterministic total order prevents cycles;
there is no arbitrary iteration cap that could artificially weaken the baseline.
Seeds are chosen from planning results only and never from the joint optimum.
Duplicate seeds are removed. The reliability-first and time-first seeds use
the same already-queried uniform candidates; they help avoid having all starts
collapse onto a single cost-optimal schedule.

Simulation results are cached and shared for efficiency. `candidate_queries`
counts the unique candidates each policy asks for, including its seed search.
It is a diagnostic, **not measured standalone planner runtime**. The joint
reference can tie but cannot lose under the planning objective. It can lose on
held-out failure times, which the report explicitly records.

## Workload and predeclared grid

The graph is `t1 || (t2; t3)`. Each task represents training work divided into
epoch-sized units; there is no actual ML computation in this screen. A task
always saves its final output; t3 starts only after t2 finishes. Intermediate
checkpoints preserve completed work and survive node loss. Checkpoint saving
blocks computation on that worker and can itself be interrupted.

| Axis | Default screen values |
| --- | --- |
| t1 compute duration | 90 or 120 modeled seconds |
| t2 and t3 compute duration | 60 modeled seconds each |
| Cost of each save, including final output | 0.3 or 6 modeled seconds |
| Safe price | 1 assumed currency unit/hour |
| Each spot price | 0.25 or 0.75 units/hour |
| Total scenario mass with any node loss | 0.10 or 0.50 |
| Ratio of each spot node's single-loss weight to safe single-loss weight | 1 or 20 |
| Share of loss mass assigned to simultaneous loss of both spot nodes | 0 or 0.50 |
| Deadline / fastest healthy final-only completion time | 1.2, 1.5, 2.0 |
| Required weighted deadline success | 0.90, 0.95 |

All machines have identical compute speed. Task initialization/restoration costs
0.3 modeled seconds per attempt. Per-task interval menus are the Cartesian
closure of the uniform rules: final-only, approximately 2/4/8 chunks per task,
and common epoch intervals based on the shortest task. The three-task dispatch
order respects t2 before t3. All 27 placements and three legal orders are allowed.

Loss mass is divided into correlated loss and single-node loss. Single-node
weights are proportional to `(1, ratio, ratio)` for `(safe, spot_a, spot_b)`.
These are **finite scenario weights, not continuous-time hazard rates**. Ratio 1
is an equal-risk control only when correlated share is zero. Each node may fail
once and then stays dead. Surviving nodes can execute failed tasks from their
last checkpoint. Relocation is the same existing greedy rule for every policy:
use the cheapest free survivor, otherwise wait.

The healthy reference, failure clocks and deadline are fixed before selecting
plans. All policies face the same absolute event times:

- Planning: 0.20, 0.50, 0.80 and 1.10 times the healthy reference.
- Held-out evaluation: 0.10, 0.35, 0.65, 0.95 and 1.20 times that reference.

Each timing receives equal weight within its loss category. Healthy execution
has probability `1 - failure_mass`. Plans are selected only with planning times;
the held-out evaluation neither retunes nor reselects them. Inspect individual
scenario outcomes because some losses hit idle nodes or occur after completion.

The simulation horizon is eight times the healthy reference. Reports include
completion probability separately from deadline success. Elapsed time and cost
are capped at that horizon if a scenario does not complete.

## Accounting differs from the previous training experiment

The earlier experiment billed every worker in a warm pool from time zero, which
largely couples cost to elapsed time rather than the chosen placement. Here a
node is billed from its **first use** until node loss or whole-workflow completion,
including subsequent idle time. Unused nodes are free. Recovery may start billing
an additional node. There is no scale-down before completion, no minimum billing
increment and no startup delay. Those are explicit modeling assumptions and must
be revisited before a cloud claim. Do not compare the numerical cost directly
with the previous warm-pool results.

## Read the result without selecting only favorable cases

`summary.md` counts cheaper plans, cost ties, losses and feasibility differences.
`comparisons.csv` contains every completed case against every baseline, with
planning and held-out costs/success shown separately. `summary.json` also groups
held-out results against alternating optimization by each grid axis. A partial
run is labelled partial; its completed prefix is not evidence across the full grid.

The predeclared practical flag requires at least **5% lower cost against both
multistart baselines**, with joint and baseline plans all meeting their required
success target in both planning and held-out evaluation. Feasibility-only wins
are reported separately, not converted to cost gains. The flag does not require
equal achieved success probabilities; it requires the same threshold. The raw
success differences are retained so that extra reliability is visible.

There are no stochastic repetitions: repeating the same deterministic case
does not add statistical evidence. Held-out times are a temporal-sensitivity
check within the model, not an estimate of production reliability.

- If alternating matches joint across the grid, the more elaborate optimizer
  has no demonstrated benefit in this model.
- If joint helps only at isolated tight thresholds or loses at held-out times,
  regard it as fragile model evidence, not a useful training result.
- If an advantage survives over a range of settings, the next step is to test
  those conditions using independently measured task/checkpoint costs and
  actual training. Retain neighboring losses and ties in that follow-up.

These methods are experimental controls, not implementations of published
algorithms. The literature comparison identified Dyna, uHEFT, SpotOn and
workflow-specific checkpoint-interval optimization as relevant further
comparisons. Passing this screen does not establish novelty or superiority to
those methods, TorchFT, Bamboo, Oobleck or Varuna.
