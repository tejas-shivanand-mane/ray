# Joint placement, node risk and checkpoint scaling

This owner-run experiment asks whether jointly choosing a task's node and its
checkpoint improves results over cheaper planners. It does not assume Z3 wins.
It extends the seven-decision checkpoint experiment to 7, 20, 50 and 100 tasks,
two data sizes, and individually configurable nodes.

## Run

From the repository root in your `ray-dev` environment:

```bash
git pull --ff-only origin main
python -m pip install -r gossip_benchmarks/selective_replication/data_processing/requirements-comprehensive.txt
python gossip_benchmarks/selective_replication/data_processing/joint_scaling.py
```

The default is a 28-minute controller budget, including downloading, profiling
and planning. Dependency installation and final archive writing are outside that
budget. Slow downloads/planners can leave a partial run. The watchdog stops the
controller and its children; completed atomic results remain available.

Resume using the output directory printed by the command:

```bash
python gossip_benchmarks/selective_replication/data_processing/joint_scaling.py --resume ray_gcp_results/data-processing-joint/YOUR_RUN_DIRECTORY
```

Resume uses the saved configuration and data; only `--minutes` changes the new
session budget. Source/dependency changes require a fresh run. Do not run two
controllers on one output directory. If the supervisor itself was killed,
confirm its controller stopped before removing a stale `controller.lock`.

Send **`analysis.zip`**, including after a failure/timeout. It contains node
configuration, profiles, all planner results, SMT encodings, query status
traces, summaries and source snapshots. Raw documents and pickle payloads are
excluded. No project code, tests or experiments were run by the assistant.

## Measured versus modeled

Measured locally:

- Real 20 Newsgroups text, seeded shuffle, headers/footers/quotes removed.
- 1,500 and 6,000 distinct records; the smaller sample is nested within the
  larger one, not an independent dataset.
- Real 64-component MinHash computation, durable local checkpoint writes,
  checkpoint reads, and the final LSH/duplicate-verification reducer.
- Two profiling repetitions per partition/reducer; medians supply coefficients.
- Three independently timed planner invocations on each fixed case.

Seven partitions use natural source families; 20 use categories. Larger counts
subdivide categories deterministically, allocating more partitions to larger
categories. Total input stays fixed when task count changes. Final application
results must match across task counts. No padded runtimes or fabricated output
sizes are used to force a greedy counterexample.

Parallel execution, node-specific speeds, transfer overheads, node deaths,
replacement delays, rental costs and deadline probabilities are **modeled, not
measured on cloud VMs**. This is one application pattern with measured local
coefficients, not a Ray/Spark/cloud performance evaluation. Planner repetitions
measure planning-time variability, not distributed execution variability. No
significance claim is made from repeated evaluations of the same fixed model.

## Editable assumptions

Copy `joint_nodes.example.json` to your own JSON file and pass `--nodes FILE`.
Its prices and risks are illustrative sensitivity assumptions, not current
provider quotations or measured spot-interruption rates.

| Node field | Meaning |
|---|---|
| `name` | This particular node's identity |
| `kind` | `spot` or `regular`; either can fail |
| `failure_probability` | Probability of death during the modeled post-map recovery window |
| `usd_per_hour` | Assumed rental price for this node/replacement |
| `speed` | Computation speed relative to the profiling machine |
| `restart_s` | Time to obtain its replacement after loss |
| `transfer_mib_s` | Assumed per-node input/output bandwidth |

Defaults: two individually specified spot nodes and one regular node. Two to
four nodes are supported. Risks are rounded to thousandths and recorded after
applying each risk profile. Default profiles use configured probabilities and
double only spot probabilities, capped at one.

These are probabilities for one assumed exposure window, **not hourly hazards**.
They do not automatically rise for longer jobs. Keep them fixed to isolate size
effects, or provide estimates for your intended exposure. Interruptions during
running tasks and recurring failures need a later continuous-time experiment.

## Failure, scheduling and billing model

Each task chooses one node and whether to durably checkpoint its output. Tasks
on one node execute serially; nodes execute in parallel. After all map tasks and
selected checkpoints finish, node loss occurs **before the reducer pulls any
outputs**. All unprotected outputs on a dead node are lost together. Those tasks
are recomputed from durable original inputs; protected outputs are restored.

Nodes fail independently. Tasks on the same node share a failure event. All
node-loss subsets are evaluated (eight for three nodes), including simultaneous
losses. This does not model zone-wide common-cause failures. Each failed active
node gets one replacement of the same speed and price; replacements do not
fail again. Recovery is serial on each replacement and parallel across them.
Even an all-protected failed node pays replacement delay. After recovery, the
reliable reducer pulls outputs serially and computes once. The reducer and
durable checkpoint/input stores are assumed to survive.

The local I/O profiles are combined with explicit modeled network overheads;
they are not object-store latency measurements. There is no shared-link
contention. Increasing replacement delay alone need not favor checkpointing:
both plans pay it if they use the same failed nodes.

For a node-loss subset S:

```
map_time = maximum map/checkpoint workload on any node
recovery_time(S) = maximum (restart + restore/recompute workload)
                   over failed active nodes, or zero if none
finish_time(S) = map_time + recovery_time(S) + output_transfer + reducer_time
expected_time = sum over S of probability(S) * finish_time(S)
```

All active worker slots and the reducer are charged for the full job duration,
including idle barriers and startup. The original node and replacement form one
continuously charged slot. Expected dollars are expected seconds times summed
hourly prices divided by 3,600. This is a provisioned-slot rental model, not an
exact cloud bill. Controller compute, storage charges, egress fees, billing
minima and price changes are excluded.

## Optimization and comparisons

Minimize expected execution time subject to a checkpoint byte budget (default
65% of map output bytes), expected rental-dollar budget, and minimum deadline
success probability (default 95%). The reference plan puts all tasks on the
fastest regular node without checkpoints. Default rental budget equals that
reference's expected cost; deadline is twice its healthy completion time.
These fixed rules are not selected by searching for Z3 wins.

Edit `budget_factors`, `deadline_factor`, `minimum_deadline_success`,
`storage_fraction` and node properties for other tradeoffs. Infeasible cases
are retained. Multiple budget factors produce constrained tradeoff points, not
the entire Pareto frontier.

| Method | Rule |
|---|---|
| `cheapest_unprotected` | All tasks on cheapest node; no checkpoints |
| `regular_unprotected` | All tasks on fastest regular node; no checkpoints |
| `regular_checkpoint_all` | Same regular placement; checkpoint all |
| `greedy_joint` | Multiple starting placements, local changes to one task's node/protection |
| `greedy_swap` | Same plus exchanges of two tasks' complete assignments |
| `sequential_dp` | Risk-aware greedy placement without protection, then exact checkpoint DP at that placement |
| `joint_dp` | Exact multidimensional DP for placement and protection |
| `z3` | Joint constraints with bounded integer-objective search |

These heuristics are experimental baselines, not claimed reproductions of named
papers. The previous simple knapsack DP cannot directly solve the expanded
model: node loads, shared loss, rental budgets and deadline success couple the
choices. The new DP retains nondominated partial states: active-node identity,
bytes, output-transfer time, and per-node map/recovery loads. It can grow rapidly.

Search methods have the same default five-second allowance per invocation;
sequential planning divides it between phases. They use the same simple initial
placements. Z3 is not seeded from greedy/DP answers. DP has a declared 10,000
candidate-state memory guard (`--dp-max-states`). Hitting it returns `state_limit`,
not a falsely optimal or approximately pruned result. A DP resource-limit loss
does not establish that all possible DP implementations lose.

Z3 uses N * M * 2 one-hot Booleans: 42 at seven tasks, 600 at 100 tasks for three
nodes. Conceptually these encode N node choices and N protection decisions.
Every SAT/UNSAT/unknown query is logged. Returned Z3 scenario times and
constraints are checked with a separate Python evaluator. Completed binary
search certifies the primary objective; optimal tied plans may differ.
Before the sweep, three-task cases are audited against exhaustive search.
Completed DP/Z3 optima are cross-checked; contradictions invalidate results.

Timeouts keep feasible incumbents when available without claiming optimality.
`infeasible` means a completed proof, not failure to find a plan. Sequential DP
can prove its fixed-placement subproblem optimal, never global joint optimality.

## Reading results

`results.csv` and `summary.json` report feasibility, timeouts, proof status,
modeled completion time, expected dollars, deadline probability, actual planning
time, execution plus planning, and estimated 20-job reuse. Full assignments are
in `results/*.json`; every task/node option and scenario is in `cases/*.json`.

The optimization deadline is an **execution** deadline. The report additionally
checks success including planning: a plan may meet the former and miss the
latter. Shared profiling is separately recorded and charged in
`including_profile_s`. Controller planning is excluded from cloud rental costs.
Twenty-job reuse is an amortization estimate, not 20 executed cloud jobs.

Compare matched cases/repeats; retain timeouts and infeasible outcomes. Report
plan quality at the allowed budget and proof status separately. Z3 beating
greedy but tying DP supports joint optimization, not unique Z3 superiority.
Wins here justify later distributed fault injection; they do not themselves
establish real-cloud speedups or novelty.

Optional owner-run checks, not executed by the assistant:

```bash
python -m unittest discover -s gossip_benchmarks/selective_replication/data_processing -p test_joint_model.py
```
