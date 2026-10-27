# Ranked placement and selective checkpointing, without Z3

Question: does a fixed, inexpensive placement score plus selective output
checkpointing improve modeled time/cost over uniform checkpointing and random
assignment? This study can produce a negative result. It does not prove a
universal advantage or research novelty.

## Run locally

No Ray cluster, cloud access, downloads, workload execution, or Z3 dependency is
needed. The simulator uses Python 3.10's standard library. Plotting optionally
uses matplotlib. The assistant has not executed this code or its tests.

From the repository root in your existing environment:

```bash
git pull --ff-only origin main
conda activate ray-dev
python gossip_benchmarks/selective_replication/data_processing/ranked_simulation.py --output ray_gcp_results/ranked-local-v1
```

Repeat that exact Python command to resume. Or use
`gossip_benchmarks/Ranked_Recovery_Local.ipynb`, whose settings and plot are
ordinary editable cells. Do not run the CLI and notebook on the same output
directory concurrently. Change the output name when changing settings/source.
No existing notebook or experiment result is overwritten.

The default session budget is 10 minutes, checked between cases; it is not a
hard watchdog. A case finishes before a budget pause. Summary/bootstrap/archive
writing is outside that budget. Ctrl+C preserves finished cases and exports a
partial report; interrupting a case discards that incomplete case. Repeating
the command recomputes it. The saved manifest freezes settings, Python version
and source hashes. If a process is forcibly killed, confirm it stopped before
removing its `controller.lock` file. Completed cases are saved atomically.

Send `ray_gcp_results/ranked-local-v1/analysis.zip`. It includes every generated
case, complete assignments, observations, sources, manifest, summary and CSV.

## What the score means

Node risk and hourly price are inputs. Placement and output checkpoint choices
are decisions; checkpoint choice is not a static node property. The node score
also needs task duration, speed and current load to avoid putting everything
on the same attractive node.

Tasks are considered longest-first for every placement method. For a candidate
placement of the next task, let:

- `L_j`: projected uncheckpointed compute/input-transfer load on node j.
- `H = max_j L_j`: projected map completion time.
- `R = sum_j p_j * (restart_j + L_j)` over active nodes: a conservative proxy
  for expected recovery delay (the evaluator uses the expected maximum instead).
- `C = (H + R) * sum_active_hourly_rates / 3600`, including the reducer rate.
- `T_ref`, `C_ref`: healthy time and expected cost of the same case's fixed
  reference plan, all tasks unprotected on the fastest node. All node kinds are
  labelled regular here; failure probabilities and prices are individual inputs.

Choose the node minimizing:

```
score = w_time * H/T_ref + w_risk * R/T_ref + w_cost * C/C_ref
```

Default weights are (1, 1, 1), frozen before looking at results. This is a
sequential greedy score, not joint optimization or an optimality certificate.
Risk also affects the cost proxy intentionally: the weights express both
recovery-delay aversion and the rental cost of delay. We report actual modeled
time and dollars separately, not just this score.

After placement, selective checkpointing orders tasks by:

```
benefit_per_byte = [p_node * (recompute_time - restore_time)
                    - checkpoint_write_time] / output_bytes
```

Protect only positive-benefit outputs, in descending order, while they fit the
byte budget. Restart time cancels because both restore and recompute require a
replacement in this model. This local benefit ignores critical-path coupling,
so it can make worse choices than uniform checkpointing. That is measured and
retained, not repaired by an expensive optimizer.

## Controls and fairness

Eight methods:

| Placement | No checkpoints | Uniform | Selective |
|---|---|---|---|
| Uniform random node per task | random_none | random_uniform | random_selective |
| Weighted score | ranked_none | ranked_uniform | ranked_selective |
| Earliest predicted finish, ignoring risk/price | — | balanced_uniform | balanced_selective |

The three random arms share the exact same placements for each case/repetition;
there are ten random assignments per case, averaged before case-level comparison.
Ranked and balanced arms each have one deterministic plan per case. All methods
share input tasks, nodes, exposure probabilities, deadlines and budgets.

Uniform means an evenly spaced subset of task outputs, with density equal to
the storage-budget fraction and a fixed per-case random phase. Outputs that
would exceed the byte budget are skipped. It is not periodic model-state
checkpointing during training. Selective and uniform have the same maximum
byte budget, not necessarily the same bytes written. Saved bytes and plan
identities are reported. No checkpoint-all arm is treated as feasible when it
would exceed the budget.

Primary comparison: ranked_selective vs random_uniform. Ablations isolate
placement (ranked_uniform vs random_uniform), checkpointing
(random_selective vs random_uniform; ranked_selective vs ranked_uniform),
whether protection helps at all (ranked_selective vs ranked_none), and stronger
baselines (balanced_uniform and balanced_selective). Beating random alone is a
weak result and does not show the score beats ordinary load balancing.

## Prespecified 100 cases

Twenty independently seeded synthetic cases per regime:

1. Homogeneous nodes, 0.5% failure probability: a low-risk negative control.
2. Independently varied speed, price and risk.
3. Cheaper nodes have higher risk.
4. Faster nodes have higher risk.
5. Independent nodes with expensive local checkpoint writes.

Each case has three nodes and 20, 50 or 100 independent map tasks, followed by
one reducer. Compute times are log-uniform from 0.2 to 20 seconds; output sizes
are independently log-uniform from 0.1 to 32 MiB. Thus sizes do not predetermine
compute costs. Node speeds are 0.6..2 times reference, prices $0.04..0.20/hour,
risks 1..30%, except the explicit regime overrides in the visible generator.
Prices are illustrative, not provider quotes. Storage budgets are 25%, 50% or
75% of total output bytes. These distributions are hypotheses, not observations.

Do not search seeds/weights for wins and then report them as held-out results.
If tuning weights, use a separate output directory and development seed; freeze
the selected rule before running a new evaluation seed. Include all five
regimes, every loss, infeasible outcome and planning overhead.

## Evaluator and limits

This reuses `joint_model.make_case/evaluate` from the earlier study, without
calling any solver. It evaluates all eight independent node-loss subsets
exactly, rather than sampling a few lucky failure traces. A loss occurs after
map outputs/checkpoints finish but before the reducer reads them. Outputs on
the same lost node fail together. Original inputs, checkpoints and the reducer
survive. A failed active node gets one same-type replacement; there are no
recurring failures. Recovery is serial per node and parallel between nodes.
All active worker slots and the reducer are billed for the complete duration.
No shared bandwidth contention, correlated zone failure or mid-task loss is
modeled. See JOINT_SCALING.md for the evaluator details.

Expected cost budget equals the reference plan's expected cost; deadline equals
twice its healthy time; the target is 95% deadline success. These are diagnostics,
not constraints enforced by this simple heuristic. Report feasibility fractions
alongside time/cost; a fast plan that violates the cost budget is not an
unqualified win. All methods obey the hard checkpoint-byte cap.

Only local planner durations are measured. Task durations, overhead, time,
throughput implications, cloud prices and failure outcomes are synthetic.
The experiment establishes neither real application speedup nor cloud recovery.

Mean percentage improvements give every case equal weight. Random assignments
are averaged within cases first, not counted as independent workloads. Report
time, time plus measured planning, dollars, win/tie/loss counts, and per-regime
breakdowns. Bootstrap intervals resample case-level paired gains; they are
descriptive, conditional on this case generator, and not multiplicity-adjusted.
There is no claim that 100 simulated cases prove production superiority.

Optional checks for the user (not run by the assistant):

```bash
python -m unittest discover -s gossip_benchmarks/selective_replication/data_processing -p test_ranked_simulation.py
```
