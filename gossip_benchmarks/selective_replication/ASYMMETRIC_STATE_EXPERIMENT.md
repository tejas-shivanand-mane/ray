# A workload where selective checkpoints could help

This is a new, unexecuted experiment design, not a positive result. It tests a
specific hypothesis: protect a shorter, failure-prone training branch while
avoiding checkpoint delays on a longer, more reliable branch that determines
job completion time. It uses the existing `t1 || (t2; t3)` training graph.

The project owner runs experiments and tests. Do not run this project's
experiments, benchmarks, tests, or automatic validation commands on the owner's
behalf unless explicitly requested. Preparing and committing code is allowed.

## First run

From the repository root, using the environment with PyTorch, NumPy and
scikit-learn:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --design asymmetric --repeats 1
```

This first screen includes both actual local checkpoint costs and a controlled
0.5-second additional latency per checkpoint. All policies receive that same
latency in the controlled case. This latency is an explicit experimental input,
not a measurement of a real remote storage system.

Each invocation has a 28-minute limit and prints a resume command. Longer
training means this screen may require resuming. Use the printed directory:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --resume /absolute/path/to/run
```

Upload `analysis.zip` after completion, or after an error. It contains profiles,
every modeled candidate family, actual observations, coverage and summaries.
The first run has only one observation per plan/scenario; it is a screen, not a
repeatability claim. Do not compare its aggregates directly with older designs.

## What changes

| Task | Approximate native compute target | Placement in the matched comparisons |
| --- | ---: | --- |
| t1 | 9 seconds | spot_a, higher assumed failure risk |
| t2 | 6 seconds | safe, lower assumed failure risk |
| t3, consuming t2's model | 6 seconds | safe, lower assumed failure risk |

The 3:2:2 epoch ratio is fixed before planning. All tasks use a width-256 MLP
and the same architecture and batches; t3 fine-tunes t2's completed model.
Calibration estimates actual compute time. There is no added compute sleep or
unused model padding. Every policy performs identical training updates and
validates weights, optimizer state and input history against healthy execution.

The reliable branch has approximately 12 seconds of compute, versus 9 for t1.
That gap may let t1 checkpoint and recover while the reliable branch is still
working. Uniform checkpointing also saves the reliable branch's state, which
can delay the whole job. Selectivity can help only if avoiding those saves is
worth more than the extra recovery risk. It may still tie or lose.

Checkpoint cost and compute duration are now separate parameters:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --design asymmetric --target-seconds 12 --checkpoint-delay-s 0.5 --repeats 1
```

Here t2 and t3 target 12 seconds each, t1 targets 18, and additional checkpoint
latency stays 0.5 seconds. Native storage always adds zero artificial latency.
Use a new output directory for each setting; retain negative findings too.

## Comparisons

The default `--comparison mechanism` executes four families:

- `matched_restart`: the placement above, final outputs only, no intermediate
  checkpoints. Failed tasks can restart from original inputs or their parent.
- `matched_uniform`: exactly the same placement and dispatch order; optimize
  the shared checkpoint rule, including the option of no intermediate saves.
- `matched_selective`: exactly the same placement and dispatch order; choose
  each task's checkpoint interval independently, including no intermediate save.
- `uniform_checkpoint`: a stronger baseline that also optimizes placement and
  dispatch order. A matched-placement win can disappear against this baseline.

Uniform search includes both common epoch intervals and common fractions of
each task's work. All final task outputs remain durable. None of these policies
duplicates whole training tasks. All workers, including idle reserves, are
included in the same assumed warm-pool cost accounting.

The reference in the default report is `matched_selective`. A win over
`matched_uniform` supports selective checkpointing under fixed placement.
Beating the placement-optimized uniform baseline is stronger evidence.
Neither result establishes that joint placement/protection optimization is
necessary; a simple risk-aware policy may achieve the same thing.

All existing policy families are still planned and retained. To execute joint
planning, risk-aware sequential planning, and all controls as well, use a fresh
run:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --design asymmetric --comparison all --preset all --repeats 3
```

This is a substantially longer confirmation run. It adds equal-risk and
correlated-loss settings; repeated invocations can resume it. No setting is
selected after looking for a favorable measured result.

## Failures, deadlines and interpretation

The native/storage scenario weights are: healthy 50%, safe loss 0.5%, each
individual spot loss 24%, and correlated spot loss 1.5%. These are controlled
stress assumptions, not cloud failure-rate estimates. The two spot workers
have equal individual loss weights, so moving work to the spare spot worker
does not make its failure risk disappear.

Fault timing has two references, both frozen from profiling before selecting
plans and identical across policies:

- Planning: 25%, 55% and 85% of t1's final-only reference on spot_a, plus 80%
  and 110% of the fastest healthy final-only whole-job reference.
- Replay: held-out 40%, 80% and 95% of that t1 reference, plus 105% of the
  whole-job reference. Ordinarily this gives 17 scenarios per distinct plan
  per repetition: four failure types at four times, plus healthy execution.

This includes faults before the shorter risky branch would finish, as well as
a later fault that can exercise work prolonged by checkpoint overhead. There
is no adjustment of fault times to catch an individual policy. Idle, unreached,
and actual protected-task failures are reported separately, with restored
epochs. A speed difference without an intermediate-checkpoint restore does
not demonstrate its recovery benefit.

The measured deadline is predeclared at 1.2 times the fastest healthy final-only
reference, with a 95% weighted deadline target. Additional modeled Pareto fronts
at 1.5x and 2x remain available; the replay deadline is not chosen from gains.
Profiling is an approximation, so measured deadline failures remain failures.

The report requires at least a 5% time improvement, correct results, meeting
the target and no lower deadline attainment to flag a preliminary time advantage.
Three or more repetitions with positive gains in every repetition can support
a candidate advantage; this is still a screening rule, not a significance test.
Inspect cost, every failure scenario, and protection coverage as well.

Success only with added checkpoint latency supports a conditional mechanism
claim. Evidence for a practical workload would additionally require measured
storage costs and realistic failure assumptions. This experiment does not test
TorchFT, distributed gradient training, VM loss, network placement or loss of
the checkpoint store.
