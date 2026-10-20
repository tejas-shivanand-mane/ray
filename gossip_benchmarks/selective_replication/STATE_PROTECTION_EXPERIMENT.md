# Selective state protection: measured DAG experiment

This tests the professor's `t1 || (t2; t3)` idea using intermediate checkpoints
instead of redundant task execution. It asks two separate questions:

1. Does choosing which tasks to checkpoint, and how frequently, improve
   completion time or cost compared with a well-tuned uniform policy?
2. Does jointly choosing placement and checkpoint intervals improve on a
   simpler risk-aware method that chooses placement first?

A win on question 1 is not evidence of a win on question 2. A loss on these
small workloads does not disprove every form of selective fault tolerance.

The earlier training experiment's `checkpoint_retry` baseline already optimized
per-task intervals and placement. Here that flexibility is called
`joint_checkpoint` and compared directly with tuned uniform checkpointing on a
dependent workload. This isolates an existing mechanism; it does not introduce
a new checkpoint protocol.

The first one-repeat pilot completed 126 validated observations without an
accepted time advantage under the deadline rule. Its results and coverage
limitations are retained in
[example_results/state-protection-exploratory.md](example_results/state-protection-exploratory.md).
The revised default adds later failures; do not treat its weighted aggregates
as direct repetitions of that earlier failure distribution.

## Run and resume

Use Linux, at least three available CPU cores, and your existing Python
environment with PyTorch, NumPy and scikit-learn. No Ray build or cloud resources
are needed. From the repository root:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py
```

The default runs all four predeclared settings, three replay repetitions each,
with approximately four seconds of native compute in each parallel branch.
Each invocation stops after 28 minutes, preserving completed observations.
It prints the output directory and a resume command:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --resume /absolute/path/to/run
```

Upload **`analysis.zip`** from that directory. It includes all presets, profiles,
plans, Pareto fronts, timing results and fault evidence, without checkpoint
tensors. Partial archives are also produced after a handled interruption or
time limit. Resume uses the original source hashes and saved settings.

To run just the native-storage setting, or a quick correctness check:

```bash
python gossip_benchmarks/selective_replication/state_protection_experiment.py --preset native
python gossip_benchmarks/selective_replication/state_protection_experiment.py --smoke --preset storage
```

`--target-seconds 4` lengthens each sequential task to approximately four seconds
of compute and t1 to eight. `--repeats 5` increases timing repetitions. Use a new
output directory when changing settings. `--max-minutes 5` changes only the
per-invocation budget. Smoke results are correctness checks, not performance
evidence.

## Workload and protection

t1 and t2 start independently. t3 loads t2's completed model and fine-tunes it.
All tasks use the same digits MLP architecture and batch size; t1 has twice as
many epochs as either sequential task. Every policy executes the same updates
and must match healthy model weights, optimizer state and input history.

Protection means saving intermediate model, momentum, epoch and input-history
state to surviving storage. An unprotected task can still restart from its
original inputs or its completed parent. Every final output is durable. No
policy runs extra copies of a training task. A killed worker remains unavailable
for the rest of that observation, and work can move to a surviving worker.

The controller, original inputs and checkpoint filesystem survive all losses.
This is a CPU process-loss experiment, not VM-loss, TorchFT, distributed SGD,
or a test of losing the checkpoint store. Shared-file access does not test
network transfer savings or topology-aware backup placement.

## Predeclared settings

| Preset | Checkpoint latency | Assumed failure pattern |
| --- | --- | --- |
| native | Actual local write/flush cost only | Heterogeneous node risks |
| storage | Actual writes plus explicit 0.15 x target-seconds delay per save | Same heterogeneous risks |
| homogeneous | Same added latency as storage | Equal individual-node failure weights |
| correlated | Same added latency as storage | Greater probability of losing both spot workers |

The added latency is an experimental control, including mandatory final saves;
it is not measured remote-storage behavior. There is no artificial compute
pacing or unused model padding. The actual learning workload is identical
across presets in a suite. All failure distributions assign healthy execution
weight 0.5. Remaining weights are recorded in `search.json`; they are deliberately
stressful assumptions, not cloud failure-rate estimates.

## Strong comparisons

All methods use the same scheduling, retry behavior, hardware pool and timing
profiles. The checkpoint menu includes no intermediate checkpoints.

- **restart_only:** optimize placement; save final outputs only.
- **uniform_checkpoint:** jointly optimize placement and a shared checkpoint
  rule. Search both common epoch intervals and common fractions of each task's
  work, choosing whichever is better. Architecture and work per epoch match.
- **selective_fixed_placement:** freeze the fastest healthy final-only
  placement, then tune each task's checkpoint interval.
- **risk_aware_sequential:** freeze the placement from the best failure-aware
  uniform plan, then tune intervals independently and reoptimize dispatch order.
- **joint_checkpoint:** jointly search placements, topological dispatch orders
  and each task's interval.

The small search is exhaustive over this menu. That provides a reference for
a future SMT implementation without claiming that use of a solver is itself
an experimental contribution. Joint search contains the simpler policies, so
a modeled tie is expected whenever extra flexibility is unnecessary.

All plans and Pareto fronts obey the same assumed 95% deadline-success target.
If no plan in a family meets it, the best-effort fallback is explicitly marked
infeasible. The primary objective is expected completion time, followed by
assumed cost and worker busy time. Feasible time/cost Pareto fronts are saved at
1.25x, 1.5x and 2x the fastest healthy final-only reference. **Replay always uses
the predeclared 1.5x deadline**, not whichever case shows the largest advantage.

All three warm workers are billed, including idle time, at assumed rates of
$1/hour for safe and $0.25/hour for each spot worker. This is a fixed warm-pool
accounting model, not actual billing or an elastic provisioning experiment.

## Faults and evidence

Planning uses failures at 20%, 50%, 80% and 110% of the healthy reference duration.
Replay uses held-out 35%, 65% and 105% times. Each time tests safe loss, either spot
worker's loss, and both spot workers' correlated loss, plus a healthy control:
13 scenarios per distinct plan per repetition. All plans within a preset
receive identical wall-clock failure schedules. There is no per-policy retiming.
The reference is profiled separately per preset, so cross-preset aggregates
also reflect their different reference durations.

The additional late times were added after the first exploratory pilot found
that t3 could be protected but never resume from that checkpoint under the two
earlier replay times. A time beyond the final-only reference can still interrupt
a job with checkpoint overhead. Faster jobs may legitimately finish before it;
we never move the fault earlier for an individual policy. The original pilot
uses schema 1 and is retained separately; the revised default uses schema 2.

Actual SIGKILL exits, active versus idle fault exposure, restored epochs,
committed checkpoint bytes, completion time, worker busy time, assumed cost,
deadline attainment and correctness are retained. Checkpoint bytes count
committed files only, not partial writes killed before commit. Identical plans
share observations and are reported as identical, not independent trials.
Coverage is also reported per protected task, distinguishing no intermediate
checkpoints, un-interrupted protected tasks, interruption without restoration,
and an observed checkpoint restore. A speed difference without a restore does
not demonstrate the recovery benefit of the protected state.

An incorrect state stops the experiment. A correct but incomplete observation
is retained as a failure rather than discarded. Only complete comparisons
receive aggregate verdicts. A preliminary time advantage requires at least 5%
lower weighted elapsed time, correct completion, at least 95% weighted deadline
attainment, and no reduction in deadline attainment versus that baseline.
Three or more repetitions with positive gains in every repetition label it a
candidate advantage. These are practical screening rules, not statistical
significance tests or reliability guarantees. Cost changes and scenario-specific
losses remain visible even when the aggregate time improves.

Evidence supporting selective protection would be a repeatable improvement
over uniform checkpointing under acceptable reliability. Evidence supporting
joint planning additionally requires beating risk-aware sequential planning.
An advantage only under added latency supports a conditional hypothesis that
must later be checked with measured storage costs. Native ties, homogeneous
ties, correlated-failure losses and missed deadlines all remain in the report.
