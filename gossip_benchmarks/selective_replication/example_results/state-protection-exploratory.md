# Selective state checkpoints: exploratory results

Code: `abf7e385cc7b86427fa17933d6c008d20992bd89`.

One observation per plan/scenario, nine scenarios per distinct plan, four predeclared settings. Real digits training and local worker SIGKILL; no cloud or TorchFT. Target compute: one second per sequential task and two seconds for t1 before profiling variation. These are exploratory results, not a statistical conclusion. The default runnable experiment uses three repetitions.

A time advantage is not accepted if weighted deadline attainment drops below 95% or below the comparator. The weights are assumed failure probabilities, not measured reliability.

| Setting | Method | Elapsed s | Busy s | Assumed cost | Deadline fraction | Active faults | Restored attempts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| native | restart_only | 2.048 | 3.894 | 0.000796 | 0.8300 | 6 | 0 |
| native | uniform_checkpoint | 2.129 | 4.121 | 0.000837 | 0.9500 | 6 | 5 |
| native | selective_fixed_placement | 2.354 | 4.041 | 0.000906 | 0.6200 | 6 | 4 |
| native | risk_aware_sequential | 2.153 | 4.185 | 0.000846 | 0.9500 | 6 | 4 |
| native | joint_checkpoint | 1.993 | 3.804 | 0.000772 | 0.9450 | 6 | 4 |
| storage | restart_only | 2.751 | 4.951 | 0.001059 | 0.7200 | 6 | 0 |
| storage | uniform_checkpoint | 2.751 | 4.951 | 0.001059 | 0.7200 | 6 | 0 |
| storage | selective_fixed_placement | 3.335 | 5.493 | 0.001275 | 0.5050 | 6 | 2 |
| storage | risk_aware_sequential | 2.893 | 5.171 | 0.001112 | 0.7200 | 6 | 2 |
| storage | joint_checkpoint | 2.955 | 5.044 | 0.001132 | 0.7250 | 6 | 0 |
| homogeneous | restart_only | 2.594 | 4.820 | 0.000950 | 0.8000 | 6 | 0 |
| homogeneous | uniform_checkpoint | 2.594 | 4.820 | 0.000950 | 0.8000 | 6 | 0 |
| homogeneous | risk_aware_sequential | 2.594 | 4.820 | 0.000950 | 0.8000 | 6 | 0 |
| homogeneous | selective_fixed_placement | 2.792 | 5.191 | 0.001008 | 0.7250 | 6 | 1 |
| homogeneous | joint_checkpoint | 2.792 | 5.191 | 0.001008 | 0.7250 | 6 | 1 |
| correlated | restart_only | 2.756 | 4.432 | 0.000998 | 0.7850 | 6 | 0 |
| correlated | uniform_checkpoint | 2.756 | 4.432 | 0.000998 | 0.7850 | 6 | 0 |
| correlated | selective_fixed_placement | 3.186 | 5.187 | 0.001170 | 0.6450 | 5 | 4 |
| correlated | risk_aware_sequential | 2.883 | 4.642 | 0.001062 | 0.6500 | 6 | 4 |
| correlated | joint_checkpoint | 2.883 | 4.642 | 0.001062 | 0.6500 | 6 | 4 |

All 126 measured observations completed with validated final state.

No setting produced an accepted time improvement under the predeclared rule.
The native result traded faster average completion for slightly lower deadline
attainment. The other settings were slower than the uniform policy, and none
of their joint plans met the 95% target in replay. The profiling-based
predictions therefore did not establish the requested reliability on held-out
failure times. This does not establish that selective protection is generally
ineffective, nor does it support a joint-planning advantage.

There is a specific coverage limitation: the storage preset's joint plan
protected t3, but none of its replays restored that checkpoint. The original
35% and 65% fault times did not demonstrate that later stage's backup benefit.
The revised default adds a 105% reference-time replay fault (and a separate
110% planning fault) plus per-protected-task exposure reporting. This original
schema-1 pilot is retained unchanged; the new schema-2 timing distribution
needs a new run. A later time can hit training prolonged by checkpoint overhead;
jobs finishing before it remain valid completed observations.

A separate [late-failure coverage probe](state-protection-late-probe.json)
reused the original storage plan with a fault at 105% of the reference duration.
It killed t3, restored epoch 23, and completed with zero model/optimizer state
error. This verifies that the new late time can exercise the missing recovery
path. It is not a policy comparison or a replacement for the original results.

## Joint versus uniform and sequential

| Setting | Versus uniform: time gain | Verdict | Versus sequential: time gain | Verdict |
| --- | ---: | --- | ---: | --- |
| native | 6.38% | no_claim_deadline_or_correctness | 7.42% | no_claim_deadline_or_correctness |
| storage | -7.43% | no_claim_deadline_or_correctness | -2.13% | no_claim_deadline_or_correctness |
| homogeneous | -7.66% | no_claim_deadline_or_correctness | -7.66% | no_claim_deadline_or_correctness |
| correlated | -4.62% | no_claim_deadline_or_correctness | 0.00% | identical_plan |

Positive gain means lower elapsed time. `no_claim_deadline_or_correctness` rejects the speed claim under the predeclared rule; inspect the deadline fractions above. A `preliminary_time_advantage` requires replication before treating it as repeatable evidence.

Native adds no artificial delay. The other settings add 0.15 seconds per checkpoint, including final outputs. All use identical training updates. Failure times are fixed across policies within each setting; profiles determine a separate reference duration per setting. Do not infer real cloud failure probabilities or storage costs from these controls.

The earlier checkpoint-retry baseline already supported selective intervals. This experiment isolates that mechanism against uniform saves and adds the t2-to-t3 training dependency. It does not test network locality, backup destination selection, loss of shared storage, or TorchFT replicated training.

See [state-protection-exploratory.json](state-protection-exploratory.json) for configuration, profiles, plans, Pareto fronts, per-scenario outcomes and fault/restore evidence.
