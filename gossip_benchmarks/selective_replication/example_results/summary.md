# Selective replication feasibility study

Real digits training; local worker-process loss only. No Ray/TorchFT, Fixed-R, SMT, or GCP VM-loss claim. Monetary rates and finite scenario probabilities are user assumptions. This is a warm-worker experiment; dataset preparation, pool startup, cloud provisioning, and external storage costs are outside the measured interval.

## Modeled policy selection

Best means minimum modeled cost among enumerated assignments and priority orders meeting the supplied probability/deadline constraint. It does not mean an optimal adaptive schedule. A machine lease starts on first use and includes subsequent idle time until completion/loss.

| Case | Joint cost | Best baseline cost | Modeled reduction |
|---|---:|---:|---:|
| 0: price-0.25-risk-0.1-deadline-1.25 | 0.0002663 | 0.0002663 | 0.00% |
| 1: price-0.25-risk-0.1-deadline-2.0 | 0.0002268 | 0.0002268 | 0.00% |
| 2: price-0.25-risk-0.5-deadline-1.25 | 0.0006883 | 0.0006883 | 0.00% |
| 3: price-0.25-risk-0.5-deadline-2.0 | 0.0002936 | 0.0002936 | 0.00% |
| 4: price-1.0-risk-0.1-deadline-1.25 | 0.0009600 | 0.0009600 | 0.00% |
| 5: price-1.0-risk-0.1-deadline-2.0 | 0.0008104 | 0.0008104 | 0.00% |
| 6: price-1.0-risk-0.5-deadline-1.25 | 0.0012864 | 0.0012864 | 0.00% |
| 7: price-1.0-risk-0.5-deadline-2.0 | 0.0008104 | 0.0008104 | 0.00% |

All policies, profiles, scenarios, and replay observations are in local-smoke.json.

## Measured process-loss replay

Selected cases, not a representative sample. Policies producing the same plan share the same replay. Holdout correlated losses use a time excluded from optimization. Deadline hit counts below are observations, not estimates certifying the requested probability.

| Case/policies/scenario | Runs | Valid complete | Deadline hits | Median seconds [min, max] |
|---|---:|---:|---:|---:|
| 2/uniform_replication/healthy | 2 | 2 | 2 | 2.076 [2.065, 2.087] |
| 2/joint_enumeration/holdout-correlated-loss | 2 | 2 | 0 | 2.331 [2.269, 2.394] |
| 2/uniform_replication/loss-spot_a | 2 | 2 | 2 | 1.862 [1.811, 1.913] |
| 2/checkpoint_retry/loss-spot_a | 2 | 2 | 2 | 1.922 [1.883, 1.961] |
| 0/checkpoint_retry + joint_enumeration/loss-spot_a | 2 | 2 | 2 | 1.995 [1.978, 2.012] |
| 0/placement_first + greedy_replication/loss-spot_a | 2 | 2 | 2 | 1.956 [1.930, 1.983] |
| 0/placement_first + greedy_replication/holdout-correlated-loss | 2 | 2 | 0 | 2.517 [2.464, 2.569] |
| 0/placement_first + greedy_replication/healthy | 2 | 2 | 2 | 1.898 [1.876, 1.920] |
| 2/placement_first/healthy | 2 | 2 | 2 | 2.004 [1.960, 2.049] |
| 2/checkpoint_retry/holdout-correlated-loss | 2 | 2 | 2 | 1.886 [1.821, 1.951] |
| 0/uniform_replication/holdout-correlated-loss | 2 | 2 | 0 | 2.452 [2.433, 2.470] |
| 2/joint_enumeration/healthy | 2 | 2 | 2 | 2.012 [1.885, 2.138] |
| 2/placement_first/loss-spot_a | 2 | 2 | 2 | 2.023 [2.021, 2.025] |
| 0/uniform_replication/loss-spot_a | 2 | 2 | 1 | 2.093 [1.989, 2.197] |
| 2/uniform_replication/holdout-correlated-loss | 2 | 2 | 1 | 2.112 [1.880, 2.343] |
| 0/uniform_replication/healthy | 2 | 2 | 1 | 2.248 [1.999, 2.497] |
| 0/checkpoint_retry + joint_enumeration/healthy | 2 | 2 | 1 | 2.084 [1.906, 2.261] |
| 2/checkpoint_retry/healthy | 2 | 2 | 1 | 2.154 [2.079, 2.229] |
| 2/placement_first/holdout-correlated-loss | 2 | 2 | 0 | 2.313 [2.259, 2.367] |
| 2/joint_enumeration/loss-spot_a | 2 | 2 | 2 | 1.920 [1.827, 2.012] |
| 0/checkpoint_retry + joint_enumeration/holdout-correlated-loss | 2 | 2 | 1 | 2.236 [1.903, 2.569] |

Median absolute timing prediction error: 14.5%. Predicted deadline hits that missed in replay: 14/42. This is a calibration check on selected scenarios, not a reliability estimate.

Validation compares every committed model and optimizer tensor with its healthy reference using absolute tolerance 1e-5, plus exact epoch and input-order history. It also checks the selected hyperparameter trial. Events record PID/exit code, requested and actual fault time, checkpoint restoration, and one commit per trial.

A worthwhile next step requires reproducible measured improvements over the strongest baseline across more workloads and held-out faults. A modeled advantage alone is insufficient evidence for adding an SMT solver.

Recorded on 2026-10-14: 18 healthy profiling runs and 42 final replays. All committed states matched exactly. These short runs on three CPU slots of one shared host do not support a performance claim. The joint search had zero modeled cost improvement over the best baseline in all eight settings.
