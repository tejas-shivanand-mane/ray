# Revised benchmark validation

Implementation validation only. The constructed controls do not demonstrate practical benefit. The dependent-workload smoke uses one repeat, reduced epochs, and overlapped repository hook installation; do not interpret its timing differences as performance findings.

| Control | Retry median seconds | Replica median seconds | Retry / replica deadline hits |
|---|---:|---:|---:|
| positive-late-loss | 12.401 | 7.741 | 0/3 ; 3/3 |
| negative-healthy | 7.491 | 7.590 | 3/3 ; 3/3 |

All 12 controls and 54 dependent-workflow replays passed state checks; maximum workflow tensor error 0.0. All seven control criteria passed.
Workflow smoke: 51/54 deadline hits, 54/66 injected losses hit active scheduled tasks, 20 checkpoint-restored attempts.

The workflow trains two shard models, averages their weights, then fine-tunes. Seven policy labels share six distinct plans for this profile. Every planning scenario and both holdout scenarios were replayed. Model/optimizer tensors, input histories, and final states were checked with absolute tolerance 1e-5.

Twenty regression tests pass, including analytic positive/negative controls, dependency ordering, cancellation fencing, independent fault injection, accounting, and exact shard-average-finetune restoration. Helper and notebook syntax was checked for Python 3.10. Repository pre-commit checks pass.

Raw configs, measured control results, workflow profiles, selected plans, and replay evidence are in `audit-validation.json`. Run `Selective_Replication_Audit.ipynb` for a new local study. Old experiment results are preserved.
