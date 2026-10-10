# Recovery experiments

Run from the repository root in the compiled `ray-dev` environment. Start with
one focused suite; older exploratory entry points have been retired.

| Question | Entry point | Details / separate plot |
| --- | --- | --- |
| Can ML training recover after abrupt worker-node loss with low overhead? | `validate_fashion_training.sh` | `FASHION_TRAINING.md`; `plot_fashion_training.py` |
| Can surviving training workers resume after input-coordinator loss? | `validate_coordinator_training.sh` | `COORDINATOR_INPUT_RESUME.md`; `plot_coordinator_training.py` |
| Does XGBoost selective retry survive two sequential worker-node losses? | `validate_train_retry.sh --mode off --scenario none --scenario worker-node` | Full-group vs selective, same application checkpoints |
| Does Fixed-R recover controlled owner loss? | `validate_fashion_owner.sh` | `FASHION_OWNER_RECOVERY.md`; `plot_fashion_owner_recovery.py` |
| What is Fixed-R's streaming learning overhead? | `validate_streaming_learning.sh` | `STREAMING_LEARNING.md`; `plot_streaming_learning.py` |
| Are runtime recovery invariants preserved? | `05_succession_correctness.py`, `06_fixed_r_correctness.py` | Keep both core correctness suites |

The Fashion worker-node default uses Fixed-R OFF in both policies and one
middle-epoch fault, plus healthy controls (four observations). Ordinary Ray
retries remain enabled. Read FASHION_TRAINING.md for the exact command and
failure-model assumptions. This is logical-node loss on one machine, with
surviving shared storage and spare executor capacity, not a cloud spot test.

## Files retained deliberately

`_support/` provides shared harness and evidence validation. In particular,
`run_fixed_r_train_comparison.py` is also the isolated subprocess supervisor
used by the active ML comparisons; `run_fixed_r_train_coverage.py` supplies
shared input preparation. They cannot be deleted as obsolete entry points.
`run_selective_xgboost_comparison.py` remains for active-collective and boundary
regressions. Runtime correctness tests under `python/ray/tests/` remain.

Core performance studies 01–04 and their `plotting/` modules remain for research
continuity. Their command-specific options are available with `--help`.
Ownership audits remain because they document where Fixed-R does and does not
apply. No workload/model code or runtime recovery implementation was removed.

## Retired experiments

The broad collaborator/entrypoint coverage and overhead runners, duplicate
worker-node shell wrappers, old training log collector, and separate Fashion
whole-workload restart comparison were removed. The latter's harness-only test
was removed with it; core checkpoint, node-loss and recovery tests remain.
The active-epoch Fashion instructions are consolidated into FASHION_TRAINING.md.
Historical scripts, plots and detailed old instructions are preserved in Git at
`f24a384e2de97132f47341f6843d5d0078323078`. Restore an individual historical file
with `git show <commit>:<path>` if it is needed to reproduce an old report.
Existing local datasets, JSON results and untracked files are untouched.

Agent source review does not replace local validation. No tests, benchmarks,
builds, lint or rendering were run by the agent for this change.
