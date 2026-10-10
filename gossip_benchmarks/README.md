# Recovery experiments

Run from the repository root in the compiled `ray-dev` environment. Start with
one focused suite; older exploratory entry points have been retired.

| Question | Entry point | Details / separate plot |
| --- | --- | --- |
| Can ML training recover after abrupt worker-node loss with low overhead? | `validate_fashion_training.sh` | `FASHION_TRAINING.md`; `plot_fashion_training.py` |
| Can surviving training workers resume after input-coordinator loss? | `validate_coordinator_training.sh` | `COORDINATOR_INPUT_RESUME.md`; `plot_coordinator_training.py` |
| Does XGBoost selective retry survive two sequential worker-node losses? | `validate_train_retry.sh --mode off --scenario none --scenario worker-node` | Full-group vs selective, same application checkpoints |
| Does Fixed-R recover controlled owner loss? | `validate_fashion_owner.sh` | `FASHION_OWNER_RECOVERY.md`; `plot_fashion_owner_recovery.py` |
| What does model checkpointing leave to reconstruct after worker loss? | `validate_streaming_learning.sh --comparison checkpoints` | `STREAMING_LEARNING.md`; `plot_streaming_learning.py` |
| Can a saved input cursor avoid repeated decoding at the same checkpoint cadence? | `validate_streaming_learning.sh --comparison input-resume` | `STREAMING_LEARNING.md`; `plot_streaming_learning.py` |
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


## Partial Train replacement: dataset lifecycle regression

The `ml-partial-data-context` branch fixes a runtime integration issue rather
than adding another performance benchmark. Partial replica replacement uses a
replacement-specific callback, keeps the surviving dataset manager and global
rank mapping, updates future locality hints, and propagates DataContext to new
actors. It does not rerun dataset factories or reset survivors' iterators.

Run from the repository root in `ray-dev` (no native rebuild):

```bash
RAY_TRAIN_V2_ENABLED=1 timeout --signal=INT --kill-after=20s 300s \
  python -m pytest -x -vv --tb=long \
  python/ray/train/v2/tests/test_data_integration.py::test_dataset_provider_cache_is_rank_specific \
  python/ray/train/v2/tests/test_data_integration.py::test_dataset_replacement_rejects_unrestored_streaming_cursor \
  python/ray/train/v2/tests/test_data_integration.py::test_dataset_partial_replacement_after_node_loss
```

Start with these focused tests, which stop at the first failure and have a
five-minute process timeout. Keep the complete traceback; the final pytest
summary omits the remote deserialization exception. Run the existing worker
replacement tests separately after these pass, rather than the full worker
suite (which includes deliberate zombie-process tests). The broader data
integration suite also requires `freezegun==1.1.0`, as pinned in the repository's
Python 3.10 test requirements; the focused tests above do not use it.

The added node-loss test kills all processes of either rank's logical worker
node, replaces that replica on surviving capacity, checks the survivor actor
and its next input row, and verifies the replacement's independent partition
(including a dataset first requested after the failure). It exercises the same
WorkerGroup replacement path used by TorchFT without importing TorchFT or
claiming model-state recovery. Head, dataset manager and storage survive.

The replacement can obtain an independently replayable partition with its
original global rank. Restoring its consumed cursor is still application work.
Coordinated streaming splits are explicitly rejected on replacement because
this change does not restore their in-flight cursor or epoch. Full-group
recovery behavior remains available. Supporting such streams and reporting
with missing replicas remain separate runtime work; this patch makes no
claim of complete end-to-end TorchFT + Ray Data recovery or speedup.

Tests have been added for local validation, not executed by the agent.
