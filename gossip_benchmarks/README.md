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


## Experimental step-aligned streaming input

Branch `ml-step-aligned-input` adds an opt-in internal `StepAlignedDataConfig`.
The coordinator retains one full round of Arrow batches. A worker requests its
rank's batch using the committed model step **after synchronous peer healing**.
Repeating an uncommitted step returns the same bytes; advancing the healed step
retires that round. Replacement fences the old consumer generation without
resetting surviving iterators or reexecuting the source. There are no durable
input or model checkpoint writes in this opt-in path.

This is a prototype with a strict contract: fixed membership, full synchronous
quorum, one batch per rank per optimizer update, and a surviving input coordinator,
Train controller, head and source storage. A supplied step is trusted as the
application's committed model step; delivery counts do not prove model commit.
It rejects stale/skipped steps, partial final rounds, and rounds over its byte
limit. Coordinator loss, whole-group checkpoint resume, epoch reset, reduced
quorum, gradient accumulation and arbitrary stochastic model state are outside
its scope. The ordinary `streaming_split` replacement guard remains in place.

The default 64 MiB limit bounds retained serialized input per dataset, not total
process memory. Arrow conversion, worker/RPC copies and the source pipeline add
memory. Central serialization and a synchronous quorum can cost throughput;
there is no measured performance result yet. This mechanism should be evaluated
before proposing it for industrial workloads.

The existing TorchFT linear example supports `--step-aligned-input`. It selects
input after synchronous quorum/healing, uses `Manager.current_step()`, and ends
with a final quorum and a single metrics report, with no model checkpoint.
Intermediate reporting across worker generations remains unresolved. See the
[TorchFT Manager contract](https://meta-pytorch.org/torchft/manager.html) for
quorum, healing and committed-step semantics.

Run the lightweight input protocol and real WorkerGroup node-loss tests first:

```bash
RAY_TRAIN_V2_ENABLED=1 timeout --signal=INT --kill-after=20s 600s \
  python -m pytest -x -vv --tb=long \
  python/ray/train/v2/tests/test_data_integration.py -k step_aligned
```

These kill either rank's entire logical node and check batch replay/advance,
survivor identity, and old-generation fencing. They supply the healed model
step explicitly; they do not establish model recovery.

Then, in an environment with the checkout's TorchFT dependencies installed:

```bash
RAY_TRAIN_V2_ENABLED=1 timeout --signal=INT --kill-after=20s 600s \
  python -m pytest -x -vv --tb=long \
  python/ray/train/v2/tests/test_torch_trainer.py::test_torchft_step_aligned_node_loss
```

The second test compares a seeded uninterrupted run with a run that loses a
whole worker node after batch delivery and before forward. It checks final
weights/bias against the reference, six committed updates, exactly one worker
restart, and no durable model checkpoints. Both failure ranks are covered.
It is a single-machine CPU/Gloo logical-node test, not a GPU/NCCL or cloud
spot-instance experiment. No native rebuild or prepared CIFAR manifest is needed.

All new tests are **unexecuted by the agent**. Passing them would establish the
specified recovery behavior, not low overhead or general production readiness.


### Node-loss follow-up: communication hook and cleanup failures

The first local TorchFT node-loss run reached the injected fault, then failed
in the surviving replica at `torchft/ddp.py: assert fut._fut`. Managed Work.wait()
records a Gloo failure and can return False without initializing that private
future. The CPU step-aligned example now returns a completed public Future after
waiting on managed work, allowing the wrapped optimizer to reject failed updates.
This is an example-local compatibility hook, not an upstream TorchFT fix or a
GPU/NCCL result. The default example path is unchanged.

A second failure occurred when Ray replaced the replica whose backward raised
and attempted to clear the result queue of the already-dead peer. Queue-clear
actor errors now feed the next worker health poll rather than becoming controller
errors. This preserves failure handling and does not silently accept an ambiguous
queue clear. The test also disables the dashboard, avoiding an unrelated missing
frontend startup delay in source checkouts.

Focused regression commands (followed by the end-to-end node test above):

```bash
RAY_TRAIN_V2_ENABLED=1 timeout --signal=INT --kill-after=20s 180s \
  python -m pytest -x -vv --tb=long \
  python/ray/train/v2/tests/test_worker_group.py::test_replace_replica_group_with_another_dead_worker \
  python/ray/train/v2/tests/test_torch_trainer.py::test_torchft_step_aligned_hook_failed_work
```

These follow-up changes have not been executed by the agent. Successful input
protocol tests (10 passed locally) do not establish end-to-end model recovery.


## Plot the input-recovery contribution

`run_step_input_experiment.py` is a controlled ablation using the already-tested
CPU TorchFT linear example. Both arms use the same peer model recovery,
synchronous quorum, failure-aware communication hook, seeds and zero durable
model checkpoints. The independent-input arm uses conventional rank-partitioned
iteration, which advances on delivery and starts over in a replacement process.
The aligned arm uses the model's healed committed step. This comparison isolates
our input contribution; it is **not** ordinary Ray full-group checkpoint recovery.

For each seed, run healthy and node-loss cases in both arms. Kill one complete
logical worker node after delivery at step 20, before forward/update, while the
head, coordinator, peer and shared storage survive. Check exact expected sample
IDs for each requested model step, final weights/bias versus the healthy aligned
reference (absolute tolerance 1e-5), agreement between final replicas, and exactly
one worker restart in fault cases. A completed run can still be numerically wrong.
Neither mismatches nor failed/timed-out observations are hidden.

```bash
RAY_TRAIN_V2_ENABLED=1 python gossip_benchmarks/run_step_input_experiment.py \
  --repeats 3 --output ~/ray-coverage/step-input-ablation.json
python gossip_benchmarks/plot_step_input_experiment.py \
  ~/ray-coverage/step-input-ablation.json \
  --output ~/ray-coverage/step-input-ablation.png
```

Use `--repeats 1` and a different output name for an initial smoke run. Use
`--failed-rank 1` with a fresh output name to exercise the other rank. Each sample
has a subprocess timeout, JSON result and log; existing reports are never
overwritten. The plot writes PNG and PDF and shows individual observations and
medians for model error, input mismatches, and end-to-end fit time. Fit time includes
worker startup, data execution, telemetry, and recovery; cluster creation is
excluded. The report also records training-loop span and raw input request events.
These request events are not a global committed-progress trace.

Evidence supporting the contribution would be: both healthy arms agree; aligned
node-loss runs have no input mismatches and match the reference; independent
node-loss runs expose wrong input/model trajectories despite peer model recovery.
If this pattern does not occur, report it rather than claiming a benefit. Three
repetitions are preliminary. This small synthetic linear workload establishes
input semantics, not model convergence or industrial throughput. The independent
arm scans/filters its own partitions, while the aligned arm centrally splits
batches; timing includes those different data paths. A realistic workload and
matched infrequent-checkpoint baseline remain separate performance work.

Scripts have been source-reviewed only; the agent has not executed experiments,
tests, compilation or plotting for this addition.

## Direct comparison with ordinary Ray

The same runner now accepts `--comparison ordinary-ray`. This uses a CPU MLP
(256 input features, three configurable-width hidden layers, 10 classes, Adam)
and deterministic synthetic classification batches. It measures training-system
behavior; it is not a claim about real-data prediction accuracy.

* Ordinary Ray: standard TorchConfig/DDP, full worker-group retry from the last
  durable model/Adam/step checkpoint. Input is directly indexed by the restored
  step, giving the baseline correct resume without decoding/skipping a prefix.
* Our integrated path: TorchFT peer model recovery, partial worker replacement,
  and commit-aligned Ray Data input. The same scheduled durable checkpoints are
  written, but a single worker loss recovers newer state from its surviving peer.

Both use the same initialization seed, model, optimizer, batches, checkpoint
interval, resources, failure point and optional simulated startup delay. Both
must match the ordinary Ray healthy reference within absolute parameter tolerance
1e-5. Final replica agreement, expected invocation counts and input IDs are also
checked. This comparison measures the combined system, including existing TorchFT
benefits; the earlier ablation separately isolates our input contribution.

Start with one repetition (four observations):

```bash
RAY_TRAIN_V2_ENABLED=1 python gossip_benchmarks/run_step_input_experiment.py \
  --comparison ordinary-ray --repeats 1 --steps 240 --batch-size 128 \
  --model-width 512 --checkpoint-every 120 --fault-step 180 \
  --startup-delay-s 0 --timeout-s 600 \
  --output ~/ray-coverage/ordinary-ray-peer-smoke.json
python gossip_benchmarks/plot_step_input_experiment.py \
  ~/ray-coverage/ordinary-ray-peer-smoke.json \
  --output ~/ray-coverage/ordinary-ray-peer-smoke.png
```

The chosen case loses a node between checkpoints: ordinary Ray should restore
step 120 and repeat about 60 committed updates per worker, while peer recovery
should retain step 180. The failure is injected after input delivery and before
forward at that step; it is not a failure during checkpoint publication.
Checkpointing occurs at steps 0 and 120; both methods report completion at 240.

After the smoke run passes, use `--repeats 3` and a fresh output name for measured
results. To simulate costly application initialization, repeat with
`--startup-delay-s 10` and a new output name. The sleep occurs once at the start
of **every worker invocation**, including initial startup and replacement, in
both methods. It does not delay model updates, failure detection, or only the
baseline. Worker delays overlap: restarting two workers does not necessarily
cost twice as much elapsed time. Plots prominently label the simulated delay;
retain the zero-delay control rather than treating simulated results as measured
startup performance. Provisioning/VM boot delay itself is not modeled.

The four-panel plot shows healthy runtime, node-loss runtime, node-loss-to-new-
progress time, and repeated committed updates. New progress means both ranks
have exceeded the pre-fault model step, including any checkpoint recomputation.
Runtime reductions are signed: negative means our path is slower. Timing plots
exclude incorrect runs and display correct/attempted counts. Pairwise speedup
and healthy overhead are reported only when all four observations are correct.
Raw events, version metadata, source hashes, checkpoint bytes and final model
arrays remain beside the JSON for audit. Model arrays are needed if revalidating
the report; the plotting script only needs the JSON.

The ordinary baseline generates its cheaply indexed synthetic input locally;
our mode additionally incurs Ray Data and retention costs. Thus a win is not
created by forcing the baseline to replay input preparation. Realistic I/O-heavy
workloads, GPUs, larger scale and different checkpoint intervals remain to be
measured. Long checkpoint intervals can increase the benefit but also increase
the baseline's rollback; always state the interval alongside any speedup.

Source review only: this new comparison and its plots have not been run by the
agent. It may reveal overhead or a recovery/reporting limitation instead of an
improvement. Do not suppress those results.
