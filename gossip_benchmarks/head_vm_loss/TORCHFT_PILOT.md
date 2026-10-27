# TorchFT with Fixed-R OFF versus ON

Owner-run entry point: `gossip_benchmarks/Ray_GCP_TorchFT_Fixed_R.ipynb`.
Nothing in this addition has been executed by the assistant: no project imports,
tests, notebook cells, calibration, training or cloud calls. Static review only.

## Question and scope

Compare exactly two configurations: real TorchFT DDP with Fixed-R disabled or
enabled. Each runs healthy and with actual original-head VM loss (four observations
per repetition). Both use the same Redis/proxy, five reserved VMs, dataset, seeds,
optimizer steps, placement, task graph, instrumentation and detection settings.
Two repetitions are supported; the second reverses configuration order. Each
28-minute invocation resumes by skipping recorded outcomes, including failures.

This is a composite ML pipeline, not an actor-recovery claim. Eight normal Ray tasks
(default) shuffle/normalize Fashion-MNIST into input shards. Their owner actor is on
the head. The training actors consume those ObjectRefs, keeping only their current
materialized shard. A task spec refers to immutable local input files; the dataset
is not replicated by Fixed-R. Shard outputs contain real normalized image arrays,
not padding. Their hashes and payload sizes are recorded when consumed.

Two **surviving actors** run one single-thread CPU CNN using actual TorchFT Manager,
DistributedDataParallel, Optimizer, Gloo and PGTransport. The synchronous full-
membership, step-aligned loop and failure-aware CPU communication hook follow the
existing `python/ray/train/v2/examples/pytorch/torchft_linear_example.py` approach.
This pilot uses TorchFT directly inside explicitly placed actors, not TorchTrainer;
it does not claim to test Ray Train actor replacement. Both actors are owned by the
surviving driver. Fixed-R can replay their head-owned input-producing **normal tasks**;
it cannot restore the training actors or the head's owner actor.

No training worker is deliberately killed. Thus this experiment tests coexistence
of TorchFT training and input-task owner recovery after head loss, not TorchFT peer
healing after training-worker loss. State-load callback counts are evidence, not a
claim that every callback represents a failed-worker recovery.

## Surviving dependencies and failure

- Original head: GCS, Raylet/object store, input-owner actor; all stop with the VM.
- w1/w2: normal input tasks, one training actor/replica on each; both VMs survive.
- w3: reserved replacement GCS host.
- w4: driver, TorchFT Lighthouse actor, Redis, GCS proxy, NFS, HTTP evidence recorder.
- Notebook host: external GCP controller.

The controller uses the existing `gcloud ... instances stop --no-graceful-shutdown`
path, confirms TERMINATED, starts a replacement GCS from off-head Redis, and changes
the proxy target. It never reattaches or reads the original head's disk. All five
VM reservations are present in both arms. Loss of support VM/Redis/Lighthouse,
network partitions, storage loss and correlated worker loss are outside this test.
The driver and input dataset must survive. Actor scheduling logic is not recreated.

After both training replicas pass 15% of the calibrated fixed step count, the driver
writes readiness evidence and **keeps training**. ON additionally checks that the
owner's full-spec witness writes completed. There are no release gates, forced
unavailable outputs, actor restarts, application task resubmission or job retries.
If OFF also completes, that is a valid negative result for added Fixed-R benefit.
If the head stops after training already finished, the observation is invalid for
this question; output correctness alone is not called successful recovery.

Failure-window classification compares controller stop-request UTC with w4 event
UTC. Clock skew and progress sampling granularity apply; timestamps are preserved
for inspection. The controller's GCS recovery polling latency is not reported as a
measured interruption of training. Input waits, progress and failures are separate.

## Correctness and measurements

The two replicas use identical initial model seeds, a fixed two-member quorum and
deterministic step-indexed batches from disjoint input shards. No membership shrink
is allowed: it would change the effective training batch and prevent a clean paired
comparison. Actor failures remain failures. TorchFT timeouts are long enough to
permit a normal input wait during GCS replacement, but still bounded.

Final model and optimizer hashes must match between replicas and the healthy OFF
reference. We also check trained-weight bytes, fixed final step, finite loss,
loss reduction, accuracy floor, input hashes, VM placement and surviving GCS/driver
identities. Exact hash discrepancies remain negative outcomes. The result captures
actual returned model metrics; an HTTP progress event alone is not job completion.

A short single-worker CNN calibration chooses common work for all observations.
This predicts compute only, not TorchFT communication or input-recovery costs. Each
replica's measured forward/backward/optimizer time must exceed 60 seconds for a
healthy overhead sample. No sleeping or per-configuration extension meets the floor.
Quorum/input wait and full job time are reported separately. Longer actual training
can require a second bounded run invocation for the matrix.

Logs include committed/attempted steps, input waits, input replay flags, task attempts,
TorchFT progress/state loads, native recovery profiles/log tails, actual VM-stop
records and final hashes. Whole-VM CPU/network/disk counters include instrumentation
and other activity; they do not isolate Fixed-R or Redis traffic. Native task-spec
and witness counters provide narrower evidence. Final head counters are unavailable
because the failed head stays stopped. Models are not periodically checkpointed to
NFS; PGTransport is TorchFT's peer-state transport. Evaluation inputs stay on disk.

## Running

1. Pull main without discarding notebook edits; restart the notebook kernel.
2. Open the new notebook. Keep a new suite name, e.g. `torchft-fixedr-head-v1`.
3. Existing cluster: set EXISTING_STATE_FILE, run settings, sections 1, 3, 4, 6.
   Section 3 starts stopped, ownership-checked recorded VMs. Section 5 is unnecessary
   if the earlier node helper setup exists. Fresh VMs: set the state path to None
   and run sections 1–6. Do not share a cluster with another active experiment.
4. Section 6 installs psutil==7.0.0 in the image's existing Python environment,
   checks TorchFT API/imports, confirms counters work, and prepares/calibrates data.
   It does not upgrade Torch, TorchFT or Ray. Missing/incompatible TorchFT fails
   before the experiment; record the preflight error rather than substituting a
   simulated implementation. Existing suite manifests must not be overwritten.
5. Run section 8. If it pauses for budget, rerun section 8 unchanged. If the kernel
   restarted, settings + section 7 reconnect first. The runner revives the original
   head only for the next observation, never as recovery from the failed one.
6. Sections 9/plot/10 inspect and download evidence. Keep failures. Optional section
   11 stops the five recorded VMs after evidence collection.

A timeout is not silently retried. Inspect a failed case and use a new suite for a
changed workload. Do not change sources/settings mid-suite or clear locks blindly.
The optional VM stop retains billable disks and evidence; it does not delete VMs.

Sources used for API review (the installed image version is recorded at preflight):
- https://meta-pytorch.org/torchft/manager.html
- https://meta-pytorch.org/torchft/assumptions_and_recommendations.html
- https://github.com/meta-pytorch/torchft
