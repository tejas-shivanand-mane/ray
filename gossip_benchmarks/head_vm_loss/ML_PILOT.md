# Real ML training overhead and head-VM loss

Open [Ray_GCP_ML_Head_VM_Loss.ipynb](../Ray_GCP_ML_Head_VM_Loss.ipynb).
The native diagnostic notebook and all previous results are retained. Code has
been statically reviewed, not executed or tested by the assistant.

## Workload and timing contract

Two independent CPU CNN training trials classify real Fashion-MNIST images. This
is a small hyperparameter comparison (two learning rates), not distributed DDP,
Ray Train actor recovery, TorchFT, or a simulation. Each trial is a stateless
normal Ray task with one static return containing trained weight bytes and
metrics. One trial is pinned to w1 and the other to w2, with one CPU thread each.
The owner actor is on the original head; two surviving borrower actors retain
the original ObjectRefs. The owner actor itself is not recovered.

Defaults: 10,000 training images, the next 2,000 training-set images for validation,
and 2,000 official test images. Validation accuracy chooses the better trial;
test accuracy is reported separately. The model has two convolution/pooling
layers and two linear layers, trained with Adam. Seed, learning rates, batch size,
data subsets and the calibration target are visible in the first notebook cell.
Dataset sources: [publisher](https://github.com/zalandoresearch/fashion-mnist)
and [torchvision loader](https://docs.pytorch.org/vision/stable/generated/torchvision.datasets.FashionMNIST.html).
Downloads use the publisher's HTTPS mirror and torchvision's resource checksums.

Preparation measures ten seconds of optimizer iterations on each executor. It
chooses one shared fixed step count, targeting 90 seconds on the faster executor
plus 15% headroom. These are calibration predictions, not experiment results.
Every comparison uses exactly that step count; there is no per-arm time-based
extension or sleeping to reach the requested training duration.

Both completed trials must have **at least 60 seconds of measured optimizer-loop
wall time** for an overhead sample to be accepted. Timing includes minibatch
construction, forward/backward passes and optimizer updates. It excludes input
loading, model initialization, HTTP gate waits, progress reporting and evaluation.
If a trial finishes below the floor, correctness can still pass, but the row is
excluded from overhead comparisons. Increase the calibration target under a NEW
suite name; do not change steps inside a partially completed comparison.

## Matched comparisons and fault model

Each repetition contains the same five arms as the native pilot: plain healthy;
Redis/proxy with Fixed-R OFF and ON, healthy; and OFF/ON with actual head VM stop.
All arms use the same five reserved VMs, workload, placements, inputs, fixed steps,
seeds, gates, profiling and 10-second keepalive settings. This is a tuned plain
baseline, not an unmodified-default Ray configuration. The default is one
repetition; `REPEATS=2` counterbalances OFF/ON healthy order in the second repeat.
Two repeats provide only preliminary variability evidence.

At 10% of optimizer steps, both original tasks pause with model/optimizer state
in memory. The driver checks witness publication in enabled arms before signaling
readiness. Healthy observations release the gates. Fault observations stop the
original head VM, confirm TERMINATED, start a new GCS on w3 using w4's Redis, and
switch the surviving TCP proxy. Original task gates remain closed. Native replay
reruns each entire training trial from its original seed and inputs, with the same
ObjectID; no checkpoint or partial optimizer state is restored. Both replayed
tasks must finish for the fault case to pass.

The notebook controller, driver (w4), Redis/proxy/gate/NFS (w4), immutable inputs
(w4 plus executor-local copies), borrowers and native holders/witnesses survive.
No data is recovered from the original head disk. That disk remains allocated
but inaccessible to recovery. Support-VM failure, actor state, streaming returns,
`ray.put`, simultaneous worker loss, and whole-job checkpoint restart are outside
this experiment. See [native pilot details](NATIVE_PILOT.md) for the VM evidence.

## Correctness and reported measurements

The plain healthy arm establishes a deterministic reference. Both borrowers
verify weight payload hashes and original ObjectIDs. Subsequent arms compare
model and optimizer digests, steps, inputs, losses and accuracies with that same
reference. Training loss must decrease and test accuracy must meet the explicit
sanity threshold (default 0.65). A mismatch is retained as a failed observation;
it is not silently treated as numerical noise. Driver/job/session and surviving
node identities must remain unchanged after recovery.

The notebook reports two overhead measures separately:

- Healthy job time excluding the measured supervisor wait: startup, loading,
  training, evaluation and result transfer remain included. Raw elapsed time is
  also retained. The gate adjustment is an instrumented estimate, not steady-state
  application throughput.
- Actual optimizer-loop wall time of the slower trial, plus per-trial times,
  aggregate examples/second over that training interval, and summed training CPU
  seconds. This does not include all submission/protection costs, so it is not
  sufficient by itself to estimate total recovery-enablement overhead.

Fault timing includes cloud stop/replacement/control overhead. Resumption is a
controller-observed upper bound based on the first recorded replay optimizer
progress (or later returned output), not merely successful GCS startup. Total
completion, abandoned optimizer steps, replay attempts and unchanged final state
are recorded. Progress snapshots give lower bounds on CPU/work if a process dies
between reports. There are zero training checkpoint bytes.

Whole-VM CPU/network/disk counters include SSH, instrumentation and background
services. They do not isolate replication traffic. The stopped head's final
counters are unavailable. Support and replacement resources are reserved in all
arms; runtime ratios do not measure their additional monetary cost.

## Run and resume

From `/home/tejas/Downloads/ray`, pull main and restart the ray-dev kernel. Open
the ML notebook and set `EXISTING_STATE_FILE` to the cluster's state.json to reuse
it. Run settings, then section 6. That cell can revive a stopped original head
only after checking surviving services and the suite lock. It then prepares the
current helpers, inputs and calibration. For fresh VMs leave the state setting
None and run sections 1–6. The default suite is `fashion-head-v1`.

Run section 8, inspect section 9, plot, then collect section 10. A run invocation
has a 28-minute work budget and at most 90 seconds cleanup. The full five-arm
matrix may need a second invocation; rerun section 8, preserving recorded rows.
For a kernel restart during an unchanged suite, run settings and section 7 before
section 8. Do not repeat calibration or change settings during a suite. Dataset
preparation and calibration have a separate bounded preparation budget.

Download `native-report.json` and `download/evidence.tar.gz`; the report embeds
the dataset/calibration metadata, which is also saved as `ml-preparation-<suite>.json`.
Failed/timeout/below-duration cases remain visible. No automatic lock deletion or
retry of uncertain cloud mutations is introduced. Preserve these results before
stopping or deleting the VMs.
