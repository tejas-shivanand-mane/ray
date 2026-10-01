# CPU learning with streaming input

This workload trains a ResNet-18 classifier on actual CIFAR-10 images. Encoded
PNG images are read, decoded and normalized lazily through Ray Data every epoch.
There is no pre-training materialization or feature cache. The 32x32 model uses
a 3x3 stride-1 input convolution and no initial max pool; all ResNet layers are
trained from scratch with Adam. No pretrained weights are downloaded.

This follows the image-classification pattern of Ray's existing
`release/train_tests/benchmark` (Ray Data transforms, Torch batches, DDP and
application checkpoints), using a local CPU application and an accessible small
dataset. It does not reproduce that benchmark's ImageNet scale or accuracy.
Model/data APIs: [torchvision ResNet-18](https://docs.pytorch.org/vision/stable/models/generated/torchvision.models.resnet18.html)
and [CIFAR-10](https://docs.pytorch.org/vision/0.18/generated/torchvision.datasets.CIFAR10.html).

## Comparison and scope

Both arms enable one standard full-group Ray Train retry and save application
model, optimizer, epoch and per-rank CPU RNG checkpoints every epoch. Fixed-R is
OFF versus ON; selective retry is not part of this comparison. The external
harness operates failures and configures recovery. No new runtime recovery
capability is implemented by this workload.

The first run uses 2,048 training images and 512 held-out images, sampled without
replacement using a recorded seed from the separate official splits, four
epochs and two CPU training workers. At batch size 32 that is 32 updates per rank
per epoch. The default failure is after update 16 in epoch 3, with epoch 2 as the
last checkpoint. Early/middle/late at four epochs mean epochs 2/3/4, respectively.
This is a short correctness/recovery experiment, not evidence of convergence,
production performance, or full-dataset accuracy.

- **Head-node:** replaces logical head processes, including GCS, using surviving
  local RocksDB. The driver/controller, training workers, executors and storage
  survive. Ordinary Ray may continue successfully. ON replay is reported only
  when recovery counters actually show it.
- **Worker-node:** kills a training worker's logical node, including raylet and
  object store. The standard Train checkpoint-retry path remains enabled in both
  arms. This can expose Fixed-R's unsupported executor/coordinator-loss cases;
  success is not promised and failure is not relabeled as success.
- Both ranks pause after real optimizer updates for injection. It is not an
  arbitrary in-flight collective/kernel failure or physical-machine loss.
- Data ownership remains the ordinary ownership of the workload in the OFF arm.
  The ON adapter still introduces its existing helper-owner topology. We do not
  force OFF owners onto the head.

## Local commands

Use the existing compiled fork and compatible CPU torch/torchvision in `ray-dev`.
These changes do not require a new native build relative to the working main.
The branch is unvalidated until these commands are run locally.

```bash
git fetch origin
git switch fixed-r-streaming-learning
git pull --ff-only
conda activate ray-dev

python gossip_benchmarks/workloads/cifar_streaming.py \
  --prepare-data --data-directory ~/ray-coverage/cifar-streaming

bash gossip_benchmarks/validate_streaming_learning.sh \
  --data-directory ~/ray-coverage/cifar-streaming
```

Preparation downloads the CIFAR-10 archive through torchvision and writes the
recorded subset. Download/preparation is outside the timed experiment. Reuse the
directory on subsequent runs; preparation refuses to overwrite an existing
manifest. Use a new directory and `--train-rows`/`--validation-rows` to select a
larger dataset, keeping the training count divisible by two worker batches.

The validation command first runs focused tests, then four observations:
OFF/ON no-failure controls and OFF/ON middle-training head loss. Each observation
has a 300-second cap, including cluster startup, workload, final model check and
cleanup. Four caps sum to 20 minutes, plus tests and timeout-cleanup overhead;
this is a budget, not a measured duration. No duration has been measured yet.
If controls fail or do not demonstrate streaming overlap, fault cases are
skipped and the partial report is retained.

For only the initial two controls, add `--controls-only`. For both node kinds
and all failure points, explicitly add:

```bash
--failure-kind head-node --failure-kind worker-node \
--failure-point early --failure-point middle --failure-point late
```

That expanded matrix has 14 observations per repetition. Do not start it until
the short case is checked. `--repeats N` retains every trial and alternates arm
order between repetitions; `--timeout-s` changes the per-observation cap.

Generate plots separately, without running training or needing original result
directories:

```bash
python gossip_benchmarks/plot_streaming_learning.py \
  ~/ray-coverage/streaming-learning-comparison.json \
  --output ~/ray-coverage/streaming-learning.png
```

Both PNG and PDF are written. The top row shows committed epochs; the bottom
shows measured decode completions and batch deliveries, including repeated
work. Delivery is recorded before computation and is not a completed optimizer
update. The active-fault evidence separately records completed and repeated
optimizer updates. Failed, timed-out and unrun observations remain visible.
Timeout stop markers use the parent-observed stop and include cleanup; they are
not completed workload timings.

## Evidence and interpretation

The full report embeds producer events, batch/update events, committed epoch
reports, dataset execution metrics, source/native identities and fault evidence.
No original result directories are needed to replot it.

- Each committed epoch must consume every selected training sample exactly once
  across the two ranks, with the expected rows and optimizer updates per rank.
  Fingerprints of the actual normalized image tensors and labels must match
  the prepared inputs, including after replay/retry.
- A retry must receive the exact committed checkpoint on both ranks. The shared
  active-step harness checks rollback/recomputation and actual worker identities.
- The final persisted model is independently evaluated on the selected held-out
  images and checked against its reported accuracy.
- Producer completion must occur between optimizer updates in every epoch.
  Otherwise validation fails: a lazy plan alone is not proof of overlap.
  Timestamps are comparable because all logical nodes use one physical machine.
- `streaming_overlap.decode_calls_started_after_fault_in_interrupted_epoch`
  distinguishes additional data work in the interrupted epoch from production
  only in later epochs. A zero count must not be described as an in-flight data
  recovery demonstration. The gate does not guarantee an active protected task.
- ON must show task enrollment. `fixed_r_recovered_tasks` reports actual replay;
  zero replay is allowed and means completion did not demonstrate that mechanism.
- Ray's streaming splitter does not guarantee identical batch assignment/order.
  Identical final weights or control predictions are not claimed. The report
  retains training losses, held-out accuracy and between-arm/control accuracy
  differences; short-run accuracy is not a convergence equivalence test.

Transforms are deterministic decode/normalization with no random augmentation.
Telemetry is written separately from task outputs, so timestamps are not part
of replayed data. Both arms use the same two-data-CPU budget, 4 MiB object-store
scheduling budget, block sizing and prefetch settings. The memory budget is a
scheduler target, not a hard physical memory limit. Local telemetry costs are
included in both workloads; this instrumented run is not a minimal-overhead
measurement. Streaming overhead optimization remains outside this change.
