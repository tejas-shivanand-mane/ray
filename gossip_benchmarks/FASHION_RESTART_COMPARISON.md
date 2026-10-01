# Does recovery beat restarting the application?

This comparison measures time from the original trial start through verified
completion. It gives ordinary Ray one automatic whole-application restart after
verified head-owner loss. The restarted application uses the same repaired
cluster and surviving driver, not a newly created cluster. Ray workers and OS
caches may remain warm. Standard full-group Train retries stay enabled in both
arms. Fixed-R ON uses its task replay path, with no application restart.

The budget is per trial, including both OFF attempts. No-failure controls have
one application invocation in each arm. Fault trials inject only once; the OFF
restart is not subjected to a second failure. The failed first attempt remains
failed in the JSON. Only actual successful completion produces a timing ratio.

## Workloads

- **Raw pixels:** the existing Fashion-MNIST MLP, for a quick comparison with the
  earlier results. It may show little or no practical benefit from Fixed-R.
- **Frozen image features:** pretrained MobileNet V3 Small processes all 60,000
  training images before the controlled shuffle stage. It then processes the
  10,000 validation images and trains a 576–256–128–10 MLP. Inference uses CPU
  tasks, batches of 64 and the official ImageNet-weight transforms. This is real
  feature extraction, not repeated dummy work or artificial compute delays.

The feature workload follows the [TorchVision weights and transforms](https://docs.pytorch.org/vision/main/models/generated/torchvision.models.mobilenet_v3_small.html).
Grayscale images are expanded to RGB, resized to 256 and center-cropped to 224.
The frozen backbone is not trained; the MLP uses the existing application's
model, Adam, epoch and per-rank RNG checkpoint policy. The original pixel
workload changes only to accept an optional input width, defaulting to 784.

Weights and dataset downloads occur once, outside measurements. Weight hashes,
dataset hashes, source and native-extension identities are checked. Training
features are not persisted between application attempts: restarting reruns
their extraction. In-memory read-only model caches do not constitute actor
state and can be rebuilt from the surviving weight file.

This intentionally compares whole-application restart, not application-specific
stage retries or durably checkpointed feature tables. Such alternatives may be
competitive and need a separate comparison. Image feature extraction makes
discarded work substantial; it does not turn these into training-epoch faults.

## Failure scope

Both arms and controls use the same explicitly controlled head ownership and
ordered computation of four shuffle maps. Early/middle/late faults follow 0,
2 and 3 completed shuffle-map computations. For the feature workload, all
60,000 training-image features have already been computed at every fault point.
The report records that completed work and verifies its timestamp precedes loss.

Head processes and the map owners are killed and the head is externally
replaced. GCS RocksDB disk, the off-head driver/controller, executors, original
input files and weight files survive on one physical machine. This is not
default ownership, physical-machine failure, driver recovery or storage loss.

OFF must demonstrate `OwnerDiedError` for the exact selected task output before
its restart is allowed. ON must demonstrate replay of that task and complete
correctly. Final all-test-set logits must match each arm's control and each
other. A timeout, unrelated failure or prediction mismatch fails the comparison.

## Local commands

Use the compiled fork and the existing prepared Fashion-MNIST directory in
`ray-dev`. No native rebuild is needed. For the substantial feature workload,
prepare weights once:

```bash
python gossip_benchmarks/workloads/fashion_features.py \
  --prepare-weights --feature-directory ~/ray-coverage/fashion-features
```

Run both controls and all early/middle/late pairs:

```bash
bash gossip_benchmarks/validate_fashion_restart.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --feature-directory ~/ray-coverage/fashion-features \
  --epochs 8 --repeats 1 --timeout-s 600
```

This runs focused checks and eight trials, with up to eleven application
invocations. A 600-second cap bounds each entire trial; cleanup can add 15
seconds. This is a cap, not a measured runtime estimate. For a smaller first
screen, add `--failure-point late` for four trials. To measure the original
pixel workload instead, omit `--feature-directory` and use `--timeout-s 180`.
Three repetitions mean 24 trials, alternating arm order between repetitions.

The JSON is `~/ray-coverage/fashion-restart-comparison.json`. Each invocation
also keeps its own result directory, including individual application attempts.
Plotting remains a separate operation:

```bash
python gossip_benchmarks/plot_fashion_restart.py \
  ~/ray-coverage/fashion-restart-comparison.json \
  --output ~/ray-coverage/fashion-restart-comparison.png
```

The PNG/PDF show total time to verified completion and training progress on the
original trial clock, including restart gaps. Bars use only valid matched pairs;
individual timings and failed/censored trials remain visible. Earlier attempts
are never relabeled as successful. Startup, all attempted work, correctness
probes and cleanup are included in the primary wall-clock metric; workload and
training durations remain separately recorded per attempt. Unmatched or failed
pairs never generate a speedup.

A negative ON-versus-OFF percentage means Fixed-R finished sooner in that pair;
a positive percentage means restart finished sooner. Report no-failure overhead
alongside fault-case benefit. One pair is preliminary and does not establish
variance, a break-even failure rate or a general advantage. Expensive feature
extraction is still preprocessing; recovery during sustained training and
checkpoint-versus-replay comparisons remain separate objectives.
