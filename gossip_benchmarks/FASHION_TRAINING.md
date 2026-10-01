# Full-dataset CPU training comparison

This experiment uses the official Fashion-MNIST split (60,000 training images,
10,000 test images) and a 784–256–128–10 MLP with 235,146 parameters. Each worker
processes 30,000 images in 118 Adam steps per epoch. This is a real learning
workload with substantially more data and work than the 256-row regression
demonstration, but remains a modest CPU model on one physical machine.

The settings stay consistent across the no-failure, early, middle and late cases:

| Arm | Ray Data | Train recovery | Checkpoints / retry budget |
| --- | --- | --- | --- |
| Ordinary | Fixed-R OFF | Standard full-group retry | Model, Adam, epoch, CPU RNG every epoch; one retry |
| Integrated | Fixed-R ON | Selective worker retry | Identical checkpoint policy and retry budget |

Both arms use two CPU Gloo workers, fixed epochs, seeded pull-based shuffle and
preserved input order. Preprocessing materializes the training/validation
datasets before training. The application contains only ordinary Data/Train
calls and saves/restores its own checkpoints. Recovery, communicator reset and
worker reuse remain inside Ray; the external harness selects policies and
injects/observes failures. No existing workload file is changed.

## Local validation

Use the compiled fork in `ray-dev`. No native rebuild is required by this change.
The dataset preparation command needs a torchvision version compatible with
your installed PyTorch. It downloads and verifies Fashion-MNIST through
torchvision and writes local Parquet plus SHA-256 input identities. Preparation
is done once and is excluded from all measured observations.

```bash
conda activate ray-dev
python gossip_benchmarks/workloads/fashion_mnist.py \
  --prepare-data --data-directory ~/ray-coverage/fashion-mnist

# First verify both controls and one middle-failure pair: four observations.
bash gossip_benchmarks/validate_fashion_training.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --epochs 4 --failure-point middle --repeats 1 --timeout-s 420

python gossip_benchmarks/plot_fashion_training.py \
  ~/ray-coverage/fashion-training-comparison.json \
  --output ~/ray-coverage/fashion-training-comparison.png
```

The timeout bounds each entire observation, including cluster startup and
correctness checks; workload durations are not calibrated or guaranteed. If a
control fails, remaining fault trials are skipped. Inspect its error/log tail
before increasing any budget. Every invocation retains its own result directory;
the top-level JSON is the latest report.

After the first run passes, use `--epochs 8 --repeats 3` with no
`--failure-point` filter. This runs 24 observations: both arms at no failure and
after committed epochs 1, 4 and 7, repeated three times. Pair order alternates.
The default remains one repetition to keep initial local validation bounded.

## Evidence and limits

Each successful observation verifies input hashes, full epoch/row/step counts,
the exact committed checkpoint delivered to every resumed rank, and worker actor
identities. Selective fallback to full-group restart is not accepted as selective
success. ON additionally requires completed Fixed-R repartition and shuffle
enrollment evidence. Final checkpoint logits on all 10,000 held-out images must
match between arms (`rtol=1e-5`, `atol=1e-7`); each fault trial must also match its
own no-failure control. This strict comparison may expose nondeterministic input
or numerical behavior; a mismatch is a failed validation, not a speed result.

The JSON embeds epoch timelines, final test accuracy, per-rank epoch training
time (including batch ingestion), rank-zero validation time, total workload and
`trainer.fit` durations. Its recovery records include:

- Failure request to invocation of all resumed train functions. Invocation is
  recorded after checkpoint hashing, before application checkpoint restoration.
- Failure request to the next committed epoch, including that epoch's useful
  computation, validation and checkpoint/report overhead.
- The next-report interval minus the corresponding no-failure epoch interval.
  This is an estimate of extra interruption time, not a pure runtime measurement.
- Workload completion-time change against the matching no-failure control.

Failure injection is gated after a committed epoch. It does not intentionally
discard a partially executed epoch; lost uncommitted optimizer work is recorded
as unknown, not inferred from wall time. Mid-epoch interruption, checkpoint
frequency tradeoffs, repeated failures and logical executor-node loss remain
separate experiments.

This comparison measures the combined integration. Fixed-R protects
preprocessing, but killing a training worker does not establish owner-loss replay
or isolate Fixed-R's recovery benefit. The driver, head, object stores and local
checkpoint storage survive. It is not spot-machine loss, actor-state recovery,
automatic checkpointing, or physical head-machine recovery. The existing matched
head-owner regression figure remains a separate experiment.

Failed and timed-out observations stay failed. Timing summaries use only pairs
that pass all checks and show the requested/valid counts. Plots read embedded
timelines from JSON and never invent missing curves or require a rerun.
