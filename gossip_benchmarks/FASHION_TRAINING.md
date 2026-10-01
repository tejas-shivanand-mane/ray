# Full-dataset CPU training comparison

For matched owner loss during active preprocessing, see
[FASHION_OWNER_RECOVERY.md](FASHION_OWNER_RECOVERY.md). That experiment has its
own benchmark and separate plotting command. The matrix below concerns node
failures during training, which ordinary Ray may already recover from.

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

Both arms use two CPU Gloo workers on distinct logical executor nodes
(`STRICT_SPREAD`), fixed epochs, seeded pull-based shuffle and preserved input
order. Preprocessing materializes the training/validation
datasets before training. The application contains only ordinary Data/Train
calls and saves/restores its own checkpoints. Recovery, communicator reset and
worker reuse remain inside Ray; the external harness selects policies and
injects/observes failures. The Fashion-MNIST workload is unchanged by the node
failure matrix.

## Failure matrix

The default runs 14 observations per repetition: one ordinary/integrated control
pair and six fault pairs. Both node scopes use the same early/middle/late
milestones during training. With eight epochs, these follow committed epochs
1, 4 and 7; each leaves useful training work after injection.

| Scope | Injection | Surviving state |
| --- | --- | --- |
| Head-node processes | Kill the head processes, including GCS, then replace the head | Local RocksDB GCS storage, off-head driver/controller, all executors and checkpoint files |
| Worker node | Kill the executor containing rank 0, including its raylet, object store and worker processes | Head, driver/controller, three other executors and checkpoint files |

Four two-CPU logical executor nodes are started on the same physical machine.
Worker-node trials do not add a replacement node: training recovers on the
surviving executors with reduced cluster resources. The controller and workers
wait at the selected report while the main-thread supervisor operates the nodes.
This makes the requested failure point reproducible; it does not measure
mid-minibatch interruption or uninterrupted progress during head replacement.

Head trials use default dataset ownership after materialized preprocessing.
Ordinary Ray may recover, and training may continue without a worker retry.
Actual worker retention, retry and replay evidence are recorded. This differs
from the earlier matched head-owner experiment during preprocessing and does
not force the ordinary arm to fail.

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

# Both node scopes, all three failure points, plus shared controls: 14 observations.
bash gossip_benchmarks/validate_fashion_training.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --epochs 8 --repeats 1 --timeout-s 180
```

Plotting is a separate operation. The benchmark only writes JSON and result
files; it does not import or call the plotting code. Run this command after the
benchmark, including after a nonzero exit to inspect failed trials:

```bash
python gossip_benchmarks/plot_fashion_training.py \
  ~/ray-coverage/fashion-training-comparison.json \
  --output ~/ray-coverage/fashion-node-failures.png
```

The standalone plot writes PNG and PDF. Its two rows separate head-node and
worker-node failures; columns show the shared control and early/middle/late
trials. The control curves appear in both rows for reference but were only
measured once per arm per repetition. All curves come from embedded JSON
timelines; original result directories and another training run are unnecessary.

The timeout bounds each entire observation, including cluster startup and
correctness checks; workload durations are not calibrated or guaranteed. If a
control fails, remaining fault trials are skipped. Inspect its error/log tail
before increasing any budget. Every invocation retains its own result directory;
the top-level JSON is the latest report.

After the first run passes, `--epochs 8 --repeats 3` with no filters runs 42
observations. Arm order alternates between repetitions. The default remains one
repetition to keep initial local validation bounded. Optional repeated
`--failure-kind head-node` / `--failure-kind worker-node` and
`--failure-point early` / `middle` / `late` arguments select a subset. Every
invocation includes its own matched controls. The explicit `--failure-kind worker`
retains the separate worker-process-only experiment; it is never
presented as node loss. The plot also accepts older process-only JSON reports.

## Evidence and limits

Each successful observation verifies input hashes, full epoch/row/step counts,
the exact committed checkpoint delivered to every resumed rank, and worker actor
identities. Worker-node and worker-process trials require selective retry to
retain rank 1 and replace rank 0; a full-group fallback fails that check. Head
trials record the actual retained/replaced workers and accept continued training
without a retry. ON additionally requires completed Fixed-R repartition and shuffle
enrollment evidence. Final checkpoint logits on all 10,000 held-out images must
match between arms (`rtol=1e-5`, `atol=1e-7`); each fault trial must also match its
own no-failure control. This strict comparison may expose nondeterministic input
or numerical behavior; a mismatch is a failed validation, not a speed result.

The JSON embeds epoch timelines, final test accuracy, per-rank epoch training
time (including batch ingestion), rank-zero validation time, total workload and
`trainer.fit` durations. Its recovery records include:

- Failure request to invocation of all resumed train functions. Invocation is
  recorded after checkpoint hashing, before application checkpoint restoration.
  This value is null if head replacement did not require a worker retry.
- Node operation duration and proof of process exit, the selected node/worker,
  GCS node status and surviving nodes; head trials include replacement identity.
- Failure request to the next committed epoch, including that epoch's useful
  computation, validation and checkpoint/report overhead.
- The next-report interval minus the corresponding no-failure epoch interval.
  This is an estimate of extra interruption time, not a pure runtime measurement.
- Workload completion-time change against the matching no-failure control.

Failure injection is gated after a committed epoch. It does not intentionally
discard a partially executed epoch; lost uncommitted optimizer work is recorded
as unknown, not inferred from wall time. Mid-epoch interruption, checkpoint
frequency tradeoffs and repeated failures within one observation remain
separate experiments.

This comparison measures the combined integration. Fixed-R protects
preprocessing, but these training faults do not by themselves establish
owner-loss replay or isolate Fixed-R's recovery benefit. The driver and local
checkpoint storage survive all cases. It is not physical spot-machine loss, actor-state recovery,
automatic checkpointing, or physical head-machine recovery. The existing matched
head-owner regression figure remains a separate experiment.

Failed and timed-out observations stay failed. Timing summaries use only pairs
that pass all checks and show the requested/valid counts. Plots read embedded
timelines from JSON and never invent missing curves or require a rerun.
