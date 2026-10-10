# ML worker-node recovery

Start here for spot-worker recovery. The default comparison uses Fixed-R OFF
in both arms: ordinary full-group Train retry versus selective Train retry.
Both allow one retry and restore application checkpoints. Selective retry keeps
healthy worker actors, but still rolls their model/Adam/RNG state back to the
committed checkpoint. It does not preserve uncheckpointed optimizer updates.

## Failure model

Two CPU/Gloo training workers run on separate logical executor nodes. At a
synchronized optimizer boundary inside an unfinished epoch, the supervisor
abruptly kills rank 0's entire logical node: raylet, object store and child
processes. No interruption warning or graceful checkpoint is provided.

The head, driver/controller, shared input and checkpoint storage survive.
Four executor nodes are initially available; replacement actors use surviving
spare capacity. The test verifies process death, GCS node death, replacement
placement, checkpoint delivery, training progress, and the final live-node set.
This models abrupt spot-worker loss but is NOT an actual cloud VM termination.
Local disk loss, delayed provisioning, simultaneous/repeated PyTorch node loss,
GPU/NCCL recovery and arbitrary in-flight collective failures are not covered.
Input and checkpoints must live outside the failed worker's ephemeral disk in a
real deployment. The local shared directory models that surviving storage.

## First local run

Use the existing prepared Fashion-MNIST data and compiled fork. No native
changes or rebuild are required by this change. Run from the repository root:

```bash
conda activate ray-dev
bash gossip_benchmarks/validate_fashion_training.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --comparison retry --failure-kind worker-node \
  --failure-point middle --failure-timing active \
  --epochs 4 --repeats 1 --timeout-s 420 \
  --output ~/ray-coverage/worker-node-training.json
```

The wrapper runs focused regressions, then four observations: full/selective
healthy controls followed by full/selective worker-node failures. Each fault
follows 59 real Adam updates per rank in epoch 3, with epoch 2 checkpointed.
Failed controls stop fault trials. Each observation has its own 420-second
limit including startup and validation; this is not a whole-suite time limit.
The workload uses all 60,000 training and 10,000 test images, four epochs and
the existing 235,146-parameter MLP. It is a modest CPU learning workload.

Plot only after the report is saved:

```bash
python gossip_benchmarks/plot_fashion_training.py \
  ~/ray-coverage/worker-node-training.json \
  --output ~/ray-coverage/worker-node-training.png
```

For regressions without a workload run:

```bash
RAY_TRAIN_V2_ENABLED=1 python -m pytest -q \
  python/ray/tests/test_train_selective_retry.py \
  python/ray/tests/test_fashion_training_comparison.py
```

## What counts as success

- Every requested epoch commits exactly once, with the expected rows/updates.
- The replacement receives the exact committed model/Adam/epoch/RNG checkpoint.
- Both ranks repeat the interrupted 59 updates and then continue training.
- Selective recovery retains rank 1's actor; full-group fallback fails that arm's
  selective-success assertion even if training completes.
- All final predictions match the corresponding healthy control within the
  existing numerical tolerance, and the two policy results agree.
- Only the selected node is lost; head, driver/job and other nodes survive.

Compare healthy-run workload time, faulted workload time, time to resumed train
functions, time to the next committed epoch, retained/replaced actors and
repeated updates. Time to the next report includes remaining training,
validation and checkpoint work; it is not pure recovery latency. One pair is a
correctness smoke experiment, not evidence of a speedup or negligible overhead.

After this passes, add early/late points with repeated --failure-point options,
then --repeats 3. Diagnose recovery-time components before changing the runtime.
For XGBoost, validate_train_retry.sh --mode off --scenario none --scenario
worker-node retains the existing two sequential node-loss comparison.

## Historical comparison

--comparison integrated restores the old OFF/full versus ON/selective arms.
Choose head-node and boundary timing explicitly for the old head-process cases.
Those cases preserve local GCS storage and do not establish head-machine
resilience. Fixed-R protects eligible stateless work; worker model recovery in
this experiment comes from Ray Train and application checkpoints.

See COORDINATOR_INPUT_RESUME.md for the separate CIFAR input-coordinator protocol
and FASHION_OWNER_RECOVERY.md for the explicitly controlled owner-loss study.
