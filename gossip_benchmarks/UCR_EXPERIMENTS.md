# UCR physical-node ML recovery experiments

This suite uses the existing matched MLP comparison on separate Slurm hosts.
Default allocation: **5 nodes × 4 allocated CPU cores**, 16 GB per node.
One head survives; two training ranks occupy separate worker hosts, each using
three Torch threads and reserving three Ray CPUs. Two other worker hosts provide
spare capacity and can also execute input preprocessing. The remaining CPU per
training host and the head's two advertised Ray CPUs permit input/control work.
This is a fixed two-rank recovery experiment, not a five-rank scaling study.
The backend accepts 4–6 hosts; use five initially.

## Setup

Use this branch's Ray build in the shared ray-dev environment on every node.
A checkout alone does not rebuild Ray native extensions. The node identity files
record the actual imported Ray location/version and Torch version; inspect them
in the smoke run. TorchFT, NumPy, PyArrow and Matplotlib must be available.
The synthetic MLP needs no downloaded dataset, manifest, scikit-learn, or GPU.
The checkout and results/checkpoint paths must be shared and writable on all hosts.
Module and Conda paths in the sbatch file come from your supplied UCR script.

```bash
cd /rhome/tmane002/work/ray
git fetch origin
git switch ucr-physical-node-experiments
```

Run a short smoke allocation first (four observations, one repeat):

```bash
sbatch --array=0 --export=ALL,REPEATS=1,STEPS=60,FAULT_STEP=49,WIDTH=512,BATCH_SIZE=32,CHECKPOINT_EVERY=30 gossip_benchmarks/ucr_training.sbatch
```

Inspect the top-level Slurm log, report JSON, and per-observation
`*-runs/*/run.log`, `cluster.json`, and `slurm/node-*/ray-start.log`.
Require completed observations, correct final models, expected restart/rollback
counts, two training ranks on different physical worker hosts, and a replacement
on a surviving host. Failed cases remain in the report; a finished runner does
not mean every sample passed. Do not launch the full sweep to work around a
failing smoke test.

Then submit the full array:

```bash
sbatch gossip_benchmarks/ucr_training.sbatch
```

Array concurrency is capped at one, so this suite never holds more than five
nodes at once. Each task requests up to three hours; the four tasks run
sequentially. Three repetitions × two methods × healthy/fault = 12 observations
per task (48 total). If jobs hit walltime, retain their partial JSON/logs and
submit a smaller fresh run; do not relabel partial runs as successes.

| Array index | Comparison | Checkpoint interval | Question |
|---|---|---:|---|
| 0 | Ordinary Ray DDP retry vs integrated TorchFT/aligned input | 30 | Does frequent durable recovery erase the benefit? |
| 1 | Same | 60 | How does the tradeoff change? |
| 2 | Same | 120 | Does avoided rollback outweigh integration overhead? |
| 3 | TorchFT indexed input vs TorchFT retained aligned input | 120 | What does our input path add beyond TorchFT? |

All use 300 steps, batch 256 per rank, MLP width 2048, failure before update
239, and **zero artificial startup sleep**. Both methods in each pair have
identical checkpoint schedules and seeded inputs/model initialization.
Failures occur 29, 59, and 119 committed updates after the latest checkpoint.
Ordinary Ray here means its conventional DDP/full-group retry path in the same
fork/build, not a separately installed upstream wheel.

For rank symmetry, optionally repeat with
`sbatch --export=ALL,FAULT_RANK=1 gossip_benchmarks/ucr_training.sbatch`.
Do not submit it concurrently if you want to preserve the five-node limit.
For a separately labelled startup sensitivity study, change
`--startup-delay-s 0` in a copy of the submission script; this is simulated
initialization overhead, not evidence of real preprocessing or model loading.

## Failure and timing semantics

Each observation creates a fresh physical cluster. Ray head and workers each
run as separate numeric Slurm steps. At the existing deterministic pre-update
gate, the driver calls `scancel --signal=KILL JOB.STEP` for the selected worker.
This kills that job step's Ray node/process tree, including its object store and
actors. The failed node is not restarted. Slurm process tracking must encompass
all Ray child processes; verify the step is gone and replacements use surviving
hosts. This simulates abrupt loss of the entire Ray worker node. It does not
power off a machine, induce a network partition, or erase shared storage.

Cleanup targets recorded steps belonging to this job only. No global
`ray stop --force`, fixed signal filenames, SSH, or heartbeat tuning is used.
Head/storage remain available. Node-local data loss is outside this shared-input
experiment. The five-node capacity is identical in both methods.

All progress/checkpoint events use recorder-arrival timestamps on the surviving
head, where the driver also runs. Cross-host monotonic timestamps are not
subtracted. Recovery is measured from kill request to acknowledgement of new
progress by both ranks; it includes cancellation latency and recorder RPCs.
Fine-grained single-host `--profile-recovery` is rejected with `--slurm`.
Startup of the physical Ray cluster is outside fit time; trainer/worker startup
and recovery are included. Per-step instrumentation is identical in both arms.

## Plots and defensible claims

Each task writes JSON plus PNG/PDF automatically under
`/rhome/tmane002/results/ucr-ARRAYJOB-INDEX/`. Four panels show:

1. Healthy completion time (normal-operation overhead).
2. Completion time with worker-node loss.
3. Kill request to new model progress.
4. Committed updates repeated after rollback.

Bars are medians with individual-run points and validated/total counts.
Timing excludes failed or correctness-invalid samples; the denominators and raw
logs keep these failures visible. Report paired improvements only when all four
observations for a repetition validate. Preserve negative results.

Final parameters must agree with the same seed's healthy baseline within the
existing 1e-5 tolerance. Controller-acknowledged checkpoints, restored steps,
restart counts and repeated updates are audited. Tiny numerical differences
across host CPU types can fail validation; investigate before changing tolerance.

The ordinary-Ray comparison measures the **combined integration**. TorchFT owns
model/optimizer state transfer; our input retention/fencing and Ray lifecycle
integration are not a new weight-transfer protocol. Array task 3 isolates input
strategy against a correct indexed baseline. Retention may be slower, as in
earlier local experiments. A positive ordinary-Ray result alone cannot attribute
the speedup specifically to our input changes.

These CPU measurements do not establish giant-LLM/GPU behavior, S3 performance,
or model-state transfer cost at GB scale. Fixed-R owner-loss reconstruction is
also a separate experiment: the old `reconstruction_hpcc.py` named in the
example submission is absent from this branch, and this ML suite does not
claim to exercise Fixed-R metadata recovery.

Source reviewed only. The agent has not executed tests, submitted Slurm jobs,
run benchmarks, or generated plots from invented measurements.
