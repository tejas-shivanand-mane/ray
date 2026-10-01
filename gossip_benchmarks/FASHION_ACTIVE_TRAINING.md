# Recovery inside an unfinished training epoch

The Fashion-MNIST node matrix now accepts `--failure-timing active`. It uses
the original 60,000/10,000 raw-pixel workload and its application checkpoints.
There is no CNN feature extraction and no change to the application file.
Fault injection, observation and gating belong to the external harness; worker
replacement, communication reinitialization and retry remain Ray Train's work.

Both arms complete the same fixed epochs. Ordinary Ray uses Fixed-R OFF and
standard full-group checkpoint retry; the integrated arm uses Fixed-R ON and
selective CPU/Gloo retry. Standard checkpoint retries are enabled in both.
Preprocessing uses default ownership and finishes before training. This does
not force ordinary Ray to fail and does not demonstrate Fixed-R task replay
recovering model or optimizer state.

## What is interrupted

The harness observes the existing Adam optimizer. After a selected checkpoint
is committed, both ranks complete 59 real optimizer updates in the following
epoch and wait at a controlled gate. The supervisor then kills the selected
logical node and releases surviving workers. The application has not saved
those 59 updates. No artificial compute workload or user exception is added.
This is a fault inside an unfinished epoch, after an optimizer update; it is
not an arbitrary mid-kernel or in-flight-collective failure.

For four epochs, early/middle/late mean step 59 of epochs 2/3/4, respectively.
`--fault-after-step` can select any step from 1 to 117; there are 118 updates per
rank in each full epoch. The first epoch establishes a usable checkpoint.

- **Worker-node loss:** raylet, object store and child processes on one logical
  executor are killed. Training resumes on surviving executors from the prior
  epoch's model, Adam and per-rank RNG checkpoint. The report must show those
  uncheckpointed updates being recomputed on both ranks. Selective success
  requires retaining rank 1's actor; full-group fallback is not counted as
  selective recovery.
- **Head-process loss:** head processes, including GCS, are killed and replaced
  from surviving local RocksDB storage. Driver and executors survive. Workers
  may continue without rollback; the report distinguishes continuation from
  actual checkpoint restoration. Ordinary Ray may also continue successfully.

The driver must run node supervision on its main thread. All files, checkpoints
and logical nodes share one physical machine. This does not cover physical
machine, driver or storage loss, or automatic application checkpointing.

## Short local validation

Use the existing prepared data and compiled fork in `ray-dev`. No native rebuild
is needed. Start with the controls and one middle-epoch fault of each node kind
(six observations):

```bash
bash gossip_benchmarks/validate_fashion_training.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --epochs 4 --failure-timing active --failure-point middle \
  --repeats 1 --timeout-s 180
```

Remove `--failure-point middle` for all early/middle/late cases (14 observations).
Each observation has its own 180-second total timeout, including startup and
validation. This is a cap, not an estimated runtime. Use `--failure-kind
worker-node` for an initial four-observation worker-only check. Process-only
`--failure-kind worker` remains supported with boundary timing, not active mode.

Generate the plot separately, using only the report:

```bash
python gossip_benchmarks/plot_fashion_training.py \
  ~/ray-coverage/fashion-training-comparison.json \
  --output ~/ray-coverage/fashion-active-training.png
```

The JSON keeps exact checkpoint hashes, worker identities, committed epoch
reports and per-rank optimizer events. Validation requires matching complete
test-set predictions against the same arm's no-failure control and across arms.
The plot shows committed epochs, mid-epoch failure markers and verified repeated
updates per rank. Failed/censored trials remain failed.

Recovery metrics include failure-to-next-report time, whether a checkpoint was
restored, actors retained/replaced, completed but uncheckpointed updates, and
updates recomputed per rank. The comparison with control epoch time uses the
same committed-checkpoint origin, because failure-to-next-report in active mode
starts partway through an epoch. Gate waiting time is recorded and included in
workload timing. Recomputed updates on two DDP ranks are replica work, not twice
as many global training steps.

This extends experimental coverage of interrupted training; it is not evidence
of a new recovery guarantee or speed advantage before successful local runs.
One repetition is preliminary. Checkpoint restoration remains the application's
responsibility, and the runtime's existing selective retry support is under test.
