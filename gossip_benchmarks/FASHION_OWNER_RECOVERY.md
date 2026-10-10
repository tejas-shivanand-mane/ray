# Fashion-MNIST: owner loss during preprocessing

The historical whole-workload restart comparison is retained in Git at
`f24a384e`. For current worker-node experiments, see [FASHION_TRAINING.md](FASHION_TRAINING.md).
The experiment below measures recovery coverage without whole-application retry.

This experiment targets the ownership failure that ordinary checkpoint-based
Train recovery does not address. Both arms process the official 60,000/10,000
Fashion-MNIST split, then train the existing CPU MLP for the same fixed epochs.
The application is unchanged. Fixed-R is OFF versus ON; both arms retain standard
full-group Train retry and the application's model/optimizer/RNG checkpoints.
Selective worker retry is not the comparison axis here.

## Controlled topology and failure points

Both arms place random-shuffle map owners on the head while the map tasks run
on surviving executors. OFF uses ordinary tasks submitted by head actors. ON
uses Ray's existing Fixed-R submission, copying and replay. This is explicitly
controlled ownership, not ordinary Ray Data's default placement.

The workload has four shuffle input maps. The harness orders their computation
in both controls and fault trials, and gates the selected task before it
computes partitions. Later maps wait until the failure operation finishes.

| Case | Completed map computations before injection | Selected map (zero-based) |
| --- | --- | --- |
| Early | 0 of 4 | 0 |
| Middle | 2 of 4 | 2 |
| Late | 3 of 4 | 3 |

These labels describe progress within the shuffle-map stage. They are not
fractions of total execution time, training epochs or completed shuffle
outputs. A map that has computed partitions may still be exporting them; its
outputs may not have been copied or consumed. The read, normalization and
repartition stages precede this controlled shuffle stage. Recorded task IDs
and timestamps verify the completed prefix before each failure.

The supervisor waits for the selected task, its observed head owner, and a
settled submission batch. ON additionally requires confirmed enrollment.
It kills/replaces the head processes, including GCS, using the existing Ray
local recovery harness on the main thread, then releases computation. Local
RocksDB GCS storage, the off-head driver/controller, executors and input files
survive. The replacement operation is identical in OFF and ON.

All these failures precede training. Successful ON execution subsequently
starts and completes training; it does not resume an interrupted training
epoch. This is neither physical head-machine loss nor driver/storage recovery.
Ordered computation and owner placement make this a controlled recovery
demonstration, not a default-topology throughput benchmark.

## Run locally

Use the compiled fork and the already prepared Fashion-MNIST directory in
`ray-dev`. No native rebuild or dataset download is needed for this change.

```bash
bash gossip_benchmarks/validate_fashion_owner.sh \
  --data-directory ~/ray-coverage/fashion-mnist \
  --epochs 8 --repeats 1 --timeout-s 180
```

The wrapper runs focused tests, then eight observations: one matched no-failure
pair and three owner-failure pairs. Each observation has a fresh subprocess and
cluster. The timeout includes startup and correctness checks; cleanup can add
15 seconds. Failed controls stop subsequent fault trials. `--repeats 3` gives
24 observations, alternating arm order between repetitions. Optional repeated
`--failure-point early`, `middle` or `late` arguments select a subset.

The benchmark writes `~/ray-coverage/fashion-owner-comparison.json`, preserving
each invocation's detailed result directory. It does not run plotting.

```bash
python gossip_benchmarks/plot_fashion_owner_recovery.py \
  ~/ray-coverage/fashion-owner-comparison.json \
  --output ~/ray-coverage/fashion-owner-recovery.png
```

The separate command writes PNG/PDF using embedded JSON only. The top row
shows map computations; the bottom shows training epochs. Columns separate
controls and early/middle/late faults. Dotted lines mark failure requests;
crosses remain failed/censored workloads. The existing training-node matrix
and its plot command remain separate.

## What counts as evidence

- Both no-failure arms must finish with matching full-test-set logits. Dataset,
  workload, build, epochs, placement and numeric native settings must match;
  only the Fixed-R enable flags differ.
- Fault records must prove the requested compute prefix, the exact target task
  and head owner, completed head replacement, and the surviving target executor.
- ON must record Fixed-R replay of the selected task, finish all training epochs
  with expected rows/steps, and match its ON control's logits (`rtol=1e-5`,
  `atol=1e-7`). Checkpoints remain application-provided.
- A failed OFF run counts as demonstrated owner loss only for Ray's real
  `OwnerDiedError` on the selected task's metadata object, with matching owner
  node and worker identity, observed after replacement. Timeouts, actor-RPC
  errors and unrelated failures fail the comparison criteria.
- Such an OFF sample still has `status: failed`. The top-level comparison may
  pass because the intended ownership failure and ON recovery were verified.
  There is no completion-time ratio or speedup against that failed sample.
  If OFF instead completes correctly, its measured success is retained.

The model is modest, the runs are on one physical machine, and one pair per
case is preliminary. The experiment evaluates Fixed-R, not adaptive Succession.
It does not establish a general advantage over ordinary Ray or measure the
checkpoint-versus-replay tradeoff during interrupted training.
