# Head VM loss: first experiment

For the native Fixed-R OFF/ON pilot with surviving GCS state and actual head VM
loss, see [NATIVE_PILOT.md](NATIVE_PILOT.md). The training experiment below is a
separate whole-job checkpoint-restart comparison.

Open `../Ray_GCP_Head_VM_Loss.ipynb`. The original worker-loss notebook is
unchanged, including its locally edited cells. This notebook imports these plain
Python files; edit them directly. It requires the existing five-VM state file and
never creates or deletes instances. Inventory includes older labeled clusters.
The notebook host needs authenticated `gcloud` and must remain alive during the
experiment. GCP is controlled only from that host; guests need no cloud keys.

## Inspected dependencies and placement

Inspection baseline: `b437edda83983b0d0e0c8ee70e2d3ca5c03f78fd`, including the
embedded helpers in `Ray_GCP_Experiments.ipynb` and Train V2 source.

| Dependency | Existing worker-loss harness | Head-loss experiment |
|---|---|---|
| GCS, head raylet, object store | Original head | Lost; fresh GCS on w3 |
| Driver, Train controller, checkpoint synchronization | Head; controller attached to driver node | w4, then whole job explicitly restarted |
| Recorder, checkpoint acknowledgements | Head recorder actor, memory | Per-rank append-only files and acknowledged checkpoint manifest on w4 |
| Checkpoints, models, suite logs | `/srv/ray-gcp`, NFS server on head | Separate `/srv/ray-headloss`, NFS server on w4 |
| TorchFT Lighthouse | Actor pinned to controller node; head-IP binding | w4/controller IP; recreated with the new job |
| Retained-input coordinator and dataset manager | Pinned to controller node; zero actor restarts | w4, then recreated; synthetic suffix regenerated |
| Ray Data blocks and object ownership | Distributed blocks; owners/controllers can reside on head | Old object references abandoned on full restart |
| Failure injector, cloud status, local report | Driver on head for worker service kill | Notebook host outside all five VMs |
| Training | Two of four workers, two replacement slots | w1/w2 only; w3 reserved head, w4 support |

`StepInputRounds` retains only one serialized round, not a durable iterator.
Its module explicitly requires the dataset manager, coordinator and head to
survive. Moving its actor does not make its Ray ownership or GCS state durable.
Similarly, moving Lighthouse alone does not provide Ray control-plane recovery.

## Failure and recovery contract

Both ranks stop at exactly `fault_step` committed updates. The supervisor saves
the latest acknowledged checkpoint, calls GCP `instances stop` with
`--no-graceful-shutdown`, records the operation and exact instance identity, and
waits for `TERMINATED`. Unsupported CLI flags fail before training; no service-kill
fallback is allowed. This models loss of the whole head VM and access to its
disk. It does not model a zone outage or destruction of a persistent disk.

The old head stays stopped for the entire recovery and validation. Its disk is
never attached to the replacement. The supervisor fences the old Ray services,
starts a fresh four-node cluster with w3 as head, then starts a fresh driver/job
on w4. Both methods load the same scheduled model/Adam/step checkpoint. Healthy
and faulted runs use identical placement, storage, seeds, one Torch thread,
checkpoint schedule, input hashing and evidence writes. No artificial startup
delay or implicit worker retry is used. Fixed-R remains disabled.

The fresh TorchFT manager starts at local step zero. The explicit job-restart
offset maps it to the checkpoint's global step. Retained input also restarts at
local zero over a regenerated synthetic suffix. This is benchmark-specific
random-access input reconstruction, **not durable retained-input recovery**.

This experiment establishes **checkpoint-based job restart after actual VM
loss** if its live validation passes. It does not establish worker continuation,
restoration of the previous GCS session, automatic driver recovery, peer healing
across whole-job restart, or a new Fixed-R benefit. The gate intentionally prevents
continuation; report it as *not tested*, not as a failed continuation test.

Ray supplies Train scheduling/checkpoint persistence; TorchFT supplies its
existing synchronization and peer-recovery implementation. Added code supplies
VM-loss orchestration, off-head evidence/storage, explicit checkpoint selection,
whole-job restart, synthetic input offset and validation. Both arms should roll
back `fault_step - checkpoint_step` updates per rank (19 for the smoke settings).

## Evidence, timing and operation

Results include cloud stop evidence, source/native/package identities, node
placements, per-rank input hashes, checkpoint model/optimizer hashes, final model
and Adam tensors, service journals and failure logs. Validation requires exact
update sequences, two distinct assigned worker VMs, the expected restored state,
input agreement with the healthy baseline, and final model **and optimizer**
absolute error at most `1e-5` (`rtol=0`). Missing/invalid evidence fails the suite.

The external supervisor records stop dispatch, confirmed VM termination, fencing,
new-cluster readiness, driver relaunch, observed progress beyond the pre-loss step,
and completion on one monotonic clock. New-progress latency is a polling **upper
bound** from stop dispatch, not an exact worker commit timestamp. IAP/cloud calls,
checkpoint rollback, diagnostic I/O, and restart orchestration are included. These
timings are not directly comparable to the earlier worker-service plots. This
first correctness experiment makes no speedup claim.

The original head is restarted only **between completed observations** so another
healthy/fault pair can run. The last fault leaves it stopped. `restore_worker_layout()`
explicitly restores the original head and NFS mounts for the worker notebook.
Do not run both notebooks concurrently. Existing suite names refuse execution;
`download(name)` collects without re-running. Exceptions collect partial evidence
and stop reachable experiment services; if cleanup fails a lock remains and its
errors appear in the local report. After a notebook-host crash, inspect the saved
report and cloud status, collect evidence, stop the `ray-headloss-*` services on
running VMs, and remove `/var/lib/raybench/ray-headloss-lock` on w4 only after
confirming there is no live supervisor. Interrupted suites are never replayed.

No live GCP run was performed during implementation. Start with the default smoke
suite and inspect correctness before interpreting timings.

References: [GCP VM stop](https://docs.cloud.google.com/sdk/gcloud/reference/compute/instances/stop),
[Ray GCS fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/gcs.html).
