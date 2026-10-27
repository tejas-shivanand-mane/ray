# Native head-machine loss pilot

Prepared against main `c8bde84a7becb1fd81d7e9e76782701d16b62f75`.
Implementation has been inspected, **not executed or tested** by the assistant.
Open [Ray_GCP_Native_Head_VM_Loss.ipynb](../Ray_GCP_Native_Head_VM_Loss.ipynb)
for a settings-first workflow matching `Selective_Replication_GCP`: visible VM
creation/setup cells, direct `cloud.prepare()` and `cloud.run_pilot(...)` calls,
and standalone plotting. No subprocess wrapper runs the experiment.
Set `EXISTING_STATE_FILE` to reuse a cluster, or leave it `None` to prepare fresh
VMs on the default network. Edit `TASKS`, `PAYLOAD_BYTES`,
`OBSERVATION_TIMEOUT_S`, `MAX_EXPERIMENT_MINUTES`, and `OBSERVATIONS` in the first
cell. The runner records these settings and refuses changed settings on resume.
Use a new suite name after this helper update or when changing workload/matrix.
Run cells individually; skip creation/setup/preparation on an ordinary resume.
After restarting the kernel, run settings and the optional reconnect cell before
run/resume. The plot cell works independently with `RESULTS_DIR`.
The original selective and training notebooks are unchanged.

## What inspection established

- `selective_replication/gcp/launcher.py`: defaults to worker process kills;
  its controller, lock and `/srv/ray-gcp` NFS are on the head. Do not use it to
  establish head-machine recovery.
- `head_vm_loss/launcher.py`: actual ungraceful VM stop, surviving w4 NFS,
  but fresh GCS and whole-job checkpoint restart; Fixed-R disabled.
- `python/ray/experimental/recovery/_local.py`: local process loss; surviving
  RocksDB directory and same loopback endpoint. It does not lose a machine.
- `python/ray/experimental/recovery/_head_supervisor.py`: external Redis and
  stable endpoint design exists, but fencing/start/switch are deployment hooks.
- `src/ray/gcs/store_client/rocksdb_store_client.cc`: sync WAL protects against
  process restart with surviving files, not loss of the only disk copy.
- Native holders retain replay recipes; witnesses retain compact manifests in
  live raylets. They are not backups of GCS, driver, arbitrary actors, or NFS.
  The pilot never uses RocksDB or the original head disk for recovery.

## Failure model and placement

| Item | Placement / survival |
|---|---|
| Original GCS, owner actor, raylet | head VM; lost with confirmed `TERMINATED` |
| GCS persistent session/metadata | Redis on w4; AOF fsync always, local w4 disk |
| Stable GCS address | socat TCP proxy on w4 private IP, port 6379 |
| New GCS | w3, previously idle; same Redis namespace and original session |
| Notebook/cloud supervisor | your computer, outside all five VMs |
| Driver and job | w4; no relaunch/reconnect via `ray.init` after failure |
| Borrowers / executor | w1/w2; original tasks on w1, hard-pinned there |
| Native manifests/holders/witnesses | surviving raylets, R=2 W=2; aggregate admission evidence saved |
| Inputs | deterministic scalar arguments in task specs; no `ray.put` |
| Gates, attempt markers, evidence | w4-backed `/srv/ray-headloss` NFS |
| Checkpoints | none |

The stopped head's persistent disk still exists in GCP, but is not read, mounted
or reattached during recovery. This tests machine unavailability with inaccessible
head storage, not physical disk destruction, support-VM failure or zone loss.
The pilot switches the proxy only after confirming old VM termination and the
replacement's original cluster/session identity. It verifies unchanged driver/job
and surviving node IDs at completion. This is injected-fault supervision, not a
production automatic failure detector. Redis and the proxy are single failure
points outside the selected fault domain. Redis is bound only to w4's private IP;
use the existing trusted experiment VPC, not an untrusted shared network.

Five observations, one replicate each:

| Label | Native protection | GCS infrastructure | Fault |
|---|---|---|---|
| plain-healthy | OFF | memory, direct endpoint | none |
| off-healthy | OFF | Redis/proxy | none |
| on-healthy | Fixed-R | Redis/proxy | none |
| off-loss | OFF | Redis/proxy | head VM stop |
| on-loss | Fixed-R | Redis/proxy | identical head VM stop |

All arms reserve the same five VMs and use the same placements, inputs, gates and
payloads. w3 is reserved capacity even when idle; w4 also supplies NFS and driver
in the plain baseline. Thus the fifth arm estimates Redis/proxy *runtime* cost,
not the monetary cost of reserving support/replacement capacity. Native profiling
is enabled in both arms; overhead includes instrumentation. No streaming/Data or
TorchFT options are enabled. No assumption is made that ON succeeds or is faster.

By default the owner exports 16 static-return normal task ObjectRefs directly to two
borrowers. Original executions wait on per-object NFS gates. One is consumed;
15 remain unfinished when the owner/head is lost. OFF and ON receive the same
replacement GCS operation. Original gates stay closed in fault observations;
replay attempts bypass the gate based on attempt markers. This intentionally
forces recovery, adapted from benchmark 04. It is a **diagnostic workload with
instrumentation**, not a realistic document-processing throughput benchmark.
It cannot establish practical workload speedup. The actor itself is not recovered.

Correctness checks both borrowers' original ObjectIDs and full SHA-256 payloads,
plus session/job/node continuity. ON requires aggregate completed witness RPCs
before injection. This is not a per-task audit of the exact holder/witness VM
placement. Native recovery success remains an empirical question.

## Run from your existing checkout

Do not run the older worker/selective suite concurrently. These commands do not
create VMs, networks, firewall rules, or disks. They require the recorded five-VM
state file, existing image/UID/environment, and authenticated gcloud/IAP. Inspect
inventory first; a saved state file is not evidence that VMs are still running.
Preparation requires all five recorded VMs RUNNING and the image identity expected
by the old harness (`dc470375...`). It refuses mismatched images instead of silently
rebuilding. Restore/provision your experiment cluster explicitly if necessary.

```bash
cd /home/tejas/Downloads/ray
git pull --ff-only origin main
conda activate ray-dev
export STATE_FILE=/absolute/path/to/your/rayexp-...-state.json
python gossip_benchmarks/head_vm_loss/native_pilot.py inventory --state "$STATE_FILE"
python gossip_benchmarks/head_vm_loss/native_pilot.py prepare --state "$STATE_FILE"
python gossip_benchmarks/head_vm_loss/native_pilot.py run --state "$STATE_FILE" --suite native-head-v1
```

Preparation installs Redis/socat and sets up surviving NFS; budget <=28 minutes.
A run invocation uses <=28 minutes work plus <=90 seconds cleanup, with a 300-second
workload deadline per observation and a seven-minute admission reserve. Cloud
provisioning/initial package setup is not included in workload timings. Stop/start
and SSH may consume the session: completing the whole matrix in one invocation
is not guaranteed. Re-run the **run** command to skip recorded rows and continue
remaining ones; do not rerun prepare between ordinary resumes. Failed/timeout rows
stay in the matrix and are never automatically retried. A new suite name is needed
for another replicate. A running/interrupted row is preserved, not counted as a
successful completion. Unknown cloud/start outcomes abort and require inspection.

After completion, or to collect partial evidence without rerunning:

```bash
python gossip_benchmarks/head_vm_loss/native_pilot.py download --state "$STATE_FILE" --suite native-head-v1
# Replace RUN_ID with the run_id inside your state JSON.
python gossip_benchmarks/head_vm_loss/native_report.py \
  ray_gcp_results/head-vm-loss/RUN_ID/native-head-v1/native-report.json \
  --plot ray_gcp_results/head-vm-loss/RUN_ID/native-head-v1/completion.png
```

The report resides on your notebook host; detailed gates/results and Redis AOF
remain on w4. Download gathers the latter. Keep both. The point plot has no bars
or bottom annotations and explicitly labels unsuccessful observations.

## Evidence and limits

- `result.correct`, output reads, ObjectIDs and hashes; unsuccessful outcomes
  remain explicit. A pre-injection error is not a demonstrated recovery failure.
- Healthy elapsed time is reported both raw and excluding supervisor polling-gate
  wait. Comparisons require correct healthy rows and are one-replicate diagnostics.
- External monotonic stop, confirmed termination, endpoint switch, observed
  resumption and completion times. Resumption is a polling upper bound; on fast
  completion it may use completion observation as that upper bound.
- Task start/finish attempts, reexecutions, final outputs not obtained, native RPC
  counters; no checkpoint bytes. Original unfinished tasks are intentionally
  abandoned and eventually fenced, not counted as lost completed outputs.
- Whole-VM CPU seconds, interface bytes, disk I/O and evidence-directory size
  snapshots. Subtract before/after counters for healthy runs. They include system,
  SSH, NFS and instrumentation activity; they do not isolate replication traffic.
  Head final counters are unavailable after loss; never impute zero overhead.
- Five reserved e2-standard-4 VMs if the expected image/layout checks pass;
  exact instance descriptions and image/package hashes are captured.

No VM is restarted as failure cleanup. The old head is revived only before a new
observation; after the last fault it remains stopped. Remote services have finite
systemd lifetimes. Cleanup fences reachable Ray processes; failure leaves the lock
at `/var/lib/raybench/ray-headloss-lock` on w4. Inspect cloud state, local report
and services before manually releasing it. Never erase the lock while another
controller or uncertain remote operation could still be active.

Next decision: inspect the first report before repeating or extending to real
CPU/data workloads. If the enabled arm fails, locate the failed identity,
admission, endpoint, or same-ObjectID recovery check before claiming support.
