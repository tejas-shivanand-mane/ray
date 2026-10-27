# Native head-machine loss pilot

Prepared against main `c8bde84a7becb1fd81d7e9e76782701d16b62f75`.
Implementation has been inspected, **not executed or tested** by the assistant.
Open [Ray_GCP_Native_Head_VM_Loss.ipynb](../Ray_GCP_Native_Head_VM_Loss.ipynb)
for a settings-first workflow matching `Selective_Replication_GCP`: visible VM
creation/setup cells, direct `cloud.prepare()` and `cloud.run_pilot(...)` calls,
and standalone plotting. No subprocess wrapper runs the experiment.
Set `EXISTING_STATE_FILE` to reuse a cluster, or leave it `None` to prepare fresh
VMs on the default network. For a fresh cluster, pull main, restart the kernel,
leave `EXISTING_STATE_FILE = None`, and run sections 1–6 and 8–10 in order.
Skip reconnect section 7. Never reuse a run ID after deleting its VMs.
Edit `TASKS`, `PAYLOAD_BYTES`, `KEEPALIVE_INTERVAL_MS`, `KEEPALIVE_TIMEOUT_MS`,
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
| Gate decisions / attempt registration | in-memory HTTP service on w4:8765, identical in all arms |
| Attempt markers, evidence | written locally on w4, exported as `/srv/ray-headloss` NFS |
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
Protected mode stays enabled. Each observation generates a password in an
owner-only file under `/srv/ray-headloss/credentials`, outside the downloadable
suite. The readiness probe and both heads authenticate with that password;
binding to a private IP alone is insufficient for modern Redis protected mode.
Probe output is saved in `redis_probe`; setup failures also collect a service
journal into `<label>-startup-failure.log` before cleanup when transport is available.

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
not the monetary cost of reserving support/replacement capacity. HTTP gate traffic
and evidence writes are also instrumentation present in every arm. Native profiling
is enabled in both arms; overhead includes instrumentation. No streaming/Data or
TorchFT options are enabled. No assumption is made that ON succeeds or is faster.

By default the owner exports 16 static-return normal task ObjectRefs directly to two
borrowers. Original executions poll per-object HTTP gates on w4. One is consumed;
15 remain unfinished when the owner/head is lost. OFF and ON receive the same
replacement GCS operation. Original gates stay closed in fault observations;
replay attempts bypass the gate based on serialized registration in that surviving
service. The gate does not store task outputs or implement Ray recovery. This intentionally
forces recovery, adapted from benchmark 04. It is a **diagnostic workload with
instrumentation**, not a realistic document-processing throughput benchmark.
It cannot establish practical workload speedup. The actor itself is not recovered.

Correctness checks both borrowers' original ObjectIDs and full SHA-256 payloads,
plus session/job/node continuity. ON requires aggregate completed witness RPCs
before injection. This is not a per-task audit of the exact holder/witness VM
placement. Native recovery success remains an empirical question.

### Changes after the first failed loss observations

Both earlier fault rows timed out while reading an original ObjectRef; neither
showed replay starts. Slow owner RPC failure detection is a hypothesis, not an
established cause. This revision uses 10,000 ms client keepalive interval and
10,000 ms keepalive timeout, in every arm and every launched Ray process. These
are also passed in the GCS system configuration and checked by the driver. This
is a matched tuned baseline, not an unmodified-default Ray comparison. The
borrower read remains bounded at 45 seconds and the observation at 300 seconds;
the two keepalive settings are not a guaranteed end-to-end recovery deadline.

HTTP gates remove NFS metadata caching from task release and attempt registration.
The driver and controller still exchange evidence files locally on w4. The
successful fault check additionally requires completed replay evidence for every
remaining output, with original gates never released. Healthy comparisons still
include diagnostic and startup costs, and cannot establish application benefit.

Before reset, the runner captures final GCS membership and bounded native log tails
from accessible VMs (64 KiB per file, at most 128 files per VM). These include
core-worker, raylet, GCS and worker stderr logs under each row's `diagnostics/`.
Missing/truncated logs are recorded; the stopped original head is never restarted
to obtain its logs. Download the evidence before deleting the support VM.
The old suite must not be resumed with changed helpers; the current default is
`native-head-v5`.

### IAP connection failures

The controller retries only the explicit IAP connection-creation error containing
both `Error while connecting [4003:` and `Failed to connect to port 22` for SSH/SCP.
There are at most three attempts, with two/four-second waits; all attempts and
waits share the original command timeout and session budget. No timeout, generic
SSH exit 255, remote-command error, or VM operation is automatically repeated.
Persistent connection failure still stops the suite and remains an infrastructure
error. Retries do not repair an unavailable VM, SSH service or network path.

Events are appended to local `native-transport.jsonl`, including during setup and
cleanup; per-observation events also appear in `native-report.json`. Retry delays
are not silently subtracted from observed fault timing. A failure in
`plain-healthy` before injection is not evidence about head-loss recovery.

Use the notebook's optional connection diagnostic cell after restarting the
kernel and setting `EXISTING_STATE_FILE` to the existing cluster's state file.
It reads VM status, checks SSH, and reports the support services/lock. If all VMs
are running and reachable, cleanup succeeded, and the lock is absent, use a new
suite (`native-head-v5`), rerun preparation to upload/fingerprint the new helpers,
then run the matrix. Preserve the previous report. Do not bypass a remaining lock
or cleanup error; inspect it first. This update does not require VM recreation.

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
python gossip_benchmarks/head_vm_loss/native_pilot.py run --state "$STATE_FILE" --suite native-head-v5
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
python gossip_benchmarks/head_vm_loss/native_pilot.py download --state "$STATE_FILE" --suite native-head-v5
# Replace RUN_ID with the run_id inside your state JSON.
python gossip_benchmarks/head_vm_loss/native_report.py \
  ray_gcp_results/head-vm-loss/RUN_ID/native-head-v5/native-report.json \
  --plot ray_gcp_results/head-vm-loss/RUN_ID/native-head-v5/completion.png
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
