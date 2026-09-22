# One head-machine replacement

This experimental deployment path adds an off-head supervisor to the existing
Fixed-R implementation. It has not yet been validated by powering off a physical
head machine. The earlier `--local` evaluations kill processes on one machine and
remain a different test.

The supported target is one head-machine failure while the driver, Train
controller, Data coordinator, required executors, Redis, endpoint router, and
supervisor survive. Place model checkpoints and input data on surviving shared
storage. GCS persistence does not store a model checkpoint or Ray object-store
contents. Recovery of a lost Train worker still needs application checkpoints
and the configured Train retry budget.

## Deployment prerequisites

Use a fresh, dedicated cluster with the same source build on every machine:

| Role | Example IP | Requirement |
| --- | --- | --- |
| Original head | `10.0.0.10` | May lose power and local disk |
| Replacement head | `10.0.0.11` | Initially idle; no existing Ray cluster |
| Redis | `10.0.0.20` | Surviving durable instance, persistence enabled, no eviction |
| Coordinator | `10.0.0.30` | Driver, supervisor, and required controllers survive here |
| Executor 1 | `10.0.0.31` | At least two CPUs |
| Executor 2 | `10.0.0.32` | At least two CPUs |
| Stable GCS endpoint | `10.0.0.100:6379` | Surviving TCP router forwards to the active head |

These must be separate failure domains where required, not logical nodes on one
machine. The supervisor cannot establish that from IP addresses. This first
version requires a stable IPv4 endpoint, with direct access to each head's GCS
port for identity checks. It does not manage Kubernetes, DNS, cloud VMs, Redis
replication, or network routes. Do not combine it with another automatic head
restart service. Redis loss, supervisor loss, and a second head failure require
operator intervention.

Copy `head_recovery.example.json` to an absolute configuration path such as
`/opt/ray-recovery/head.json` on both head machines and the coordinator. Replace
the example addresses and use a fresh storage namespace for each new cluster.
Keep the same namespace across the replacement. The Python `redis` dependency
must be installed in the supervisor's Ray environment. Supply Redis credentials
through `RAY_HEAD_REDIS_USERNAME` and `RAY_HEAD_REDIS_PASSWORD` on all three hosts
if the Redis service requires them; the JSON contains no credentials.

## Deployment hooks

Create the three executable scripts named in the configuration. The supervisor
runs these argv lists directly on the coordinator, in this order:

1. `fence-old-head`: use the cloud, hypervisor, or hardware management API to
   power off or isolate the original machine. Wait for authoritative confirmation
   that it cannot run or reconnect to the cluster, and keep it fenced. Exit zero
   only then. Failed ping, failed SSH, and an issued-but-unconfirmed shutdown
   request are insufficient. Use the same path even if the machine already
   appears dead. This hook must work when SSH to the head is unavailable.
2. `start-replacement SESSION_NAME`: remotely invoke the following command on
   the replacement machine, using its Ray environment and credentials. Pass the
   first script argument as `SESSION_NAME`; do not discard it.

   ```bash
   python -m ray.experimental.recovery._head_supervisor start-head \
     --config /opt/ray-recovery/head.json --node-ip 10.0.0.11 \
     --expected-session SESSION_NAME --temp-dir "$HOME/raytmp"
   ```

   The helper refuses to start if Redis does not contain that session. The
   supervisor additionally checks the restored cluster ID, session, and native
   configuration through the replacement's direct IP before switching traffic.
3. `switch-gcs-endpoint`: change the surviving router's forwarding target from
   `10.0.0.10:6379` to `10.0.0.11:6379`. Close old connections so clients reconnect.
   Return zero only after the routing change is applied.

The infrastructure-specific scripts are deliberately not filled with assumed
cloud commands. They need the actual VM/power API and endpoint routing mechanism.
They should be bounded and idempotent. There is no automatic hook retry: after
any uncertain remote result, the supervisor stops for operator inspection.

## Start and arm

On the original head, with an unused Redis namespace:

```bash
python -m ray.experimental.recovery._head_supervisor start-head \
  --config /opt/ray-recovery/head.json --node-ip 10.0.0.10 \
  --temp-dir "$HOME/raytmp"
```

This starts a zero-CPU head with external Redis, Fixed-R enabled, and a
300-second GCS reconnect allowance. The router must initially forward the stable
endpoint to this head. Start a Ray worker on each surviving machine, substituting
its own IP. Use zero CPUs on the coordinator if the benchmark/controller setup
supports that; otherwise reserve sufficient coordinator CPUs for controllers.

```bash
RAY_gcs_rpc_server_reconnect_timeout_s=300 ray start \
  --address 10.0.0.100:6379 --node-ip-address 10.0.0.31 --num-cpus 2
```

Always join through the stable endpoint, including the native driver. Pin
controllers/coordinators outside both heads. A zero-CPU head alone does not
guarantee placement of zero-CPU actors.

On the coordinator, arm the supervisor before injecting failure:

```bash
python -m ray.experimental.recovery._head_supervisor supervise \
  --config /opt/ray-recovery/head.json \
  --report "$HOME/ray-coverage/head-machine-recovery.json"
```

Wait for `Head supervisor: armed`. The default total supervision deadline and
replacement budget are both 120 seconds. For a longer observation window, opt in
with `--timeout-s 600` (or the needed duration). Recovery still has its own
120-second budget. Increase `recovery_timeout_s` and `reconnect_timeout_s`
together in the JSON if the platform takes longer to fence/start a VM; restart
the fresh cluster with the matching settings. Data recovery timeouts must also
cover detection and replacement.

Run a supported application from the coordinator using the existing external
launcher, with an explicit application deadline:

```bash
python -m ray.experimental.recovery \
  --address 10.0.0.100:6379 --driver-node-ip 10.0.0.30 \
  --timeout-s 600 --report "$HOME/ray-coverage/application.json" \
  path/to/benchmark.py BENCHMARK_ARGUMENTS
```

During active protected work, power off the original head machine using the
management interface. Do not kill only its processes, and do not restart the
application or surviving Ray workers. This is an intentionally destructive
injection for a dedicated test cluster, performed by the operator.

## Interpret the evidence

The supervisor writes phase transitions and hook output beside its JSON report.
Exit zero / `recovered` means the stable endpoint reports the original cluster
and session, a replacement head, a dead old head, and the original surviving
node IDs. Exit one is a failure; exit two means the watch deadline expired
without a failure. Native probes and hooks are bounded subprocesses; cleanup
can add a short interval after the deadline.

`recovered` is a control-plane check. The report deliberately sets
`application_recovery_validated` to false. Retain the provider's power-state
evidence and separately verify application output, replay counters, driver and
controller continuity, and checkpoint restoration when a training worker also
fails. The ordinary external launcher checks script exit only; a successful
exit alone does not prove that a protected task replayed.

The supervisor takes a non-expiring Redis claim for the namespace. After fencing
starts, the claim stays even on success, timeout, or interruption. This prevents
another run from blindly repeating promotion while a remote command could still
be executing. Do not delete that claim to retry without inspecting both heads
and routing state. A supervisor process crash before fencing also leaves its
claim for inspection. A clean no-failure exit releases only its own claim.

## Focused source regression command

The added tests cover ordering, failed fencing, identity mismatch, and survivor
identity checks using fakes; they do not power off machines or establish host
fault tolerance. They have not been run by the assistant.

```bash
python -m pytest -q python/ray/tests/test_fixed_r_head_supervisor.py
```

These changes are Python-only; they do not require a native rebuild of the
existing compatible source build.
