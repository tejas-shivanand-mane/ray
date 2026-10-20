# GCP selective recovery: short test

Open `../../Selective_Replication_GCP.ipynb`. Settings and gcloud infrastructure
commands are visible in the notebook. Start with fresh VMs from `ray-built-v1`
on your existing network/subnet. Guests use `/opt/ray-env/ray-dev/bin/python`.

## Default failure mode

`FAILURE_MODE = "process"` sends SIGKILL to the selected worker's entire
`ray-exp-node.service` cgroup, including Ray and its training actors. It verifies
that the group is empty and the service is inactive, then checks again after the
observation. The service uses `Restart=no`; Ray restarts only between observations.
The VM stays running, so repeated experiments avoid VM stop/start delays.
The notebook must remain awake to supervise failures.

This simulates loss of worker processes and in-memory state, not full machine
unavailability. Guest OS, SSH, disks and NFS mounts remain available. Reports say
`failure_mode=process` and `PROCESSES_KILLED`, never verified VM loss. Set the mode
to `vm` for actual GCP stops; that mode still requires cloud termination evidence.
The head, driver, input data and NFS checkpoint storage survive in both modes.
Neither mode tests TorchFT peer recovery, Fixed-R or SMT synthesis.

## Short comparison

Defaults: shard A uses 600 epochs, shard B 900, and fine-tuning 300. The
unequal shard durations expose placement/protection choices without synthetic
sleep delays. One shared profile (one repeat) supports both mechanism controls
and comparison. The planner enumerates checkpoint intervals of 100 epochs or
final-only saving, as well as placement and one/two copies. Final-only saving
means restarting from zero after a mid-task loss.

Every policy family receives the same checkpoint choices, deadline/reliability
requirement, and objective: minimize expected summed worker busy time. This is
an occupancy proxy, not measured CPU utilization or monetary cost. The existing
modeled lease-cost objective remains available. Fixed placement still optimizes
backup additions; placement-first fixes healthy primary placement/order before
optimizing protection. Neither baseline is forced to replicate unnecessarily.

The notebook runs at most **10 observations**:

- Two explicit retry controls: active single loss and active correlated loss.
  Checkpoint restoration and verified kills are required before comparison.
- Four policies: optimized checkpoint retry, fixed-placement replication,
  placement-first, and joint enumeration.
- Two matched comparison scenarios: healthy and early correlated loss.
  Identical executable plans share runs. Single-loss policy comparisons and
  extra repetitions are deliberately outside this short pilot.

The work budget is **28 minutes from the first cloud command**, including
provisioning, profiling, enumeration and replay. Pauses between cells count.
Cloud command timeouts use the remaining budget, and each remote controller
has a systemd runtime limit. Another 90 seconds is reserved for controller
fencing/lock release. Expiry preserves completed observations and marks a
partial comparison invalid/incomplete; it does not promise all runs complete.
Cleanup remains available after expiry. **VMs remain billable until cleanup.**

The two predeclared planning cases differ only in deadline (1.15x and 1.5x the
profiled ideal time); the pilot replays case 0. Both modeled cases, ties and
infeasible outcomes remain visible. One repetition is preliminary evidence.
An offline check using extrapolated older timings selected different shard
placements but predicted only a small improvement and a tie with optimized
checkpoint retry; this is not new measured evidence. Do not tune cases after
seeing replay outcomes or claim weighted reliability from this scenario subset.

Recovery starts when Ray reports actor loss, while independent process/VM kill
verification still gates final validity. Busy-time accounting stops at detected
loss rather than delayed external confirmation. The simulator does not model
cloud injection/detection delay; compare predictions with measured results.

## Correctness and evidence

All policies use `TRAIN_DTYPE="float64"`. The training thread sets one CPU thread
and deterministic algorithms. Absolute model/optimizer tolerance remains 1e-5;
input history and metadata must match exactly. Float64 reduces rounding
sensitivity but cannot guarantee cross-hardware equality. Mismatches stop the
run and write `study/profile-failure.json` with tensor/build/CPU details.

Run cells individually. The two mechanism controls must pass before comparison. Use new suite
names after failures or setting changes. Local files under
`ray_gcp_results/selective-vm/<run-id>/<suite>` include `profile.json`,
`search.json`, `replay.json`, `summary.json`, `jobs.json` and `manifest.json`.
Send those files for analysis. The optional download includes remote checkpoints.
Old results are never overwritten. Intermediate checkpoints unused by recovery
may be compacted after validation; `checkpoint-retention.json` records hashes.

The cleanup cell deletes only this run's VMs, boot disks and two firewall rules.
Download needed evidence first. The saved image and existing VPC/subnet survive.
No live GCP execution was available during development; local tests are not cloud
validation. Reprofile on fresh instances after pulling these changes.

If a controller is interrupted, keep the existing kernel/cluster. The supervisor
retries failed SSH service-status queries rather than treating them as service
exit. Missing status after a confirmed exit saves the driver journal locally.
`cloud.archive_interrupted(suite, job_id)` explicitly moves an unsuccessful
attempt's control files and checkpoints into an archive before retrying. It
refuses active controllers and completed results. Then rerun `run_suite` with the
same matrix: completed local observations are skipped. An archived retry is
recorded locally as `<job-id>-retry.json`; it must not be concealed in analysis.

Read-only result polling retries SSH exit 255 and timeouts up to four attempts
with bounded backoff. Successful rows record `supervisor_transport_retries` so
communication delays are visible. Remote program errors are not retried, and
mutation commands are not automatically replayed. Before archiving an interrupted
attempt, all experiment Ray services are stopped to fence surviving training
actors. A retry uses a fresh output-directory suffix while retaining the logical
matrix ID; prior retry records and remote evidence remain archived.
