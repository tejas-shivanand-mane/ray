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

Defaults: 300 shard epochs, 600 fine-tuning epochs, one repetition, one setting.
Six smoke observations test healthy, single-worker and correlated worker loss.
Smoke checkpoints every 20 epochs so early process kills can exercise restoration.
The focused test adds at most twelve observations (identical plans share runs):

- Fixed primary placement: retry versus selective replication.
- Optimized checkpoint retry versus jointly selected placement/replication.
- Healthy, late single-worker loss, and late correlated loss.

This targets a shorter run, not a guaranteed two-hour completion time. Profiling,
Ray startup and SSH add overhead. One repetition supplies initial evidence, not
statistical assurance. Negative results remain visible. Correct recovery alone
is not proof of a speedup; compare matched scenario runtimes and worker occupancy.
Costs are normalized assumptions, not a GCP invoice. Selected scenarios are not
used to claim the full weighted expected-cost objective.

## Correctness and evidence

All policies use `TRAIN_DTYPE="float64"`. The training thread sets one CPU thread
and deterministic algorithms. Absolute model/optimizer tolerance remains 1e-5;
input history and metadata must match exactly. Float64 reduces rounding
sensitivity but cannot guarantee cross-hardware equality. Mismatches stop the
run and write `study/profile-failure.json` with tensor/build/CPU details.

Run cells individually. Smoke must pass before the focused test. Use new suite
names after failures or setting changes. Local files under
`ray_gcp_results/selective-vm/<run-id>/<suite>` include `profile.json`,
`search.json`, `replay.json`, `summary.json`, `jobs.json` and `manifest.json`.
Send those files for analysis. The optional download includes remote checkpoints.
Old results are never overwritten. Intermediate checkpoints unused by recovery
may be compacted after validation; `checkpoint-retention.json` records hashes.

The cleanup cell deletes only this run's VMs, boot disks and two firewall rules.
Download needed evidence first. The saved image and existing VPC/subnet survive.
No live GCP execution was available during development; local tests are not cloud
validation. The original GCP notebooks are unchanged.
