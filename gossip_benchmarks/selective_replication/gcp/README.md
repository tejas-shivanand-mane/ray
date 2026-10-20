# GCP selective replication

Open `../../Selective_Replication_GCP.ipynb`. It imports visible Python files;
the original GCP and local notebooks are unchanged. Each new notebook session
creates a fresh five-VM cluster from `ray-built-v1`, with a unique state file and
an existing network/subnet (default: `default`). No old instances or state files
are required or auto-reused. Fresh VMs do not require fresh VPCs.
Edit project, zone, image, existing network/subnet, machine type and experiment sizes in the first cell.
The notebook shows the actual gcloud create/SSH/SCP/NFS/delete commands in ordinary
functions and loops; no provisioning class or original-notebook setup is needed.
The small `node_control.py` helper starts/stops Ray, while `GCPStudy` retains the
training, fault injection and correctness logic. The notebook host needs
authenticated gcloud and SSH tools and must stay awake. Guests use
`/opt/ray-env/ray-dev/bin/python`. No Ray rebuild is needed.
A read-only CLI check selects stable or beta `compute instances stop` with
`--no-graceful-shutdown` before allocation. Worker NFS mounts persist across stops.
Names stay fixed if cells are rerun in one kernel; a new kernel creates new names.
A partial create leaves its state file for inspection/cleanup rather than silently
allocating another cluster. `provision.py` remains available for older notebook
sessions; the simplified notebook does not call it.
The editable dependency-install switch installs `scikit-learn==1.7.2` only when
missing, in the guest virtual environments. Package equality is checked across
all five VMs. No system Python is modified.

The six-observation smoke suite checks healthy, single-VM and correlated two-VM
loss with explicit retry/replication placements. It gates on model **and optimizer**
absolute error <=1e-5, exact input history, actual terminated VMs, faults hitting
active training, and a nonzero checkpoint restoration. It does not require a
favorable timing result. A short task can finish before the cloud stop takes
effect: that is missing recovery evidence, not a passing failure test.

The full matrix uses five policy families, eight price/risk/deadline settings,
healthy + early/late single/correlated failures and two held-out failure scenarios,
with five randomized repetitions. Identical plans within a case share runs.
There can be up to 1,800 observations. Preview the count and conservative runtime
limit before running; reducing editable settings also reduces claim scope.
To test checkpoint sensitivity, change `FULL_CHECKPOINT_EVERY` and use a new
`FULL` suite name before profiling/replaying the same matrix. Keep the smoke workload long enough that a VM stop can hit training.

## Contract

- Head: driver, Ray control services, input dataset, NFS checkpoints and evidence.
  w1/w2/w3: one training actor each; w4 remains idle and unavailable to the scheduler.
- Notebook host: independent cloud fault supervisor. GCP `instances stop
  --no-graceful-shutdown` targets only recorded, label- and ID-checked worker VMs.
  This bypasses GCP's optional graceful-shutdown period; it does not independently
  establish that the guest received no OS shutdown notification. The experiment
  establishes recovery from a stopped, unavailable VM, not an instantaneous
  power-cut model. A failed VM stays stopped through completion and validation; its local disk is
  never consulted during recovery. Only between observations is it restarted.
- Recovery begins after cloud `TERMINATED` evidence reaches the controller. Actor
  loss observations, request handoff, stop/confirmation duration and dispatch
  epoch restorations are separate evidence. Elapsed time includes this conservative
  external detection protocol. It is not native Ray detection latency, nor the
  earlier local process-kill timing. A correlated request is concurrent; actual
  loss skew is retained, not assumed simultaneous.
- Ray supplies actor placement/transport. Benchmark code supplies scheduling,
  duplicate execution/cancellation and checkpoint restart. Torch supplies training.
  **This is not TorchFT peer recovery, SMT synthesis, head-loss tolerance, a Spot
  preemption trace, or Fixed-R.** The finite enumerator remains a reference solver.
- Compare both fixed-placement replication vs retry and joint selection vs the
  exhaustive single-copy placement baseline. Correctness does not imply benefit.
  Final results separate correctness gates, deadline hits, completion, useful-worker
  occupancy and modeled costs. Paired bootstrap intervals with few repeats are
  exploratory and are not a multiple-comparison-corrected significance test.
- Default costs are normalized assumptions: safe=1, vulnerable=0.25 or 1, plus
  head+idle w4=2 per hour. All warm training VMs are charged through loss/completion.
  VM acquisition is not optimized. Actual stop/start, profiling, notebook idle time,
  disks and networking are not included in this policy model. Observation wall time
  including resets and a conservative five-VM-hour bound are recorded separately;
  neither is a cloud invoice. Failure probabilities are hypothetical scenarios.

## Results and interruption

Cluster metadata lives in `ray_gcp_results/<run-id>/state.json`. Results live in
`ray_gcp_results/selective-vm/<run-id>/<suite>` locally and
`/srv/ray-gcp/selective-vm/<suite>` on the head. Send `profile.json`, `search.json`,
`jobs.json`, `replay.json`, `summary.json` and `manifest.json` for analysis.
`download(name)` additionally collects checkpoint/event/control evidence; it may
be large. The editable `keep_all_checkpoints=False` default keeps final states,
states used by restarts, and the latest state from each attempt. After validation
it hashes and removes other intermediate checkpoints from that new observation;
`checkpoint-retention.json` records this. Set it to `True` to retain everything.
Execution refuses to start another observation with less than 5 GiB free.
New suite names never overwrite old results. Re-running `run_suite`
with the exact same matrix skips locally completed runs. An interrupted remote
observation is refused: collect its evidence and use a new suite name.

Only one notebook may own the cluster. Preparation rejects active suites and
outstanding selective/head-loss locks. Exceptions stop the selective driver and
leave failed VMs stopped. A notebook-host crash leaves the lock; inspect cloud
status and `journalctl -u ray-selective-driver.service` on the head, stop that
service, collect evidence, then remove `/var/lib/raybench/ray-selective-lock` only
when no supervisor is live. Start stopped recorded workers before preparing a
new session. The optional final cleanup cell deletes only this new cluster and its two firewall rules,
including remote results on the head disk; download evidence first. The saved
image and existing network/subnet are retained. Resources remain billable until explicitly stopped/deleted.

No live GCP execution was possible in the implementation environment. The smoke
suite is mandatory before interpreting the larger experiment.
