# Original backpressure workload on eight physical workers

## Current evidence

The supplied consumer-only JSON passed in 57.0313 seconds: 16 producer tasks
and 32 consumer tasks completed, four producers and one consumer replayed,
4096 producer rows and 32 final output blocks were validated. The user then
reported all five additional consumer-failure repetitions passed. Those five
JSON files have not been inspected. The earlier timeout remains unexplained;
the intervening commit added diagnostics, not a recovery fix.

The next profile targets the collaborator's compute configuration in
`release/nightly_tests/dataset/fixed_size_8_cpu_compute.yaml`: one m5.2xlarge
head and eight m5.2xlarge workers. It requires nine distinct Linux hosts.
It does not simulate eight hosts with local raylets and does not provision EC2.
Hardware checks establish minimum CPU/RAM, not EC2 instance identity.

## Setup and one suite command

Use dedicated hosts with the same clean commit of this fork and its source-built
native Ray installed in the configured Python environment. Pull the new Python
files on every host; this change does not itself require a native rebuild.
A fresh host still needs the existing native Fixed-R build and the benchmark's
Python dependencies. The preflight checks the source revision, import path,
native release API, and Python major/minor version, and records binary hashes.
It cannot prove an independently compiled binary matches all source changes.

The first worker runs the driver and SSH controller and remains a task executor.
All eight workers advertise eight CPUs. The head advertises zero task CPUs.
Each node has an explicit 8 GiB object store; this is a controlled recovery
profile, not a claim to reproduce the collaborator's unspecified memory settings.
The driver competes for CPU/memory on worker zero, so compare timings only against
the copy baseline produced by this same profile.

Each host needs at least 20 GiB available RAM, 8 GiB free `/dev/shm`, and 128 GiB
free temporary/spill disk. The latter is a conservative capacity check, not a
promise of peak disk use. Retained Ray logs and GCS directories need additional
space across reruns. Set `TMPDIR` in the host's agent environment if `/tmp` is
too small. Permit Ray traffic between the nine private IPs, including dynamic
ports. Worker zero needs noninteractive SSH access and known host keys for
the head and other seven workers; `ssh` entries can be SSH config aliases.
There must be no existing raylet or GCS process on these dedicated machines.
The launcher does not stop an existing deployment or install released Ray.

Copy `gossip_benchmarks/fixed_r_eight_worker_hosts.example.json` outside the
tracked checkout and fill in the nine IPs, SSH destinations, repository paths,
and absolute Python paths. Worker zero is the first entry in `workers`, and
needs no SSH destination. From that worker's checkout/environment, run:

```bash
python release/nightly_tests/dataset/streaming_recovery_multihost.py \
  --hosts /path/to/hosts.json \
  --output-dir /tmp/fixed-r-eight-worker-run1
```

The output directory must be new. The command runs fresh clusters for copy,
Fixed-R without failure, and head failure at producer-before-output,
producer-after-output, and consumer submission. It stops at the first failed
case, saves `backpressure.json`, and retains per-host agent logs in case
subdirectories. Ray session directory paths are recorded in the JSON and stay
on their respective hosts. Setup/teardown are outside benchmark timing.
`--case consumer` runs just consumer failure; use a new output directory.
The default completion timeout is 1800 seconds per case; `--timeout-s` overrides
it. This timeout includes workload execution and is not a recovery-latency SLA.

Workload arguments come directly from the original benchmark parser defaults:
128 input blocks, eight output batches per input, 128 rows per batch,
1 MiB per row, one second consumer sleep. That generates 128 GiB per case.
With one consumer task per 128 MiB block, consumer sleeps alone take about
17 minutes per case; allow substantially more than an hour for the suite.
Normal Ray block shaping remains enabled, so final block counts are validated
from observed task accounting rather than hard-coded to this estimate.

## Failure and recovery scope

All Ray processes owned by the head agent are killed; its SSH management agent,
operating system, IP address and GCS RocksDB storage survive. The agent restarts
the head at the same endpoint and verifies cluster/session identity. The driver
stays attached to worker zero with the same job ID. All eight worker node IDs
must survive, and the old head must be dead in GCS before execution resumes.
This does not exercise loss of the physical head machine or its disk, driver
failure, a second owner failure, or a managed service's head replacement policy.

The original producer, consumer, and Dataset builder are unchanged. The
controller supplies explicit runtime placement so worker zero participates in
execution as well as hosting the driver. Streaming outputs have unknown counts;
no calibration or whole-task buffering is used. As in the local experiments,
fusion and eager freeing are disabled, outputs are copied to the coordinator,
and task affinity is fixed by the recovery adapter. Later tasks after head death
are coordinator-owned. Full payload validation remains part of measured time.
This profile is a correctness run, not an unmodified-Ray performance comparison.

Agents clean up only the Ray nodes they created. Normal completion and errors
request cleanup through the SSH channel. If an agent or network connection
becomes unresponsive, cleanup cannot be confirmed; inspect the reported agent
log and the affected host before another run. Hosts/VMs themselves remain running.

## Validation status of this change

Source reviewed only: no build, test, lint, or benchmark was run by the assistant.
An additional local regression case exercises consumer head failure with the
driver's node included in the executor pool. To run it in the source environment:

```bash
python -m pytest -q --tb=long python/ray/tests/test_fixed_r_streaming_data.py \
  -k 'test_original_backpressure_runtime_head_failure and True'
```

No nine-host result is claimed until the suite has run on the user's machines.
