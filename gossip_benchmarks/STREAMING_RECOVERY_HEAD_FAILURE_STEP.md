# Backpressure workload across head replacement

The priority is head failure support, not phase profiling or optimization. No
profiling changes were made. Base: `f48678ade3c3ea4c27c47eec5d99c026daebdb18`.
The user reported all 24 Data tests passing and supplied successful four-mode
smoke and 1 GiB runs. Those runs killed only a separate owner worker and kept
the head/GCS alive; they are not evidence of head failure recovery.

## Failure model and components

The new `fixed_r_head_failure` mode uses the controlled version of the
collaborator's fast-producer/slow-consumer workload. It retains the validated
finite-count physical map chain, original payload generation and consumer sleep,
and copied-output Fixed-R adapter. It is still an adaptation: arbitrary public
Dataset plans, dynamic output counts, and the 5,000-worker benchmark are not
supported by this step.

The local Linux harness creates four logical nodes: a zero-CPU head hosting GCS
and the protected owner actors, a surviving driver/coordinator node, and two
surviving task executors. More executor nodes can be selected. Separate loopback
IPs make the driver attach to the coordinator raylet instead of the head.
Inputs, copied outputs, the Data stats actor, and the failure gate stay on the
coordinator. Holders/witnesses and task executors must survive.

At the existing deterministic gate, two initial producers and one consumer have
enrolled. The controller kills **all managed head processes**, including GCS and
the raylet, with `allow_graceful=False`. It verifies their process handles exited
before starting one replacement head at the same GCS address/port. The driver
does not call `ray.shutdown()` or `ray.init()` during replacement.

GCS uses the fork's existing embedded RocksDB backend. Its directory survives
the injected process failure. The replacement loads the original cluster ID,
session, job metadata, and node records; registering a new head causes the old
head node to be marked dead. Workers reconnect to the same endpoint. The
existing Fixed-R protocol can then use authoritative owner-node death to adopt
and replay the enrolled streams. New tasks after that loss use the existing
surviving-coordinator path, not a second protected owner. No native recovery
protocol, durability barrier, retry rule, or output copying contract changes.

This is **full head-process loss and replacement with surviving storage**, not
physical host/disk-loss testing or driver-process recovery. A real machine-loss
deployment needs the driver outside the head failure domain, a replacement
head controller, surviving Redis or a reattachable RocksDB volume, and a stable
GCS endpoint. Exactly one GCS may use that state at a time. Merely killing a
normal head with in-memory GCS, or losing the driver with the head, does not meet
those requirements. RocksDB GCS is alpha in this fork; this composed experiment
must be validated rather than assumed to inherit full application HA support.

## One command to exercise the new path

With the existing compiled fork and `ray-dev` environment in
`/home/tejas/Downloads/ray`:

```bash
git pull --ff-only && \
TEST_OUTPUT_JSON=/tmp/fixed-r-head-failure.json \
python release/nightly_tests/dataset/backpressure_benchmark.py \
  --case fast-producer-slow-consumer --recovery-mode fixed_r_head_failure \
  --local-executor-nodes 2 --num-input-blocks 4 \
  --output-batches-per-input-batch 3 --output-batch-rows 4 \
  --output-row-bytes 64 --consumer-sleep-s 0.01
```

Head replacement is automatic after `FIXED_R_HEAD_FAILURE_READY`. This change
is Python-only and uses the already-present Linux RocksDB backend; no native
rebuild is required. The existing four-mode `suite` is unchanged. The new mode
requires a fresh driver and local nodes; it does not execute remote machine
termination commands or silently substitute an owner-only failure.

Success requires the same cluster/session and driver job, a different head node
ID, all original managed head processes exited, the old head recorded dead, and
the original coordinator/executor node IDs still alive. Existing checks require
12 ordered valid outputs, both producers and the consumer recovered, later
survivor tasks completed, and every stream closed. JSON includes old/new head
IDs and GCS PIDs, cluster/job IDs, survivor IDs, and replacement duration.
Failure-to-drain time includes head restoration and remaining work; it is not
isolated replay latency.

`python/ray/tests/test_streaming_recovery_head_failure.py` adds a focused
integration regression for this scenario. The agent inspected source only and
did not run builds, tests, lint, benchmarks, rendering, or Actions. No successful
head-failure result is claimed until the user runs this harness.
