# Fixed-R streaming: combined owner-loss integration

Starting checkpoint: `1dc4a4fecf3dbfe59cd7a24ae93e65775f958151`, fetched from
latest `main` before editing. The user reported the 19 consumer-state cases and
18 submission cases passed together: **37 passed in 68.97s**.

This checkpoint combines witness claims, checked native adoption, ownership
repair, replay dispatch, frontend handle transfer, a reader transport,
acknowledged close, and owner-node-loss tests. It is one implementation and
validation checkpoint rather than separate commits for each integration seam.

## Reader and native recovery

`StreamingRecoveryOwnerActor` is an internal actor class that can be wrapped
with `ray.remote(num_cpus=0)` and placed on the protected owner node. It holds
the original `StreamingRecoveryOwner` generator. Its normal producer task is
the recovery unit; the actor itself is not reconstructed.

`StreamingRecoveryReader.submit(owner_actor, producer, expected_returns=N, ...)`
runs the complete explicit enrollment handshake. It retains the descriptor on
the current designated consumer before sending its receipt and observing native
all-R readiness. Iteration forwards one owner read at a time, recording each
ObjectRef and advancing c before exposing it. Producer errors propagate through
the original path. An owner-actor failure settles that read, then invokes
bounded native recovery. A timeout alone is not treated as a settled read.

`reader.recover()` also supports explicit recovery of retained consumed outputs
after original EOF. The native operation first requires GCS-known owner-node
death; worker-only failure is insufficient. It requests the existing ordered
Fixed-R witness claim using the exact descriptor and designated consumer address.
The witness independently checks owner-node death and replicates its reservation
before granting attempt 1. Invalid claims due to propagation lag are retried
within the deadline. Terminal claims and grants to another worker are rejected.

The existing checked TaskManager handoff adopts the original completion and
listed live consumed returns, creates the native stream at c, and preserves
deterministic IDs and reference counts. FutureResolver serializes this adoption
against old status callbacks, so an old callback cannot pass its ownership check
before adoption and store OWNER_DIED afterward. Subsequent old replies fail the
current-owner check. Its extra callback lock is enabled only by the streaming
experiment flag.

The consumer next obtains an explicit local-raylet preparation ACK. That raylet
switches any existing location subscription to the consumer and fences old
success, failure, and queued cache callbacks by owner identity. The consumer
removes only stale OWNER_DIED values from local Plasma, releases inspection
buffers, and waits for deletion to finish before dispatch. Healthy local values
are preserved. The TaskManager handoff already clears matching memory-store
errors. Application gets and exports must be paused during this transition.

Only after these operations does the native call schedule replay and return its
one acquired completion reference. The Cython binding transfers it with
`skip_adding_local_ref=True`; the consumer creates exactly one local generator
handle and attaches it to the same recovery snapshot. Replay suppresses the
consumed prefix while regenerating live consumed outputs and restores the
prefix's backpressure credit. No earlier output is delivered a second time.

All blocking native RPC waits share a finite operation deadline. RPC callbacks
only resolve private promises; a late reply cannot initiate adoption or dispatch.
Failure after native adoption cancels the held task, requests stream deletion,
and releases the completion reference that was never transferred to Python.
The recipe and local submission state remain until generator retirement.

## Lifetime and scope

Keep the reader on its designated surviving worker, serialize reader operations,
and retain consumed refs through recovery. `release(index)` is valid only after
all aliases and application uses of that output end. During handoff there must
be no outstanding application gets, downstream tasks, or other workers using
those outputs. The bounded protocol does not migrate their independent readers.

Use `reader.close()` when all stream/output use has ended, preferably through
the reader's context manager. It cancels the original owner helper if reachable
and any local replay, then waits for a tombstone ACK from every selected holder
not authoritatively known dead. A timeout/error is exposed and close can be
retried; partial publication is not reported as durable success. Generator GC
retains the earlier best-effort cancellation/tombstone fallback. If an enrollment
begin times out before returning its descriptor, a close is queued on its owner.

The contract remains finite deterministic normal tasks, known N, one object per
yield, by-value inputs without ObjectRefs/sidecars, no prior executor retry, and
one owner-node loss with the consumer, driver/job/head, and runtime surviving.
This is a private experimental API, not automatic eligibility for ordinary
streams or exactly-once external side effects. Descriptor/grant bytes use Ray's
trusted internal transport assumptions rather than cryptographic authority.

Ray Data coordinator-state survival, object dependencies, broader failures, and
Succession streaming are not implemented by this checkpoint. No performance or
benchmark compatibility result is claimed. README is unchanged.

## Validation to run locally

The agent performed manual source review only: no builds, tests, lint,
benchmarks, or rendering were run. The prior 37-test pass applies to the starting
checkpoint. Rebuild the complete fork once for the new RPC and native bindings,
then run this validation batch:

```bash
bazel test //src/ray/object_manager/tests:ownership_object_directory_test \
  --test_output=errors

python -m pytest -q \
  python/ray/tests/test_streaming_recovery_consumer.py \
  python/ray/tests/test_streaming_recovery_submission.py \
  python/ray/tests/test_streaming_recovery_owner_loss.py
```

The Python batch contains the existing 37 cases and 11 new owner-loss cases.
The new file is also a complete local harness/example for the reader API. It
uses two surviving raylets (consumer/head and executor/holder), plus a separate
owner raylet removed with `allow_graceful=False`. R=W=2. Local files record
attempt numbers and control one in-flight-read timing case; they do not determine
output values or supply replay data.

Cases cover c=0, a retained prefix, and original EOF with all outputs retained,
for both small and Plasma-sized bytes; an outstanding owner read; N=0; recovery
after observing OWNER_DIED on a retained ref; rejection of live-owner and foreign
consumer claims; and rejection after acknowledged close. Checks compare original
IDs, cursor, output count/bytes, and exactly the recorded attempts 0 and 1. The
native directory test injects delayed old-owner success/failure callbacks after
rebinding and checks that new-owner failures still propagate normally.

## First integration run and follow-up fixes

The user reported the native ownership-directory test passing and the Python
batch finishing with **41 passed, 7 failed in 244.19s**. The original 37 cases
passed. Starting from `3497c119d28a25f0f7487d84153ca52bcb2ee9bc`, the follow-up
addresses three issues exposed by that run:

- Four c=0/c=1 cases completed with only attempt 0 recorded. Removing a local
  raylet does not synchronously kill its actor workers; the short stream could
  drain before the owner's periodic parent-death check. The crash fixture now
  waits for an owner-actor RPC to fail before allowing subsequent reads. The
  tests continue to require both attempts 0 and 1 and identical IDs/values.
- The Plasma-sized c=N case reached replay EOF but timed out fetching all three
  retained refs. Source review found that a reused Plasma copy can have sent its
  creation notification only to the original owner. Recording its primary pin
  does not populate the location set published to the new consumer's raylet.
  Recovery-stream reports now explicitly advertise the reporting executor for
  Plasma returns. A native regression case checks publication and snapshots for
  retained and unread returns without injecting a creation notification, and
  verifies that a stale attempt cannot advertise a location.
- The foreign-consumer and tombstone checks rejected the requests as intended,
  but expected ValueError. Ray's binding maps Status::Invalid to RaySystemError;
  the tests now require that exception and retain their message checks. The
  live-owner timeout check also requires the specific GetTimeoutError.

The local cluster disables the dashboard frontend, which is unnecessary for
these tests and was missing from the user's source build. No builds, tests,
lint, or benchmarks were run by the agent. The large-output timeout still needs
runtime confirmation after the native fix; the earlier pass does not validate
these changes. After rebuilding Ray, run the focused native cases and the
same Python batch:

```bash
bazel test //src/ray/core_worker/tests:task_manager_test \
  --test_arg='--gtest_filter=StreamingRecoveryTest.*' \
  --test_output=errors

python -m pytest -q \
  python/ray/tests/test_streaming_recovery_consumer.py \
  python/ray/tests/test_streaming_recovery_submission.py \
  python/ray/tests/test_streaming_recovery_owner_loss.py
```
