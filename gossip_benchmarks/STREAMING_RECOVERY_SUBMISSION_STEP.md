# Fixed-R streaming recovery: original enrollment and dispatch

Starting checkpoint: `ef65547300ff1b7e26212852725b1520799e9c1c`, latest `main`
fetched before this change. The user reported the full Ray build and all 19
consumer-state cases passed there. Earlier native protocol, TaskManager, and
reference-counter targets also passed as recorded in their respective notes.

## Implemented

An explicit task-only `_streaming_recovery` option supplies declared finite N
and the designated consumer's serialized worker address. It requires the
Fixed-R and streaming flags, K=1, no protection sampling, and all R distinct
non-owner holder nodes to be selectable. Native recipe validation rejects
unsupported tasks before acquiring pending-task references. Python propagates
the synchronous error after releasing any temporary argument put references.
Malformed address bytes are rejected before argument serialization.

The owner creates the immutable descriptor and registers the original task and
generator with TaskManager, while withholding submission to the scheduler. It
sends the full recipe to each selected holder. Each callback is bound to the
exact selected address and recipe request; compact-manifest ACKs cannot release
this task. Original dispatch is posted exactly once only after every holder
ACK and the matching designated-consumer receipt pass the existing native gate.
Receipt may precede the last holder ACK. Invalid receipts leave a valid offer
available; installation failure cancels it.

The owner keeps a separate immutable recipe through EOF. It does not enroll the
stream in the existing static-return succession manager. Ordinary tasks and
streams use the existing dispatch path unless explicitly opted in.

`StreamingRecoveryOwner` is a private owner-side helper for submission, reading
the offered descriptor/readiness, confirming receipt, reading original refs,
and cancellation. `streaming_recovery_address()` returns the actual local
CoreWorker address. The designated surviving consumer constructs and retains
`StreamingRecoveryConsumer` before acknowledging the offer. An application
transport must deliver this receipt to the owner, observe native readiness, and
then call the consumer's `mark_ready`. Descriptor/address bytes alone are not
evidence that the consumer retained state; this is a trusted internal transport
contract, not an authenticated public recovery API.

Keep the original owner helper alive until the consumer is finished with the
stream, including retained consumed refs after EOF. Closing/dropping the
original generator cancels its enrollment and publishes a newer tombstone to
the selected witnesses. Cancellation and dispatch execute on the same owner IO
loop: a still-held task is failed directly in TaskManager; a submitted task is
cancelled through the normal submitter. Later force cancellation can escalate
an earlier graceful request. Late installation callbacks cannot revive a
cancelled enrollment. Graceful worker shutdown also cancels unready tasks so
TaskManager does not wait indefinitely for their missing receipt. A readiness
race is rechecked under the same state mutex before shutdown cancellation.

## Boundaries and remaining work

This checkpoint implements original enrollment/dispatch, not owner-loss replay.
The consumer-side witness RPC/claim orchestration, resolver and plasma rebinding,
checked adoption plus replay dispatch/frontend-handle transfer, and complete
distributed cancellation/lifetime handling remain to be connected. Tests here
do not kill an owner node and recover its stream. No Ray Data or benchmark
support is claimed, and README is unchanged.

Cancellation uses the existing compact tombstone publisher, which sends to all
selected witnesses but can report success after one stored ACK. `close` does
not wait for an all-R tombstone barrier. A transport must not treat it as durable
distributed revocation or as permission to race an owner-loss recovery claim.
There is no automatic enrollment timeout: the owner must cancel abandoned
offers. Explicit close happens only after consumer use ends in these tests.

Version 1 still requires finite deterministic normal tasks with known N, one
object per yield, retry enabled, by-value inputs without nested ObjectRefs or
dependency sidecars, a consumer on a different node, and no previous executor
retry. The eventual recovery interval permits one owner-node loss while the
consumer, head/job/runtime, and enough holders remain available. Executor retry
and consumer/new-owner failure are not newly supported by this submission path.

## Local validation

Manual source review covered native/Cython signatures, option defaults,
pre-registration rejection, immutable recipe retention, dispatch/cancellation
serialization, and shutdown cleanup. No builds, tests, lint, benchmarks, or
rendering were run by the agent. The previous user passes apply to the starting
checkpoint, not these new changes.

After updating to this commit and rebuilding the Python/native extension using
the usual full-Ray build, run:

```bash
python -m pytest -q \
  python/ray/tests/test_streaming_recovery_consumer.py \
  python/ray/tests/test_streaming_recovery_submission.py
```

The new submission file contains 18 parameterized cases. It starts three local
raylets: driver/consumer on the head, an owner actor on a separate node, and the
producer on another node. The two non-owner nodes hold R=W=2 full recipes. It
covers empty/nonempty dispatch, readiness and duplicate/invalid receipts,
original deterministic IDs and EOF, cancellation before and after dispatch,
actual owner-process exit with an unready task, malformed address and unsupported
recipe rejection, and private-option validation. A shared filesystem marker
observes producer execution only because this is a local cluster; it is not a
replay dependency or a supported external-side-effect guarantee.

The existing generator regression file is relevant to the updated submission
binding and generator cleanup path:

```bash
python -m pytest -q python/ray/tests/test_streaming_generator.py
```
