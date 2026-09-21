# Fixed-R streaming recovery: consumer delivery state

Starting checkpoint: `71f9f7c5a7b538e1dafc0ded14ba2ea9dcb4ed3f`, latest `main`
fetched before this change. The user reported all three native test targets and
the full Ray build passed at that checkpoint.

## Implemented

`python/ray/_private/streaming_recovery.py` adds `StreamingRecoveryConsumer`, an
internal consumer-state component for the bounded experiment. It accepts an
offered descriptor and the designated consumer address. New read-only `_raylet`
helpers use the existing native descriptor validator and ObjectID derivation;
they neither create references nor expose native ownership adoption.

The component records each accepted original yield's ObjectRef and advances c
before returning the ref to application code. It accepts exactly the expected
deterministic ID at each index and bounds delivery by declared N. It retains
strong references to live consumed outputs, with explicit release after all
application uses end. Releasing a ref does not rewind c.

One local ticket identifies each outstanding owner read. Another read and a
recovery snapshot are prohibited until that read is settled. A duplicate or
late response cannot use a newer ticket or change the cursor. Transport failure
does not itself authorize recovery; the witness still needs authoritative
owner-node failure information.

The recovery snapshot holds current c and the ordered live consumed refs. It
can be taken after the original EOF when consumed outputs remain live, as well
as before the first yield or partway through delivery. Only one recovery is
admitted. Tickets, snapshots, and consumer state cannot be serialized; descriptor
bytes can be transferred to the designated consumer.

After a trusted transport performs checked native adoption and dispatch, it
can attach the local ObjectRefGenerator to the same snapshot. The component
verifies the original completion ID and rejects stale/duplicate attachment.
Replay reads use the existing native generator reader, preserving its consumed
prefix suppression and backpressure behavior. Nil/timeout refs do not advance
c; early EOF, count overflow, wrong IDs, and completion/error refs are not
delivered as ordinary outputs. A generator completion error is re-raised through
`ray.get`. Closing during a read prevents its eventual result from being exposed.

## Integration sequence and lifetime contract

The following is the required sequence for the upcoming transport. This file
does not implement those network operations.

1. Hold initial producer dispatch; install the complete descriptor/recipe on
   every selected holder and offer the descriptor to its designated consumer.
2. Construct this consumer state before acknowledging receipt. Only after the
   native all-R + consumer-receipt gate succeeds may transport call `mark_ready`
   and dispatch the original producer. Matching descriptor bytes alone are not
   readiness evidence.
3. Reserve an owner-read ticket, fetch one original item, and record it through
   `accept_owner_item` before exposing it. Resolve completion successfully before
   calling `accept_owner_eof`; route producer errors as terminal failures.
4. On owner loss, settle the outstanding read without accepting a result, retain
   its ticket for discarding any late callback, and take the recovery snapshot.
5. Request a real Fixed-R witness claim, repair resolver/plasma state, and pass
   the snapshot's current c and live refs to the checked native adoption API.
   Keep the snapshot alive until that operation finishes.
6. Transfer the one native-acquired completion ref into exactly one frontend
   handle with `skip_adding_local_ref=True`, dispatch the registered replay, and
   attach the handle. Drop the temporary transport snapshot after attachment.
   If attachment fails or cancellation wins, the transport must clean up the
   native task/stream and untransferred handle.
7. Release an item only after all aliases and submitted/exported uses end.
   Native adoption rejects omitted references still present in its reference
   table. Explicit holds can retain substantial data if the application never
   releases them; they are not a proposed Ray Data lifetime optimization.
8. Transport owns cancellation, recipe lifetime, and tombstone publication.
   Call `close` after that handling to drop local holds. It is idempotent and
   uses the existing ObjectRefGenerator deletion path when dropping a handle;
   it does not send cancellation/tombstone RPCs or revoke exported refs.

Version 1 still assumes one owner-node failure, a surviving consumer, no earlier
executor retry, finite deterministic output, one object per yield, and by-value
inputs without ObjectRefs. Arbitrary application aliasing or moving the consumer
to another worker is outside this contract.

## Remaining work

This component has no production transport caller yet. It does not enroll,
forward, claim, adopt, or dispatch a task by itself. The CoreWorker enrollment
and dispatch barrier, owner/consumer transport, claim orchestration, resolver
and plasma rebinding, distributed lifetime/cancellation, and owner-loss harness
remain to be connected. No ordinary streaming eligibility or Ray Data code is
changed. This is not an end-to-end owner-loss or benchmark pass.

## Local validation

`test_streaming_recovery_consumer.py` contains 14 test functions (19 cases after
parameterization). They exercise native descriptor/ID helpers and local consumer
transitions with a deterministic fake replay reader. They do not start a Ray
cluster or simulate native adoption/dispatch. Native ownership behavior remains
covered separately by the existing C++ tests.

No builds, tests, lint, benchmarks, or rendering were run by the agent. Rebuild
the Python/native extension with the usual full-Ray build so the new `_raylet`
helpers are available, then run:

```bash
python -m pytest -q python/ray/tests/test_streaming_recovery_consumer.py
```

The existing Python generator regression target is also relevant after the
extension rebuild:

```bash
python -m pytest -q python/ray/tests/test_streaming_generator.py
```

README is unchanged. The previous native/build passes do not validate these
new bindings or this consumer-state component.

### Subsequent user validation

At published checkpoint `ef65547300ff1b7e26212852725b1520799e9c1c`, the user
reported a successful full Ray compilation and:

```text
python -m pytest -q python/ray/tests/test_streaming_recovery_consumer.py
19 passed in 0.05s
```

This validates the consumer-state checkpoint. The next enrollment/submission
changes and their local-cluster tests are described separately in
`STREAMING_RECOVERY_SUBMISSION_STEP.md`.
