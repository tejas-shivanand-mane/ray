# Fixed-R streaming recovery: native adoption primitive

Starting checkpoint: `e2f5b2c6460616ed165d8722b30a0584b832616d` (latest `main`
fetched on 2026-09-21). This implements the first native step proposed in
`STREAMING_RECOVERY_INVESTIGATION.md`.

## Implemented

`TaskManager::AddPendingStreamingTaskForRecovery` registers a finite streaming
task on a surviving consumer/new owner. It takes an already-authorized replay
TaskSpec, declared object count N, next-delivery cursor c, the consumer's live
consumed ObjectIDs, and an output completion reference.

- Validates normal-task streaming mode, one object per yield, enabled retries,
  a nonzero replay attempt, by-value arguments without nested ObjectRefs,
  `0 <= c <= N`, and space for the EOF sentinel in the generator ID range.
- Rejects duplicate registration, conflicting ownership, unlisted yielded refs,
  missing/dead references, explicitly freed refs, and live refs outside the
  consumed prefix. Invalid registration does not partially promote a batch.
- Reuses the static recovery ownership helper under one reference-table lock.
  Existing local/submitted/borrower counts are retained on promoted entries.
- Adds exactly one completion local ref for the future frontend handle. An
  existing borrowed completion reference retains its own independent count.
- Creates the original TaskID's stream at c and restores consumption credit.
  Reports below c are not delivered again, but can materialize a live consumed
  output. No unread output or EOF is marked ready by adoption alone.
- Copies the TaskSpec, changes the caller address, and registers the pending
  task without submitting it. The supplied attempt is not incremented again.
- Checks N on the first local successful completion, including a declared N=0.
  The existing ordinary owner-alive reconstruction behavior is unchanged.
- Clears stale OWNER_DIED values for adopted refs in the native memory store.
  Failure handling also writes errors for reported plasma outputs even though
  the adopted task had a declared count before its first local completion.

Source clarification: Ray sets BOTH `streaming_generator` and `returns_dynamic`
for a streaming task. Streaming mode is distinguished by `IsStreamingGenerator`;
the legacy non-streaming dynamic-return mode is not admitted by this API.

## API lifetime and synchronization contract

The caller must establish recovery authority before calling this primitive,
retain the listed frontend refs, serialize submission/adoption for this TaskID,
and wait for success before allowing replay reports. Concurrent calls to the new
adoption API are serialized; registration already present is rejected without
rewinding it or returning another counted handle. Ordinary task submission must
not race this recovery registration for the same TaskID.

The returned completion reference represents one newly acquired local ref.
Transfer it to exactly one frontend generator handle using the corresponding
skip-adding-local-ref convention. If no handle is created, request stream
deletion and explicitly release that ref. The native stream is removed only
after completion and its remaining reference/lifetime conditions are satisfied.

All locally tracked yielded refs must be listed and already consumed. Peeked
or buffered borrowed refs beyond c are deliberately rejected in this first
step. This is a native assumption, not a claim about arbitrary application
consumer state or complete Ray Data support.

The current N check runs on task completion. It detects count drift but does
not compare values or retract any data already delivered. The eventual adapter
must bound delivery by the declared contract and handle error returns explicitly.
Inputs must still obey the deterministic-producer contract from the design note.

## Not yet connected

The new API has no production call site. Existing streaming eligibility remains
disabled for both Fixed-R and Succession; no feature flag or RPC contract changes
are included. This commit does not claim an owner-node-failure pass.

Still required for the first complete owner-loss experiment:

1. A transferable stream descriptor, explicit all-R installation readiness, and
   a single-consumer adapter retaining delivery progress before owner loss.
2. Integration with Fixed-R's claim/redirect paths and retry accounting.
3. Stream-aware metadata/export and lifetime/tombstone handling.
4. CoreWorker future-resolution, plasma owner-death cleanup, and pin/location
   rebinding, including already-exported refs and delayed old-owner activity.
5. Dispatch and frontend-handle construction around this native primitive, with
   full cancellation, completion, and end-to-end backpressure validation.
6. Owner-node failure at the planned yield positions, followed by the separate
   Ray Data coordinator/dependency integration.

Known future boundary: a copied recipe is not evidence of all-R ACKs, and a
caller-provided cursor is valid here only because the designated consumer
survives. This primitive neither authenticates a recovery descriptor nor
recreates a failed coordinator. It must not be exposed as unrestricted public
ownership adoption.

## Tests and user validation

Eight `StreamingRecoveryTest` cases were added to the existing native
`task_manager_test` target. They exercise the real ReferenceCounter and native
stream report/read methods; the fixture's plasma callback is simulated.

Coverage includes cursor boundaries and empty streams, consumed-ref ownership
and count preservation, delayed stale reports, out-of-order unread reports,
duplicate adoption, invalid argument/count/mode rejection, atomic ownership
failure, freed refs, independent completion-handle counts, declared-count drift,
pre-completion failure, plasma error placement, and cleanup after references die.
The backpressure assertion checks restored owner consumption updates; it does
not run a distributed executor or demonstrate end-to-end production bounds.

Run from the repository root in your existing Ray build environment:

```bash
bazel test //src/ray/core_worker/tests:task_manager_test \
  --test_arg='--gtest_filter=StreamingRecoveryTest.*' \
  --test_output=errors
```

Then run the existing native tests for the interfaces touched by this patch:

```bash
bazel test //src/ray/core_worker/tests:task_manager_test \
  //src/ray/core_worker/tests:reference_counter_test \
  --test_output=errors
```

If your usual build command supplies extra Bazel flags, retain those flags.
After rebuilding the Python/native fork, the existing Python streaming tests
and recovery correctness matrix remain useful regression checks:

```bash
python -m pytest -q python/ray/tests/test_streaming_generator.py
python gossip_benchmarks/11_generalized_succession_correctness.py
```

No builds, tests, lint, benchmarks, or rendering were run by the agent.
On 2026-09-21 the user reported that the focused `StreamingRecoveryTest.*`
command and then the full `task_manager_test` plus `reference_counter_test`
command above passed for commit `565816c09fb8fe897641c238b8c888e7d44a0345`.
These results validate that native step; they do not cover later protocol changes
or a distributed owner-loss run. README is unchanged.
