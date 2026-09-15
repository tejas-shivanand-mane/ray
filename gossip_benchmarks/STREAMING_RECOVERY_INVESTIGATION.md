# Streaming recovery: source trace and first implementation contract

Investigation date: 2026-09-15.
Starting/recovery checkpoint: `a515fcefd7832452ca70be8988bf31408afe2ced`.
Repository: `tejas-shivanand-mane/ray`, latest `main` fetched for this investigation.

This is a design and source-inspection checkpoint. Streaming recovery is NOT
implemented or enabled by this change. No builds, tests, lint, benchmarks, or
rendering were run. README and runtime code are unchanged. All source findings
below refer to the checkpoint, not an assumption about current upstream Ray.

## Recommendation

Start with Fixed-R, K=1, a deterministic finite normal-task generator, and one
surviving consumer that can become the new owner. Reuse Fixed-R's existing
witness claim and borrower-owned replay. Introduce an explicit experimental
recovery descriptor and a consumer-held cursor; do not serialize or manufacture
an ordinary `ObjectRefGenerator` around a remotely owned completion reference.

The first complete implementation milestone should resume that one stream after
the original owner dies, using the same TaskID and yielded ObjectIDs. Recovering
only an already-exported yield is a useful intermediate diagnostic, but does not
meet the stream-continuation milestone or establish Ray Data recovery.

## What the current source already provides

| Question | Source and finding | Consequence for owner-loss recovery |
| --- | --- | --- |
| Yield identity | `src/ray/common/task/task_spec.cc`, `StreamingGeneratorReturnId`; `task_manager.cc`, `ObjectRefStream::GetObjectRefAtIndex` | Completion is `FromIndex(T, 1)`; zero-based stream object i is `FromIndex(T, 2+i)`. Preserve TaskID. Do not encode a new attempt in ObjectID. |
| Executor allocation | `python/ray/_raylet.pyx`, `create_generator_return_objs` and `allocate_dynamic_return_id_for_generator` | Explicit stream position determines the ID. With multiple objects per yield, positions count objects, not Python yield statements. |
| Duplicate delivery | `task_manager.cc`, `ObjectRefStream::InsertToStream` | Previously consumed positions and repeated writes are not inserted again. The surviving owner normally retains the cursor and written-ref set. A new owner's empty stream lacks both. |
| Ordinary retries | `task_manager.cc`, `ResubmitTask`, `RetryTaskIfPossible`; `task_submission/normal_task_submitter.cc` | Reuse retained task/stream state; reconstruction of a running generator may be queued until its current attempt ends. These are owner-alive paths. |
| Attempt filtering | `task_manager.cc`, `HandleReportGeneratorItemReturns` | Reports older than the TaskSpec's current attempt are rejected. This is not a global ownership epoch or complete owner-failure protocol. |
| Reports and ACKs | `core_worker.cc`, `ReportGeneratorItemReturns`, `HandleReportGeneratorItemReturns` | Reports go to the original caller/owner address and can batch objects. Report acceptance and consumption updates are distinct. Failed report RPCs release executor backpressure; they do not reattach the stream. |
| Completion | `task_manager.cc`, `CompletePendingTask`, `MarkEndOfStream` | First successful completion records the generated count and writes EOF sentinel refs. The completion ref also carries task success/failure. Recreating yielded objects alone is insufficient. |
| Determinism checks | `task_manager.cc`, `FailStreamingGeneratorReplayIfInconsistent` | The existing check compares object counts after a previous successful execution with a known nonzero count. It does not compare values, validate a partial first attempt, or detect drift from a recorded zero count. |
| Cancellation/cleanup | `task_manager.cc`, `TryDelObjectRefStreamInternal`, `MarkTaskReturnObjectsFailed` | Deletion tracks caller deletion, releases unconsumed/peeked refs, unblocks the executor, and can retain lineage for consumed live refs. Failure also handles plasma returns reported before the first completion. |
| Consumer handle | `python/ray/_private/object_ref_generator.py`, `_next_sync`, `_next_async`, `__getstate__` | The generator uses its local worker's stream table. Serialization explicitly raises TypeError. A surviving downstream borrower can hold yielded ObjectRefs, but does not thereby possess a working iterator. |

The owner keeps `ObjectRefStream` state: next index, EOF index, written refs,
temporary peek refs, reported plasma refs, generated/consumed counters, and caller
deletion status. Its TaskManager also keeps the TaskSpec, retry/cancellation state,
successful-execution count, reconstructable refs, and consumption callbacks.
ReferenceCounter holds ownership and borrower/lifetime information. The Python
handle keeps completion/error-observation state.

The executor keeps execution-local stream position, output/report state, and
backpressure waiters. It reports to a concrete owner address. Application work
can run again during replay; suppressing duplicate delivery does not prevent
repeated external side effects.

## Fixed-R path and concrete extension points

1. `RecoverySuccessionManager::IsEligibleTask` explicitly excludes streaming and
   dynamic returns. Its many callers affect recipe retention, installation,
   metadata, and replay. Do not globally relax it as an initial change.
2. `CoreWorker::TryPopulateRecoveryMetadataForObject` separately checks that
   the exported ID is one of `TaskSpecification::ReturnId(0..NumReturns-1)`.
   Streaming yields fail that static-return check even if eligibility changes.
3. Fixed-R installs full TaskSpecs on the selected raylet witness holders.
   `PublishRecoveryManifestToWitnesses` requires all successful storage ACKs
   when publishing a full recipe; ordinary compact Succession publication needs
   one successful ACK.
4. Important readiness distinction: the ungrouped K=1 activation path calls that
   publication function asynchronously and can return locally registered
   metadata before its completion callback. The K>1 Frontier path has an explicit
   acknowledged-prefix barrier. For the new stream descriptor, expose a distinct
   all-R-complete readiness result; metadata presence or R sends is not proof
   of protection. This observation is not a claim that the existing K=1 export
   API itself waits for all R acknowledgements.
5. `CoreWorker::TryRecoveryWitnessHolders` requests a witness claim, receives the
   full recipe, and calls `StartRecoveryReplay` on the requesting CoreWorker.
   The borrower becomes acting owner; the raylet storing the recipe does not
   execute the Python generator. `NodeManager::HandleGetRecoveryWitness` and
   `ReplicateFixedRRecoveryClaim` retain the existing claim/redirect machinery.
6. `StartRecoveryReplay` changes caller address, increments the task attempt,
   restores dependency recovery metadata, and uses `AddPendingTaskForRecovery`.
   It currently selects a replacement from the static returned-ref vector.
   For a streaming task this vector contains the completion ref, not its yields.
7. `TaskManager::AddPendingTaskInternal` creates a fresh stream at cursor zero and
   asserts that it did not already exist. Recovery needs an explicit, idempotent
   adoption path that restores the intended cursor before execution starts.
8. `ReferenceCounter::AddOrPromoteOwnedObjectForRecovery` preserves a borrower's
   existing references when promoting a static return. Streaming reports instead
   use `OwnDynamicStreamingTaskReturnRef`, which attempts ordinary owned-object
   insertion and does not promote an existing borrowed entry. Extend ownership
   handling deliberately for live streamed refs, including consumed refs that
   a surviving consumer still holds.
9. Recovery metadata and `RecoverTaskOutputRequest` carry a static `return_index`.
   `PrepareTaskReplay` bounds it by `num_returns`. Introduce an explicit stream
   target/index representation; audit both Fixed-R's direct claim path and the
   generic acting-owner redirect path. Do not reinterpret the old field silently.
10. Static return deletion callbacks govern recipe retention/tombstones today.
    The stream descriptor, completion ref, unconsumed outputs, and consumed live
    outputs need a coherent lifetime rule. Dropping the original Python generator
    must not prematurely retire a still-live transferred recovery capability.

Also audit stale OWNER_DIED cleanup for yielded IDs in memory and plasma, old
owner addresses in pin/location state, cached task results, and nested references.
`PinExistingReturnObject` already notes that reusing a stored copy does not verify
value equality. Stable IDs do not prove deterministic values or correct re-pinning.

## Bounded stream-continuation contract (proposed)

### Participants and eligibility

- Original owner O submits one normal streaming task T. Its executor E is on a
  different node. Consumer C, the driver/job, GCS/head, and required code/runtime
  environment survive. C is the sole designated consumer and recovery claimant.
- Start validation with R=W=1, then R=W=2 on independently identified nodes.
  K=1 only. Keep performance sampling disabled (`recovery_baseline_perf_protect_every_n=1`).
  Preserve the existing placement independence requirements; do not count sends
  or duplicated node addresses as independent protection.
- One owner-node crash per experiment. No network partition, concurrent stream
  migration, consumer failure, acting-owner failure, redundancy repair, actors
  as recoverable producers, dynamic-return mode, or arbitrary `ray.put` recovery.
- Finite deterministic producer with known N and one object per yield initially;
  identical inputs produce the same order, count, boundaries, and values. Use
  explicit small by-value inputs without nested ObjectRefs or large captured
  closures that become hidden object-store dependencies. Retain the live job's
  function definition/runtime environment. Reject unsupported modes explicitly.

### Protection and delivery

Use a separate opt-in recovery descriptor containing a version, TaskID,
completion ID, fixed holder manifest, designated consumer, and declared object
count N. Store the immutable stream contract with the recipe on every R holder.
Descriptor readiness must wait for all R installation ACKs and consumer receipt;
the capability remains live throughout the experiment. To cover failure before
the first yield, explicitly protect/export this descriptor before any output is
required. This is new enrollment behavior compared with lazy protection on a
first yielded-object export.

C retains its next-delivery cursor c and all still-live delivered refs. Before
owner loss, O forwards yielded refs to C through an explicit experimental
adapter. The adapter advances c only when C accepts delivery and updates that
state before exposing the result to its application. O's `next(gen)` position
alone is not C's cursor. A delayed or repeated forwarding message must be deduped
at C. This survives O's loss because C is assumed to remain alive; it is not a
durable application transaction or an exactly-once side-effect guarantee.

After O's confirmed failure, C claims through existing Fixed-R machinery and
becomes the new owner. Before dispatch, create/adopt T's local stream, promote
completion and live yielded refs, retain necessary lifetimes, restore c, and
bind the new execution/report callbacks. Replay the generator from its beginning
using original object IDs. Reports for positions below c must not be redelivered,
but must still regenerate any such object whose live reference requires it.
Unread reports at or above c rebuild the new stream normally.

Backpressure must reconcile the consumed prefix with replay's generated counts:
do not simply disable it, or set a cursor while leaving counters/callbacks at
zero. Verify a replay can pass the skipped prefix when c is larger than the
configured backpressure window. Distinguish report-acceptance credit from object
consumption credit. Reuse existing waiter and consumption-update mechanisms.

Completion must enforce the declared N even after a partial initial execution
and for N=0. The existing successful-reexecution count check is insufficient for
that contract. Surface EOF only with the proper completion status, and surface
failure/cancellation through the relevant pending refs. Count drift must fail
explicitly; equal-count value drift remains outside the deterministic-producer
contract. Already delivered data cannot be retracted by a later mismatch check.

The stream must retain the recipe while either its descriptor or any protected
yield is live. Closing/deleting it unblocks production and releases unconsumed
refs; surviving consumed refs retain reconstruction support until released.
Terminal cancellation/tombstones must not be cleared into a fresh replay.

This design adds a transferable recovery capability, stream-lifetime tracking,
and consumer-local resume state. It does not propose a new election or consensus
protocol. It depends on the existing Fixed-R claim protocol and the deliberately
restricted single surviving consumer; broader failure guarantees require a
separate argument.

## First bounded code step and acceptance criteria

The smallest safe first patch is a native recovery-stream adoption primitive,
with tests written for the user to run, leaving production streaming eligibility
disabled. Give it explicit T, N, c, and live-yield inputs; validate them before
mutating ownership or task state. Cover stream initialization, live-ref promotion,
completion-ref retention, consumed-prefix handling, and duplicate adoption.
Place this beside `AddPendingTaskForRecovery`/`ObjectRefStream` and the recovery
reference-counter helpers. Keep static replay unchanged.

That primitive alone does NOT validate owner-loss recovery. The first complete
vertical slice then adds the all-R descriptor enrollment barrier, pre-failure
forwarding/consumer adapter, Fixed-R claim integration, completion/error checks,
and cleanup. Use an explicit disabled-by-default experimental mode rather than
admitting every streaming task. Only this slice should carry an owner-loss pass.

| Validation point | Required observation |
| --- | --- |
| State primitive | c=0, middle, and N; invalid indices rejected; repeated adoption does not duplicate TaskManager entries or reference counts. |
| Protection gap | Block one holder ACK: descriptor is not ready and full-R protection is not reported. Failure before readiness is outside the protected interval. |
| Failure timing | Owner loss before yield 0, after a delivered prefix, while reports/ACKs are in flight, during backpressure, after final yield before completion, and after EOF with live yielded refs. |
| Identity and delivery | Same TaskID and each original yielded ObjectID; consumer observes exactly positions 0..N-1 in order, with no duplicate delivery or missing values. |
| Retained consumed output | Lose/regenerate a still-live output below c without delivering it twice or leaving it owned by O. |
| Backpressure | c exceeds the configured window; replay progresses and a slow consumer still limits production. |
| Terminal behavior | Empty stream, too few/many replayed objects, producer error, explicit cancellation, and dropped descriptor settle rather than hang. |
| Lifetime | Live yield prevents premature recipe deletion; releasing all refs eventually removes stream/recipe state and preserves tombstone behavior. |
| Attempt isolation | Delayed old report and duplicate claim cannot create another local stream/task or advance c. |

Log actual owner/executor/consumer/holder node and worker IDs, installation ACKs,
failure event time, claim result, TaskID, each output index/ID, delivered cursor,
replay starts, completion/error, and cleanup state. Avoid timing-only sleeps as
proof of installation or consumer receipt. Logical nodes on one host are useful
for correctness debugging but do not establish physical failure independence.

## Ray Data integration gate

The fork's `task_pool_map_operator.py` forces `num_returns="streaming"` and keeps
the returned generator in `DataOpTask` via `MapOperator::_submit_data_task`.
`physical_operator.py`'s `DataOpTask` retains pending block/metadata refs, callbacks,
queue/emit progress, and completion/error state. These live in the execution
process, not in the producer TaskSpec. `map_operator.py` puts the transformer and
DataContext in `ray.put`; block dependencies and function/runtime availability
also require a survival argument.

For the inspected fast-producer/slow-consumer task-map path, the process retaining
the generator also submits the task and is its owner. Killing that owner loses
the corresponding Data execution state. A surviving downstream map worker does
not replace it. Conversely, killing only a producer executor while that process
survives principally exercises native streaming retry, not owner-loss recovery.

Therefore the original benchmark's owner-loss experiment needs either retained
execution/coordinator state with an explicitly separated submission/ownership
mechanism, or a separate recovery design for that state. This is a runtime/Data
architecture extension, even if the user-level benchmark code remains unchanged.
Do not describe the bounded forwarding adapter as the original Ray Data workload.

Data's block and metadata objects also require paired ordering/lifetime handling.
The initial one-object-per-yield experiment does not establish that integration.
Document surviving `ray.put` owners and input storage explicitly; full recipes
alone do not recover those objects. Preserve streaming/backpressure in the
eventual benchmark. A static-return adaptation is only a diagnostic.

Only after Fixed-R's slice is established should Succession reuse the stream
machinery, with its own holder admission and independent witness confirmation.
One consumer supplies at most its actual eligible failure domains. Achieving
R>1 may need more borrowers; explicit-placement fallback would be a hybrid.
Frontier K>1 needs stream-aware member addressing/lifetimes before inclusion.

## Local validation commands (not run here)

This notes-only commit has no new executable recovery test. After implementation
and rebuilding the native fork, use the focused new tests added with that patch.
Existing regression commands include:

```bash
python -m pytest -q python/ray/tests/test_streaming_generator.py
python gossip_benchmarks/11_generalized_succession_correctness.py
```

The second command validates the existing Succession matrix, not streaming.
Neither command is evidence of a pass until executed against the rebuilt code.
The original workload remains:

```bash
python release/nightly_tests/dataset/backpressure_benchmark.py --case fast-producer-slow-consumer
```

Do not interpret its successful execution with recovery flags as owner-loss
protection until the integration gate and failure assertions above are met.
