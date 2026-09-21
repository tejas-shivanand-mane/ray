# Fixed-R: polling transport for the Data scheduler

Starting checkpoint: `7bbd74f67851bc0ffef5c4848ffd3f1d7eca9a6b`, fetched from
`main` before editing. The user reported the three real `_map_task` retained-input
cases passing. Together with earlier runs, all 52 Python cases had passed across
separate runs; that is not a combined run at this new checkpoint.

## Why polling comes before wiring the operator

`TaskPoolMapOperator._try_schedule_task` currently submits `_map_task` directly.
`MapOperator._submit_data_task` registers its native generator in `DataOpTask`.
The executor collects waitables, applies output budgets, and calls
`DataOpTask.on_data_ready`, which may stop between a block and its metadata.
Metadata fetching can also run on a background thread. Its completion callback
does not mean that downstream tasks have finished using its output blocks.

The previous reader blocked until an owner read completed and immediately ran
native recovery on actor failure. Substituting it in the scheduler would stall
unrelated operators and could adopt refs while downstream workers still use
them. A synchronous owner actor blocked in a read also could not process its
queued close. This change addresses the transport part of those problems.

## Implemented contract

- `reader.get_waitable()` returns a handle accepted by `ray.wait`. On the
  original path it starts at most one owner-read RPC and returns that same
  handle until `poll_next` settles it. During replay it returns the local
  generator, or its completion ObjectRef once the stream is exhausted.
- `reader.poll_next(timeout_s=0)` returns one output ObjectRef, `None` when
  pending, or raises `StopIteration` at successful EOF. A timeout leaves the
  RPC, ticket, and delivery cursor unchanged. Polling accepts finite,
  nonnegative timeouts or `None`; recovery's timeout remains separate.
- Each owner RPC bounds its native read to 0.1 seconds, returning a small
  pending envelope if no item is ready. The caller timeout does not cancel
  this RPC. A pending envelope settles the ticket without consuming output;
  a subsequent poll may start another read. This lets a queued close run
  behind a read even when the producer is stalled.
- A settled actor error raises `StreamingRecoveryRequired`. Polling does not
  claim, adopt, dispatch, or consume the one recovery attempt. The caller must
  first quiesce output users, then explicitly call `reader.recover()`. Native
  recovery still verifies GCS-known owner-node death. A transient actor outage
  is not sufficient authorization. Producer failures remain terminal errors.
- The blocking iterator uses the same transport and preserves its automatic
  recovery behavior for callers already satisfying the quiescence contract.
  Enrollment, recovery, and acknowledged close remain blocking operations.
- Polls fetch the small owner response envelope locally, not its nested block
  value. Replay fetches the completion object locally at EOF before trying a
  zero-timeout read; this avoids repeated zero-timeout gets cancelling a remote
  completion pull. Ordinary replay block values are not prefetched locally.
- Close discards pending transport state and forbids later delivery, including
  after a timed-out close. Input refs remain held until the tombstone barrier
  succeeds on a retry. Reader operations must still be serialized.

There is at most one transport read/response ahead of application acceptance;
accepting it does not automatically prefetch the next one. However, starting
that read can return one native backpressure credit on the original owner.
**Only request a waitable when there is output budget.** The current executor
collects all waitables before computing those budgets, so it cannot simply
replace each native generator with `reader.get_waitable()` without changing
that scheduling order or adding a separate readiness-only mechanism.

This bounds transport prefetch, not the total retained output set. Existing
`release(index)` rules still require all application/exported uses to end;
forgetting to release outputs can retain the entire stream. No general Data
memory-bound or protected benchmark result is claimed.

## Added coverage (not run by the agent)

Local transport cases cover repeated timeouts with one outstanding RPC, a
readiness/get race, pending responses, explicit recovery notification, producer
errors, the blocking iterator's automatic recovery, invalid timeouts, early EOF,
replay polling/completion, and retryable close with an unread response.

Three local-cluster cases exercise a delayed metadata yield from Ray Data's
actual `_map_task`: resume the original attempt, lose the separate owner node
then explicitly recover, and close while metadata is still gated. They retain
the first block on the consumer, check its stable ID and Arrow contents, check
schema/row metadata, and distinguish attempts `[0]` from `[0, 1]`. The wrapper
forwards serialization-stat feedback into `_map_task`; it does not buffer the
stream. Its timing gate affects only attempt 0. No downstream worker holds
outputs during recovery in these cases.

No builds, tests, lint, benchmarks, rendering, or GitHub Actions were run.
This is Python-only; it introduces no native binding or rebuild requirement.
Run the new coverage on the existing compiled fork:

```bash
python -m pytest -q --tb=long \
  python/ray/tests/test_streaming_recovery_consumer.py \
  python/ray/tests/test_streaming_recovery_owner_loss.py \
  -k 'poll or blocking_iterator or owner_pending_response or close_discards_pending'
```

The existing three-file suite remains the regression batch for the shared
blocking-iterator path; its earlier passes do not validate this change.

## Remaining full-executor work

1. Define an opt-in, known-output-count contract at the physical map-task
   boundary. Shaping/fusion can change the final count; twice the UDF yield
   count is not generally a valid declaration. Unknown-count support remains
   a separate protocol extension, not something this transport infers.
2. Add operator submission/retirement plumbing with an explicitly separate
   owner node and a surviving Dataset coordinator. Avoid serializing enrollment
   RPC waits on the scheduler if concurrent dispatch is required.
3. Integrate waitables with output budgets and the block/metadata pending pair,
   then coordinate background metadata gets and downstream tasks before
   recovery. Task IDs/cancellation must refer to the producer, not its temporary
   owner-read actor RPC. Do not close protection merely because EOF is observed.
4. Track output releases through downstream completion, queue removal, and
   exported bundles. Ray Data's `RefBundle.owns_blocks` flag expresses eager-free
   permission, not native ObjectRef ownership. General upstream-owned inputs
   still fail the consumer-owned-input contract and need a separate design.
5. Demonstrate enrollment and owner-loss recovery through a complete Dataset
   execution before adding or reporting protected collaborator benchmark runs.
   Keep baseline/protected shaping, fusion, work, and placement comparable, and
   report any restrictions and polling/enrollment overhead.

The benchmark scripts and README are unchanged. Succession streaming and
head-node/driver recovery remain deferred and unimplemented.
