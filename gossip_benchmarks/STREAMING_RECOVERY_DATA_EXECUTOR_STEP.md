# Fixed-R: opt-in full Data executor path with copied outputs

Base: `40e3dff3f8d7af6198fbb3452bf0890edd3dde04`, fetched from `main` before
editing. The user reported the polling batch passing: **16 passed, 34 deselected
in 19.19s**. This new executor change has not been run by the agent.

## Integration and explicit trade-off

This step connects actual `TaskPoolMapOperator` submissions to Fixed-R readers,
the Data scheduling loop, block/metadata handling, output queues, task metrics,
and stream retirement. It includes a public Dataset pipeline test and a complete
two-stage physical executor crash test. It is an opt-in experiment, not general
support for every Ray Data workload.

The central choice is to **copy one completed output block into the surviving
Dataset coordinator's object store before emitting it downstream**. The original
protected block and metadata refs stay private to the task adapter. Once the
copy is created, their temporary aliases leave scope and both original refs are
released from the reader. There is no pre-run or whole-stream materialization.

This resolves two integration problems within the existing protocol:

- Downstream tasks and exported bundles use the copy, which never changes owner
  during this failure. They do not need to stop when an upstream stream adopts
  its current unpublished pair. Original refs never enter the background
  metadata fetcher. A synchronous failed get has settled before recovery starts.
- A later protected stage receives ready coordinator-owned input refs, satisfying
  the native input validation contract. Its own transformer and DataContext refs
  are also created and retained on the same coordinator.

Original stream IDs remain deterministic within the existing replay protocol.
**Exported block IDs are new IDs belonging to the copies.** Already-exported
copies keep their IDs and values through owner loss. Their RefBundles set
`owns_blocks=False` to prevent explicit eager freeing while exported aliases or
downstream tasks remain live. This Data flag is separate from native ObjectRef
ownership. The existing Core reference callbacks account for copy lifetimes.

Copying adds coordinator network traffic, serialization work, object-store
writes, and a transient extra block. It may be expensive for the collaborator's
large blocks. Performance of this mode must not be described as the cost of
recipe replication alone or of a zero-copy protocol.

## Execution contract

- Supported plans are one `InputDataBuffer` followed by a linear chain of normal
  task-map operators, with unique physical names. Every map must have an explicit
  nonnegative final block count **per task**, including zero. Count declarations
  apply after block shaping/splitting; UDF yield counts are not inferred to be
  equivalent. Missing/extra names and unsupported operators fail before dispatch.
- Operator fusion is disabled in both experimental modes. Block shaping is not
  changed. Counts are enforced on both original delivery and replay; a mismatch
  fails execution. Earlier blocks might already have been delivered before an
  undercount/overcount is discovered; this is not transactional Dataset output.
- Configure distinct protected-owner and task-executor node IDs. The Dataset
  coordinator must be on a surviving node. All producer/replay tasks are pinned
  to the configured surviving executor; each enrolled stream has a private
  zero-CPU owner helper pinned to the protected owner. Node IDs are explicit to
  avoid accidentally protecting the Dataset coordinator itself. The protected
  owner must be alive when an execution starts in `fixed_r` mode.
- Inputs must be ready and natively owned by that coordinator, without contained
  refs or tensor transport. Borrowed input blocks from arbitrary preexisting
  datasets are rejected. Configure `eager_free=False`, no UDF retries, and zero
  ignored block errors. The driver/job, coordinator, executor, and head/GCS must
  survive. Prior task-worker/executor failure is outside the contract.
- The executor computes output budgets before requesting protected waitables.
  A zero budget starts no read; pending work stays retained. Each adapter emits
  at most one completed pair per scheduling pass and does not collect all task
  outputs. The bound is one unpublished pair per active task, plus the native
  stream buffer and the temporary copy; total memory still depends on concurrency,
  block sizes, downstream queues, and application-held copies.
- Recovery handles an owner-read failure or `OwnerDiedError` fetching the current
  pair. Native claims still require authoritative owner-node death. It restores
  the stream cursor and any current pair without re-emitting earlier copies.
- Once GCS reports the configured owner dead, **new** tasks are submitted from
  the surviving coordinator with ordinary ownership, the same copying/count
  rules, and a separate metric. They are not reported as enrolled tasks. This is
  a single-failure design; it does not rotate owners to tolerate another failure.
- Failure during the enrollment handshake is exposed as an execution error;
  this step does not retry an ambiguous enrollment on another owner. The crash
  test waits for both protected tasks to enroll before removing their owner.
- Enrollment, recovery, copy creation, and acknowledged close can still block
  the scheduler. The normal read/fetch readiness path polls. EOF retires the
  reader only after all original pairs have been copied/released. Exported copies
  remain valid after that retirement. Cancellation closes the reader and retires
  its helper; tombstone failures are surfaced, not counted as successful closes.

The implementation is private and disabled unless the DataContext configuration
below is supplied. README, native code, and collaborator benchmark scripts are
unchanged. Succession, arbitrary head loss, actor pipelines, unknown output
counts, and general upstream-owner recovery remain outside scope.

## Usage on the existing configured cluster

The runtime flags are the same as in the existing Fixed-R streaming tests. For
R=W=2, two surviving witness/holder raylets must be available outside the
protected owner. Do not replace the locally compiled Ray fork with a wheel.

Set the context before constructing the Dataset. This small example has one
input block and one final output block from each task; substitute actual live
node IDs, not machine hostnames:

```python
import pyarrow as pa
import ray
from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, FixedRDataConfig

def increment(batch):
    return pa.table({"value": [x + 1 for x in batch["value"].to_pylist()]})

def double(batch):
    return pa.table({"value": [x * 2 for x in batch["value"].to_pylist()]})

context = DataContext.get_current().copy()
context.eager_free = False
context.retried_map_errors = False
context.max_errored_blocks = 0
context.set_config(CONFIG_KEY, FixedRDataConfig(
    owner_node_id=OWNER_NODE_ID,
    executor_node_id=EXECUTOR_NODE_ID,
    expected_blocks={"MapBatches(increment)": 1, "MapBatches(double)": 1},
    mode="fixed_r",
))
with DataContext.current(context):
    ds = ray.data.from_blocks([pa.table({"value": [1, 2, 3]})])
    ds = ds.map_batches(increment, batch_format="pyarrow").map_batches(
        double, batch_format="pyarrow"
    )
    for bundle in ds.iter_internal_ref_bundles():
        print(ray.get(bundle.block_refs))
```

Use `mode="copy"` for a no-failure copying control: same fusion policy, declared
counts, task placement, output copying, and release behavior, with ordinary
coordinator-owned task submission. It does not use remote owner helpers or
Fixed-R enrollment and has a different original-owner failure exposure. Include
an ordinary unmodified Data baseline separately when measuring the total cost
of this experimental integration. No performance results are claimed here.

Per-operator extra metrics expose:

- `fixed_r_enrolled_tasks`: completed enrollment handshakes.
- `fixed_r_recovered_tasks`: successful native recovery calls (not proof that a
  subsequent replay completed successfully).
- `fixed_r_survivor_tasks`: new coordinator-owned tasks after GCS-known owner loss.
- `fixed_r_copy_baseline_tasks`: ordinary tasks in the copying control.
- `fixed_r_copied_blocks`: completed copies emitted to Data.
- `fixed_r_closed_streams`: successful stream retirements.

## One local validation batch

No builds, tests, lint, benchmarks, rendering, or GitHub Actions were run by the
agent. This change is Python-only and needs no native rebuild. Pull the commit
and run the new integration file:

```bash
python -m pytest -q --tb=long python/ray/tests/test_streaming_recovery_data.py
```

The file covers public Dataset execution in both modes, declared-count errors,
empty-stream completion, configuration rejection, and scheduler zero-budget
behavior. Its two-stage crash case holds a first copied block in both an active
downstream task and an external alias, loses their original owner node, and
checks replay attempts, final values/order, copy ownership, later survivor-owned
submissions, and enrollment/recovery/copy/close metrics.

The next benchmark decision is whether this explicit copying mode is an
acceptable prototype for the collaborator workload, and how to declare its
actual post-shaping counts and spread tasks across multiple surviving workers.
Do not claim that setting flags protects the unmodified benchmark or that this
single-executor prototype implements the 5,000-worker experiment.
