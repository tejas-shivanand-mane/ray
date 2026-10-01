# Fixed-R applicability to the CIFAR streaming-training workload

Source baseline: `73910a15bd0982141ab4fd960a12346f4bcf45de`.
Method: source inspection and analysis of the user's existing JSON reports.
No tests, benchmarks, builds, lint or rendering were run for this audit.

## Decision

No naturally occurring failure with a demonstrated Fixed-R advantage was found
in this workload's current ownership topology. Pause further overhead
optimization and the early/middle/late performance matrix for this application.
Streaming transforms during training make tasks eligible for protection, but
do not by themselves create an owner-loss case that this integration handles
better than ordinary Ray.

This is a scoped applicability finding, not a claim that Fixed-R cannot benefit
any ML application, or that ordinary Ray always recovers. The control reports
contain no injected failure. Recovery conclusions below are source-based
expectations, with the remaining experimental uncertainty stated explicitly.

## Trace the actual owners

The application is `workloads/cifar_streaming.py`: lazy Parquet reads and
deterministic PNG decode/normalization, two-worker CPU DDP ResNet-18 training,
and application model/Adam/epoch/per-rank RNG checkpoints. Only the training
dataset is split. Validation runs on rank 0 through an ordinary unsplit iterator.

| Stage | Ordinary Ray's submitter/owner | Current Fixed-R path |
| --- | --- | --- |
| Training file listing, reading and decoding | `SplitCoordinator` actor, which runs the streaming executor | Head helpers own protected producers; the split coordinator retains independent inputs and delivered output copies |
| Validation file listing, reading and decoding | Rank-0 training worker, which executes its unsplit iterator locally | Head helpers own protected producers; rank 0 retains the inputs and delivered copies |
| Model forward/backward and optimizer updates | State in the DDP training worker actors | Unchanged; Fixed-R does not recover model/optimizer actor state |

The source chain is:

1. `python/ray/train/v2/api/data_parallel_trainer.py`,
   `_initialize_and_run_controller`, pins the controller to the driver's node.
2. `python/ray/train/v2/_internal/callbacks/datasets.py`,
   `RayDatasetShardProvider`, pins the dataset manager to the controller's node.
3. `python/ray/train/v2/_internal/data_integration/dataset_manager.py` calls
   `DataConfig.configure`. In `python/ray/train/_internal/data_config.py`,
   split datasets use `streaming_split`, while unsplit datasets use `iterator`.
4. `python/ray/data/_internal/iterator/stream_split_iterator.py`,
   `StreamSplitDataIterator.create`, places the coordinator on its caller's
   node. `SplitCoordinator.start_epoch` creates the executor in that actor.
   `iterator_impl.py`, `DataIteratorImpl._to_ref_bundle_iterator`, executes
   the unsplit dataset in the consuming worker instead.
5. `python/ray/data/_internal/execution/operators/task_pool_map_operator.py`,
   `_try_schedule_task`, submits ordinary tasks from the executor's process.
   `src/ray/core_worker/core_worker.cc`, normal task submission, passes the
   caller address to `TaskManager::AddPendingTask`; `task_manager.cc`,
   `AddPendingTaskInternal`, registers return ownership with that address.
   The process executing a task is not thereby the owner of its outputs.
6. `python/ray/data/_internal/execution/streaming_recovery.py`, `get_config`
   and `_submit_stream`, instead select head helpers, retain coordinator-owned
   inputs with `ray.put`, and require the coordinator to survive. Helper reuse
   changes actor creation frequency, not this ownership boundary.

The latest report's execution node IDs corroborate the placement trace in both
arms: the two training executions with `split(2, equal=True)` run on a node
distinct from the head and training workers; the two unsplit validation
executions run on rank 0's node. These are executor-location observations, not
direct per-ObjectRef owner-address measurements. The owner assignment follows
from the inspected submission code.

## Evaluate plausible failures without changing placement

| Failure | Ordinary recovery path | What Fixed-R adds here | Finding |
| --- | --- | --- | --- |
| Logical head processes; off-head driver/controller and executors survive | Needed training/validation owners remain alive; restoring control services can permit continuation | Replay of producers owned by the added head helpers | No ordinary owner-loss disadvantage established; ON replay alone is insufficient evidence |
| Data task process with its owner alive | Ordinary Ray Data system-failure task retries, subject to inputs/resources | Protected-task execution and replay have their own limits | No new owner-loss coverage established |
| Training worker process or executor node | Full-group Train retry and application checkpoint restoration, if errors propagate, retries remain and resources suffice | No model-state recovery from Fixed-R; protected executor loss is not its owner-loss guarantee | Do not attribute checkpoint recovery to Fixed-R |
| Split coordinator process, with driver/controller alive | Iterator errors can reach training workers and trigger a full-group retry with new dataset iterators | Loses coordinator-owned inputs/copies and mutable split state; no in-place coordinator recovery | A natural owner loss, but outside current Fixed-R stream recovery; both arms may fall back to Train retry |
| Rank-0 process during validation | Worker-group checkpoint retry can rebuild validation and training state | Also loses the validation consumer and its retained copies | No independent surviving Fixed-R consumer; not an added recovery case |
| Driver/controller node or full physical machine | Requires additional job/control-state recovery | Required surviving state is lost | Outside this project's current local failure scope |

Ordinary Data's `cached_remote_fn` in
`python/ray/data/_internal/remote_fn.py` defaults to `max_retries=-1` for
system failures unless overridden. The benchmark adapter explicitly enables
one full-group Train retry in both arms; those retries must remain enabled.

The coordinator-process case needs particular care. It is not enough to show
an `OwnerDiedError` or a dead actor and conclude that the entire ordinary job
cannot recover. `DefaultFailurePolicy` accepts worker-group errors within the
retry budget. The standard controller path shuts down and starts the worker
group; `DatasetsCallback.before_init_train_context` creates a fresh shard
provider and dataset manager. `CheckpointManager` supplies the latest checkpoint
to the new attempt, and the application restores model/optimizer/RNG state and
restarts at the saved epoch. This is a source-level restart path, not a measured
coordinator-failure result: exception propagation, collective timeouts, cleanup
and resource availability still need validation before promising completion.

In-place recovery would additionally have to restore/fence the split actor's
epoch barrier, pending bundles, output iterator and per-client progress.
Current Fixed-R does not implement that protocol. A task recipe held by witnesses
does not restore these states or guarantee survival of coordinator-owned inputs.

## Existing measurements, not recovery evidence

The user's `streaming-learning-helper-reuse.json` records clean source at the
baseline above, two epochs, one OFF/ON control pair, and passed validation:

- OFF workload: 87.898997666 seconds; ON: 151.284798807 seconds, 72.1121% overhead.
- Both epochs overlap data decoding and training updates. Both arms' per-epoch
  checkpoint hash dictionaries match; final accuracy is 0.36328125.
- ON enrolls 244 tasks, creates 92 helpers, reuses helpers 152 times and requests
  cleanup of all 92. Recovered task count is zero; no fault was injected.
- The previous profiled control pair at `7a5a437` used identical input identity
  and measured 85.9338 seconds OFF versus 213.2146 seconds ON (148.1149% overhead).
  These separate single pairs show an encouraging overhead reduction, not a
  repeated performance estimate or an owner-loss advantage.

## Gate for further ML work

Keep this workload as a correctness/overhead regression workload. Do not present
it as evidence that Fixed-R improves ML fault tolerance, and do not move the
OFF coordinator/owners to the head merely to produce such a result.

Before another performance benchmark, an existing application must supply a
normal, independently failing owner of eligible stateless work, plus a consumer
and all required recovery state that survive that same failure. Document the
actual owner addresses and restart behavior. A nested-task pipeline is only a
candidate until that evidence exists; none was established by this audit.

If continuing with this specific Ray Data/Train pattern, the meaningful new
research target is coordinator recovery that avoids full-group model rollback.
That requires additional runtime design and a comparison with the existing
checkpoint-restart path. It cannot be labeled an existing Fixed-R capability.
First establish whether preserving training progress is correct and cheaper
than restarting; only then compare one short middle-training fault, and expand
to early/late failures and repetitions if a benefit is demonstrated. Selective
Train retry remains a separate axis. Physical-machine and driver recovery stay
deferred.
