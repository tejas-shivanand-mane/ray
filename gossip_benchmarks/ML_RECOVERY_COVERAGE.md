# Fixed-R applicability to training: source audit and next experiment

Audit base: `06444402929e604670fae1af20cfe5bcf52dd189`.
This is a source review and experiment design, not a new measured result.

## What the measurements establish

The tested Fashion-MNIST workloads show no overall performance benefit from
the integrated configuration. In the four-epoch raw-input restart comparison,
ordinary application restart completed sooner at all three preprocessing fault
points. In the middle-epoch active-training comparison, both arms survived
head-process loss and recovered from worker-node loss. The integrated arm was
slower over the whole workload, including its preprocessing overhead.

Selective Train retry retained the healthy worker actor and reached the next
report about 1.5 seconds sooner after worker-node loss in that single pair.
Both ranks still restored their checkpoint and repeated 59 optimizer updates.
That result concerns selective worker retry, not Fixed-R data-task replay, and
does not establish a repeatable speed advantage.

The matched-head-owner experiment separately establishes recovery coverage:
ordinary Ray raised `OwnerDiedError`, while Fixed-R replay completed. Its
explicit owner placement is not the default Ray Data topology. Neither that
experiment nor a longer training run establishes usefulness for ordinary
streaming training.

## Where ownership lives in this checkout

These findings concern the Train v2 path and ordinary task-map operators;
custom dataset configuration or other execution paths can differ.

| Component | Placement or ownership in the inspected path | Source |
| --- | --- | --- |
| Train controller | Pinned to the driver's node | `python/ray/train/v2/api/data_parallel_trainer.py`, `_initialize_and_run_controller` |
| Dataset manager | Pinned to the controller's node | `python/ray/train/v2/_internal/callbacks/datasets.py`, `RayDatasetShardProvider` |
| Streaming split coordinator | Pinned to the dataset manager's node when it calls `streaming_split` | `python/ray/train/v2/_internal/data_integration/dataset_manager.py`, `_create_dataset_iterators`; `python/ray/train/_internal/data_config.py`, `configure`; `python/ray/data/_internal/iterator/stream_split_iterator.py`, `create` |
| Ordinary read/map task submission | Streaming executor runs inside the split coordinator; task-map submission occurs there | `python/ray/data/_internal/iterator/stream_split_iterator.py`, `SplitCoordinator`; `python/ray/data/_internal/execution/operators/task_pool_map_operator.py`, `_try_schedule_task` |
| Fixed-R protected producer | Separate owner helper on the configured owner node; automatic configuration chooses the head | `python/ray/data/_internal/execution/streaming_recovery.py`, `get_config`, `submit_stream` |
| Fixed-R retained inputs and delivered copies | Held by the surviving coordinator | Same module, module contract and `submit_stream` |

Consequently, in the current off-head-driver topology, killing head processes
does not normally kill the ordinary streaming coordinator or its task-output
owner. Fixed-R introduces separate head owner helpers in its own execution
path. A successful ON replay therefore does not, by itself, establish a failure
that ordinary OFF execution would suffer.

The split coordinator also contains mutable state: epoch barriers, per-split
pending bundles, output iterators, dispatch counters, and client progress.
Restoring GCS metadata does not reconstruct that live Python state. The current
Fixed-R Data contract explicitly requires the coordinator to survive and does
not recover failed actors.

| Failure | What must recover | Current interpretation |
| --- | --- | --- |
| Head processes; off-head controller and workers survive | Control services, using surviving local RocksDB | Both arms may continue; not a Fixed-R advantage by itself |
| Training worker process or executor node | Worker group, communication, model/optimizer checkpoint, input stream | Ordinary Train retries already cover supported cases; selective retry is a separate contribution |
| Separate protected task owner; coordinator and executors survive | Eligible task ownership and stream replay | Fixed-R's demonstrated domain; natural workload relevance still needs evidence |
| Streaming coordinator actor | Actor endpoint, iterator/barrier state, task ownership and consumed-data boundary | Not covered by current Fixed-R Data |
| Driver/controller node or physical machine | Driver/controller state and potentially data/checkpoint storage | Outside current coverage; physical-machine work remains deferred |

Fixed-R is therefore not intrinsically restricted to the beginning of a job.
Its eligible tasks can execute during training. However, ongoing data work alone
does not establish an advantage: the lost owner must be in the protected domain,
and all required surviving state must actually survive.

## A realistic source workload, with unresolved compatibility

Ray's existing `release/train_tests/benchmark` image-classification benchmark
provides a useful implementation to adapt: ResNet-50 training with Ray Data
Parquet reads, image decoding, random resized crops and horizontal flips,
streaming Torch batches, Adam, and periodic model/optimizer checkpoints.
Relevant files are `image_classification/parquet/factory.py`,
`image_classification/parquet/imagenet.py`, `image_classification/factory.py`,
`ray_dataloader_factory.py`, and `runner.py`.

The previously considered Food-101 tutorial,
`doc/source/train/tutorials/ci/py_scripts/04a_vision_pattern.py`, uses a PyTorch
DataLoader. It is not evidence of a streaming Ray Data task pipeline.

The release benchmark is not yet a ready local Fixed-R benchmark:

- Its defaults depend on cluster paths/resources and an internal ImageNet
  bucket. A CPU adapter needs explicit accessible local inputs and output paths.
- Training augmentation is stochastic. Fixed-R's current streaming contract
  requires deterministic finite tasks. Replayed random transforms cannot be
  assumed to reproduce a partially delivered stream. A reproducible augmentation
  contract must be defined before enabling protection on that stage.
- The optional row limit adds an operator outside the current supported linear
  map/exchange chain. Select a bounded input manifest before constructing the
  dataset instead of assuming `limit()` is supported.
- The existing checkpoint implementation records model, optimizer, epoch and
  batch position. Reiterating and skipping batches is not proof of identical
  input order, augmentations, or per-rank RNG restoration.
- The inspected Parquet factory reads validation from the training directory.
  A learning-quality experiment must use a genuine held-out split.

## Next implementation gate

The next step is to establish recoverable ownership and input progress, before
running another early/middle/late timing matrix. Use a small real-image input
manifest and the existing streaming ResNet training pattern. A short run can
validate this mechanism; it cannot establish production-scale performance.

1. Record the driver, controller, dataset manager, split coordinator, producer
   owners and training worker identities and node IDs under default placement.
   Record actual overlapping data production and optimizer updates. Leave
   baseline retries enabled and do not relocate OFF owners onto the head.
2. Verify deterministic replay for every protected stage, including sample
   identity, ordering and random augmentation. If only decode is eligible,
   protect only decode and state that narrower coverage explicitly.
3. Establish whether a natural owner failure is separable from coordinator
   loss. If the owner is the coordinator, classify that as unsupported rather
   than claiming Fixed-R recovery or adding a benchmark-only owner helper.
4. For coordinator-process recovery, design a Ray-owned replacement protocol:
   fence the old stream generation, reconnect clients, restore a common
   checkpoint/input boundary, and rebuild iterator/barrier state. Establish
   which task recipes and retained outputs survive coordinator death; the
   current coordinator-owned copies alone do not satisfy this requirement.
   This is additional runtime work, not an existing Fixed-R capability.
5. Only after coverage is established, compare ordinary full-group checkpoint
   retry with Fixed-R plus full-group retry to isolate Fixed-R. Add selective
   retry as a separate arm. Include checkpoint/restart as an alternative.
   Start with one middle-training failure; expand to early/middle/late and
   repetitions after correctness is verified.

The report must distinguish continuation, task replay, checkpoint restoration,
and application restart. Measure end-to-end completion, interruption until the
next optimizer update, repeated samples/updates, checkpoint age, and steady-state
overhead. Plot from saved JSON in a separate command. Failed and unsupported
observations must remain visible.

If default ownership offers no naturally separable owner failure, report that
negative applicability result. Neither a larger model nor expensive artificial
preprocessing resolves this architectural limitation.
