# Fixed-R applicability to existing batch ML workloads

Source baseline: `b66a808a349212f3c0f0205cd1a3e0d2b3a1f435`.
Method: read workload, execution, ownership and retry implementations. No tests,
benchmarks, builds, lint or model downloads were run for this audit.

## Decision

No natural owner-loss case within current Fixed-R coverage was identified in
the inspected batch ML workloads. Do not build an early/middle/late performance
matrix for them yet. This is a source-based applicability finding, not a measured
claim that ordinary Ray always recovers or performs equally well.

These workloads submit their work from the driver. Losing a compute worker
does not normally kill that owner. Losing the driver does kill the owner but
also destroys execution state that the current integration requires to survive.
The Fixed-R Data path introduces separate owner helpers; adding those helpers
to the OFF arm would create the already-studied controlled ownership experiment,
not establish a new failure under the workload's normal placement.

At the time of this audit, streaming work was deferred. The user subsequently
requested [a learning workload with streaming input](STREAMING_LEARNING.md);
that experiment does not change the ownership findings here. Overhead
optimization remains deferred. A finite Ray Data batch job still
uses Ray Data's streaming executor internally; calling the job "batch inference"
does not bypass the current integration or its copying/enrollment costs.

## Workloads inspected

Paths are relative to the repository root.

| Existing workload | Main computation | Execution and model state | Applicability finding |
| --- | --- | --- | --- |
| `release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py`, `predict` | XGBoost/LightGBM batch prediction from the trained checkpoint | Callable predictor class becomes an actor pool; each actor loads the model | Inference actor recovery is not Fixed-R task replay. Driver owns submissions. |
| `release/nightly_tests/dataset/batch_inference_benchmark.py` | Pretrained ResNet-50 image classification | Explicit actor pools for image loading and prediction; driver creates model with `ray.put` | No separate ordinary task owner; driver/model-reference loss is outside this integration. |
| `release/nightly_tests/dataset/image_embedding_from_uris/main.py` | Image download, channel processing, patches, pretrained ViT inference | Function transforms plus `EmbedPatches` actor pool; driver-owned model reference | Transform tasks and inference actors have different recovery contracts. The expensive inference is not protected task replay. |
| `release/nightly_tests/dataset/text_embedding/main.py` | SentenceTransformer text embeddings | `EncodingUDF` actor pool loads the model | Actor/node retries already have an ordinary Ray path. Fixed-R does not reconstruct this actor state. |
| `gossip_benchmarks/workloads/fashion_features.py` | Frozen MobileNet-V3 features, then MLP training | Function-based feature tasks; read-only model cache within each worker process | Feature tasks fit the stateless computation model, but their ordinary owner is still the materializing driver. Task eligibility alone does not establish an owner-loss advantage. |

The release embedding examples are realistic computation patterns, not ready
local CPU commands: they contain GPU/cloud-resource assumptions and private
input locations. No credentials were retrieved and no cloud chaos code was run.
The existing Fashion feature workload is included as a task-based contrast, not
as independent evidence of production workload relevance.

## Ownership trace

For these directly executed inference/materialization calls:

1. `Dataset.write_datasink` or `Dataset.materialize` executes the plan in the
   calling process. `Dataset._execute_to_iterator` creates a local executor;
   `_execute_dag` starts it. The executor is a Python thread, not a replacement
   driver or independent fault-tolerant coordinator actor.
2. `TaskPoolMapOperator._try_schedule_task` submits ordinary read/map tasks.
   `ActorPoolMapOperator._start_actor` creates inference actors, and
   `_try_schedule_tasks_internal` submits their methods from that same process.
3. `CoreWorker::SubmitActorTask` passes its own `rpc_address_` to
   `TaskManager::AddPendingTask`. `AddPendingTaskInternal` registers ordinary
   return ownership with the caller address. Inference execution inside an actor
   does not make that actor the owner of its method return values.
4. Fixed-R's `get_config` chooses a head owner and requires an off-head
   coordinator plus surviving executors. `submit_stream` creates separate owner
   helpers and retains independent inputs in the coordinator. This is a changed
   submission path, not discovery of a pre-existing head owner in these jobs.

Sources: `python/ray/data/dataset.py`,
`python/ray/data/_internal/execution/streaming_executor.py`,
`python/ray/data/_internal/execution/operators/task_pool_map_operator.py`,
`python/ray/data/_internal/execution/operators/actor_pool_map_operator.py`,
`src/ray/core_worker/core_worker.cc`, `src/ray/core_worker/task_manager.cc`, and
`python/ray/data/_internal/execution/streaming_recovery.py`.

The actor classification follows `get_compute_strategy` in
`python/ray/data/_internal/util.py`: a callable class with integer concurrency
uses an actor pool; a function uses a task pool. Model inference can be
mathematically stateless while still using Ray actors to cache model weights.

## Failure and retry boundaries

| Failure | Ordinary execution | Current Fixed-R integration | Decision |
| --- | --- | --- | --- |
| Head processes, with off-head driver and inference workers surviving | Does not remove the ordinary caller/owner; control-service restoration is still required | Can lose introduced head helpers and replay eligible tasks | No distinct ordinary owner-loss case identified |
| Task executor process/node, with caller alive and inputs available | Ray Data has a system-failure task retry path | Protected executor survival is part of this owner-failure experiment's contract | Do not claim stronger executor-loss coverage |
| Inference actor process/node, with caller alive | Ray Data configures actor recreation and actor-task retries by default | Actor survival path disables actor/method retries and requires executor affinity | Outside current protected failure domain |
| Caller/driver process/node | Loses execution and ownership state | Loses required coordinator state and coordinator-owned copies | Unsupported; not a fair demonstration of current Fixed-R recovery |
| Parquet writer process/node | Subject to ordinary retry and sink semantics | Writer is a survivor-only task; not enrolled for replay | No added writer recovery or exactly-once guarantee |

Ordinary defaults are visible in `python/ray/data/_internal/remote_fn.py`
(`cached_remote_fn`, task `max_retries=-1`) and
`ActorPoolMapOperator._apply_default_remote_args` (actor `max_restarts=-1`,
actor-task `max_task_retries=-1` unless overridden). These are retry mechanisms,
not unconditional success guarantees: dependencies, resources, exceptions and
external side effects still matter.

The Fixed-R restrictions are explicit in `surviving_actor_options`, the actor
operator's recovery-specific method options, and
`python/ray/data/_internal/planner/plan_write_op.py`. They prevent silently
replaying stateful calls or external writes beyond the supported contract.
Do not disable ordinary baseline retries to make either arm look favorable.

## What would justify the next benchmark

A candidate must have an independently meaningful submitter that owns eligible
stateless ML tasks, can fail under its normal topology, and leaves a live
consumer plus the recovery state required by Fixed-R. A nested-task application
could have that topology, but none of the inspected entry points establishes
it. This audit does not claim that no such ML application exists.

Once such an existing application is identified, first validate one short
middle-work failure, without changing baseline ownership. Keep ordinary retries
enabled, preserve identical useful work and resources, and save results for a
separate plotting command. Only then expand to repetitions and early/late
failures. Until then, no new timing run is warranted by this audit.

Recovering the current driver/coordinator or model actors would be additional
runtime design work. It must be evaluated against ordinary restart/checkpoint
alternatives and cannot be represented as an already-implemented Fixed-R
capability. Selective Train retry remains separate from this applicability
question.
