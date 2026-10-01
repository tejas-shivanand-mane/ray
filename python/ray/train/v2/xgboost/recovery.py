"""Opt-in helpers for immutable input reuse across CPU XGBoost retries.

Use XGBoostTrainer(..., xgboost_config=XGBoostConfig(selective_recovery=True))
with FailureConfig(max_failures=...) and a resumable train function. Inside
that function, get_cached_input(dataset_version, load_rank_partition) retains
the returned dataframe/array if this actor survives. The loader must always
return the same immutable partition for this world size and global rank.

Every retry still calls the train function from the beginning. Restore the
model through ray.train.get_checkpoint(), rebuild DMatrix on every rank and
train only the remaining rounds. Report checkpoints synchronously. Healthy
workers pause and roll back too; this does not retain speculative model state.
The cache is process-local and is not a replicated checkpoint or a substitute
for durable input storage. It does not support caching streaming iterators.
"""

_communicator_cleared = False
_input_cache = {}


def get_cached_input(key, loader):
    """Load a fixed rank partition once per actor and training run.

    The key must identify immutable data (include a dataset version). The
    returned value must not be mutated. Do not cache DMatrix, Booster, iterators,
    or objects owning a communicator. Use a dataframe/array and rebuild DMatrix
    for every training attempt. Call this only from a Ray Train worker.
    """
    from ray.train.v2._internal.execution.context import get_train_context

    context = get_train_context()
    scope = (context.train_run_context.run_id, context.get_world_size(),
             context.get_world_rank(), key)
    if scope not in _input_cache:
        value = loader()
        _input_cache[scope] = value
    return _input_cache[scope]
