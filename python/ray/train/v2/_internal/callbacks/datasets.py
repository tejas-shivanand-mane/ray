import copy
import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import ray
import ray.train
from ray.data.context import DataContext
from ray.train.v2._internal.data_integration.dataset_manager import DatasetManager
from ray.train.v2._internal.data_integration.interfaces import (
    DatasetShardMetadata,
    DatasetShardProvider,
    GenDataset,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.worker_group import (
    Worker,
    WorkerGroup,
    WorkerGroupContext,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, NodeIdStr

logger = logging.getLogger(__name__)


class RayDatasetShardProvider:
    def __init__(
        self,
        datasets: Dict[str, GenDataset],
        data_config: ray.train.DataConfig,
        data_context: DataContext,
        world_size: int,
        worker_node_ids: List["NodeIdStr"],
    ):
        self._dataset_names = set(datasets)
        self._world_size = world_size
        self._datasets_to_split = (
            set(datasets) if data_config._datasets_to_split == "all"
            else set(data_config._datasets_to_split)
        )
        self._replacement = False
        self._dataset_manager = (
            ray.remote(DatasetManager)
            .options(
                num_cpus=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(), soft=False
                ),
            )
            .remote(
                datasets=datasets,
                data_config=data_config,
                data_context=data_context,
                world_size=world_size,
                worker_node_ids=worker_node_ids,
            )
        )
        self._cached_dataset_shards: Dict[Tuple[str, int], "DataIterator"] = {}

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> "DataIterator":
        dataset_name = dataset_info.dataset_name
        if dataset_name not in self._dataset_names:
            raise KeyError(
                f"Dataset shard for '{dataset_name}' not found. "
                "Please ensure that the dataset is passed through the Trainer `datasets` "
                "argument."
            )

        if not 0 <= dataset_info.world_rank < self._world_size:
            raise ValueError("Dataset shard rank is outside the training world")
        if self._replacement and dataset_name in self._datasets_to_split:
            raise ValueError(
                "Partial worker replacement cannot resume coordinated Ray Data "
                "streaming splits: the failed worker's input cursor is not restored. "
                "Use full-group checkpoint recovery, or independently replayable "
                "datasets with DataConfig(datasets_to_split=[]) and application "
                "sharding/cursor recovery. Disabling splitting alone does not "
                "restore input progress."
            )
        key = (dataset_name, dataset_info.world_rank)
        if key not in self._cached_dataset_shards:
            self._cached_dataset_shards[key] = ray.get(
                self._dataset_manager.get_dataset_shard.remote(dataset_info)
            )

        return self._cached_dataset_shards[key]

    def for_replacement(self, worker_node_ids: List["NodeIdStr"], replacement_ranks=None):
        """Share the surviving manager, but never claim to restore split cursors."""
        if len(worker_node_ids) != self._world_size:
            raise ValueError("Partial replacement must preserve dataset world size")
        ray.get(self._dataset_manager.update_worker_locations.remote(worker_node_ids, replacement_ranks))
        provider = copy.copy(self)
        provider._replacement = True
        provider._cached_dataset_shards = {}
        return provider

    def shutdown_data_executors(self) -> None:
        """
        Attempts to eagerly shutdown the data executors for datasets, freeing resources allocated to data execution.
        """
        try:
            self._dataset_manager.shutdown_data_executors.remote()
        except Exception:
            logger.debug("Failed to invoke remote cleanup of Dataset Manager.")

    def abort(self, timeout_s: float) -> None:
        """Fence this generation, including pending split/barrier requests."""
        try:
            ray.get(self._dataset_manager.abort.remote(), timeout=timeout_s)
        finally:
            # Also unblock workers waiting in get_dataset_shard. They must see
            # an error, not continue with a partly consumed old stream.
            ray.kill(self._dataset_manager, no_restart=True)


class DatasetsCallback(WorkerGroupCallback):
    """A callback for managing Ray Datasets for the worker group."""

    def __init__(
        self,
        train_run_context: TrainRunContext,
        datasets: Dict[str, "Dataset"],
    ):
        self._datasets = datasets
        self._data_config = copy.deepcopy(train_run_context.dataset_config)
        self._dataset_shard_provider: Optional[RayDatasetShardProvider] = None

        # Capture the current DataContext to propagate it to
        # the Train workers later.
        # The propagation works in the following way:
        # 1. This callback is created when user create the Trainer.
        # 2. Then this callback will be passed to the Controller actor.
        # 3. Lastly, when the worker group is initialized, the Controller
        #    will call the `after_worker_group_start` callback to propagate
        #    the DataContext to Train workers.
        self._data_context = copy.deepcopy(DataContext.get_current())

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def before_init_train_context(
        self, workers: List[Worker]
    ) -> Dict[str, List[DatasetShardProvider]]:
        world_size = len(workers)
        worker_node_ids = [worker.metadata.node_id for worker in workers]
        datasets = {k: v() if callable(v) else v for k, v in self._datasets.items()}

        self._dataset_shard_provider = RayDatasetShardProvider(
            datasets=datasets,
            data_config=self._data_config,
            data_context=self._data_context,
            world_size=world_size,
            worker_node_ids=worker_node_ids,
        )
        return {"dataset_shard_provider": [self._dataset_shard_provider] * world_size}

    def before_init_replacement_context(
        self, workers: List[Worker], worker_group: WorkerGroup
    ) -> Dict[str, List[DatasetShardProvider]]:
        if self._dataset_shard_provider is None:
            raise ValueError("Partial replacement requires a surviving dataset provider")
        all_workers = sorted(
            worker_group.get_workers(), key=lambda w: w.distributed_context.world_rank
        )
        if [w.distributed_context.world_rank for w in all_workers] != list(range(len(all_workers))):
            raise ValueError("Dataset replacement requires a complete global rank mapping")
        for worker in workers:
            rank = worker.distributed_context.world_rank
            if (not 0 <= rank < len(all_workers) or all_workers[rank] is not worker
                    or worker.distributed_context.world_size != len(all_workers)):
                raise ValueError("Replacement worker does not match the full training group")
        provider = self._dataset_shard_provider.for_replacement(
            [w.metadata.node_id for w in all_workers],
            [w.distributed_context.world_rank for w in workers],
        )
        # after_worker_group_start is not called for partial replacement. Only
        # initialize the new actors; survivors can still be running user code.
        def propagate_context(ctx):
            DataContext._set_current(ctx)

        ray.get([w.execute_async(propagate_context, self._data_context) for w in workers])
        return {"dataset_shard_provider": [provider] * len(workers)}

    def after_worker_group_start(self, worker_group: WorkerGroup):
        # Propagate DataContext
        def _propagate_data_context(ctx: DataContext):
            DataContext._set_current(ctx)

        worker_group.execute(
            _propagate_data_context,
            self._data_context,
        )

    def before_worker_group_reuse(self, worker_group: WorkerGroup, timeout_s: float):
        provider = self._dataset_shard_provider
        if provider is not None:
            provider.abort(timeout_s)
            self._dataset_shard_provider = None

    def after_worker_group_shutdown(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        shard_provider = self._dataset_shard_provider
        if shard_provider:
            shard_provider.shutdown_data_executors()

    def after_worker_group_abort(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        shard_provider = self._dataset_shard_provider
        if shard_provider:
            shard_provider.shutdown_data_executors()
