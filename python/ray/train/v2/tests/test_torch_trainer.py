import pytest
import torch

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.constants import TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.torch.config import _is_backend_nccl
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.config import FailureConfig
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.train.v2.torch.torchft_config import TorchftConfig


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


def test_minimal(ray_start_4_cpus):
    def train_func():
        pass

    trainer = TorchTrainer(train_func)
    trainer.fit()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_torch_linear(ray_start_4_cpus, num_workers):
    def train_func(config):
        result = linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    num_workers = num_workers
    epochs = 3
    scaling_config = ScalingConfig(num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


@pytest.mark.parametrize(
    "torch_config,expected_world_size",
    [
        (TorchConfig(backend="gloo", init_method="env"), 2),
        (TorchConfig(backend="gloo", init_method="tcp"), 2),
        (
            TorchftConfig(
                backend="gloo", init_method="env", lighthouse_kwargs={"min_replicas": 1}
            ),
            1,
        ),
        (
            TorchftConfig(
                backend="gloo", init_method="tcp", lighthouse_kwargs={"min_replicas": 1}
            ),
            1,
        ),
    ],
)
def test_torch_start_shutdown(ray_start_4_cpus, torch_config, expected_world_size):
    def check_process_group():
        assert (
            torch.distributed.is_initialized()
            and torch.distributed.get_world_size() == expected_world_size
        )

    trainer = TorchTrainer(
        train_loop_per_worker=check_process_group,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=torch_config,
    )
    trainer.fit()


@pytest.mark.parametrize("timeout_s", [5, 0])
def test_torch_process_group_shutdown_timeout(ray_start_4_cpus, monkeypatch, timeout_s):
    """Tests that we don't more than a predefined timeout
    on Torch process group shutdown."""

    monkeypatch.setenv(TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S, str(timeout_s))

    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=TorchConfig(backend="gloo"),
    )
    # Even if shutdown times out (timeout_s=0),
    # the training should complete successfully.
    trainer.fit()


def test_torchft_linear(ray_start_4_cpus):
    """Test torchft linear training: loss goes down and models are equal across workers."""

    from ray.train.v2.examples.pytorch.torchft_linear_example import (
        train_func as torchft_linear_train_func,
    )

    @ray.remote
    class WeightCollector:
        def __init__(self):
            self.weights = {}

        def report(self, rank, weight, bias):
            self.weights[rank] = {"weight": weight, "bias": bias}

        def get_weights(self):
            return self.weights

    collector = WeightCollector.remote()

    def train_func(config):
        result = torchft_linear_train_func(config)
        assert result[-1]["loss"] < result[0]["loss"]
        world_rank = ray.train.get_context().get_world_rank()
        ray.get(
            config["collector"].report.remote(
                world_rank, result[-1]["weight"], result[-1]["bias"]
            )
        )

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"collector": collector},
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=TorchftConfig(
            backend="gloo", lighthouse_kwargs={"min_replicas": 2}
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that models converged across workers.
    weights = ray.get(collector.get_weights.remote())
    assert len(weights) == 2
    assert weights[0]["weight"] == pytest.approx(weights[1]["weight"], abs=1e-4)
    assert weights[0]["bias"] == pytest.approx(weights[1]["bias"], abs=1e-4)


# TODO(tseah): Add test for lighthouse failures (e.g. lighthouse unreachable).


@pytest.mark.parametrize(
    "min_replicas,max_failures,expect_error,expected_train_fn_calls,expected_reports",
    [
        # TODO(tseah): enable this after we support training with 1/2 workers without
        # trying to restart the replica group.
        # (1, 0, False, 2),
        # This continues training with 1 replica. It does not replay step 10 and its report.
        # TODO(tseah): expected_reports should be 1 when we support training with 1/2 workers.
        (1, 1, False, 3, 0),
        # This errors immediately.
        (2, 0, True, 2, 0),
        # This stops training with 1 replica. It replays step 10 and its report.
        (2, 1, False, 3, 2),
    ],
)
def test_torchft_linear_replica_failure(
    ray_start_4_cpus,
    min_replicas,
    max_failures,
    expect_error,
    expected_train_fn_calls,
    expected_reports,
):
    """Test torchft linear training behavior when a replica fails mid-training."""

    from ray.train.v2.examples.pytorch.torchft_linear_example import (
        train_func as torchft_linear_train_func,
    )

    num_workers = 2
    # TODO(tseah): remove this check once we support training with 1/2 workers.
    training_requires_all_workers = min_replicas == num_workers

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1
            return self.count

        def get_count(self):
            return self.count

    counter = Counter.remote()

    def train_fn(config):
        counter.increment.remote()
        return torchft_linear_train_func(config)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config={
            "num_steps": 20,
            "error_step": 10,
            "error_rank": 0,
            "num_replicas": min_replicas,
            "training_requires_all_workers": training_requires_all_workers,
        },
        scaling_config=ScalingConfig(num_workers=num_workers),
        torch_config=TorchftConfig(
            backend="gloo", lighthouse_kwargs={"min_replicas": min_replicas}
        ),
        run_config=RunConfig(failure_config=FailureConfig(max_failures=max_failures)),
    )
    if expect_error:
        with pytest.raises(WorkerGroupError):
            trainer.fit()
    else:
        result = trainer.fit()
        assert len(result.best_checkpoints) == expected_reports
        assert result.error is None
    # Fewer train_fn calls indicate partial worker group restarts.
    assert ray.get(counter.get_count.remote()) == expected_train_fn_calls


@pytest.mark.parametrize("failed_rank", [0, 1])
def test_torchft_step_aligned_node_loss(failed_rank, tmp_path):
    """Compare peer-healed training with an uninterrupted reference.

    Kill an entire logical worker node after batch delivery and before forward.
    This is CPU/Gloo coverage, not a cloud spot-termination performance test.
    """
    import concurrent.futures
    import time

    import ray.data
    from ray._private.test_utils import wait_for_condition
    from ray.cluster_utils import Cluster
    from ray.train.v2._internal.data_integration.step_aligned_input import StepAlignedDataConfig
    from ray.train.v2.examples.pytorch.torchft_linear_example import train_func

    @ray.remote(num_cpus=0)
    class Observer:
        def __init__(self):
            self.failure_node = None
            self.calls = {}
            self.results = {}

        def started(self, run, rank):
            key = (run, rank)
            self.calls[key] = self.calls.get(key, 0) + 1

        def claim_failure(self, node):
            if self.failure_node is not None:
                return False
            self.failure_node = node
            return True

        def get_failure_node(self):
            return self.failure_node

        def finish(self, run, rank, result):
            self.results[(run, rank)] = result

        def snapshot(self):
            return self.calls, self.results

    cluster = Cluster()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        cluster.add_node(num_cpus=2)
        nodes = [cluster.add_node(num_cpus=1, resources={"train_slot": 1}) for _ in range(3)]
        ray.init(address=cluster.address)
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        observer = Observer.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False)).remote()

        def training(config):
            import torch
            torch.manual_seed(1234)
            rank = ray.train.get_context().get_world_rank()
            run = config["run"]
            ray.get(observer.started.remote(run, rank))
            shard = ray.train.get_dataset_shard("train")
            original = shard.batch_for_step

            def read(step):
                batch = original(step)
                if run == "failure" and rank == failed_rank and step == 2:
                    if ray.get(observer.claim_failure.remote(ray.get_runtime_context().get_node_id())):
                        # The driver kills this node; no cooperative exception.
                        while True:
                            time.sleep(0.1)
                return batch

            shard.batch_for_step = read
            results = train_func(config)
            ray.get(observer.finish.remote(run, rank, results[-1]))

        def make_trainer(run):
            ds = ray.data.range(48, override_num_blocks=1).map(
                lambda row: {"x": (row["id"] % 8) / 10, "y": 2 * (row["id"] % 8) / 10 + 5}
            )
            return TorchTrainer(
                training,
                train_loop_config={"run": run, "num_steps": 6, "num_replicas": 2,
                                   "batch_size": 4, "step_aligned_input": True},
                datasets={"train": ds},
                dataset_config=StepAlignedDataConfig(4, synchronous_full_membership=True),
                scaling_config=ScalingConfig(num_workers=2, resources_per_worker={"CPU": 1, "train_slot": 1},
                                             placement_strategy="STRICT_SPREAD"),
                torch_config=TorchftConfig(backend="gloo", lighthouse_kwargs={"min_replicas": 2}),
                run_config=RunConfig(name=run, storage_path=str(tmp_path),
                                     failure_config=FailureConfig(max_failures=1)),
            )

        reference = make_trainer("reference").fit()
        assert reference.error is None
        trainer = make_trainer("failure")
        future = pool.submit(trainer.fit)
        wait_for_condition(lambda: ray.get(observer.get_failure_node.remote()) is not None,
                           timeout=90)
        node_id = ray.get(observer.get_failure_node.remote())
        cluster.remove_node(next(n for n in nodes if n.node_id == node_id), allow_graceful=False)
        result = future.result(timeout=180)
        assert result.error is None
        assert not reference.best_checkpoints and not result.best_checkpoints
        calls, results = ray.get(observer.snapshot.remote())
        assert calls[("failure", failed_rank)] == 2
        assert calls[("failure", 1 - failed_rank)] == 1
        for rank in (0, 1):
            assert results[("failure", rank)]["step"] == 6
            for key in ("weight", "bias"):
                assert results[("failure", rank)][key] == pytest.approx(
                    results[("reference", rank)][key], abs=1e-5)
    finally:
        ray.shutdown()
        cluster.shutdown()
        pool.shutdown(wait=False, cancel_futures=True)


def test_is_backend_nccl():
    assert _is_backend_nccl("nccl")
    assert _is_backend_nccl("cuda:nccl")
    assert _is_backend_nccl("cpu:gloo,cuda:nccl")
    assert not _is_backend_nccl("gloo")
    assert not _is_backend_nccl("cpu:nccl")
    assert not _is_backend_nccl("cuda:gloo")
    assert not _is_backend_nccl("cpu:gloo,cuda:gloo")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
