import argparse
import tempfile
from datetime import timedelta
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from torchft import (
    DistributedDataParallel,
    DistributedSampler,
    Manager,
    Optimizer,
    ProcessGroupGloo,
)
from torchft.checkpointing.pg_transport import PGTransport

import ray.data
import ray.train
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.v2.torch.torchft_config import TorchftConfig


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def train_func(config):
    data_size = config.get("data_size", 1000)
    batch_size = config.get("batch_size", 4)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    num_steps = config.get("num_steps", 100)
    num_replicas = config.get("num_replicas", 1)
    report_interval = config.get("report_interval", 10)
    error_step = config.get("error_step")
    error_rank = config.get("error_rank", 0)
    step_aligned = config.get("step_aligned_input", False)

    context = ray.train.get_context()
    world_rank = context.get_world_rank()
    world_size = context.get_world_size()
    if step_aligned and num_replicas != world_size:
        raise ValueError("Step-aligned input requires full fixed-membership quorum")
    # Each worker is its own replica group with rank 0.
    group_rank = 0
    replica_group_id = world_rank

    # Model and optimizer
    model = nn.Linear(1, hidden_size)
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()

    # torchft process group and checkpoint transport.
    # Timeouts must be generous enough to re-form the gloo process group after a
    # replica fails. On loaded CI machines a 5s gloo store wait is too short, which
    # makes the post-failure reconfigure time out (DistStoreError) and breaks
    # recovery. Keep these <= the Manager timeout so the PG wait isn't cancelled
    # by the outer quorum timeout first.
    pg = ProcessGroupGloo(timeout=timedelta(seconds=30))
    transport = PGTransport(
        pg,
        timeout=timedelta(seconds=30),
        device=torch.device("cpu"),
    )

    # State dict callbacks for torchft recovery
    def load_state_dict(state_dict):
        model.load_state_dict(state_dict["model"])
        optimizer.load_state_dict(state_dict["optim"])

    def state_dict():
        return {
            "model": model.state_dict(),
            "optim": optimizer.state_dict(),
        }

    manager = Manager(
        pg=pg,
        min_replica_size=num_replicas,
        load_state_dict=load_state_dict,
        state_dict=state_dict,
        world_size=1,
        rank=0,
        replica_id=f"train_ddp_{world_rank}",
        timeout=timedelta(seconds=60),
        checkpoint_transport=transport,
        use_async_quorum=not step_aligned,
    )

    # Wrap model and optimizer with torchft primitives
    model = DistributedDataParallel(manager, model)
    optimizer = Optimizer(manager, optimizer)

    # Data
    train_dataset = LinearDataset(2, 5, size=data_size)
    sampler = DistributedSampler(
        train_dataset,
        replica_rank=replica_group_id,
        num_replica_groups=world_size,
        group_rank=group_rank,
        num_replicas=1,
        shuffle=False,
    )
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size, sampler=sampler
    )

    # Training
    results = []
    train_iter = iter(train_loader)
    running_loss = 0.0
    num_batches = 0

    input_shard = ray.train.get_dataset_shard("train") if step_aligned else None
    while step_aligned or manager.current_step() < num_steps:
        if step_aligned:
            # Synchronous quorum heals model/optimizer and Manager step before
            # selecting input. Repeat the same round when an update was aborted.
            optimizer.zero_grad()
            manager.wait_quorum()
            if manager.num_participants() != world_size:
                raise RuntimeError("Step-aligned input requires every replica")
            if manager.current_step() >= num_steps:
                # This final quorum also heals a replica that missed the final
                # commit, before any survivor tears down its peer state server.
                break
            batch = input_shard.batch_for_step(manager.current_step())
            X = torch.tensor(batch["x"], dtype=torch.float32).reshape(-1, 1)
            y = torch.tensor(batch["y"], dtype=torch.float32).reshape(-1, 1)
        else:
            try:
                X, y = next(train_iter)
            except StopIteration:
                train_iter = iter(train_loader)
                X, y = next(train_iter)
            optimizer.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        optimizer.step()
        running_loss += loss.item()
        num_batches += 1

        step = manager.current_step()
        if error_step is not None and step >= error_step and world_rank == error_rank:
            marker = Path(
                ray.train.get_context()
                .get_storage()
                .build_checkpoint_path_from_name("error_marker")
            )
            if not marker.exists():
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.touch()
                raise RuntimeError(
                    f"Simulated replica failure at step {step} on rank {world_rank}"
                )
        if not step_aligned and (step % report_interval == 0 or step >= num_steps):
            avg_loss = running_loss / max(num_batches, 1)
            weight = model.module.weight.detach().flatten().tolist()
            bias = model.module.bias.detach().flatten().tolist()
            result = {"loss": avg_loss, "weight": weight, "bias": bias, "step": step}
            # TODO(tseah): remove this check once we support reporting with 1/2 workers.
            if config.get("training_requires_all_workers", True):
                with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                    ray.train.report(
                        result,
                        checkpoint=ray.train.Checkpoint.from_directory(
                            temp_checkpoint_dir
                        ),
                    )
            results.append(result)
            running_loss = 0.0
            num_batches = 0

    if step_aligned:
        # Report once, after the terminal full quorum, without writing a model
        # checkpoint. Intermediate reports across worker generations remain an
        # independent Ray Train limitation, not solved by retaining input.
        result = {
            "loss": running_loss / max(num_batches, 1),
            "weight": model.module.weight.detach().flatten().tolist(),
            "bias": model.module.bias.detach().flatten().tolist(),
            "step": manager.current_step(),
        }
        ray.train.report(result)
        results.append(result)

    # Needed to avoid "split brain" where worker X dies, worker Y finishes, worker X resumes,
    # and worker X gets stuck in loss.backward()
    print(f"Shutting down manager on rank {world_rank}")
    manager.shutdown()

    return results


def train_torchft(num_workers=2, num_steps=100, storage_path=None, step_aligned_input=False):
    config = {
        "num_steps": num_steps,
    }
    dataset_kwargs = {}
    if step_aligned_input:
        from ray.train.v2._internal.data_integration.step_aligned_input import StepAlignedDataConfig

        batch_size = 4
        config.update(num_replicas=num_workers, batch_size=batch_size, step_aligned_input=True)
        rows = num_steps * num_workers * batch_size
        dataset = ray.data.range(rows).map(
            lambda row: {"x": (row["id"] % 100) / 10, "y": 2 * (row["id"] % 100) / 10 + 5}
        )
        dataset_kwargs = {
            "datasets": {"train": dataset},
            "dataset_config": StepAlignedDataConfig(batch_size, synchronous_full_membership=True),
        }
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=False),
        torch_config=TorchftConfig(
            lighthouse_kwargs={"min_replicas": num_workers if step_aligned_input else 1}, backend="gloo"
        ),
        run_config=RunConfig(storage_path=storage_path),
        **dataset_kwargs,
    )
    result = trainer.fit()

    print(result.metrics)
    return result.metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--num-steps", type=int, default=100, help="Number of training steps."
    )

    parser.add_argument("--step-aligned-input", action="store_true")

    args, _ = parser.parse_known_args()
    train_torchft(num_workers=args.num_workers, num_steps=args.num_steps,
                  step_aligned_input=args.step_aligned_input)
