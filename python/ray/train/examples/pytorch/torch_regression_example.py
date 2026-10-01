import argparse
import os
import tempfile
from typing import Tuple

import pandas as pd
import torch
import torch.nn as nn

import ray
import ray.train as train
from ray.data import Dataset
from ray.train import Checkpoint, DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def get_datasets(split: float = 0.7, data_path=None) -> Tuple[Dataset]:
    dataset = ray.data.read_csv(
        data_path or "s3://anonymous@air-example-data/regression.csv"
    )

    def combine_x(batch):
        return pd.DataFrame(
            {
                "x": batch[[f"x{i:03d}" for i in range(100)]].values.tolist(),
                "y": batch["y"],
            }
        )

    dataset = dataset.map_batches(combine_x, batch_format="pandas")
    train_dataset, validation_dataset = dataset.repartition(
        num_blocks=4
    ).train_test_split(split, shuffle=True, seed=0)
    return train_dataset, validation_dataset


def train_epoch(iterable_dataset, model, loss_fn, optimizer, device):
    model.train()
    for X, y in iterable_dataset:
        X = X.to(device)
        y = y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(iterable_dataset, model, loss_fn, device):
    num_batches = 0
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in iterable_dataset:
            X = X.to(device)
            y = y.to(device)
            num_batches += 1
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"loss": loss}
    return result


def train_func(config):
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 10)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)
    torch.manual_seed(config.get("seed", 0))

    train_dataset_shard = train.get_dataset_shard("train")
    validation_dataset = train.get_dataset_shard("validation")

    model = nn.Sequential(
        nn.Linear(100, hidden_size), nn.ReLU(), nn.Linear(hidden_size, 1)
    )
    model = train.torch.prepare_model(model)

    loss_fn = nn.L1Loss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []
    start_epoch = 0
    base_model = model.module if hasattr(model, "module") else model
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as path:
            state = torch.load(os.path.join(path, "training.pt"), map_location="cpu",
                               weights_only=True)
            base_model.load_state_dict(torch.load(
                os.path.join(path, "model.pt"), map_location="cpu", weights_only=True))
            optimizer.load_state_dict(state["optimizer"])
            start_epoch = state["epoch"]
            results = state["results"]
            rank = train.get_context().get_world_rank()
            rng = torch.load(os.path.join(path, f"rng-{rank}.pt"), weights_only=True)
            torch.set_rng_state(rng["cpu"])
            if train.torch.get_device().type == "cuda":
                torch.cuda.set_rng_state(rng["cuda"], train.torch.get_device())

    def create_torch_iterator(shard):
        iterator = shard.iter_torch_batches(batch_size=batch_size)
        for batch in iterator:
            yield batch["x"].float(), batch["y"].float().reshape(-1, 1)

    for epoch in range(start_epoch, epochs):
        train_torch_dataset = create_torch_iterator(train_dataset_shard)
        validation_torch_dataset = create_torch_iterator(validation_dataset)

        device = train.torch.get_device()

        train_epoch(train_torch_dataset, model, loss_fn, optimizer, device)
        if train.get_context().get_world_rank() == 0:
            result = validate_epoch(validation_torch_dataset, model, loss_fn, device)
        else:
            result = {}
        result["epoch"] = epoch + 1
        result["resumed_from_epoch"] = start_epoch
        results.append(result)

        with tempfile.TemporaryDirectory() as tmpdir:
            rank = train.get_context().get_world_rank()
            if rank == 0:
                torch.save(base_model.state_dict(), os.path.join(tmpdir, "model.pt"))
                torch.save({"optimizer": optimizer.state_dict(), "epoch": epoch + 1,
                            "results": results}, os.path.join(tmpdir, "training.pt"))
            rng = {"cpu": torch.get_rng_state()}
            if device.type == "cuda":
                rng["cuda"] = torch.cuda.get_rng_state(device)
            torch.save(rng, os.path.join(tmpdir, f"rng-{rank}.pt"))
            train.report(result, checkpoint=Checkpoint.from_directory(tmpdir))

    return results


def train_regression(num_workers=2, use_gpu=False, data_path=None):
    train_dataset, val_dataset = get_datasets(data_path=data_path)
    config = {"lr": 1e-2, "hidden_size": 20, "batch_size": 4, "epochs": 3}

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": train_dataset, "validation": val_dataset},
        dataset_config=DataConfig(datasets_to_split=["train"]),
    )

    result = trainer.fit()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Use GPU for training."
    )
    parser.add_argument("--data-path", help="Optional local CSV with x000..x099 and y")

    args, _ = parser.parse_known_args()

    if args.smoke_test:
        # 2 workers, 1 for trainer, 1 for datasets
        ray.init(num_cpus=4)
        result = train_regression(data_path=args.data_path)
    else:
        ray.init(address=args.address)
        result = train_regression(num_workers=args.num_workers, use_gpu=args.use_gpu,
                                  data_path=args.data_path)
    print(result)
