"""CPU Fashion-MNIST application with ordinary Ray Data and Train APIs.

Prepare once, outside timed observations (requires torchvision):
  python gossip_benchmarks/workloads/fashion_mnist.py --prepare-data --data-directory DIR

The full official 60,000/10,000 train/test split is used. Recovery policies and
fault injection belong to the external harness. This application owns its
model/optimizer checkpoints, as an ordinary resumable Train application must.
"""

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
import time

import numpy as np
import torch
from torch import nn

import ray
import ray.train as train
from ray.train import Checkpoint, DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def make_model():
    return nn.Sequential(
        nn.Linear(784, 256), nn.ReLU(), nn.Linear(256, 128), nn.ReLU(),
        nn.Linear(128, 10),
    )


def prepare_data(directory):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from torchvision.datasets import FashionMNIST

    directory.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, training, rows in (("train", True, 60000), ("test", False, 10000)):
        dataset = FashionMNIST(str(directory / "download"), train=training, download=True)
        pixels = dataset.data.numpy().reshape(rows, 784)
        labels = dataset.targets.numpy().astype(np.int64)
        table = pa.table({
            "image": pa.FixedSizeListArray.from_arrays(pa.array(pixels.reshape(-1)), 784),
            "label": pa.array(labels),
        })
        path = directory / f"{name}.parquet"
        temporary = path.with_suffix(".tmp")
        pq.write_table(table, temporary, row_group_size=1000)
        temporary.replace(path)
        files[path.name] = {"rows": rows, "bytes": path.stat().st_size,
                            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest = {"dataset": "Fashion-MNIST", "split": "official", "files": files}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Prepared full Fashion-MNIST: {directory}")


def normalize(batch):
    return {"x": np.stack(batch["image"]).astype(np.float32) / 255.0,
            "y": batch["label"].astype(np.int64)}


def train_loop(config):
    torch.manual_seed(0)
    torch.set_num_threads(1)
    model = make_model()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    start_epoch = 0
    checkpoint = train.get_checkpoint()
    rank = train.get_context().get_world_rank()
    if checkpoint:
        with checkpoint.as_directory() as path:
            state = torch.load(Path(path) / "training.pt", map_location="cpu", weights_only=True)
            model.load_state_dict(state["model"])
            optimizer.load_state_dict(state["optimizer"])
            start_epoch = state["epoch"]
            torch.set_rng_state(torch.load(Path(path) / f"rng-{rank}.pt", weights_only=True))
    model = train.torch.prepare_model(model)
    base_model = model.module if hasattr(model, "module") else model
    train_shard = train.get_dataset_shard("train")
    validation = train.get_dataset_shard("validation")
    loss_fn = nn.CrossEntropyLoss()
    for epoch in range(start_epoch, config["epochs"]):
        started = time.monotonic()
        model.train()
        rows, steps = 0, 0
        for batch in train_shard.iter_torch_batches(batch_size=256):
            optimizer.zero_grad()
            loss_fn(model(batch["x"]), batch["y"]).backward()
            optimizer.step()
            rows += len(batch["y"])
            steps += 1
        metrics = {"epoch": epoch + 1, "resumed_from_epoch": start_epoch,
                   "train_rows": rows, "optimizer_steps": steps,
                   "epoch_training_s": time.monotonic() - started}
        if rank == 0:
            # Use the underlying module: validation on one rank must not start
            # DDP collectives while the other rank is waiting in report().
            base_model.eval()
            validation_started = time.monotonic()
            total, correct, loss = 0, 0, 0.0
            with torch.no_grad():
                for batch in validation.iter_torch_batches(batch_size=512):
                    logits = base_model(batch["x"])
                    total += len(batch["y"])
                    correct += int((logits.argmax(1) == batch["y"]).sum())
                    loss += float(loss_fn(logits, batch["y"])) * len(batch["y"])
            metrics.update(validation_rows=total, accuracy=correct / total,
                           validation_loss=loss / total,
                           epoch_validation_s=time.monotonic() - validation_started)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            if rank == 0:
                torch.save({"model": base_model.state_dict(), "optimizer": optimizer.state_dict(),
                            "epoch": epoch + 1}, path / "training.pt")
            torch.save(torch.get_rng_state(), path / f"rng-{rank}.pt")
            train.report(metrics, checkpoint=Checkpoint.from_directory(tmp))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--prepare-data", action="store_true")
    parser.add_argument("--epochs", type=int, default=8)
    args = parser.parse_args()
    if args.prepare_data:
        prepare_data(args.data_directory)
        return
    if args.epochs < 2:
        parser.error("Use at least two epochs")
    ray.init()
    training = (ray.data.read_parquet(str(args.data_directory / "train.parquet"), override_num_blocks=4)
                .map_batches(normalize, batch_format="numpy")
                .repartition(4).random_shuffle(seed=0).materialize())
    validation = (ray.data.read_parquet(str(args.data_directory / "test.parquet"), override_num_blocks=2)
                  .map_batches(normalize, batch_format="numpy").materialize())
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop, train_loop_config={"epochs": args.epochs},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        datasets={"train": training, "validation": validation},
        dataset_config=DataConfig(datasets_to_split=["train"]),
    )
    print(trainer.fit())


if __name__ == "__main__":
    main()
