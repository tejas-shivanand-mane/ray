"""Checkpointed CPU ResNet-18 with lazy, deterministic CIFAR-10 decoding.

Preparation writes encoded images, not precomputed tensors/features. Each epoch
reads and transforms them again through Ray Data. No failure/recovery policy is
implemented here. Optional local telemetry observes work without changing outputs.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import uuid

import numpy as np
from PIL import Image
import torch
from torch import nn
from torchvision.models import resnet18

import ray
import ray.train as train
from ray.data import DataContext
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.train import Checkpoint, DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def make_model():
    model = resnet18(weights=None, num_classes=10)
    # CIFAR images are 32x32: retain spatial resolution in the input stem.
    model.conv1 = nn.Conv2d(3, 64, kernel_size=3, stride=1, padding=1, bias=False)
    model.maxpool = nn.Identity()
    return model


def normalize_pixels(pixels):
    pixels = pixels.astype(np.float32) / 255.0
    pixels = (pixels - np.array([0.4914, 0.4822, 0.4465], dtype=np.float32)) / np.array(
        [0.247, 0.243, 0.261], dtype=np.float32)
    return pixels.transpose(0, 3, 1, 2).copy()


def input_fingerprint(pixels, label):
    return hashlib.sha256(np.ascontiguousarray(pixels, dtype=np.float32).tobytes()
                          + int(label).to_bytes(8, "little", signed=True)).hexdigest()


def event(directory, kind, **values):
    if directory is None:
        return
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{uuid.uuid4().hex}.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"kind": kind, **values}))
    temporary.replace(path)


def prepare_data(directory, train_rows, validation_rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from torchvision.datasets import CIFAR10

    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "manifest.json").exists():
        raise ValueError("Prepared directory already exists; use a new directory to change the subset")
    files, selected, fingerprints = {}, {}, {}
    for split, count, training in (("train", train_rows, True), ("validation", validation_rows, False)):
        dataset = CIFAR10(str(directory / "download"), train=training, download=True)
        # Seeded sample without replacement from the official, separate splits.
        ids = np.random.default_rng(0).permutation(len(dataset))[:count]
        selected[split] = ids.tolist()
        fingerprints[split] = {}
        for start in range(0, count, 64):
            part = ids[start:start + 64]
            normalized = normalize_pixels(dataset.data[part])
            fingerprints[split].update({str(int(index)): input_fingerprint(pixels, dataset.targets[index])
                                        for index, pixels in zip(part, normalized)})
            encoded = []
            for index in part:
                buffer = io.BytesIO()
                Image.fromarray(dataset.data[index]).save(buffer, format="PNG")
                encoded.append(buffer.getvalue())
            name = f"{split}/{start // 64:05d}.parquet"
            path = directory / name
            path.parent.mkdir(exist_ok=True)
            table = pa.table({"image": encoded, "label": [int(dataset.targets[i]) for i in part],
                              "sample_id": part})
            pq.write_table(table, path)
            files[name] = {"rows": len(part), "bytes": path.stat().st_size,
                           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest = {"dataset": "CIFAR-10", "selection_seed": 0, "selected_ids": selected,
                "training_rows": train_rows, "validation_rows": validation_rows,
                "files": files, "encoding": "PNG RGB 32x32", "input_sha256": fingerprints}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(directory / "manifest.json")


def decode(batch, telemetry_directory=None, split="train"):
    started = time.monotonic_ns()
    pixels = np.stack([np.asarray(Image.open(io.BytesIO(bytes(value))).convert("RGB"))
                       for value in batch["image"]])
    output = {"x": normalize_pixels(pixels),
              "y": np.asarray(batch["label"], dtype=np.int64),
              "sample_id": np.asarray(batch["sample_id"], dtype=np.int64)}
    if telemetry_directory is not None:
        runtime = ray.get_runtime_context()
        event(telemetry_directory, "decode", split=split, started_ns=started,
              finished_ns=time.monotonic_ns(), sample_ids=output["sample_id"].tolist(),
              node_id=runtime.get_node_id(), worker_id=runtime.get_worker_id())
    return output


def train_loop(config):
    torch.manual_seed(0)
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    rank = train.get_context().get_world_rank()
    model = make_model()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    start_epoch = 0
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as path:
            state = torch.load(Path(path) / "training.pt", map_location="cpu", weights_only=True)
            model.load_state_dict(state["model"])
            optimizer.load_state_dict(state["optimizer"])
            start_epoch = state["epoch"]
            torch.set_rng_state(torch.load(Path(path) / f"rng-{rank}.pt", weights_only=True))
    model = train.torch.prepare_model(model)
    base_model = model.module if hasattr(model, "module") else model
    training = train.get_dataset_shard("train")
    validation = train.get_dataset_shard("validation")
    loss_fn = nn.CrossEntropyLoss()
    telemetry = config.get("telemetry_directory")
    invocation = uuid.uuid4().hex
    for epoch in range(start_epoch, config["epochs"]):
        model.train()
        ids, fingerprints, weighted_loss, steps = [], [], 0.0, 0
        started = time.monotonic_ns()
        event(telemetry, "epoch_start", rank=rank, epoch=epoch + 1,
              invocation=invocation, time_ns=started, resumed_from_epoch=start_epoch)
        for batch in training.iter_torch_batches(batch_size=config["batch_size"], prefetch_batches=1):
            batch_ids = batch["sample_id"].tolist()
            event(telemetry, "batch", rank=rank, epoch=epoch + 1, step=steps + 1,
                  invocation=invocation, time_ns=time.monotonic_ns(), sample_ids=batch_ids,
                  resumed_from_epoch=start_epoch)
            optimizer.zero_grad()
            loss = loss_fn(model(batch["x"]), batch["y"])
            loss.backward()
            optimizer.step()
            steps += 1
            ids.extend(batch_ids)
            fingerprints.extend(input_fingerprint(pixels, label)
                                for pixels, label in zip(batch["x"].numpy(), batch["y"].tolist()))
            weighted_loss += float(loss.detach()) * len(batch_ids)
            event(telemetry, "update", rank=rank, epoch=epoch + 1, step=steps,
                  invocation=invocation, time_ns=time.monotonic_ns(), sample_ids=batch_ids,
                  resumed_from_epoch=start_epoch)
        metrics = {"epoch": epoch + 1, "resumed_from_epoch": start_epoch,
                   "rank": rank, "train_rows": len(ids), "sample_ids": ids,
                   "input_sha256": fingerprints,
                   "optimizer_steps": steps, "training_loss": weighted_loss / len(ids),
                   "epoch_training_s": (time.monotonic_ns() - started) / 1e9}
        if rank == 0:
            base_model.eval()
            total, correct, validation_loss = 0, 0, 0.0
            with torch.no_grad():
                for batch in validation.iter_torch_batches(batch_size=config["batch_size"], prefetch_batches=1):
                    logits = base_model(batch["x"])
                    total += len(batch["y"])
                    correct += int((logits.argmax(1) == batch["y"]).sum())
                    validation_loss += float(loss_fn(logits, batch["y"])) * len(batch["y"])
            metrics.update(validation_rows=total, accuracy=correct / total,
                           validation_loss=validation_loss / total)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            if rank == 0:
                torch.save({"model": base_model.state_dict(), "optimizer": optimizer.state_dict(),
                            "epoch": epoch + 1}, path / "training.pt")
            torch.save(torch.get_rng_state(), path / f"rng-{rank}.pt")
            train.report(metrics, checkpoint=Checkpoint.from_directory(temporary))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--prepare-data", action="store_true")
    parser.add_argument("--train-rows", type=int, default=2048)
    parser.add_argument("--validation-rows", type=int, default=512)
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--telemetry-directory")
    args = parser.parse_args()
    if not 128 <= args.train_rows <= 50000 or not 32 <= args.validation_rows <= 10000:
        parser.error("Use 128..50000 training and 32..10000 validation images")
    if args.prepare_data:
        prepare_data(args.data_directory, args.train_rows, args.validation_rows)
        return
    manifest = json.loads((args.data_directory / "manifest.json").read_text())
    if (args.epochs < 2 or args.batch_size < 2
            or manifest["training_rows"] % (2 * args.batch_size)):
        parser.error("Use >=2 epochs and a training subset divisible by two worker batches")
    ray.init()
    context = DataContext.get_current()
    context.execution_options.preserve_order = True
    # Equal bounded prefetch/resource settings in both arms. No synthetic delays.
    context.target_min_block_size = 0
    context.target_max_block_size = 128 * 1024
    context.execution_options.resource_limits = ExecutionResources.for_limits(
        cpu=2, object_store_memory=4 * 1024**2)
    context._max_num_blocks_in_streaming_gen_buffer = 1

    def dataset(split):
        paths = [str(args.data_directory / name) for name in sorted(manifest["files"])
                 if name.startswith(split + "/")]
        return ray.data.read_parquet(paths, concurrency=2).map_batches(
            decode, batch_size=16, batch_format="numpy", concurrency=2,
            fn_kwargs={"telemetry_directory": args.telemetry_directory, "split": split})

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config={"epochs": args.epochs, "batch_size": args.batch_size,
                           "telemetry_directory": args.telemetry_directory},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        datasets={"train": dataset("train"), "validation": dataset("validation")},
        dataset_config=DataConfig(datasets_to_split=["train"]),
    )
    print(trainer.fit())


if __name__ == "__main__":
    main()
