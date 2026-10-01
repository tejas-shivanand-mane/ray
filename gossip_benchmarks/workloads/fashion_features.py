"""Frozen pretrained CPU image features followed by checkpointed MLP training.

Preparation downloads weights only. Every workload invocation computes features
from all original images; no persisted training features are reused. Recovery
policies and fault injection are exclusively configured by the external harness.
"""

import argparse
import hashlib
import json
from pathlib import Path
import runpy
import time

import numpy as np
import torch
import torchvision
from torchvision.models import MobileNet_V3_Small_Weights, mobilenet_v3_small

import ray
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer

# Load the existing checkpointed application functions by value for Ray workers.
application = runpy.run_path(str(Path(__file__).with_name("fashion_mnist.py")), run_name="fashion_feature_training")
make_model, train_loop = application["make_model"], application["train_loop"]
_BACKBONES = {}


def prepare_weights(directory):
    directory.mkdir(parents=True, exist_ok=True)
    state = MobileNet_V3_Small_Weights.IMAGENET1K_V1.get_state_dict(progress=True, check_hash=True)
    path = directory / "mobilenet-v3-small.pt"
    temporary = path.with_suffix(".tmp")
    torch.save(state, temporary)
    temporary.replace(path)
    manifest = {"model": "MobileNet_V3_Small_Weights.IMAGENET1K_V1", "features": 576,
                "torchvision_version": torchvision.__version__, "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "transform": "official weights transform: RGB, resize 256, center crop 224, ImageNet normalization"}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Prepared frozen feature weights: {directory}")


def extract_features(batch, weights_path):
    # A read-only per-process cache is an optimization of a deterministic task
    # function, not an actor or mutable application state that needs recovery.
    torch.set_num_threads(1)
    if weights_path not in _BACKBONES:
        model = mobilenet_v3_small(weights=None)
        model.load_state_dict(torch.load(weights_path, map_location="cpu", weights_only=True))
        model.eval()
        _BACKBONES[weights_path] = model
    images = np.stack(batch["image"]).astype(np.uint8).reshape(-1, 1, 28, 28)
    images = torch.from_numpy(images).expand(-1, 3, -1, -1)
    with torch.inference_mode():
        images = MobileNet_V3_Small_Weights.IMAGENET1K_V1.transforms()(images)
        model = _BACKBONES[weights_path]
        features = torch.flatten(model.avgpool(model.features(images)), 1).numpy()
    if features.shape != (len(images), 576) or not np.isfinite(features).all():
        raise ValueError("Invalid frozen image features")
    return {"x": features, "y": batch["label"].astype(np.int64)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-directory", type=Path, required=True)
    parser.add_argument("--prepare-weights", action="store_true")
    parser.add_argument("--data-directory", type=Path)
    parser.add_argument("--validation-output", type=Path)
    parser.add_argument("--epochs", type=int, default=8)
    args = parser.parse_args()
    if args.prepare_weights:
        prepare_weights(args.feature_directory)
        return
    if args.data_directory is None or args.validation_output is None or args.epochs < 2:
        parser.error("Supply data directory, validation output and at least two epochs")
    manifest = json.loads((args.feature_directory / "manifest.json").read_text())
    if manifest["torchvision_version"] != torchvision.__version__:
        raise ValueError("Prepare weights with the torchvision version used for this run")
    ray.init()
    started = time.monotonic_ns()

    def dataset(name, blocks):
        return ray.data.read_parquet(str(args.data_directory / f"{name}.parquet"), override_num_blocks=blocks).map_batches(
            extract_features, batch_size=64, batch_format="numpy", num_cpus=1,
            fn_kwargs={"weights_path": str(args.feature_directory / "mobilenet-v3-small.pt")},
        )

    features = dataset("train", 4).materialize()
    if features.count() != 60000:
        raise ValueError("Expected frozen features for every training image")
    args.validation_output.parent.mkdir(parents=True, exist_ok=True)
    # Small audit record; no durable training features or restart checkpoint.
    progress = args.validation_output.parent / "feature-progress.json"
    temporary = progress.with_suffix(".tmp")
    temporary.write_text(json.dumps({"feature_started_ns": started, "feature_ready_ns": time.monotonic_ns(),
                                     "training_images": 60000, "feature_width": 576}))
    temporary.replace(progress)
    training = features.repartition(4).random_shuffle(seed=0).materialize()
    validation = dataset("test", 2).materialize()
    batches = list(validation.iter_batches(batch_size=512, batch_format="numpy"))
    np.savez(args.validation_output, x=np.concatenate([b["x"] for b in batches]),
             y=np.concatenate([b["y"] for b in batches]))
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop, train_loop_config={"epochs": args.epochs, "input_features": 576},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        datasets={"train": training, "validation": validation},
        dataset_config=DataConfig(datasets_to_split=["train"]),
    )
    print(trainer.fit())


if __name__ == "__main__":
    main()
