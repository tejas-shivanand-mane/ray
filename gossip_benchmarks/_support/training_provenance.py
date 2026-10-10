"""Source and loaded-extension identity for local training comparisons."""

import hashlib
import importlib.metadata
from pathlib import Path
import platform
import subprocess
import sys


SOURCE_PATHS = (
    "gossip_benchmarks/run_coordinator_training_comparison.py",
    "gossip_benchmarks/plot_coordinator_training.py",
    "gossip_benchmarks/validate_coordinator_training.sh",
    "gossip_benchmarks/_support/coordinator_comparison.py",
    "gossip_benchmarks/_support/coordinator_training.py",
    "python/ray/data/_internal/iterator",
    "gossip_benchmarks/run_streaming_learning_comparison.py",
    "gossip_benchmarks/plot_streaming_learning.py",
    "gossip_benchmarks/validate_streaming_learning.sh",
    "gossip_benchmarks/workloads/cifar_streaming.py",
    "gossip_benchmarks/_support/streaming_learning.py",
    "gossip_benchmarks/workloads/fashion_features.py",
    "gossip_benchmarks/run_fashion_owner_comparison.py",
    "gossip_benchmarks/plot_fashion_owner_recovery.py",
    "gossip_benchmarks/validate_fashion_owner.sh",
    "gossip_benchmarks/run_fashion_training_comparison.py",
    "gossip_benchmarks/plot_fashion_training.py",
    "gossip_benchmarks/validate_fashion_training.sh",
    "gossip_benchmarks/workloads/fashion_mnist.py",
    "gossip_benchmarks/_support/fashion_comparison.py",
    "gossip_benchmarks/plot_train_failure_progress.py",
    "gossip_benchmarks/run_train_retry_comparison.py",
    "gossip_benchmarks/validate_train_retry.sh",
    "gossip_benchmarks/_support/train_retry.py",
    "gossip_benchmarks/_support/train_workload.py",
    "python/ray/train/examples/pytorch/torch_regression_example.py",
    "gossip_benchmarks/run_selective_xgboost_comparison.py",
    "gossip_benchmarks/_support/selective_train.py",
    "gossip_benchmarks/_support/active_train.py",
    "gossip_benchmarks/run_fixed_r_train_comparison.py",
    "gossip_benchmarks/run_fixed_r_train_coverage.py",
    "gossip_benchmarks/_support/train_comparison.py",
    "gossip_benchmarks/_support/training_provenance.py",
    "release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py",
    "release/nightly_tests/dataset/streaming_recovery_progress.py",
    "python/ray/experimental/recovery",
    "python/ray/_private/streaming_recovery.py",
    "python/ray/data/context.py",
    "python/ray/data/dataset.py",
    "python/ray/data/_internal/execution",
    "python/ray/data/_internal/planner",
    "python/ray/data/_internal/split.py",
    "python/ray/data/datasource/file_based_datasource.py",
    "python/ray/train",
    "src/ray/core_worker/streaming_recovery_submission.cc",
    "src/ray/core_worker/streaming_recovery_replay.cc",
)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_provenance(root):
    root = Path(root)

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root).decode().strip()

    tracked = git("ls-files", "-z", "--", *SOURCE_PATHS).split("\0")
    # Include newly written, uncommitted comparison files too.
    paths = set(tracked) | {p for p in SOURCE_PATHS if (root / p).is_file()}
    hashes = {p: file_sha256(root / p) for p in sorted(paths) if p and (root / p).is_file()}
    missing = sorted(p for p in tracked if p and not (root / p).is_file())
    digest = hashlib.sha256()
    for path, value in hashes.items():
        digest.update(f"{path}\0{value}\n".encode())
    return {
        "git_commit": git("rev-parse", "HEAD"),
        "working_tree_status": git("status", "--porcelain", "--untracked-files=normal"),
        "source_sha256": digest.hexdigest(), "source_hashes": hashes,
        "unavailable_tracked_sources": missing,
        "python": sys.version, "platform": platform.platform(),
    }


def runtime_provenance(root):
    import ray
    import ray._raylet
    import xgboost

    ray_path = Path(ray.__file__).resolve()
    extension = Path(ray._raylet.__file__).resolve()
    result = {
        "ray_version": ray.__version__, "ray_path": str(ray_path),
        "ray_from_checkout": ray_path == (Path(root) / "python/ray/__init__.py").resolve(),
        "native_extension_path": str(extension),
        "native_extension_sha256": file_sha256(extension),
        # Identifying a binary does not prove which source revision built it.
        "native_source_match_verified": False,
        "xgboost_version": xgboost.__version__,
    }
    for package in ("numpy", "pyarrow", "pandas"):
        result[f"{package}_version"] = importlib.metadata.version(package)
    return result
