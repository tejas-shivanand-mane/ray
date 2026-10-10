"""Evidence checks for the full-dataset Fashion-MNIST comparison."""

import hashlib
import json
import math
from pathlib import Path
import runpy

import numpy as np


PROVENANCE_KEYS = (
    "source_sha256", "native_extension_sha256", "python", "platform", "ray_version",
    "numpy_version", "pyarrow_version", "pandas_version", "xgboost_version",
)


def input_identity(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("dataset") != "Fashion-MNIST" or manifest.get("split") != "official":
        raise ValueError("Prepare the official Fashion-MNIST split first")
    expected = {"train.parquet": 60000, "test.parquet": 10000}
    if set(manifest["files"]) != set(expected):
        raise ValueError("Unexpected dataset files")
    for name, rows in expected.items():
        path, entry = directory / name, manifest["files"][name]
        if (entry["rows"] != rows or path.stat().st_size != entry["bytes"]
                or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]):
            raise ValueError(f"Dataset identity mismatch: {name}")
    return manifest


def feature_identity(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    path = directory / "mobilenet-v3-small.pt"
    if (manifest.get("model") != "MobileNet_V3_Small_Weights.IMAGENET1K_V1"
            or manifest.get("features") != 576
            or manifest.get("bytes") != path.stat().st_size
            or manifest.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest()):
        raise ValueError("Prepare and verify MobileNet feature weights first")
    return manifest


def validate_fashion(directory, options, diagnostics):
    import pyarrow.parquet as pq
    import torch

    epochs = options["training_epochs"]
    failure_epoch = options["fault_after_epoch"] if options["scenario"] != "none" else 0
    retry_epoch = failure_epoch if len(diagnostics["groups"]) == 2 else 0
    reports = diagnostics["reports"]
    if [r["metrics"][0]["epoch"] for r in reports] != list(range(1, epochs + 1)):
        raise ValueError("Training did not commit every requested epoch exactly once")
    for index, report in enumerate(reports):
        metrics = report["metrics"]
        resumed = retry_epoch if retry_epoch and index >= retry_epoch else 0
        if len(metrics) != 2 or any(
            m["epoch"] != index + 1 or m["resumed_from_epoch"] != resumed
            or m["train_rows"] != 30000 or m["optimizer_steps"] != 118 for m in metrics
        ):
            raise ValueError("Wrong epoch, restored progress, rows or optimizer steps")
        first = metrics[0]
        if (first["validation_rows"] != 10000 or not math.isfinite(first["accuracy"])
                or not 0 <= first["accuracy"] <= 1
                or not math.isfinite(first["validation_loss"]) or first["validation_loss"] < 0):
            raise ValueError("Invalid full-test-set validation")
    if failure_epoch and diagnostics["fault"].get("report_number") != failure_epoch:
        raise ValueError("Failure occurred at a different epoch than requested")
    if options.get("placement_strategy") == "STRICT_SPREAD":
        if any(len(g) != 2 or len({w["node_id"] for w in g}) != 2 for g in diagnostics["groups"]):
            raise ValueError("Training workers did not occupy separate logical nodes")
    if input_identity(Path(options["data_directory"])) != diagnostics["input_identity"]:
        raise ValueError("Input changed during observation")
    state = torch.load(directory / "final-checkpoint/training.pt", map_location="cpu", weights_only=True)
    if state["epoch"] != epochs or not state["optimizer"]["state"]:
        raise ValueError("Final checkpoint lacks model progress or Adam state")
    application = runpy.run_path(options["workload"], run_name="fashion_probe")
    features = options.get("feature_directory") is not None
    model = application["make_model"](576) if features else application["make_model"]()
    model.load_state_dict(state["model"])
    model.eval()
    torch.set_num_threads(1)
    table = pq.read_table(Path(options["data_directory"]) / "test.parquet")
    labels = np.asarray(table["label"].to_pylist(), dtype=np.int64)
    if features:
        if feature_identity(Path(options["feature_directory"])) != diagnostics["feature_identity"]:
            raise ValueError("Feature weights changed during observation")
        with np.load(directory / "validation-features.npz", allow_pickle=False) as arrays:
            pixels = arrays["x"]
            if pixels.shape != (10000, 576) or not np.array_equal(arrays["y"], labels):
                raise ValueError("Invalid exported full validation features or labels")
        if not np.isfinite(pixels).all():
            raise ValueError("Nonfinite validation features")
    else:
        pixels = np.asarray(table["image"].to_pylist(), dtype=np.float32) / 255.0
    with torch.no_grad():
        logits = np.concatenate([model(torch.from_numpy(pixels[start:start + 512])).numpy()
                                 for start in range(0, len(pixels), 512)])
    if logits.shape != (10000, 10) or not np.isfinite(logits).all():
        raise ValueError("Invalid final predictions")
    accuracy = float((logits.argmax(1) == labels).mean())
    if not math.isclose(accuracy, reports[-1]["metrics"][0]["accuracy"], abs_tol=1e-12):
        raise ValueError("Checkpoint predictions disagree with the final validation report")
    np.save(directory / "predictions.npy", logits)
    diagnostics.update(final_accuracy=accuracy, model_parameters=sum(p.numel() for p in model.parameters()),
                       training_rows_per_epoch=60000, validation_rows_per_epoch=10000,
                       optimizer_steps_per_worker_per_epoch=118,
                       checkpoint_policy="application model, Adam state, epoch and per-rank CPU RNG every epoch")


def predictions_match(left, right):
    a = np.load(Path(left["directory"]) / "predictions.npy", allow_pickle=False)
    b = np.load(Path(right["directory"]) / "predictions.npy", allow_pickle=False)
    if a.shape != (10000, 10) or b.shape != a.shape or not (np.isfinite(a).all() and np.isfinite(b).all()):
        raise ValueError("Missing finite predictions for the full test set")
    np.testing.assert_allclose(a, b, rtol=1e-5, atol=1e-7)
    return float(np.max(np.abs(a - b)))


def compare_pair(ordinary, integrated, comparison="integrated"):
    if comparison not in ("retry", "integrated"):
        raise ValueError("Unknown comparison policy")
    protected = comparison == "integrated"
    if any(s["status"] != "passed" or not s.get("workload_completed")
           or s.get("timeout") for s in (ordinary, integrated)):
        raise ValueError("Both workloads must complete and pass correctness checks")
    if (ordinary["mode"], ordinary["restart_scope"], integrated["mode"], integrated["restart_scope"]) != (
        "off", "full", "on" if protected else "off", "selective"
    ):
        raise ValueError("Unexpected Fixed-R mode or retry policy for comparison")
    for key in ("scenario", "failure_point", "fault_after_epoch", "training_epochs", "input_identity",
                "workload_sha256", "torch_version", "owner_placement", "model_parameters",
                "training_rows_per_epoch", "validation_rows_per_epoch", "checkpoint_policy"):
        if ordinary[key] != integrated[key]:
            raise ValueError(f"Mismatched {key}")
    if ordinary.get("placement_strategy") != integrated.get("placement_strategy"):
        raise ValueError("Mismatched training worker placement")
    for key in ("failure_timing", "fault_after_step"):
        if ordinary.get(key) != integrated.get(key):
            raise ValueError(f"Mismatched {key}")
    if ordinary["owner_placement"] != "default":
        raise ValueError("Training comparison uses default ownership in both arms")
    for key in PROVENANCE_KEYS:
        if ordinary["provenance"][key] != integrated["provenance"][key]:
            raise ValueError(f"Mismatched provenance: {key}")
    if not ordinary["native_settings"] or set(ordinary["native_settings"]) != set(integrated["native_settings"]):
        raise ValueError("Missing matched native settings")
    for key, value in ordinary["native_settings"].items():
        other = integrated["native_settings"][key]
        if ((key.startswith("enable_") and (value is not False or other is not protected))
                or (not key.startswith("enable_") and value != other)):
            raise ValueError(f"Mismatched native setting: {key}")
    if ordinary["scenario"] != "none":
        for sample in (ordinary, integrated):
            if not sample.get("matches_no_failure_predictions") or len(sample.get("recoveries", [])) != 1:
                raise ValueError("Missing recovery and no-failure correctness evidence")
            if sample["scenario"] in ("head-node", "worker-node"):
                fault = sample.get("node_fault", {})
                if (not fault.get("completed") or fault.get("scenario") != sample["scenario"]
                        or fault.get("report_number") != sample["fault_after_epoch"]
                        or sample.get("placement_strategy") != "STRICT_SPREAD"):
                    raise ValueError("Missing matched node-failure evidence")
    maximum_error = predictions_match(ordinary, integrated)
    values = {"predictions_max_abs_difference": maximum_error}
    for metric in ("workload_s", "training_s"):
        before, after = ordinary[metric], integrated[metric]
        if not all(math.isfinite(v) and v > 0 for v in (before, after)):
            raise ValueError(f"Invalid {metric}")
        values.update({f"ordinary_{metric}": before, f"integrated_{metric}": after,
                       f"{metric}_change_pct": 100 * (after / before - 1)})
    return values


def compare_control(sample, control):
    if control["status"] != "passed" or control["scenario"] != "none":
        raise ValueError("Recovery requires a passing no-failure control")
    for key in ("mode", "restart_scope", "training_epochs", "input_identity", "workload_sha256",
                "torch_version", "native_settings", "owner_placement", "checkpoint_policy"):
        if sample[key] != control[key]:
            raise ValueError(f"Control differs in {key}")
    for key in PROVENANCE_KEYS:
        if sample["provenance"][key] != control["provenance"][key]:
            raise ValueError(f"Control provenance differs in {key}")
    if sample.get("placement_strategy") != control.get("placement_strategy"):
        raise ValueError("Control differs in training worker placement")
    for key in ("failure_timing", "fault_after_step"):
        if sample.get(key) != control.get(key):
            raise ValueError(f"Control differs in {key}")
    error = predictions_match(sample, control)
    epoch = sample["fault_after_epoch"]
    recovery = sample["recoveries"][0]
    normal_interval = (control["reports"][epoch]["time_ns"] - control["reports"][epoch - 1]["time_ns"]) / 1e9
    if not math.isfinite(normal_interval) or normal_interval <= 0:
        raise ValueError("Invalid control epoch interval")
    sample.update(matches_no_failure_predictions=True, control_predictions_max_abs_difference=error,
                  workload_increase_vs_control_s=sample["workload_s"] - control["workload_s"])
    recovery.update(control_next_epoch_interval_s=normal_interval)
    if sample.get("failure_timing") == "active":
        if (recovery.get("failure_timing") != "active"
                or recovery.get("completed_uncheckpointed_steps_per_rank") != sample["fault_after_step"]):
            raise ValueError("Missing validated mid-epoch optimizer evidence")
        # A mid-epoch failure cannot be compared with a whole control epoch
        # starting at its boundary. Use the common committed-checkpoint origin.
        fault = sample["node_fault"]
        span = (sample["reports"][epoch]["time_ns"] - fault["checkpoint_committed_ns"]) / 1e9
        recovery.update(checkpoint_to_next_report_s=span,
                        checkpoint_interval_excess_vs_control_s=span - normal_interval)
    else:
        recovery.update(next_report_excess_vs_control_s=recovery["failure_to_next_report_s"] - normal_interval,
                        lost_uncommitted_optimizer_steps=None,
                        fault_boundary="after committed epoch; unfinished minibatch work is not measured")
