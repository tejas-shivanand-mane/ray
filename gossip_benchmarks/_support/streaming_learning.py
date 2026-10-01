"""Input identity, sample accounting and measured streaming evidence."""

import hashlib
import json
import math
from pathlib import Path
import runpy

import numpy as np


def input_identity(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("dataset") != "CIFAR-10" or manifest.get("encoding") != "PNG RGB 32x32":
        raise ValueError("Prepare the CIFAR-10 PNG subset first")
    for split, field, maximum in (("train", "training_rows", 50000),
                                  ("validation", "validation_rows", 10000)):
        ids = manifest["selected_ids"][split]
        if (not ids or len(ids) != manifest[field] or len(set(ids)) != len(ids)
                or any(type(i) is not int or not 0 <= i < maximum for i in ids)):
            raise ValueError(f"Invalid {split} sample identities")
        hashes = manifest["input_sha256"][split]
        if set(hashes) != set(map(str, ids)) or any(
                not isinstance(value, str) or len(value) != 64 for value in hashes.values()):
            raise ValueError(f"Missing {split} normalized input fingerprints")
        if sum(entry["rows"] for name, entry in manifest["files"].items()
               if name.startswith(split + "/")) != len(ids):
            raise ValueError(f"Wrong {split} row count")
    for name, entry in manifest["files"].items():
        path = directory / name
        if (len(Path(name).parts) != 2 or Path(name).parts[0] not in ("train", "validation")
                or path.suffix != ".parquet" or path.stat().st_size != entry["bytes"]
                or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]):
            raise ValueError(f"Dataset file identity mismatch: {name}")
    return manifest


def collect_evidence(directory):
    directory = Path(directory)
    return {
        "stream_events": [json.loads(p.read_text()) for p in sorted((directory / "stream-events").glob("*.json"))],
        "data_executions": [json.loads(p.read_text()) for p in sorted((directory / "data-executions").glob("*.json"))],
    }


def validate_epoch_samples(reports, manifest, epochs, batch_size, retry_epoch=0):
    if len(reports) != epochs:
        raise ValueError("Missing committed epochs")
    expected = sorted(manifest["selected_ids"]["train"])
    steps = len(expected) // (2 * batch_size)
    for epoch, report in enumerate(reports, 1):
        metrics = sorted(report["metrics"], key=lambda m: m["rank"])
        if len(metrics) != 2 or [m["rank"] for m in metrics] != [0, 1]:
            raise ValueError("Missing training ranks")
        if sorted(i for m in metrics for i in m["sample_ids"]) != expected:
            raise ValueError("Missing, duplicated or unexpected training images")
        for m in metrics:
            if m["input_sha256"] != [manifest["input_sha256"]["train"][str(i)] for i in m["sample_ids"]]:
                raise ValueError("Training image tensor or label differs from prepared input")
            if (m["epoch"] != epoch or m["resumed_from_epoch"] != (retry_epoch if epoch > retry_epoch else 0)
                    or m["train_rows"] != len(expected) // 2 or len(m["sample_ids"]) != m["train_rows"]
                    or m["optimizer_steps"] != steps
                    or not math.isfinite(m["training_loss"]) or m["training_loss"] < 0):
                raise ValueError("Invalid epoch progress, loss or checkpoint origin")
        first = metrics[0]
        if (first["validation_rows"] != manifest["validation_rows"]
                or not math.isfinite(first["accuracy"]) or not 0 <= first["accuracy"] <= 1
                or not math.isfinite(first["validation_loss"]) or first["validation_loss"] < 0):
            raise ValueError("Invalid held-out validation metrics")


def summarize_overlap(events, epochs, fault=None):
    """Producer completion between optimizer updates, using one-machine clocks.

    This proves temporal overlap of pipeline production and training progress,
    not that a CPU kernel and decode instruction execute at the same instant.
    Retried epochs include both attempts; event invocation IDs retain the detail.
    """
    producers = [e for e in events if e["kind"] == "decode" and e["split"] == "train"]
    result = []
    for epoch in range(1, epochs + 1):
        updates = [e for e in events if e["kind"] == "update" and e["epoch"] == epoch]
        if not updates or {e["rank"] for e in updates} != {0, 1}:
            raise ValueError("Missing optimizer telemetry")
        first, last = min(e["time_ns"] for e in updates), max(e["time_ns"] for e in updates)
        overlapping = [e for e in producers if first < e["finished_ns"] < last]
        result.append({"epoch": epoch, "first_update_ns": first, "last_update_ns": last,
                       "decode_calls_between_updates": len(overlapping), "overlap_observed": bool(overlapping)})
    interrupted = result[fault["report_number"]] if fault else None
    return {"epochs": result,
            "every_epoch_overlapped": all(e["overlap_observed"] for e in result),
            "decode_calls_started_after_fault_in_interrupted_epoch": (
                sum(fault["request_ns"] < e["started_ns"] <= e["finished_ns"]
                    < interrupted["last_update_ns"] for e in producers)
                if fault else None)}


def validate_learning(directory, options, diagnostics):
    import pyarrow.parquet as pq
    import torch

    manifest = input_identity(options["data_directory"])
    if manifest != diagnostics["input_identity"]:
        raise ValueError("Input changed during observation")
    epochs, reports = options["training_epochs"], diagnostics["reports"]
    retry_epoch = options["fault_after_epoch"] if len(diagnostics["groups"]) == 2 else 0
    validate_epoch_samples(reports, manifest, epochs, options["batch_size"], retry_epoch)
    if any(len(g) != 2 or len({w["node_id"] for w in g}) != 2 for g in diagnostics["groups"]):
        raise ValueError("Expected two training workers on distinct logical nodes")
    overlap = summarize_overlap(diagnostics["stream_events"], epochs, diagnostics.get("node_fault"))
    diagnostics["streaming_overlap"] = overlap
    if not overlap["every_epoch_overlapped"]:
        raise ValueError("Data production did not overlap optimizer progress in every epoch; inspect telemetry before scaling the run")
    operators = [op for execution in diagnostics["data_executions"] for op in execution["operators"]]
    diagnostics["fixed_r_enrolled_tasks"] = sum(op.get("fixed_r_enrolled_tasks", 0) for op in operators)
    diagnostics["fixed_r_recovered_tasks"] = sum(op.get("fixed_r_recovered_tasks", 0) for op in operators)
    if options["mode"] == "on" and diagnostics["fixed_r_enrolled_tasks"] == 0:
        raise ValueError("ON run lacks Fixed-R enrollment evidence")
    if options["mode"] == "on" and options.get("profile_fixed_r"):
        protected = [op for op in operators if op.get("fixed_r_enrolled_tasks", 0)]
        if not protected or any(
                not op.get("fixed_r_timing_enabled")
                or op.get("fixed_r_timing_submission_count", 0) < op["fixed_r_enrolled_tasks"]
                or op.get("fixed_r_timing_submission_s", 0) <= 0
                for op in protected):
            raise ValueError("Requested Fixed-R runtime timing evidence is missing")
    application = runpy.run_path(options["workload"], run_name="cifar_probe")
    state = torch.load(directory / "final-checkpoint/training.pt", map_location="cpu", weights_only=True)
    if state["epoch"] != epochs or not state["optimizer"]["state"]:
        raise ValueError("Final checkpoint lacks completed model/Adam state")
    torch.set_num_threads(1)
    model = application["make_model"]()
    model.load_state_dict(state["model"])
    model.eval()
    logits, labels = [], []
    for name in sorted(manifest["files"]):
        if not name.startswith("validation/"):
            continue
        batch = pq.read_table(Path(options["data_directory"]) / name).to_pydict()
        values = application["decode"](batch)
        with torch.no_grad():
            for start in range(0, len(values["y"]), options["batch_size"]):
                logits.append(model(torch.from_numpy(values["x"][start:start + options["batch_size"]])).numpy())
        labels.extend(values["y"].tolist())
    logits = np.concatenate(logits)
    if logits.shape != (manifest["validation_rows"], 10) or not np.isfinite(logits).all():
        raise ValueError("Invalid checkpoint predictions")
    accuracy = float((logits.argmax(1) == labels).mean())
    final = next(m for m in reports[-1]["metrics"] if m["rank"] == 0)
    if not math.isclose(accuracy, final["accuracy"], abs_tol=1e-12):
        raise ValueError("Saved checkpoint disagrees with reported validation accuracy")
    np.save(directory / "predictions.npy", logits)
    diagnostics.update(final_accuracy=accuracy, model_parameters=sum(p.numel() for p in model.parameters()),
                       training_rows_per_epoch=manifest["training_rows"],
                       validation_rows_per_epoch=manifest["validation_rows"],
                       optimizer_steps_per_worker_per_epoch=options["steps_per_epoch"],
                       checkpoint_policy="application model, Adam, epoch and per-rank CPU RNG every epoch",
                       batch_size=options["batch_size"])


def compare_samples(left, right, *, control=False):
    """Match useful work and configuration; do not demand identical SGD trajectories."""
    from fashion_comparison import PROVENANCE_KEYS

    if any(s["status"] != "passed" or not s.get("workload_completed") or s.get("timeout")
           for s in (left, right)):
        raise ValueError("Both observations must pass completion and evidence checks")
    if left.get("profile_fixed_r", False) != right.get("profile_fixed_r", False):
        raise ValueError("Mismatched runtime profiling configuration")
    for key in ("training_epochs", "input_identity", "workload_sha256", "torch_version", "torchvision_version",
                "owner_placement", "placement_strategy", "model_parameters", "batch_size", "checkpoint_policy"):
        if left[key] != right[key]:
            raise ValueError(f"Mismatched {key}")
    for key in PROVENANCE_KEYS:
        if left["provenance"][key] != right["provenance"][key]:
            raise ValueError(f"Mismatched provenance: {key}")
    if left["restart_scope"] != "full" or right["restart_scope"] != "full":
        raise ValueError("This comparison isolates Fixed-R with standard Train retry in both arms")
    if left["owner_placement"] != "default" or left["placement_strategy"] != "STRICT_SPREAD":
        raise ValueError("Expected default ownership and spread training workers")
    if not left["native_settings"] or left["native_settings"].keys() != right["native_settings"].keys():
        raise ValueError("Missing matched native settings")
    if control:
        if right["scenario"] != "none" or left["mode"] != right["mode"]:
            raise ValueError("Control must use the same recovery mode")
        if left["native_settings"] != right["native_settings"]:
            raise ValueError("Control native settings differ")
    else:
        if (left["mode"], right["mode"]) != ("off", "on"):
            raise ValueError("Expected OFF versus ON")
        for key in ("scenario", "failure_point", "fault_after_epoch", "fault_after_step"):
            if left[key] != right[key]:
                raise ValueError(f"Mismatched {key}")
        for key, value in left["native_settings"].items():
            other = right["native_settings"][key]
            if ((key.startswith("enable_") and (value is not False or other is not True))
                    or (not key.startswith("enable_") and value != other)):
                raise ValueError(f"Mismatched native setting: {key}")
    if not all(math.isfinite(s["workload_s"]) and s["workload_s"] > 0 for s in (left, right)):
        raise ValueError("Invalid completed timings")
    return {"accuracy_difference_pp": 100 * (right["final_accuracy"] - left["final_accuracy"]),
            "workload_s_change_pct": 100 * (right["workload_s"] / left["workload_s"] - 1),
            "prediction_equivalence_claimed": False}
