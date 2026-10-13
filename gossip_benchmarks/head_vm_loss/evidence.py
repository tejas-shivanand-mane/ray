"""Durable evidence and strict validation for whole-job checkpoint restart."""

import hashlib
import json
import os
from pathlib import Path


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest_batch(batch):
    digest = hashlib.sha256()
    for key in ("id", "x", "y"):
        value = batch[key]
        digest.update(key.encode())
        digest.update(str(value.dtype).encode())
        digest.update(str(value.shape).encode())
        digest.update(value.tobytes())
    return digest.hexdigest()


def state_digest(value):
    """Hash model and optimizer tensors without serialization metadata."""
    digest = hashlib.sha256()

    def visit(item):
        if isinstance(item, dict):
            for key in sorted(item, key=str):
                digest.update(repr(key).encode())
                visit(item[key])
        elif isinstance(item, (tuple, list)):
            for child in item:
                visit(child)
        elif hasattr(item, "detach"):
            tensor = item.detach().cpu().contiguous()
            digest.update(str(tensor.dtype).encode())
            digest.update(str(tuple(tensor.shape)).encode())
            digest.update(tensor.numpy().tobytes())
        else:
            digest.update(repr(item).encode())

    visit(value)
    return digest.hexdigest()


def load_events(folder, rank):
    events = []
    for path in sorted(Path(folder).glob(f"attempt-*/rank-{rank}.jsonl")):
        for line in path.read_text().splitlines():
            # Partial writes are evidence of interruption, never silently ignored.
            events.append(json.loads(line))
    return events


def contract_errors(events, cfg, fault, checkpoint):
    errors = []
    base = checkpoint["step"] if fault else 0
    for rank, rows in enumerate(events):
        if any(e.get("attempt") not in (0, 1) for e in rows if e["kind"] == "update"):
            errors.append(f"rank {rank}: unexpected attempt")
        starts = [e for e in rows if e["kind"] == "start"]
        if [e["base_step"] for e in starts] != ([0, base] if fault else [0]):
            errors.append(f"rank {rank}: wrong initialization or restart count")
        if fault and (
            len(starts) != 2 or starts[-1]["state_digest"] != checkpoint["state_digest"]
        ):
            errors.append(f"rank {rank}: checkpoint model/optimizer mismatch")
        before = [
            e["step"] for e in rows if e["kind"] == "update" and e["attempt"] == 0
        ]
        after = [e["step"] for e in rows if e["kind"] == "update" and e["attempt"] == 1]
        # The gate is held until the old workers are stopped. No hidden progress.
        if before != list(range(1, (cfg["fault_step"] if fault else cfg["steps"]) + 1)):
            errors.append(
                f"rank {rank}: incomplete or duplicate initial update sequence"
            )
        if after != (list(range(base + 1, cfg["steps"] + 1)) if fault else []):
            errors.append(f"rank {rank}: incorrect resumed update sequence")
        if (
            fault
            and base
            != cfg["fault_step"] // cfg["checkpoint_every"] * cfg["checkpoint_every"]
        ):
            errors.append("wrong scheduled checkpoint selected")
    return errors


def validate(folder, reference, cfg, fault):
    import torch

    folder, reference = Path(folder), Path(reference)
    events = [load_events(folder, r) for r in (0, 1)]
    expected = [load_events(reference, r) for r in (0, 1)]
    checkpoint = (
        json.loads((folder / "selected-checkpoint.json").read_text()) if fault else {}
    )
    errors = contract_errors(events, cfg, fault, checkpoint)
    for rank in (0, 1):
        mapping = {
            e["step"]: e["input_digest"]
            for e in expected[rank]
            if e["kind"] == "update"
        }
        updates = [e for e in events[rank] if e["kind"] == "update"]
        if set(mapping) != set(range(1, cfg["steps"] + 1)) or any(
            mapping.get(e["step"]) != e["input_digest"] for e in updates
        ):
            errors.append(f"rank {rank}: input differs from healthy reference")
    final_attempt = int(fault)
    ref = torch.load(
        reference / "attempt-0/final-0.pt", map_location="cpu", weights_only=True
    )
    model_error, optimizer_error = 0.0, 0.0

    def compare(a, b):
        if isinstance(a, dict):
            if a.keys() != b.keys():
                raise ValueError("State keys differ")
            return max([compare(a[k], b[k]) for k in a] or [0.0])
        if isinstance(a, (tuple, list)):
            if len(a) != len(b):
                raise ValueError("State lengths differ")
            return max([compare(x, y) for x, y in zip(a, b)] or [0.0])
        if isinstance(a, torch.Tensor):
            if (
                a.shape != b.shape
                or not torch.isfinite(a).all()
                or not torch.isfinite(b).all()
            ):
                return float("inf")
            return float((a - b).abs().max()) if a.numel() else 0.0
        return 0.0 if a == b else float("inf")

    final_states = []
    for rank in (0, 1):
        current = torch.load(
            folder / f"attempt-{final_attempt}/final-{rank}.pt",
            map_location="cpu",
            weights_only=True,
        )
        final_states.append(current)
        model_error = max(model_error, compare(current["model"], ref["model"]))
        optimizer_error = max(
            optimizer_error, compare(current["optimizer"], ref["optimizer"])
        )
        if current["step"] != cfg["steps"]:
            errors.append(f"rank {rank}: wrong final step")
    if model_error > 1e-5 or optimizer_error > 1e-5:
        errors.append("Final model/optimizer exceeds absolute 1e-5 tolerance")
    replica_error = compare(final_states[0]["model"], final_states[1]["model"])
    replica_optimizer_error = compare(
        final_states[0]["optimizer"], final_states[1]["optimizer"]
    )
    if replica_error > 1e-5 or replica_optimizer_error > 1e-5:
        errors.append("Final replicas disagree at absolute 1e-5 tolerance")
    starts = [[e for e in rows if e["kind"] == "start"] for rows in events]
    for attempt in range(final_attempt + 1):
        placements = [s[attempt]["ip"] for s in starts if len(s) > attempt]
        if set(placements) != set(cfg["training_ips"]):
            errors.append(
                f"attempt {attempt}: training did not use both assigned worker VMs"
            )
    return dict(
        valid=not errors,
        errors=errors,
        max_parameter_error=model_error,
        max_optimizer_error=optimizer_error,
        max_replica_error=replica_error,
        max_replica_optimizer_error=replica_optimizer_error,
        repeated_updates_per_rank=[
            sum(e["kind"] == "update" for e in rows) - cfg["steps"] for rows in events
        ],
    )
