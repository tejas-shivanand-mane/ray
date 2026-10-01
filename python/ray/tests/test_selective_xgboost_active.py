"""Rollback invariants; actual native fault handling is checked by the wrapper."""

from contextlib import nullcontext
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from ray.experimental.recovery import _xgboost_active as active
from ray.experimental.recovery import _xgboost_boundary as boundary


@pytest.fixture
def prepared(monkeypatch):
    monkeypatch.setattr(boundary.ray, "get_runtime_context", lambda: SimpleNamespace(
        get_actor_id=lambda: "actor-1", get_worker_id=lambda: "worker-1", get_node_id=lambda: "node-1"))
    monkeypatch.setattr(active.xgb.collective, "CommunicatorContext", lambda **kwargs: nullcontext())
    monkeypatch.setattr(active.xgb.collective, "get_rank", lambda: 1)
    monkeypatch.setattr(active.xgb.collective, "get_world_size", lambda: 2)
    monkeypatch.setattr(active.xgb.collective, "is_distributed", lambda: False)
    worker = active.ActiveWorker(1)
    frame = pd.DataFrame({"x": np.arange(32, dtype=np.float32), "labels": [0] * 16 + [1] * 16})
    worker.prepare(frame, boundary.fingerprint(frame))
    checkpoint = worker.train_segment({}, None, 0, 3, 1)["model"]
    return worker, checkpoint


def fail_second_allreduce(monkeypatch):
    calls = []

    def allreduce(values, operation):
        calls.append(len(values))
        if len(calls) == 2:
            raise active.xgb.core.XGBoostError("peer connection closed")
        return values * 2

    monkeypatch.setattr(active.xgb.collective, "allreduce", allreduce)
    return calls


def test_interrupted_training_rolls_back_real_speculative_tree(prepared, monkeypatch, tmp_path):
    worker, checkpoint = prepared
    before = worker.identity()
    cached = worker.frame
    digest = boundary.tree_digest(worker.model)
    calls = fail_second_allreduce(monkeypatch)
    result = worker.interrupt_segment({}, checkpoint, 3, 2, str(tmp_path))
    assert calls == [2, 1024]
    assert result["discarded_rounds"] == 1
    assert worker.model.num_boosted_rounds() == 3
    assert boundary.tree_digest(worker.model) == digest
    assert worker.frame is cached and worker.identity() == before
    assert worker.matrix is None
    gate = json.loads((tmp_path / "rank-1-allreduce-enter.json").read_text())
    assert gate["round"] == 4
    resumed = worker.train_segment({}, checkpoint, 3, 6, 3)
    assert resumed["first_round"] == 4
    assert resumed["end_round"] == 6
    assert boundary.tree_digest(worker.model[:3]) == digest
    with pytest.raises(ValueError, match="generation"):
        worker.interrupt_segment({}, checkpoint, 3, 2, str(tmp_path))


@pytest.mark.parametrize("cleared", [False, True])
def test_finalize_error_requires_cleared_native_state(prepared, monkeypatch, tmp_path, cleared):
    worker, checkpoint = prepared
    state = {"distributed": False}

    class Context:
        def __enter__(self):
            state["distributed"] = True

        def __exit__(self, *args):
            state["distributed"] = not cleared
            raise active.xgb.core.XGBoostError("shutdown failed")

    monkeypatch.setattr(active.xgb.collective, "CommunicatorContext", lambda **kwargs: Context())
    monkeypatch.setattr(active.xgb.collective, "is_distributed", lambda: state["distributed"])
    fail_second_allreduce(monkeypatch)
    if cleared:
        result = worker.interrupt_segment({}, checkpoint, 3, 2, str(tmp_path))
        assert result["finalize_error"] == "shutdown failed"
        assert result["communicator_cleared"] is True
    else:
        with pytest.raises(ValueError, match="safely reused"):
            worker.interrupt_segment({}, checkpoint, 3, 2, str(tmp_path))


@pytest.mark.parametrize("failure", ["before_gate", "unexpected_success"])
def test_unrelated_error_or_success_is_not_active_recovery(prepared, monkeypatch, tmp_path, failure):
    worker, checkpoint = prepared

    def allreduce(values, operation):
        if failure == "before_gate":
            raise active.xgb.core.XGBoostError("bootstrap failure")
        return values * 2

    monkeypatch.setattr(active.xgb.collective, "allreduce", allreduce)
    with pytest.raises(ValueError):
        worker.interrupt_segment({}, checkpoint, 3, 2, str(tmp_path))
    assert not (tmp_path / "rank-1-rolled-back.json").exists()


@pytest.mark.parametrize("field,value", [("communicator_cleared", False), ("allreduce_error", ""),
    ("discarded_rounds", 0), ("restored_tree_sha256", "changed"), ("generation", 1),
    ("checkpoint_round", 4), ("identity", {"pid": 999})])
def test_cleanup_rejects_incomplete_or_replaced_survivor(field, value):
    identity = {"pid": 123}
    result = {"identity": identity, "generation": 2, "checkpoint_round": 3,
              "discarded_rounds": 1, "restored_tree_sha256": "checkpoint",
              "allreduce_error": "peer died", "communicator_cleared": True}
    result[field] = value
    with pytest.raises(ValueError, match="same process"):
        active.validate_cleanup(result, identity, 3, "checkpoint", 2)


def test_comparison_rejects_mixed_fault_timing(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    import run_selective_xgboost_comparison as runner

    provenance = {key: "same" for key in ("source_sha256", "native_extension_sha256", "python",
        "platform", "xgboost_version", "ray_version", "numpy_version", "pyarrow_version", "pandas_version")}
    full = {"status": "passed", "provenance": provenance, "native_settings": {}, "failure_timing": "boundary"}
    selective = {**full, "failure_timing": "active"}
    with pytest.raises(ValueError, match="fault timing"):
        runner.compare_pair(full, selective)
