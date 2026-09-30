"""Local invariants; the wrapper runs the actual two-rank collective probe."""

import copy
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from ray.experimental.recovery import _xgboost_boundary as boundary


def identity(rank, generation=0):
    return {"rank": rank, "actor_id": f"actor-{rank}-{generation}",
            "worker_id": f"worker-{rank}-{generation}", "pid": 100 + rank + 10 * generation,
            "node_id": f"node-{rank + 2 * generation}", "data_token": f"data-{rank}-{generation}",
            "loads": 1,
            "input": {"rows": 4, "hash_sum": 10 + rank, "hash_xor": rank, "columns": ["x", "labels"]}}


@pytest.mark.parametrize("policy,replaced,preserved", [("full", [0, 1], []), ("selective", [0], [1])])
def test_replacement_policy_checks_exact_survivor_identity(policy, replaced, preserved):
    before = [identity(0), identity(1)]
    after = [identity(rank, 1 if rank in replaced else 0) for rank in range(2)]
    result = boundary.validate_transition(before, after, 0, policy, {"node-0"})
    assert result == {"preserved_ranks": preserved, "replaced_ranks": replaced}


@pytest.mark.parametrize("gap", ["actor", "worker", "pid", "data", "loads", "input", "dead", "rank", "replacement"])
def test_selective_replacement_rejects_false_preservation(gap):
    before = [identity(0), identity(1)]
    after = [identity(0, 1), identity(1)]
    if gap in ("actor", "worker", "pid", "data"):
        key = {"actor": "actor_id", "worker": "worker_id", "pid": "pid",
               "data": "data_token"}[gap]
        after[1][key] = "changed"
    elif gap == "loads":
        after[1]["loads"] = 2
    elif gap == "input":
        after[0]["input"]["hash_sum"] += 1
    elif gap == "dead":
        after[0]["node_id"] = "node-0"
    elif gap == "rank":
        after[1]["rank"] = 0
    else:
        after[0] = copy.deepcopy(before[0])
        after[0]["node_id"] = "node-2"
    with pytest.raises(ValueError):
        boundary.validate_transition(before, after, 0, "selective", {"node-0"})


@pytest.fixture
def worker(monkeypatch):
    monkeypatch.setattr(boundary.ray, "get_runtime_context", lambda: SimpleNamespace(
        get_actor_id=lambda: "actor", get_worker_id=lambda: "worker", get_node_id=lambda: "node"))
    frame = pd.DataFrame({"x": np.arange(16, dtype=np.float32), "labels": [0] * 8 + [1] * 8})
    worker = boundary.BoundaryWorker(0)
    worker.prepare(frame, boundary.fingerprint(frame))
    return worker, frame


def test_preparation_does_not_reload_healthy_input(worker):
    actor, frame = worker
    cached = actor.frame
    before = actor.identity()
    with pytest.raises(ValueError, match="new worker"):
        actor.prepare(frame, boundary.fingerprint(frame))
    assert actor.frame is cached
    assert actor.identity() == before


def test_segment_round_budget_prefix_and_cache_reuse_locally(worker, monkeypatch):
    actor, _ = worker
    # Exercise real local XGBoost model continuation without a Ray cluster.
    # Distributed correctness remains the two-worker wrapper's acceptance gate.
    monkeypatch.setattr(boundary.xgb.collective, "CommunicatorContext", lambda **kwargs: nullcontext())
    monkeypatch.setattr(boundary.xgb.collective, "get_rank", lambda: 0)
    monkeypatch.setattr(boundary.xgb.collective, "get_world_size", lambda: 2)
    cached = actor.frame
    before = actor.identity()
    first = actor.train_segment({}, None, 0, 3, 1)
    second = actor.train_segment({}, first["model"], 3, 6, 2)
    assert second["first_round"] == 4
    assert second["end_round"] == 6
    assert boundary.tree_digest(actor.model[:3]) == first["tree_sha256"]
    assert actor.matrix_builds == 2
    assert actor.frame is cached and actor.identity() == before
    with pytest.raises(ValueError, match="generation"):
        actor.train_segment({}, second["model"], 6, 10, 2)
    with pytest.raises(ValueError, match="segment start"):
        actor.train_segment({}, first["model"], 6, 10, 3)


@pytest.fixture
def runner(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    import run_selective_xgboost_comparison
    return run_selective_xgboost_comparison


def sample(directory):
    directory.mkdir()
    np.save(directory / "predictions.npy", np.array([.25, .75]))
    return {"directory": str(directory), "status": "passed", "native_settings": {"enabled": True},
            "training_s": 10., "prediction_s": 1., "pipeline_s": 11.,
            "provenance": {key: "same" for key in ("source_sha256", "native_extension_sha256", "python",
                "platform", "xgboost_version", "ray_version", "numpy_version", "pyarrow_version", "pandas_version")}}


@pytest.mark.parametrize("gap", [None, "predictions", "native", "source", "failed", "timing"])
def test_comparison_requires_matching_outputs_and_configuration(runner, tmp_path, gap):
    full = sample(tmp_path / "full")
    selective = sample(tmp_path / "selective")
    selective["training_s"] = 8.
    if gap is None:
        rows = runner.compare_pair(full, selective)
        assert rows[0]["selective_vs_full_pct"] == pytest.approx(-20)
        return
    if gap == "predictions":
        np.save(tmp_path / "selective/predictions.npy", np.array([.9, .1]))
    elif gap == "native":
        selective["native_settings"]["enabled"] = False
    elif gap == "source":
        selective["provenance"]["source_sha256"] = "different"
    elif gap == "failed":
        selective["status"] = "failed"
    else:
        selective["training_s"] = float("nan")
    with pytest.raises((ValueError, AssertionError)):
        runner.compare_pair(full, selective)
