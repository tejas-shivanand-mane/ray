"""Check that controlled execution changes timing without changing learning state."""

import numpy as np
import pytest
from workload import execution_settings, train, validate_state


def test_paced_training_and_resume_preserve_state(tmp_path):
    rng = np.random.default_rng(9)
    data = dict(
        x=rng.normal(size=(32, 64)).astype("float32"),
        y=np.arange(32, dtype="int64") % 10,
        ids=np.arange(32),
        test_x=rng.normal(size=(8, 64)).astype("float32"),
        test_y=np.arange(8, dtype="int64"),
    )
    task = dict(
        width=8,
        seed=9,
        lr=0.01,
        epochs=4,
        checkpoint_every=2,
        batch_size=8,
        dtype="float64",
    )
    ordinary = []
    train(task, data, None, tmp_path / "ordinary", ordinary.append, lambda: False)
    paced = []
    train(
        task,
        data,
        None,
        tmp_path / "paced",
        paced.append,
        lambda: False,
        execution=dict(compute_scale=2, checkpoint_delay_s=0.001),
    )
    assert validate_state(paced[-1]["path"], ordinary[-1]["path"]) == 0
    assert paced[-1]["compute_pacing_s"] > 0
    checkpoint = next(
        e["path"] for e in paced if e["kind"] == "checkpoint" and e["epoch"] == 2
    )
    restored = []
    train(
        dict(task, checkpoint_every=4),
        data,
        checkpoint,
        tmp_path / "restored",
        restored.append,
        lambda: False,
        execution=dict(compute_scale=1.5),
    )
    assert validate_state(restored[-1]["path"], ordinary[-1]["path"]) == 0
    # A copy canceled during storage delay must never publish the checkpoint.
    cancelled = []
    folder = tmp_path / "cancelled"
    train(
        task,
        data,
        None,
        folder,
        cancelled.append,
        lambda: (folder / "epoch-2.tmp").exists(),
        execution=dict(checkpoint_delay_s=0.1),
    )
    assert not (folder / "epoch-2.pt").exists()
    assert not any(e["kind"] in ("checkpoint", "result") for e in cancelled)


@pytest.mark.parametrize(
    "settings",
    [
        dict(compute_scale=0.5),
        dict(compute_scale=float("nan")),
        dict(checkpoint_delay_s=-1),
        dict(checkpoint_delay_s=float("inf")),
    ],
)
def test_invalid_pacing_is_rejected(settings):
    with pytest.raises(ValueError):
        execution_settings(settings)
