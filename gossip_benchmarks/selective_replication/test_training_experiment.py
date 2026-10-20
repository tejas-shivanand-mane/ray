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


def test_fault_schedule_covers_short_trial_and_keeps_holdout_separate():
    from types import SimpleNamespace

    from training_experiment import failure_timing, make_config, scenarios

    config = make_config(
        SimpleNamespace(
            smoke=None,
            native_workers=None,
            checkpoint_delay_s=None,
            target_seconds=None,
            repeats=None,
            profile_repeats=None,
        )
    )
    # Regression: A finished at 3.31s, while the old 13.33s window first
    # injected loss at 4s and therefore never interrupted A on the fast worker.
    measured = dict(
        tasks={"trial-a": dict(epochs=336)},
        estimates={
            "trial-a": {
                "spot_a": dict(init_s=0.00257, epoch_s=0.0096427, save_s=0.001664)
            }
        },
    )
    timing = failure_timing(config, measured, 13.33068)
    early = [p["time_s"] for p in timing["replay"] if p["band"] == "early"]
    assert len(early) == 2 and all(0 < t < 3.31 for t in early)
    assert [
        p["time_s"] for p in timing["replay"] if p["band"] == "late"
    ] == pytest.approx([9.331476])
    planned = {p["time_s"] for p in timing["planning"]}
    heldout = {p["time_s"] for p in timing["replay"]}
    assert planned.isdisjoint(heldout)
    for phase in ("planning", "replay"):
        rows = scenarios(config, timing[phase])
        assert sum(r["probability"] for r in rows) == pytest.approx(1)
        assert sum(
            r["probability"] for r in rows if r.get("timing_band") == "early"
        ) == pytest.approx(1 / 3)
        for name, expected in config["probabilities"].items():
            selected = [r for r in rows if r["name"].split("-")[0] == name]
            assert sum(r["probability"] for r in selected) == pytest.approx(expected)
        for point in timing[phase]:
            failures = [
                r["failures"]
                for r in rows
                if r["failures"]
                and next(iter(r["failures"].values())) == point["time_s"]
            ]
            assert len(failures) == 4
            assert all(set(f.values()) == {point["time_s"]} for f in failures)


def test_fault_exposure_does_not_count_idle_or_uninjected_losses_as_recovery():
    from training_experiment import fault_exposure

    rows = [
        dict(
            job=dict(
                scenario=dict(timing_band="early", failures={"spot_a": 1, "spot_b": 1})
            ),
            result=dict(fault_evidence={"spot_a": dict(active_task=None)}),
        )
    ]
    exposure = fault_exposure(rows)
    assert exposure["early_trial_a_check"] == "not_exercised"
    assert (
        exposure["active"] == 0
        and exposure["idle"] == 1
        and exposure["not_injected"] == 1
    )
    rows.append(
        dict(
            job=dict(scenario=dict(timing_band="early", failures={"spot_a": 2})),
            result=dict(fault_evidence={"spot_a": dict(active_task="trial-a")}),
        )
    )
    exposure = fault_exposure(rows)
    assert exposure["early_trial_a_check"] == "exercised"
    assert (
        exposure["trial_a"]
        == exposure["early_trial_a"]
        == exposure["early_active"]
        == 1
    )
