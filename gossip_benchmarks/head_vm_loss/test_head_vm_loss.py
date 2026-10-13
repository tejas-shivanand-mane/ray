"""Offline protocol tests. These do not replace the live GCP smoke suite."""

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from evidence import contract_errors, load_events, write_json
from launcher import HeadVMLoss


def make_launcher(tmp_path):
    ident = "rayexp-test"
    names = [ident + "-head"] + [ident + f"-w{i}" for i in range(1, 5)]
    state = tmp_path / "state.json"
    write_json(
        state, dict(project="test", zone="us-central1-a", run_id=ident, names=names)
    )
    launcher = HeadVMLoss(state, tmp_path / "results")
    launcher.nodes = {
        name: dict(id=str(i), ip=f"10.0.0.{i+1}") for i, name in enumerate(names)
    }
    return launcher


def rows(fault=True, cfg=None):
    cfg = cfg or dict(steps=60, checkpoint_every=30, fault_step=49)
    result = []
    for rank in (0, 1):
        row = [dict(kind="start", base_step=0, state_digest="initial")]
        row += [
            dict(kind="update", step=s, attempt=0)
            for s in range(1, (49 if fault else 60) + 1)
        ]
        if fault:
            row += [dict(kind="start", base_step=30, state_digest="checkpoint")]
            row += [dict(kind="update", step=s, attempt=1) for s in range(31, 61)]
        result.append(row)
    return result


def test_checkpoint_restart_expects_rollback_in_both_arms():
    cfg = dict(steps=60, checkpoint_every=30, fault_step=49)
    cp = dict(step=30, state_digest="checkpoint")
    for fault in (False, True):
        assert contract_errors(rows(fault), cfg, fault, cp) == []


@pytest.mark.parametrize(
    "corruption", ["skip", "duplicate", "wrong_step", "optimizer", "extra_restart"]
)
def test_rejects_corrupt_restart_evidence(corruption):
    events = rows()
    if corruption == "skip":
        events[0].pop()
    elif corruption == "duplicate":
        events[0].append(dict(kind="update", step=60, attempt=1))
    elif corruption == "wrong_step":
        events[0][50]["base_step"] = 0
    elif corruption == "optimizer":
        events[0][50]["state_digest"] = "different optimizer"
    else:
        events[0].append(dict(kind="start", base_step=30, state_digest="checkpoint"))
    assert contract_errors(
        events,
        dict(steps=60, checkpoint_every=30, fault_step=49),
        True,
        dict(step=30, state_digest="checkpoint"),
    )


def test_partial_event_is_not_silently_ignored(tmp_path):
    (tmp_path / "attempt-0").mkdir()
    (tmp_path / "attempt-0/rank-0.jsonl").write_text('{"kind":')
    with pytest.raises(json.JSONDecodeError):
        load_events(tmp_path, 0)


def test_stop_vm_uses_cloud_not_service_and_saves_intent(tmp_path):
    exp = make_launcher(tmp_path)
    states = iter(["RUNNING", "TERMINATED"])
    exp.describe = lambda _: dict(status=next(states), id="0")
    calls, snapshots = [], []

    def gc(*args, **kwargs):
        assert snapshots[-1]["stop_requested_monotonic"] > 0
        calls.append(args)
        return SimpleNamespace(stdout='{"name":"stop-operation"}')

    exp.gc = gc
    sample = {}
    exp._stop_vm(sample, lambda: snapshots.append(copy.deepcopy(sample)))
    assert calls == [
        (
            "compute",
            "instances",
            "stop",
            exp.head,
            "--zone=us-central1-a",
            "--no-graceful-shutdown",
            "--async",
            "--format=json",
        )
    ]
    assert sample["instance_after"]["status"] == "TERMINATED"
    assert sample["stop_requested_monotonic"] <= sample["vm_terminated_monotonic"]


def test_uncertain_stop_preserves_intent_and_does_not_claim_loss(tmp_path):
    exp = make_launcher(tmp_path)
    exp.describe = lambda _: dict(status="RUNNING", id="0")
    exp.gc = lambda *a, **kw: (_ for _ in ()).throw(TimeoutError("API outcome unknown"))
    sample, saved = {}, []
    with pytest.raises(TimeoutError):
        exp._stop_vm(sample, lambda: saved.append(dict(sample)))
    assert saved and "stop_requested_monotonic" in saved[0]
    assert "vm_terminated_monotonic" not in sample


@pytest.mark.parametrize("foreign", ["label", "id", "name"])
def test_ownership_checks(tmp_path, foreign):
    exp = make_launcher(tmp_path)
    obj = dict(labels={"ray-experiment": exp.run_id}, id="0", status="RUNNING")
    if foreign == "label":
        obj["labels"]["ray-experiment"] = "somebody-else"
    elif foreign == "id":
        obj["id"] = "replacement-with-same-name"
    exp.gc = lambda *a, **kw: SimpleNamespace(stdout=json.dumps(obj))
    with pytest.raises((ValueError, RuntimeError)):
        exp.describe("unrecorded" if foreign == "name" else exp.head)


def mock_suite(exp, invalid=False):
    exp.ready = True
    write_json(exp.local / "environment.json", {})
    states = {n: "RUNNING" for n in exp.names}
    actions = []
    exp.describe = lambda n: dict(
        status=states[n],
        id=exp.nodes[n]["id"],
        networkInterfaces=[{"networkIP": exp.nodes[n]["ip"]}],
    )

    def gc(*args, **kwargs):
        actions.append(args)
        if args[:3] == ("compute", "instances", "stop"):
            states[exp.head] = "TERMINATED"
        if args[:3] == ("compute", "instances", "start"):
            states[exp.head] = "RUNNING"
        return SimpleNamespace(stdout='{"name":"operation"}', returncode=0)

    exp.gc = gc

    def ssh(name, command, **kwargs):
        if "from evidence import validate" in command:
            actions.append(("validate",))
            return SimpleNamespace(
                stdout=json.dumps({"valid": not invalid}), returncode=0
            )
        return SimpleNamespace(stdout="", returncode=0)

    exp.ssh = ssh
    exp._stop_node = lambda n: actions.append(("fence", n))
    exp._start_cluster = (
        lambda n: actions.append(("cluster", n)) or exp.nodes[n]["ip"] + ":6379"
    )

    def launch(cfg):
        actions.append(("launch", cfg["mode"], cfg["fault"], cfg["attempt"]))
        if cfg["attempt"]:
            assert states[exp.head] == "TERMINATED"
            assert cfg["checkpoint"]["step"] == 30

    exp._launch = launch
    exp._upload_json = lambda value, remote: None
    exp._wait = lambda cfg, gate=False: None

    def remote_json(path):
        if "error" in path:
            return None
        if "latest-checkpoint" in path:
            return dict(step=30, path="/surviving/checkpoint", state_digest="digest")
        return dict(step=60)

    exp._remote_json = remote_json
    exp.download = lambda name: exp.local / name
    return actions, states


def test_full_suite_keeps_head_off_until_validation(tmp_path):
    exp = make_launcher(tmp_path)
    actions, states = mock_suite(exp)
    result = exp.run_suite("smoke", repeats=2)
    assert result["status"] == "passed"
    assert len(result["samples"]) == 8
    assert states[exp.head] == "TERMINATED"
    assert sum(a[:3] == ("compute", "instances", "stop") for a in actions) == 4
    assert sum(a == ("cluster", exp.replacement) for a in actions) == 4
    # Original head restarts only after the preceding fault was validated.
    for i, action in enumerate(actions):
        if action[:3] == ("compute", "instances", "start"):
            prior_stop = max(
                j
                for j, a in enumerate(actions[:i])
                if a[:3] == ("compute", "instances", "stop")
            )
            assert ("validate",) in actions[prior_stop:i]
    with pytest.raises(FileExistsError):
        exp.run_suite("smoke")


def test_invalid_result_is_saved_and_not_retried(tmp_path):
    exp = make_launcher(tmp_path)
    actions, _ = mock_suite(exp, invalid=True)
    with pytest.raises(RuntimeError, match="validation failed"):
        exp.run_suite("bad")
    report = json.loads((exp.local / "bad/report.json").read_text())
    assert report["status"] == "failed"
    assert len(report["samples"]) == 1
    assert report["samples"][0]["status"] == "invalid"
    assert not any(a[:3] == ("compute", "instances", "stop") for a in actions)


def test_notebook_does_not_execute_resources_by_default():
    path = Path(__file__).parents[1] / "Ray_GCP_Head_VM_Loss.ipynb"
    notebook = json.loads(path.read_text())
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            compile(source, str(path), "exec")
            assert not cell["outputs"]
    content = path.read_text()
    assert "RUN_HEAD_LOSS = False" in content
    assert "rayexp-1013-194805-c4ad9f-state.json" in content


@pytest.mark.parametrize("corruption", [None, "input", "optimizer", "nan", "replicas"])
def test_tensor_and_input_validation(tmp_path, corruption):
    torch = pytest.importorskip("torch")
    from evidence import validate

    cfg = dict(
        steps=60,
        checkpoint_every=30,
        fault_step=49,
        training_ips=["10.0.0.1", "10.0.0.2"],
    )
    healthy, faulted = tmp_path / "healthy", tmp_path / "faulted"
    state = dict(
        model={"weight": torch.tensor([0.0, 1.0])},
        optimizer={
            "state": {
                0: {"step": torch.tensor(60.0), "exp_avg": torch.tensor([0.1, 0.2])}
            }
        },
        step=60,
    )
    for folder, is_fault in ((healthy, False), (faulted, True)):
        for attempt in range(int(is_fault) + 1):
            (folder / f"attempt-{attempt}").mkdir(parents=True)
        if is_fault:
            write_json(
                folder / "selected-checkpoint.json",
                dict(step=30, state_digest="checkpoint"),
            )
        for rank, events in enumerate(rows(is_fault)):
            attempt = -1
            for event in events:
                if event["kind"] == "start":
                    attempt += 1
                    event["ip"] = cfg["training_ips"][rank]
                event["attempt"] = attempt
                if event["kind"] == "update":
                    event["input_digest"] = f"rank-{rank}-step-{event['step']}"
                    if is_fault and corruption == "input" and event["step"] == 45:
                        event["input_digest"] = "wrong batch"
                with (folder / f"attempt-{attempt}/rank-{rank}.jsonl").open(
                    "a"
                ) as stream:
                    stream.write(json.dumps(event) + "\n")
            final = copy.deepcopy(state)
            if is_fault:
                if corruption == "optimizer":
                    final["optimizer"]["state"][0]["exp_avg"][0] += 0.01
                elif corruption == "nan":
                    final["model"]["weight"][0] = float("nan")
                elif corruption == "replicas":
                    # Each rank is within 1e-5 of reference, but not each other.
                    final["model"]["weight"][0] += 9e-6 if rank == 0 else -9e-6
            torch.save(final, folder / f"attempt-{int(is_fault)}/final-{rank}.pt")
    result = validate(faulted, healthy, cfg, True)
    assert result["valid"] == (corruption is None)
    assert result["repeated_updates_per_rank"] == [19, 19]
