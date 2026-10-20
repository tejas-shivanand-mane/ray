import copy
import json
import time
from types import SimpleNamespace

import pytest
from gcp.backend import TrainingActor, VMFaultInjector
from gcp.suite import assess, jobs, paired_comparisons, smoke_jobs
from workload import prepare_data, validate_state, write_json


def test_cloud_confirmation_required_and_failure_rejected(tmp_path):
    schedule = SimpleNamespace(active={"spot_a": "task"})
    injector = VMFaultInjector(
        None, {"spot_a": 0}, time.monotonic(), schedule, tmp_path
    )
    injector.thread.join(timeout=2)
    assert injector.snapshot() == {}
    injector.observe_loss("spot_a")
    write_json(
        tmp_path / "response-spot_a.json",
        dict(status="TERMINATED", instance_id="42", machine="spot_a"),
    )
    records = injector.snapshot()
    assert records["spot_a"]["active_task"] == "task"
    assert records["spot_a"]["active_at_actor_loss"] == "task"
    injector.verify(records)
    injector.close()

    second = tmp_path / "second"
    second.mkdir()
    bad = VMFaultInjector(None, {"spot_a": 0}, time.monotonic(), schedule, second)
    bad.thread.join(timeout=2)
    write_json(second / "response-spot_a.json", dict(status="RUNNING", error="denied"))
    with pytest.raises(RuntimeError, match="VM stop failed"):
        bad.snapshot()


def test_idle_faults_and_missing_restores_do_not_pass():
    row = dict(
        correct=True,
        fault_coverage_complete=True,
        scenario=dict(name="loss", failures={"spot_a": 0}),
        fault_evidence={
            "spot_a": dict(status="TERMINATED", instance_id="42", active_task=None)
        },
        restorations=[],
        policies=["checkpoint_retry"],
        case_index=0,
        repeat=0,
        elapsed_s=10,
        met_deadline=True,
        assumed_cost=1,
        worker_busy_s=10,
    )
    result = assess([row], 1)
    assert not result["valid"]
    assert not result["checks"]["active_single_vm_loss"]
    assert not result["checks"]["checkpoint_restoration_observed"]
    assert not assess([row], 2)["checks"]["all_runs_present"]


def test_matrix_deduplicates_plans_and_keeps_holdouts_separate():
    plan = dict(order=["a"], copies={"a": ["safe"]})
    case = dict(
        policies={
            "checkpoint_retry": dict(plan=plan),
            "joint_enumeration": dict(plan=plan),
        },
        unconstrained_policies={},
        scenarios=[dict(name="healthy", probability=1, failures={})],
    )
    searched = dict(cases=[case, copy.deepcopy(case)], parallel_lower_bound_s=100)
    result = jobs(searched, 3, policies=["checkpoint_retry", "joint_enumeration"])
    assert len(result) == 18
    assert {r["case_index"] for r in result} == {0, 1}
    assert all(len(r["policies"]) == 2 for r in result)
    assert all(
        r["scenario"]["probability"] is None
        for r in result
        if r["scenario"]["name"].startswith("holdout")
    )
    controls = smoke_jobs(searched)
    assert len(controls) == 6
    correlated = next(
        r for r in controls if r["scenario"]["name"] == "control-correlated"
    )
    assert set(correlated["scenario"]["failures"]) == {"spot_a", "spot_b"}


def test_paired_summary_does_not_claim_speedup_from_correctness():
    rows = []
    for repeat in range(5):
        for policy, elapsed in [("checkpoint_retry", 10), ("joint_enumeration", 12)]:
            rows.append(
                dict(
                    case_index=0,
                    scenario=dict(name="healthy"),
                    repeat=repeat,
                    policies=[policy],
                    elapsed_s=elapsed,
                    assumed_cost=elapsed,
                    worker_busy_s=elapsed,
                )
            )
    comparison = paired_comparisons(rows)[0]
    assert comparison["mean_percent_change"] == pytest.approx(20)
    assert comparison["bootstrap_95_percent"][0] > 0


def test_threaded_remote_actor_checkpoints_cancel_and_restore(tmp_path):
    # Exercise the actual training-thread / cancellation protocol without GCP.
    data, digest = prepare_data(tmp_path)
    actor = TrainingActor(data)
    task = dict(
        width=16,
        dtype="float64",
        epochs=20,
        checkpoint_every=2,
        seed=123,
        lr=0.03,
        batch_size=32,
        dataset_sha256=digest,
    )
    events = []

    def execute(token, folder, resume=None, cancel=False):
        actor.submit(
            dict(
                generation=token,
                name="task",
                task=task,
                resume=resume,
                folder=str(folder),
                parents=[],
            )
        )
        collected = []
        deadline = time.monotonic() + 30
        cancelled = False
        while time.monotonic() < deadline:
            batch = actor.drain()
            collected.extend(batch)
            if (
                cancel
                and not cancelled
                and any(e["kind"] == "checkpoint" for e in batch)
            ):
                actor.cancel()
                cancelled = True
            if any(e["kind"] == "idle" for e in batch):
                return collected
            time.sleep(0.001)
        raise TimeoutError("Actor did not acknowledge idle")

    events = execute(1, tmp_path / "cancelled", cancel=True)
    checkpoint = next(e["path"] for e in events if e["kind"] == "checkpoint")
    resumed = execute(2, tmp_path / "resumed", resume=checkpoint)
    healthy = execute(3, tmp_path / "healthy")
    assert next(e for e in resumed if e["kind"] == "start")["restored_epoch"] > 0
    assert (
        validate_state(
            next(e["path"] for e in resumed if e["kind"] == "result"),
            next(e["path"] for e in healthy if e["kind"] == "result"),
        )
        == 0
    )
    assert all(e["generation"] == 2 for e in resumed)
    start = next(e for e in resumed if e["kind"] == "start")
    assert start["threads"] == 1 and start["dtype"] == "float64"
    assert start["deterministic_algorithms"]


def test_cloud_stop_uses_vm_identity_and_never_a_service_kill(tmp_path):
    from gcp.launcher import GCPStudy

    cloud = object.__new__(GCPStudy)
    cloud.mapping = {"spot_a": "owned-worker"}
    cloud.stop_command = ["compute", "instances", "stop"]
    cloud.local, cloud.zone = tmp_path, "us-central1-a"
    states = iter(
        [dict(id="123", status="RUNNING"), dict(id="123", status="TERMINATED")]
    )
    cloud.describe = lambda name: next(states)
    calls = []

    def gc(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(stdout='{"name":"operation-1"}', stderr="")

    cloud.gc = gc
    record = cloud.stop_vm("spot_a", dict(requested_s=10), "/control/run-0001")
    assert record["status"] == "TERMINATED" and record["instance_id"] == "123"
    assert calls[0][:4] == ("compute", "instances", "stop", "owned-worker")
    assert "--no-graceful-shutdown" in calls[0]
    assert (
        json.loads((tmp_path / "run-0001-spot_a-cloud.json").read_text())["status"]
        == "TERMINATED"
    )


def test_fence_treats_collected_driver_as_finished_but_not_ssh_failure():
    from gcp.launcher import GCPStudy

    cloud = object.__new__(GCPStudy)
    cloud.head = "head"
    cloud.ssh = lambda *a, **kw: SimpleNamespace(returncode=3, stdout="inactive\n")
    assert cloud.fence_driver()
    cloud.ssh = lambda *a, **kw: SimpleNamespace(returncode=255, stdout="")
    assert not cloud.fence_driver()


def test_checkpoint_compaction_preserves_restored_final_and_latest(tmp_path):
    from gcp.controller import compact_checkpoints

    attempt = tmp_path / "attempt-1"
    attempt.mkdir()
    for epoch in (1, 2, 3, 4):
        (attempt / f"epoch-{epoch}.pt").write_bytes(bytes([epoch]))
    compact_checkpoints(tmp_path, [attempt / "epoch-2.pt"])
    assert sorted(p.name for p in attempt.glob("*.pt")) == ["epoch-2.pt", "epoch-4.pt"]
    manifest = json.loads((tmp_path / "checkpoint-retention.json").read_text())
    assert set(manifest["removed_sha256"]) == {
        "attempt-1/epoch-1.pt",
        "attempt-1/epoch-3.pt",
    }


def test_injector_adapter_replays_actual_local_process_loss(tmp_path):
    # A local process-loss integration check, explicitly NOT cloud VM evidence.
    from replay_runtime import FaultInjector, replay_once
    from run_study import profile

    class ExtraMetadataInjector(FaultInjector):
        def snapshot(self):
            return {
                m: dict(record, machine=m, test_metadata=True)
                for m, record in super().snapshot().items()
            }

    machines = {m: dict(cost_per_hour=1) for m in ("safe", "spot_a")}
    config = dict(
        tasks={
            "train": dict(
                width=16,
                epochs=10,
                checkpoint_every=2,
                seed=123,
                lr=0.03,
                batch_size=32,
            )
        },
        machines=machines,
        profile_repeats=1,
    )
    measured = profile(config, tmp_path / "profile")
    row = replay_once(
        measured,
        dict(machines=machines, deadline_s=30, horizon_s=60),
        dict(order=["train"], copies={"train": ["spot_a"]}),
        dict(name="test-local-process-loss", failures={"spot_a": 0.01}, probability=1),
        tmp_path / "replay",
        injector_factory=ExtraMetadataInjector,
        kind="local_process_adapter_validation",
    )
    assert row["correct"] and row["fault_coverage_complete"]
    assert row["max_abs_error"]["train"] == 0
    assert row["fault_evidence"]["spot_a"]["test_metadata"]
    assert row["fault_evidence"]["spot_a"]["exitcode"] == -9


def test_fresh_cluster_never_loads_old_state_and_records_before_create(
    tmp_path, monkeypatch
):
    from gcp import provision

    (tmp_path / "old-state.json").write_text('{"ready": true}')
    first = provision.FreshCluster(workdir=tmp_path)
    second = provision.FreshCluster(workdir=tmp_path)
    assert first.run_id != second.run_id
    assert first.state_path != second.state_path
    assert not first.state.get("ready")
    calls = []
    monkeypatch.setattr(provision.shutil, "which", lambda name: "/bin/" + name)

    def gc(*args, **kwargs):
        calls.append(args)
        saved = json.loads(first.state_path.read_text())
        assert saved["names"] == first.names
        if args[:3] == ("compute", "images", "describe"):
            return SimpleNamespace(
                stdout=json.dumps(
                    dict(status="READY", id="1", selfLink="image", diskSizeGb="100")
                )
            )
        assert saved["provision_started"]
        return SimpleNamespace(stdout="{}")

    monkeypatch.setattr(
        provision, "stop_command", lambda: ["compute", "instances", "stop"]
    )
    first.gc = gc
    first._check_owner = lambda name: dict(
        networkInterfaces=[dict(networkIP="10.90.0.2")]
    )
    first.setup = lambda: calls.append(("setup",))
    first.provision()
    create = next(c for c in calls if c[:3] == ("compute", "instances", "create"))
    assert create[3:8] == tuple(first.names)
    assert "--image=ray-built-v1" in create
    assert calls[-1] == ("setup",)
    with pytest.raises(RuntimeError, match="already attempted"):
        first.provision()


def test_fresh_setup_installs_persistent_worker_mounts(tmp_path, monkeypatch):
    from gcp import provision

    cluster = provision.FreshCluster(workdir=tmp_path)
    cluster.state["nodes"] = [
        dict(name=n, ip=f"10.90.0.{i+2}", head=i == 0)
        for i, n in enumerate(cluster.names)
    ]
    cluster.key.write_text("test-key")
    cluster.key.with_suffix(".pub").write_text("ssh-ed25519 test-public-key")
    cluster._check_owner = lambda name: dict(status="RUNNING")
    cluster._wait_ssh = lambda name: None
    cluster.scp = lambda *args: None
    commands = []

    def ssh(name, command, **kwargs):
        commands.append((name, command))
        if "print(json.dumps([command(n," in command:
            return SimpleNamespace(
                stdout=json.dumps([dict(git_commit=provision.SOURCE_COMMIT)] * 5)
            )
        return SimpleNamespace(stdout="ssh-ed25519 test-host-key")

    cluster.ssh = ssh
    cluster.setup()
    assert cluster.state["ready"]
    mounts = [(name, command) for name, command in commands if "/etc/fstab" in command]
    assert {name for name, command in mounts} == set(cluster.names[1:])
    assert all("10.90.0.2:/ /srv/ray-gcp nfs4" in command for name, command in mounts)
    assert all("sudo mount /srv/ray-gcp" in command for name, command in mounts)


@pytest.mark.parametrize("formatting", ["plain", "overstrike", "stderr", "unicode"])
def test_stop_capability_accepts_rendered_help(monkeypatch, formatting):
    from gcp import capabilities

    flag = "--no-graceful-shutdown"
    if formatting == "overstrike":
        flag = "".join(c + "\b" + c for c in flag)
    if formatting == "unicode":
        flag = flag.replace("-", "\u2010")
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert kwargs["env"]["CLOUDSDK_PAGER"] == "cat"
        return SimpleNamespace(
            returncode=0,
            stdout="" if formatting == "stderr" else flag,
            stderr=flag if formatting == "stderr" else "",
        )

    monkeypatch.setattr(capabilities.subprocess, "run", run)
    assert capabilities.stop_command() == ["compute", "instances", "stop"]
    assert len(calls) == 1 and "--help" in calls[0]


def test_stop_capability_beta_fallback_and_missing_support(monkeypatch):
    from gcp import capabilities

    def run(command, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout="--no-graceful-shutdown" if "beta" in command else "stop help",
            stderr="",
        )

    monkeypatch.setattr(capabilities.subprocess, "run", run)
    assert capabilities.stop_command() == ["beta", "compute", "instances", "stop"]
    monkeypatch.setattr(
        capabilities.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(
            returncode=1, stdout="", stderr="missing component"
        ),
    )
    with pytest.raises(RuntimeError, match="missing component"):
        capabilities.stop_command()


def test_preflight_failure_does_not_create_resources(tmp_path, monkeypatch):
    from gcp import provision

    cluster = provision.FreshCluster(workdir=tmp_path)
    monkeypatch.setattr(provision.shutil, "which", lambda name: "/bin/" + name)

    def unavailable():
        raise RuntimeError("unsupported CLI")

    monkeypatch.setattr(provision, "stop_command", unavailable)
    cluster.gc = lambda *a, **kw: pytest.fail("Cloud creation must not be attempted")
    with pytest.raises(RuntimeError, match="unsupported CLI"):
        cluster.provision()
    assert not cluster.state.get("provision_started")
