"""Promotion safety regressions; no Ray processes or infrastructure hooks run."""

from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from ray.experimental.recovery import _head_supervisor as supervisor


@pytest.fixture
def config():
    return supervisor.Config(
        address="10.0.0.100:6379", old_head_ip="10.0.0.10",
        replacement_head_ip="10.0.0.11", redis_address="10.0.0.20:6379",
        storage_namespace="test-head-replacement",
        survivor_node_ips=["10.0.0.30", "10.0.0.31", "10.0.0.32"],
        fence_command=["fence"], start_command=["start"], switch_command=["switch"],
    )


def snapshots(config):
    original = {
        "cluster_id": "cluster", "session_name": "session",
        "native_config": config.native_config(),
        "nodes": [{"id": "old", "ip": config.old_head_ip,
                   "head": True, "alive": True}] + [
            {"id": f"survivor-{i}", "ip": ip, "head": False, "alive": True}
            for i, ip in enumerate(config.survivor_node_ips)
        ],
    }
    restored = deepcopy(original)
    restored["nodes"][0]["alive"] = False
    restored["nodes"].append({"id": "new", "ip": config.replacement_head_ip,
                              "head": True, "alive": True})
    return supervisor.baseline(config, original), restored


def test_fencing_failure_prevents_start_and_switch(config):
    before, restored = snapshots(config)
    commands = []

    def hook(command, *args):
        commands.append(command)
        raise RuntimeError("Power state could not be confirmed")

    with pytest.raises(RuntimeError, match="Power state"):
        supervisor.recover(config, before, lambda *args: restored, hook,
                           lambda phase: None, time.monotonic() + 10)
    assert commands == [["fence"]]


@pytest.mark.parametrize("changed", ["cluster_id", "session_name"])
def test_wrong_cluster_never_receives_endpoint_traffic(config, changed):
    before, restored = snapshots(config)
    restored[changed] = "different"
    commands = []
    with pytest.raises(ValueError, match=changed):
        supervisor.recover(
            config, before, lambda *args: restored,
            lambda command, *args: commands.append(command), lambda phase: None,
            time.monotonic() + 10,
        )
    assert commands == [["fence"], ["start"]]


def test_route_switch_follows_direct_identity_check(config):
    before, restored = snapshots(config)
    operations = []

    def read(address, deadline):
        operations.append(address)
        return restored

    result = supervisor.recover(
        config, before, read,
        lambda command, *args: operations.append(command[0]), lambda phase: None,
        time.monotonic() + 10,
    )
    assert result == restored
    assert operations == ["fence", "start", "10.0.0.11:6379", "switch", config.address]


def test_worker_restart_is_not_survival(config):
    before, restored = snapshots(config)
    restored["nodes"][1]["id"] = "new-worker-on-same-ip"
    assert not supervisor.replacement_ready(config, before, restored)


def test_old_head_must_be_marked_dead(config):
    before, restored = snapshots(config)
    restored["nodes"][0]["alive"] = True
    assert not supervisor.replacement_ready(config, before, restored)


def test_wrong_native_settings_prevent_route_switch(config):
    before, restored = snapshots(config)
    restored["native_config"]["gcs_storage"] = "memory"
    with pytest.raises(ValueError, match="gcs_storage"):
        supervisor.replacement_ready(config, before, restored)


def test_reconnect_budget_must_cover_detection_and_replacement(config):
    values = vars(config).copy()
    values["reconnect_timeout_s"] = 120
    with pytest.raises(ValueError, match="Reconnect timeout"):
        supervisor.Config(**values)


def test_replacement_refuses_missing_persisted_session(config, monkeypatch, tmp_path):
    commands = []

    class StorageProbe:
        def __init__(self, command, **kwargs):
            commands.append(command)
            Path(command[command.index("--output") + 1]).write_text("null")

        def wait(self, **kwargs):
            return 0

        def poll(self):
            return 0

    monkeypatch.setattr(supervisor.subprocess, "Popen", StorageProbe)
    with pytest.raises(ValueError, match="session mismatch"):
        supervisor.start_head(config, tmp_path / "config.json",
                              config.replacement_head_ip, "existing-session",
                              tmp_path / "raytmp", 120)
    assert len(commands) == 1
    assert commands[0][2:4] == [supervisor.MODULE, "_storage-session"]


@pytest.mark.parametrize("already_claimed", [False, True])
def test_claim_blocks_repeat_promotion_and_survives_uncertain_fencing(
    config, monkeypatch, tmp_path, already_claimed,
):
    _, original = snapshots(config)
    original["nodes"].pop()
    original["nodes"][0]["alive"] = True
    readings = iter([original, None])
    config.failure_threshold = 1
    commands = []

    class Redis:
        value = "previous-supervisor" if already_claimed else None
        released = False

        def set(self, key, value, nx):
            assert nx
            if self.value is not None:
                return False
            self.value = value
            return True

        def get(self, key):
            return self.value.encode()

        def eval(self, *args):
            self.released = True

    redis = Redis()
    monkeypatch.setattr(config, "redis_client", lambda: redis)
    monkeypatch.setattr(supervisor, "snapshot", lambda *args: next(readings))

    def fail_hook(command, *args):
        commands.append(command)
        raise TimeoutError("Fencing outcome unknown")

    monkeypatch.setattr(supervisor, "run_hook", fail_hook)
    report = tmp_path / "report.json"
    assert supervisor.supervise(config, tmp_path / "config.json", report, 120) == 1
    assert commands == ([] if already_claimed else [["fence"]])
    assert redis.value is not None and not redis.released
    assert json.loads(report.read_text())["status"] == "failed"
