"""Dry-run notebook infrastructure cells without cloud access."""

import json
import shlex
import subprocess
from pathlib import Path

import pytest

NOTEBOOK = Path(__file__).resolve().parents[2] / "Selective_Replication_GCP.ipynb"


def cell(tag):
    notebook = json.loads(NOTEBOOK.read_text())
    return next(
        "".join(c["source"])
        for c in notebook["cells"]
        if tag in c["metadata"].get("tags", [])
    )


def context(tmp_path):
    run_id = "rayexp-test"
    names = [run_id + "-head"] + [run_id + f"-w{i}" for i in range(1, 5)]
    nodes = [
        dict(name=name, ip=f"10.90.0.{i+2}", head=i == 0)
        for i, name in enumerate(names)
    ]
    return dict(
        json=json,
        shlex=shlex,
        subprocess=subprocess,
        PROJECT="test",
        ZONE="us-central1-a",
        REGION="us-central1",
        IMAGE="image",
        MACHINE_TYPE="e2-standard-4",
        DISK_GB=100,
        RUN_ID=run_id,
        NAMES=names,
        HEAD=names[0],
        WORKERS=names[1:],
        nodes=nodes,
        HEAD_IP=nodes[0]["ip"],
        NETWORK=run_id,
        SUBNET=run_id + "-subnet",
        LOCAL=tmp_path,
        STATE_FILE=tmp_path / "state.json",
        HELPERS=NOTEBOOK.parent / "selective_replication",
        run_parallel=lambda fn, items: [fn(item) for item in items],
    )


def test_notebook_fresh_creation_and_no_duplicate_allocation(tmp_path, monkeypatch):
    from gcp import capabilities

    monkeypatch.setattr(
        capabilities, "stop_command", lambda: ["compute", "instances", "stop"]
    )
    ctx = context(tmp_path)
    calls = []

    def gc(*args, **kw):
        calls.append(args)
        if args[:3] == ("compute", "images", "describe"):
            return json.dumps(dict(status="READY", diskSizeGb=150))
        if args[:3] == ("compute", "networks", "describe"):
            return "{}"
        if args[:4] == ("compute", "networks", "subnets", "describe"):
            return json.dumps(dict(network=ctx["NETWORK"]))
        assert ctx["STATE_FILE"].is_file()
        return "{}"

    ctx["gcloud"] = gc
    exec(cell("create"), ctx)
    exec(cell("create"), ctx)
    created = [c for c in calls if c[:3] == ("compute", "instances", "create")]
    assert len(created) == 1
    assert not any(c[:3] == ("compute", "networks", "create") for c in calls)
    assert not any(c[:4] == ("compute", "networks", "subnets", "create") for c in calls)
    assert created[0][3:8] == tuple(ctx["NAMES"])
    assert "--boot-disk-size=150GB" in created[0]


def test_notebook_setup_commands_and_persistent_nfs(tmp_path):
    ctx = context(tmp_path)
    commands, copies = [], []
    ctx["ssh"] = lambda name, command: commands.append((name, command))
    ctx["scp"] = lambda *args: copies.append(args)
    exec(cell("setup"), ctx)
    exec(cell("nfs"), ctx)
    assert len(copies) == 10
    for _, command in commands:
        subprocess.run(["bash", "-n"], input=command, text=True, check=True)
    mounts = [(name, cmd) for name, cmd in commands if "/etc/fstab" in cmd]
    assert {n for n, _ in mounts} == set(ctx["WORKERS"])
    assert all(
        "printf '%s\\n'" in cmd and "sudo mount /srv/ray-gcp" in cmd
        for _, cmd in mounts
    )
    assert (
        json.loads((tmp_path / (ctx["HEAD"] + ".json")).read_text())["head_ip"]
        == ctx["HEAD_IP"]
    )


def test_notebook_cleanup_rejects_foreign_instance_before_deleting(tmp_path):
    ctx = context(tmp_path)
    calls = []

    def gc(*args, **kw):
        calls.append(args)
        return json.dumps(
            [
                dict(
                    name=ctx["HEAD"],
                    zone=ctx["ZONE"],
                    labels={"ray-experiment": "foreign"},
                )
            ]
        )

    ctx["gcloud"] = gc
    with pytest.raises(AssertionError):
        exec(
            cell("cleanup").replace(
                "DELETE_INSTANCES = False", "DELETE_INSTANCES = True"
            ),
            ctx,
        )
    assert all("delete" not in call for call in calls)


def test_cleanup_preserves_shared_network_and_other_firewall_rules(tmp_path):
    ctx = context(tmp_path)
    calls = []

    def gc(*args, **kw):
        calls.append(args)
        if args[:3] == ("compute", "instances", "list"):
            return "[]"
        if args[:3] == ("compute", "firewall-rules", "list"):
            return json.dumps(
                [
                    dict(name=name, network=ctx["NETWORK"])
                    for name in [
                        ctx["RUN_ID"] + "-internal",
                        ctx["RUN_ID"] + "-iap",
                        "unrelated",
                    ]
                ]
            )
        return "{}"

    ctx["gcloud"] = gc
    exec(
        cell("cleanup").replace("DELETE_INSTANCES = False", "DELETE_INSTANCES = True"),
        ctx,
    )
    deleted = [c for c in calls if "delete" in c]
    assert len(deleted) == 2
    assert all(c[:3] == ("compute", "firewall-rules", "delete") for c in deleted)
    assert all(c[3].startswith(ctx["RUN_ID"]) for c in deleted)
