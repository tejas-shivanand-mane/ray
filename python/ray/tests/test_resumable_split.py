"""Run locally: coordinator-process replay, without model/optimizer rollback."""

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.iterator.resumable_split import (
    CONFIG_KEY, EMPTY_DIGEST, ReplayRounds, ResumeConfig,
)
from ray.data.context import DataContext


def _table(values):
    return pa.table({"id": values})


def _wait_for_coordinator(actor, old, *, restarted, timeout_s=60):
    """ray.kill submits a request; it is not a death/restart barrier.

    Observe a new Ray worker identity (not just a reusable OS PID), or a
    permanent actor error if the restart budget is zero/exhausted. Never accept
    a response from the old process or transient unavailability as completion.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Coordinator did not reach the requested fault state")
        try:
            identity = ray.get(actor.identity.remote(), timeout=remaining)
        except ray.exceptions.ActorUnavailableError:
            pass
        except ray.exceptions.RayActorError:
            if restarted:
                raise  # Do not hide failed reconstruction or an exhausted budget.
            return None
        else:
            if identity["worker_id"] != old["worker_id"]:
                if not restarted:
                    raise AssertionError("Coordinator restarted after its budget was exhausted")
                assert identity["node_id"] == old["node_id"]
                return identity
        time.sleep(min(0.02, max(0, deadline - time.monotonic())))


@pytest.mark.parametrize("restarted", [False, True])
def test_fault_barrier_ignores_old_and_transient_responses(monkeypatch, restarted):
    old = {"worker_id": "old", "pid": 10, "node_id": "same"}
    # Even a reused PID must not obscure the change of Ray worker identity.
    new = {"worker_id": "new", "pid": 10, "node_id": "same"}
    replies = iter([old, ray.exceptions.ActorUnavailableError("restarting", None),
                    old, new if restarted else ray.exceptions.ActorDiedError()])

    def get(*args, **kwargs):
        value = next(replies)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(ray, "get", get)
    monkeypatch.setattr(sys.modules[__name__], "time",
                        SimpleNamespace(monotonic=time.monotonic, sleep=lambda _: None))
    actor = SimpleNamespace(identity=SimpleNamespace(remote=lambda: None))
    assert _wait_for_coordinator(actor, old, restarted=restarted) == (new if restarted else None)


def test_fault_barrier_is_bounded(monkeypatch):
    clock = iter([0, 61])
    monkeypatch.setattr(sys.modules[__name__], "time",
                        SimpleNamespace(monotonic=lambda: next(clock)))
    with pytest.raises(TimeoutError, match="requested fault state"):
        _wait_for_coordinator(None, {}, restarted=True)


def test_replayed_prefix_and_ambiguous_reply():
    tables = [_table(list(range(i, i + 8))) for i in range(0, 40, 8)]
    original = ReplayRounds(tables, 2, 1024**2)
    prefixes = [EMPTY_DIGEST] * 2
    delivered = []
    for sequence in range(4):
        original.produce()
        replies = [original.reply(rank, sequence, prefixes[rank]) for rank in range(2)]
        delivered.append(replies)
        prefixes = [r[1] for r in replies]
    # Rank 0 has received round 3; rank 1 lost that reply and requests it again.
    restarted = ReplayRounds(tables, 2, 1024**2)
    for _ in range(5):
        restarted.produce()
    assert restarted.reply(1, 3, delivered[2][1][1]) == delivered[3][1]
    assert restarted.reply(1, 3, delivered[2][1][1]) == delivered[3][1]
    assert restarted.reply(0, 4, prefixes[0])[0] is not None
    assert len(restarted.cache) == 2
    with pytest.raises(ValueError, match="Stale"):
        restarted.reply(0, 0, EMPTY_DIGEST)


def test_changed_input_prefix_is_rejected():
    original = ReplayRounds([_table([0, 1, 2, 3])], 2, 1024**2)
    original.produce()
    _, prefix = original.reply(0, 0, EMPTY_DIGEST)
    changed = ReplayRounds([_table([99, 1, 2, 3]), _table([4, 5, 6, 7])], 2, 1024**2)
    changed.produce()
    changed.produce()
    with pytest.raises(ValueError, match="prefix mismatch"):
        changed.reply(0, 1, prefix)


def test_equal_tail_and_eof_are_explicit():
    rounds = ReplayRounds([_table([0, 1, 2, 3, 4])], 2, 1024**2)
    rounds.produce()
    replies = [rounds.reply(i, 0, EMPTY_DIGEST) for i in range(2)]
    assert [pa.ipc.open_stream(r[0]).read_all()["id"].to_pylist() for r in replies] == [[0, 1], [2, 3]]
    rounds.produce()
    for rank in range(2):
        assert rounds.reply(rank, 1, replies[rank][1]) == (None, replies[rank][1])
    with pytest.raises(ValueError, match="beyond end"):
        rounds.produce()


@pytest.mark.parametrize("options", [
    {}, {"deterministic": True, "max_restarts": -1},
    {"deterministic": True, "rows_per_chunk": 0},
    {"deterministic": True, "timeout_s": float("inf")},
])
def test_replay_contract_is_explicit_and_bounded(options):
    with pytest.raises(ValueError):
        ResumeConfig(**options)


def test_round_byte_limit():
    rounds = ReplayRounds([_table(list(range(8)))], 2, 1)
    with pytest.raises(ValueError, match="max_round_bytes"):
        rounds.produce()


@pytest.fixture
def local_ray(tmp_path):
    ray.init(num_cpus=6, object_store_memory=256 * 1024**2,
             include_dashboard=False)
    old = DataContext.get_current()
    context = old.copy()
    context.execution_options.preserve_order = True
    DataContext._set_current(context)
    ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
    yield tmp_path
    ray.cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
    DataContext._set_current(old)
    ray.shutdown()


def _dataset(directory, restarts=1):
    context = DataContext.get_current()
    context.set_config(CONFIG_KEY, {"deterministic": True, "rows_per_chunk": 2,
                                  "max_restarts": restarts, "timeout_s": 60})
    return ray.data.read_parquet(str(directory / "input.parquet"), concurrency=1)


def test_rejects_unsupported_sources_and_fixed_r(local_ray):
    ds = ray.data.from_items([{"id": i} for i in range(8)])
    ds.context.set_config(CONFIG_KEY, {"deterministic": True})
    with pytest.raises(ValueError, match="persistent reads"):
        ds.streaming_split(2, equal=True)
    ds.context.enable_fixed_r_task_recovery = True
    with pytest.raises(ValueError, match="separate from Fixed-R"):
        ds.streaming_split(2, equal=True)


def test_restart_checks_prefix_and_exhausts_budget(local_ray):
    path = local_ray / "input.parquet"
    pq.write_table(_table(list(range(16))), path)
    splits = _dataset(local_ray).streaming_split(2, equal=True)
    actor = splits[0]._coord_actor
    old = ray.get(actor.identity.remote())
    first = ray.get([actor.get.remote(0, rank, 0, EMPTY_DIGEST) for rank in (0, 1)])
    # A fast consumer must not discard the previous round before its peer can
    # retry an ambiguous response.
    _, prefix = ray.get(actor.get.remote(0, 0, 1, first[0][1]))
    pending = actor.get.remote(0, 0, 2, prefix)
    assert ray.wait([pending], timeout=0.1)[0] == []
    assert ray.get(actor.get.remote(0, 1, 0, EMPTY_DIGEST)) == first[1]
    ray.get(actor.get.remote(0, 1, 1, first[1][1]))
    ray.get(pending, timeout=60)
    ray.kill(actor, no_restart=False)
    new = _wait_for_coordinator(actor, old, restarted=True)
    # The completed first reply remains readable without the old owner.
    assert pa.ipc.open_stream(first[0][0]).read_all()["id"].to_pylist() == [0, 1]
    # Replayed prefix validation is also covered against changed persisted input
    # below, using a new coordinator so the old cache cannot mask the change.
    ray.kill(actor, no_restart=False)
    _wait_for_coordinator(actor, new, restarted=False)
    other = _dataset(local_ray).streaming_split(2, equal=True)[0]._coord_actor
    old = ray.get(other.identity.remote())
    _, prefix = ray.get(other.get.remote(0, 0, 0, EMPTY_DIGEST))
    pq.write_table(_table([999] + list(range(1, 16))), path)
    ray.kill(other, no_restart=False)
    _wait_for_coordinator(other, old, restarted=True)
    with pytest.raises(ray.exceptions.RayTaskError, match="prefix mismatch"):
        ray.get(other.get.remote(0, 0, 1, prefix), timeout=60)
    ray.kill(other, no_restart=True)


def _state_digest(model, optimizer):
    # Stable tensor/value digest independent of torch.save archive names.
    def visit(value):
        import torch
        if isinstance(value, torch.Tensor):
            return (str(value.dtype), list(value.shape), value.detach().cpu().numpy().tobytes().hex())
        if isinstance(value, dict):
            return [(str(k), visit(v)) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))]
        if isinstance(value, (list, tuple)):
            return [visit(v) for v in value]
        return value
    return hashlib.sha256(json.dumps(visit([model.state_dict(), optimizer.state_dict()])).encode()).hexdigest()


def _learning_loop(config):
    import torch
    import torch.distributed as dist
    from ray import train
    from ray.train import Checkpoint
    from ray.train.torch import prepare_model

    torch.set_num_threads(1)
    torch.manual_seed(4)
    rank = train.get_context().get_world_rank()
    directory = Path(config["directory"])
    model = torch.nn.Linear(1, 1)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    checkpoint = train.get_checkpoint()
    start = 0
    if checkpoint:
        with checkpoint.as_directory() as path:
            state = torch.load(Path(path) / "state.pt", weights_only=True)
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        start = state["epoch"]
    model = prepare_model(model)
    base = model.module
    shard = train.get_dataset_shard("train")
    invocation = uuid.uuid4().hex
    events = directory / f"updates-{rank}-{invocation}.jsonl"
    for epoch in range(start, 2):
        for step, batch in enumerate(shard.iter_torch_batches(batch_size=2, prefetch_batches=1), 1):
            x = batch["id"].float().reshape(-1, 1) / 64
            optimizer.zero_grad()
            ((model(x) - 2 * x) ** 2).mean().backward()
            optimizer.step()
            with events.open("a") as out:
                out.write(json.dumps({"rank": rank, "pid": os.getpid(), "epoch": epoch + 1,
                                      "step": step, "ids": batch["id"].tolist(),
                                      "restored_epoch": start}) + "\n")
            if config["fault"] and start == 0 and epoch == 1 and step == 4:
                # Real completed updates on both ranks; no mid-collective claim.
                dist.barrier()
                before = _state_digest(base, optimizer)
                if rank == 0:
                    actor = shard._coord_actor
                    old = ray.get(actor.identity.remote())
                    ray.kill(actor, no_restart=False)
                    new = _wait_for_coordinator(actor, old, restarted=bool(config["restarts"]))
                    (directory / "fault.json").write_text(json.dumps({"old": old, "new": new}))
                dist.barrier()
                assert _state_digest(base, optimizer) == before
        with tempfile.TemporaryDirectory() as path:
            if rank == 0:
                torch.save({"model": base.state_dict(), "optimizer": optimizer.state_dict(),
                            "epoch": epoch + 1}, Path(path) / "state.pt")
            train.report({"epoch": epoch + 1, "digest": _state_digest(base, optimizer)},
                         checkpoint=Checkpoint.from_directory(path) if rank == 0 else None)


@pytest.mark.parametrize("restarts", [0, 1])
def test_ddp_keeps_updates_or_uses_enabled_checkpoint_retry(local_ray, restarts):
    """Mechanism test, not a realistic-workload performance benchmark.

    Both policies use identical deterministic sharding. Zero coordinator
    restarts leaves ordinary full-group Train checkpoint retry enabled.
    """
    pytest.importorskip("torch")
    from ray.train import CheckpointConfig, DataConfig, FailureConfig, RunConfig, ScalingConfig
    from ray.train.torch import TorchConfig, TorchTrainer

    pq.write_table(_table(list(range(64))), local_ray / "input.parquet")
    finals = []
    for fault in (False, True):
        directory = local_ray / f"run-{restarts}-{fault}"
        directory.mkdir()
        ds = _dataset(local_ray, restarts)
        result = TorchTrainer(
            _learning_loop,
            train_loop_config={"directory": str(directory), "fault": fault, "restarts": restarts},
            scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
            torch_config=TorchConfig(backend="gloo", timeout_s=120),
            datasets={"train": ds}, dataset_config=DataConfig(datasets_to_split=["train"]),
            run_config=RunConfig(storage_path=str(directory / "results"),
                                 failure_config=FailureConfig(max_failures=1),
                                 checkpoint_config=CheckpointConfig(num_to_keep=1)),
        ).fit()
        finals.append(result.metrics["digest"])
        records = [json.loads(line) for path in directory.glob("updates-*.jsonl")
                   for line in path.read_text().splitlines()]
        for rank in (0, 1):
            own = [e for e in records if e["rank"] == rank]
            if fault and not restarts:
                assert len({e["pid"] for e in own}) == 2
                assert any(e["restored_epoch"] == 1 for e in own)
                assert len(own) > 32  # Interrupted epoch work was repeated.
            else:
                assert len({e["pid"] for e in own}) == 1
                assert len(own) == 32
                assert all(e["restored_epoch"] == 0 for e in own)
                for epoch in (1, 2):
                    events = sorted((e for e in own if e["epoch"] == epoch), key=lambda e: e["step"])
                    assert [e["step"] for e in events] == list(range(1, 17))
                    assert [i for e in events for i in e["ids"]] == [
                        i for start in range(0, 64, 4) for i in range(start + rank * 2, start + rank * 2 + 2)]
        if fault:
            evidence = json.loads((directory / "fault.json").read_text())
            assert (evidence["new"] is not None) == bool(restarts)
    assert finals[0] == finals[1]
