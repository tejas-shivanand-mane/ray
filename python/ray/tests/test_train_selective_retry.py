"""Retry safety invariants; native node loss is exercised by validate_train_retry.sh."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from ray.train.v2._internal.execution.worker_group.state import WorkerGroupContext, WorkerGroupState
from ray.train.v2._internal.execution.worker_group.worker import RayTrainWorker
from ray.train.v2._internal.execution.worker_group import selective_restart as restart
from ray.train.v2.xgboost import recovery
from ray.train.v2.xgboost.config import XGBoostConfig


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_recovery_budget_is_bounded(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        XGBoostConfig(recovery_timeout_s=timeout)


def test_selective_recovery_is_opt_in():
    config = XGBoostConfig()
    assert config.selective_recovery is False
    assert config.to_dict()["selective_recovery"] is False


@pytest.mark.parametrize("native_cleared", [True, False])
def test_communicator_cleanup_is_observed_on_training_thread(monkeypatch, native_cleared):
    from ray.train.v2.xgboost import config as module

    class Context:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            raise RuntimeError("native failure")

    monkeypatch.setattr(module, "get_train_fn_utils", lambda: NS(is_distributed=lambda: True))
    monkeypatch.setattr(module.XGBoostConfigV1, "train_func_context", property(lambda self: Context))
    monkeypatch.setattr(module.xgboost.collective, "is_distributed", lambda: not native_cleared)
    monkeypatch.setattr(recovery, "_communicator_cleared", False)
    with pytest.raises(RuntimeError, match="native failure"):
        with XGBoostConfig().train_func_context():
            assert recovery._communicator_cleared is False
    assert recovery._communicator_cleared is native_cleared


def test_input_cache_is_scoped_to_run_rank_and_world_size(monkeypatch):
    from ray.train.v2._internal.execution import context

    scope = {"run": "a", "rank": 0, "size": 2}
    monkeypatch.setattr(context, "get_train_context", lambda: NS(
        train_run_context=NS(run_id=scope["run"]),
        get_world_rank=lambda: scope["rank"], get_world_size=lambda: scope["size"]))
    monkeypatch.setattr(recovery, "_input_cache", {})
    original = recovery.get_cached_input("version-1", object)
    assert recovery.get_cached_input("version-1", lambda: pytest.fail("reloaded")) is original
    assert recovery.get_cached_input("version-2", object) is not original
    for key, value in (("rank", 1), ("size", 3), ("run", "b")):
        scope[key] = value
        assert recovery.get_cached_input("version-1", object) is not original


def test_failed_load_is_not_cached(monkeypatch):
    from ray.train.v2._internal.execution import context

    monkeypatch.setattr(context, "get_train_context", lambda: NS(
        train_run_context=NS(run_id="run"), get_world_rank=lambda: 0,
        get_world_size=lambda: 2))
    monkeypatch.setattr(recovery, "_input_cache", {})
    def fail():
        raise RuntimeError("read failed")
    with pytest.raises(RuntimeError, match="read failed"):
        recovery.get_cached_input("key", fail)
    assert recovery._input_cache == {}
    assert recovery.get_cached_input("key", lambda: 42) == 42


def test_prepare_requires_thread_exit_and_native_cleanup(monkeypatch):
    from ray.train.v2._internal.execution.worker_group import worker as module

    state = {"running": True, "shutdown": False}
    context = NS(train_run_context=NS(backend_config=XGBoostConfig()),
                 execution_context=NS(training_thread_runner=NS(
        is_running=lambda: state["running"])), checkpoint_upload_threadpool=NS(
            shutdown=lambda wait: state.update(shutdown=wait)))
    monkeypatch.setattr(module, "get_train_context", lambda: context)
    actor = RayTrainWorker()
    monkeypatch.setattr(actor, "clear_result_queue", lambda: None)
    monkeypatch.setattr(recovery, "_communicator_cleared", False)
    assert actor.prepare_worker_retry() is False
    state["running"] = False
    with pytest.raises(RuntimeError, match="cleanup"):
        actor.prepare_worker_retry()
    monkeypatch.setattr(recovery, "_communicator_cleared", True)
    assert actor.prepare_worker_retry() is True
    assert state["shutdown"] is True


def test_global_ranks_are_not_resorted_when_a_node_changes():
    workers = [NS(metadata=NS(node_id=n), distributed_context=None) for n in ("z", "a", "z")]
    restart.assign_preserved_ranks(workers)
    assert [w.distributed_context.world_rank for w in workers] == [0, 1, 2]
    assert [w.distributed_context.local_rank for w in workers] == [0, 0, 1]
    assert [w.distributed_context.local_world_size for w in workers] == [2, 1, 2]


class Remote:
    def __init__(self, function):
        self.function = function

    def remote(self, *args, **kwargs):
        return lambda: self.function(*args, **kwargs)


@pytest.fixture
def fake_group(monkeypatch):
    events = []
    def actor(name, safe=True):
        return NS(name=name, get_metadata=Remote(lambda: name),
                  prepare_worker_retry=Remote(lambda: safe),
                  run_train_fn=Remote(lambda ref: events.append(("run", name, ref))),
                  reset=Remote(lambda: events.append(("reset", name))))
    def worker(rank, node):
        return NS(actor=actor(f"old-{rank}"), metadata=NS(node_id=node),
                  resources={"CPU": 1}, placement_group_bundle_index=rank,
                  distributed_context=NS(world_rank=rank))
    workers = [worker(0, "dead"), worker(1, "alive")]
    old_sync = actor("old-sync")
    pg = NS(placement_group="pg")
    state = WorkerGroupState(0, pg, workers, old_sync)
    group = NS(_worker_group_state=state,
               _worker_group_context=WorkerGroupContext("old", "train-fn", 2, {"CPU": 1}),
               _world_rank_to_ongoing_poll={1: "stale"}, _latest_poll_status="stale",
               _replica_group_callbacks=[], _collective_timeout_s=10, _collective_warn_interval_s=5)
    group.get_workers = lambda: group._worker_group_state.workers
    def create(**kwargs):
        events.append(("create", kwargs["starting_world_rank"]))
        new = worker(kwargs["starting_world_rank"], "replacement")
        new.actor = actor("new-0")
        return [new]
    group._create_workers = create
    group._init_train_context = lambda ws, sync: events.append(("contexts", [w.actor.name for w in ws]))
    group._callbacks = [NS(
        before_worker_group_reuse=lambda g, timeout: events.append("abort-old-streams"),
        before_worker_group_shutdown=lambda g: events.append("before-shutdown"),
        after_worker_group_shutdown=lambda c: events.append("after-shutdown"),
        before_worker_group_start=lambda c: events.append(("attempt", c.run_attempt_id)),
        after_worker_group_start=lambda g: events.append("backend-and-reports-reset"),
        after_worker_group_training_start=lambda g: events.append("started"),
        on_worker_group_start=lambda: nullcontext())]
    def get(ref, timeout=None):
        return [get(r, timeout) for r in ref] if isinstance(ref, list) else ref()
    monkeypatch.setattr(restart.ray, "get", get)
    monkeypatch.setattr(restart.ray, "nodes", lambda: [{"NodeID": "alive", "Alive": True}])
    monkeypatch.setattr(restart.ray, "kill", lambda a, **kw: events.append(("kill", a.name)))
    monkeypatch.setattr(restart.ray, "get_runtime_context", lambda: NS(get_node_id=lambda: "controller"))
    monkeypatch.setattr(restart, "SynchronizationActor", NS(options=lambda **kw: NS(
        remote=lambda **kw: actor("new-sync"))))
    monkeypatch.setattr(restart, "ReplicaGroup", lambda *args: args)
    return group, events


def test_retry_preserves_actor_and_resets_generation(fake_group):
    group, events = fake_group
    healthy = group.get_workers()[1]
    assert restart.try_restart(group, "new-attempt", 5)
    assert group.get_workers()[1] is healthy
    assert group.get_workers()[0].actor.name == "new-0"
    assert ("kill", "old-1") not in events
    assert group._world_rank_to_ongoing_poll == {}
    assert group._latest_poll_status is None
    assert ("attempt", "new-attempt") in events
    assert events.index("abort-old-streams") < events.index(("create", 0))
    assert events.index("backend-and-reports-reset") < events.index(("run", "old-1", "train-fn"))


def test_unsafe_communicator_falls_back_before_replacement(fake_group):
    group, events = fake_group
    def fail():
        raise RuntimeError("communicator remains active")
    group.get_workers()[1].actor.prepare_worker_retry = Remote(fail)
    assert restart.try_restart(group, "new", 5) is False
    assert not any(isinstance(e, tuple) and e[0] == "create" for e in events)


@pytest.mark.parametrize("alive", [[], ["dead", "alive"]])
def test_no_partial_reuse_without_a_dead_and_a_healthy_worker(fake_group, monkeypatch, alive):
    group, events = fake_group
    monkeypatch.setattr(restart.ray, "nodes", lambda: [{"NodeID": n, "Alive": True} for n in alive])
    assert restart.try_restart(group, "new", 5) is False
    assert not events


def test_partial_replacements_are_tracked_for_fallback_cleanup(fake_group):
    group, events = fake_group
    def fail(*args):
        raise RuntimeError("initialization failed")
    group._init_train_context = fail
    assert restart.try_restart(group, "new", 5) is False
    assert group.get_workers()[0].actor.name == "new-0"
    assert not any(isinstance(e, tuple) and e[0] == "run" for e in events)


def test_data_fencing_failure_prevents_actor_reuse(fake_group):
    group, events = fake_group
    def fail(*args):
        raise TimeoutError("Old input stream did not stop")
    group._callbacks[0].before_worker_group_reuse = fail
    assert restart.try_restart(group, "new", 5) is False
    assert not any(isinstance(e, tuple) and e[0] in ("create", "run") for e in events)


def test_torch_cleanup_requires_gloo_and_clears_default_group(monkeypatch):
    pytest.importorskip("torch")
    from ray.train.torch import config as module

    state = {"initialized": True, "backend": "nccl", "destroyed": False}
    monkeypatch.setattr(module.dist, "is_initialized", lambda: state["initialized"])
    monkeypatch.setattr(module.dist, "get_backend", lambda: state["backend"])
    monkeypatch.setattr(module.dist, "destroy_process_group",
                        lambda: state.update(initialized=False, destroyed=True))
    with pytest.raises(RuntimeError, match="Gloo"):
        module.TorchConfig().prepare_worker_for_retry()
    assert not state["destroyed"]
    state["backend"] = "gloo"
    module.TorchConfig().prepare_worker_for_retry()
    assert state["destroyed"] and not state["initialized"]


def test_unknown_backend_cannot_be_reused():
    from ray.train.backend import BackendConfig

    with pytest.raises(NotImplementedError, match="does not support"):
        BackendConfig().prepare_worker_for_retry()


def test_aborting_dataset_generation_kills_coordinators_even_on_cleanup_error(monkeypatch):
    from ray.train.v2._internal.data_integration import dataset_manager as module

    manager = object.__new__(module.DatasetManager)
    manager._coordinator_actors = ["old-split"]
    def fail():
        raise RuntimeError("shutdown failed")
    manager.shutdown_data_executors = fail
    killed = []
    monkeypatch.setattr(module.ray, "kill", lambda actor, **kw: killed.append(actor))
    with pytest.raises(RuntimeError, match="shutdown failed"):
        manager.abort()
    assert killed == ["old-split"]
    assert manager._coordinator_actors == []


def test_aborting_provider_also_fences_pending_shard_requests(monkeypatch):
    from ray.train.v2._internal.callbacks import datasets as module

    provider = object.__new__(module.RayDatasetShardProvider)
    manager = NS(abort=Remote(lambda: None))
    provider._dataset_manager = manager
    def timeout(*args, **kwargs):
        raise TimeoutError("manager blocked")
    killed = []
    monkeypatch.setattr(module.ray, "get", timeout)
    monkeypatch.setattr(module.ray, "kill", lambda actor, **kw: killed.append(actor))
    with pytest.raises(TimeoutError):
        provider.abort(1)
    assert killed == [manager]


def test_controller_uses_standard_path_when_opt_in_is_off(monkeypatch):
    from ray.train.v2._internal.execution.controller.controller import TrainController
    from ray.train.v2._internal.execution.scaling_policy import ResizeDecision

    controller = object.__new__(TrainController)
    controller._train_run_context = NS(backend_config=XGBoostConfig())
    controller._worker_group = NS(get_workers=lambda: [1, 2], get_latest_poll_status=lambda: None)
    controller._manages_replica_groups = False
    controller._state = "old"
    controller._get_run_attempt_id = lambda: "attempt"
    controller._run_controller_hook = lambda *args: None
    calls = []
    controller._shutdown_worker_group = lambda: calls.append("shutdown")
    controller._start_worker_group = lambda **kw: calls.append("start")
    monkeypatch.setattr(restart, "try_restart", lambda *a: pytest.fail("opt-in ignored"))
    controller._execute_resize_decision(ResizeDecision(2, {"CPU": 1}))
    assert calls == ["shutdown", "start"]


@pytest.mark.parametrize("committed,reused", [(False, False), (True, False), (True, True)])
def test_controller_requires_checkpoint_and_falls_back(monkeypatch, committed, reused):
    from ray.train.v2._internal.execution.controller.controller import TrainController
    from ray.train.v2._internal.execution.scaling_policy import ResizeDecision

    controller = object.__new__(TrainController)
    controller._train_run_context = NS(backend_config=NS(selective_recovery=True, recovery_timeout_s=5))
    poll = NS(errors={0: RuntimeError("worker failed")}, failing_replica_group_indices={0},
              all_replica_group_indices={0, 1})
    controller._worker_group = NS(get_workers=lambda: [1, 2], get_latest_poll_status=lambda: poll)
    controller._checkpoint_manager = NS(latest_checkpoint_result=object() if committed else None)
    controller._manages_replica_groups = False
    controller._state = "old"
    controller._get_run_attempt_id = lambda: "attempt"
    controller._run_controller_hook = lambda *args: None
    calls = []
    controller._shutdown_worker_group = lambda: calls.append("shutdown")
    controller._start_worker_group = lambda **kw: calls.append("start")
    def attempt(*args):
        calls.append("reuse")
        return reused
    monkeypatch.setattr(restart, "try_restart", attempt)
    controller._execute_resize_decision(ResizeDecision(2, {"CPU": 1}))
    assert calls == (["reuse"] if reused else (["reuse"] if committed else []) + ["shutdown", "start"])


@pytest.mark.parametrize("module_name", ["train_retry", "train_workload"])
def test_benchmark_payload_does_not_require_support_module(monkeypatch, tmp_path, module_name):
    import importlib
    import subprocess
    import sys
    import ray.cloudpickle

    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    module = importlib.import_module(module_name)
    ray.cloudpickle.register_pickle_by_value(module)
    try:
        path = tmp_path / "payload.pkl"
        payload = ((module.Job, module.InputLoader, module.CommitEvidence, module.train_loop)
                   if module_name == "train_retry" else
                   (module.WorkloadObserver, module.ReportGate, module.observe_function,
                    module.checkpoint_files))
        if module_name == "train_workload":
            from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
            from ray.data._internal.planner.exchange.streaming_recovery import _map_outputs

            payload += (module.owner_gated_map(ShuffleTaskSpec.map, str(tmp_path), False),
                        module.owner_gated_map(_map_outputs, str(tmp_path), True))
        path.write_bytes(ray.cloudpickle.dumps(payload))
        subprocess.run([sys.executable, "-c", '''
import importlib.abc, sys
import ray.cloudpickle
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"train_retry", "train_workload", "training_provenance", "run_fixed_r_train_coverage"}:
            raise ImportError("benchmark module unavailable")
sys.meta_path.insert(0, Block())
with open(sys.argv[1], "rb") as f:
    payload = ray.cloudpickle.load(f)
assert len(payload) in (4, 6)
''', str(path)], check=True, cwd=tmp_path, timeout=20)
    finally:
        ray.cloudpickle.unregister_pickle_by_value(module)


@pytest.mark.parametrize("streaming", [False, True])
def test_owner_fault_gate_preserves_shuffle_outputs(monkeypatch, streaming):
    import importlib

    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    module = importlib.import_module("train_workload")
    calls = []
    monkeypatch.setattr(module, "wait_for_owner_fault", lambda path, index: calls.append(index))
    if streaming:
        def original(*args):
            yield "partition"
            yield "metadata"
        wrapped = module.owner_gated_map(original, "/unused", True)
        output = wrapped(NS(_map_args=[None, None, True]), 0, None, 1)
        assert next(output) == "partition"
        assert calls == [0]
        assert list(output) == ["metadata"]
    else:
        output = ["partition", "metadata"]
        wrapped = module.owner_gated_map(lambda *args: output, "/unused", False)
        assert wrapped(0, None, 1, None, None, True, 0) is output
        assert calls == [0]


@pytest.fixture
def fixed_r_workload_pair(monkeypatch, tmp_path):
    import importlib
    import numpy as np

    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    comparison = importlib.import_module("run_train_retry_comparison")
    samples = []
    for mode in ("off", "on"):
        directory = tmp_path / mode
        directory.mkdir()
        np.save(directory / "predictions.npy", np.array([1., 2.]))
        samples.append({
            "status": "passed", "scenario": "data-owner", "mode": mode,
            "restart_scope": "full", "workload_sha256": "script", "torch_version": "torch",
            "directory": str(directory), "workload_s": 10 if mode == "off" else 12,
            "numerical_probe": True,
            "provenance": {key: "same" for key in (
                "source_sha256", "native_extension_sha256", "python", "platform", "ray_version",
                "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version")},
            "native_settings": {"enable_recovery_streaming_fixed_r": mode == "on", "holders": 2},
            "data_owner_fault": {"completed": True, "stage": "RandomShuffle.map", "request_ns": 2,
                                 "fixed_r_submission_batch_settled": mode == "on",
                                 "target": {"task_id": "target", "blocked_ns": 1},
                                 "head_replacement": {"original_head_processes_exited": True,
                                                      "failure_scope": "all_head_processes_with_surviving_gcs_storage"}},
            "data_exchanges": [{"operator": "RandomShuffle", "fixed_r_recovered_tasks": 1 if mode == "on" else 0,
                                "fixed_r_recovered_task_details": [{"task_id": "target"}] if mode == "on" else []}],
        })
    return comparison, samples


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("replacement_fails", [False, True])
def test_owner_fault_runs_replacement_on_main_thread(
    monkeypatch, tmp_path, enabled, replacement_fails,
):
    import importlib
    from threading import Event, current_thread, main_thread
    from ray.data._internal.planner.exchange import streaming_recovery as exchange
    from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec

    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    module = importlib.import_module("train_workload")
    calls, diagnostics = [], {}
    crashed = Event()
    stream = NS(reader=object(), task_id=NS(hex=lambda: "target"))

    class BaseTask:
        def on_data_ready(self):
            calls.append("consume")

    class Task(BaseTask):
        pass

    monkeypatch.setattr(exchange, "_ExchangeTask", Task)
    monkeypatch.setattr(exchange, "submit_stream", lambda *a, **kw: stream)
    monkeypatch.setattr(module.ray, "get_runtime_context", lambda: NS(
        get_task_id=lambda: "target", get_node_id=lambda: "executor",
    ))
    originals = ShuffleTaskSpec.map, exchange._map_outputs, exchange.submit_stream

    def crash():
        assert current_thread() is main_thread()
        calls.append("crash")
        crashed.set()
        if replacement_fails:
            raise RuntimeError("replacement failed")
        return {"original_head_processes_exited": True}

    def workload():
        assert current_thread() is not main_thread()
        module.write_record(tmp_path / "data-owner-blocked.json", {
            "task_id": "target", "node_id": "executor", "blocked_ns": 1,
        })
        if enabled:
            exchange.submit_stream(None, None, (NS(_map_args=[None, None, True]), 0, None, 4))
            assert not crashed.wait(.05), "Owner died while further submissions could be in flight"
            task = Task()
            task.stream = stream
            task.on_data_ready()
        else:
            module.wait_for_owner_fault(tmp_path, 0)
            calls.append("consume")
        return "finished"

    if replacement_fails:
        with pytest.raises(RuntimeError, match="replacement failed"):
            module.data_owner_fault(tmp_path, enabled, crash, ("executor",), diagnostics, workload)
        assert diagnostics["data_owner_fault"]["error"] == "replacement failed"
    else:
        assert module.data_owner_fault(
            tmp_path, enabled, crash, ("executor",), diagnostics, workload,
        ) == "finished"
    assert calls == ["crash", "consume"]
    assert diagnostics["data_owner_fault"]["completed"] is not replacement_fails
    assert diagnostics["data_owner_fault"]["fixed_r_submission_batch_settled"] is enabled
    assert (ShuffleTaskSpec.map, exchange._map_outputs, exchange.submit_stream) == originals
    assert "on_data_ready" not in Task.__dict__


@pytest.mark.parametrize("fails", [False, True])
def test_owner_fault_preserves_early_workload_error(monkeypatch, tmp_path, fails):
    import importlib

    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    module = importlib.import_module("train_workload")
    diagnostics = {}

    def crash():
        pytest.fail("Head must not be killed before the workload reaches its gate")

    def workload():
        if fails:
            raise LookupError("workload failed")

    error = LookupError if fails else ValueError
    message = "workload failed" if fails else "Workload ended before the shuffle failure point"
    with pytest.raises(error, match=message):
        module.data_owner_fault(tmp_path, True, crash, ("executor",), diagnostics, workload)
    assert not diagnostics["data_owner_fault"]["completed"]
    assert diagnostics["data_owner_fault"]["error"] == message
    assert (tmp_path / "release-data-owner").exists()


def test_fixed_r_comparison_records_surviving_baseline(fixed_r_workload_pair):
    module, (off, on) = fixed_r_workload_pair
    result = module.compare_fixed_r_pair(off, on)
    assert result["off_completed"] and result["on_completed"]
    assert result["on_vs_off_pct"] == pytest.approx(20)
    assert result["on_replayed_tasks"] == 1


@pytest.mark.parametrize("invalid", ["missing_fault", "wrong_replay", "different_build", "flags", "failed_baseline", "different_predictions"])
def test_fixed_r_comparison_rejects_inadequate_evidence(fixed_r_workload_pair, invalid):
    import numpy as np

    module, (off, on) = fixed_r_workload_pair
    if invalid == "missing_fault":
        off["data_owner_fault"]["completed"] = False
    elif invalid == "wrong_replay":
        on["data_exchanges"][0]["fixed_r_recovered_task_details"] = [{"task_id": "another"}]
    elif invalid == "different_build":
        on["provenance"]["native_extension_sha256"] = "different"
    elif invalid == "flags":
        off["native_settings"]["enable_recovery_streaming_fixed_r"] = True
    elif invalid == "failed_baseline":
        off["status"] = "failed"
    else:
        np.save(Path(on["directory"]) / "predictions.npy", np.array([99., 2.]))
    with pytest.raises((ValueError, AssertionError)):
        module.compare_fixed_r_pair(off, on)
