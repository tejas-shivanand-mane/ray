"""Source regressions for explicit opt-in and cached-context disable behavior."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import ray
from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, FixedRDataConfig, get_config
from ray.experimental import recovery
from ray.util.state.exception import RayStateApiException


def configured_context():
    context = DataContext()
    context.eager_free = True
    context.enable_fixed_r_task_recovery = True
    context.set_config(CONFIG_KEY, FixedRDataConfig(
        ray.NodeID.from_random().hex(),
        (ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex()),
        {}, dynamic_task_outputs=True,
    ))
    get_config(context)
    return context


def test_disable_clears_cached_automatic_config_and_restores_eager_free():
    context = configured_context()
    assert not context.eager_free
    recovery.disable(context=context)
    assert context.eager_free
    assert context.get_config(CONFIG_KEY) is None
    assert get_config(context) is None


def test_direct_flag_disable_clears_automatic_cache():
    context = configured_context()
    context.enable_fixed_r_task_recovery = False
    assert get_config(context) is None
    assert context.eager_free


def test_disabling_new_context_does_not_mutate_an_existing_snapshot():
    context = configured_context()
    dataset_snapshot = context.copy()
    recovery.disable(context=context)
    assert get_config(context) is None
    assert get_config(dataset_snapshot) is not None


def test_explicit_declared_count_experiment_remains_opt_in():
    context = configured_context()
    config = context.get_config(CONFIG_KEY)
    recovery.disable(context=context)
    context.eager_free = False
    explicit = replace(config, dynamic_task_outputs=False, expected_blocks={"ReadRange": 1})
    context.set_config(CONFIG_KEY, explicit)
    assert get_config(context) == explicit
    recovery.disable(context=context)
    assert get_config(context) is None


def test_enable_rejects_native_disabled_cluster_without_mutating_context(monkeypatch):
    context = DataContext()
    context.eager_free = True
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray._private.state.state, "get_system_config", lambda: {})
    with pytest.raises(ValueError, match="Start the cluster"):
        recovery.enable(context=context)
    assert not context.enable_fixed_r_task_recovery
    assert context.eager_free
    assert context.get_config(CONFIG_KEY) is None


def test_enable_discovers_placement_without_application_node_ids(monkeypatch):
    from ray._common.constants import HEAD_NODE_RESOURCE_NAME

    head, driver, worker1, worker2 = [ray.NodeID.from_random().hex() for _ in range(4)]
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray._private.state.state, "get_system_config", recovery.system_config)
    monkeypatch.setattr(ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: driver))
    monkeypatch.setattr(ray, "nodes", lambda: [
        {"NodeID": head, "Alive": True, "Resources": {HEAD_NODE_RESOURCE_NAME: 1}},
        {"NodeID": driver, "Alive": True, "Resources": {}},
        {"NodeID": worker1, "Alive": True, "Resources": {"CPU": 2}},
        {"NodeID": worker2, "Alive": True, "Resources": {"CPU": 2}},
    ])
    context = DataContext()
    recovery.enable(context=context)
    config = get_config(context)
    assert config.owner_node_id == head
    assert set(config.executor_node_id) == {worker1, worker2}
    recovery.enable(context=context)
    assert get_config(context) == config


def test_generic_observer_requires_replay_in_the_actual_trigger_stage():
    from ray.experimental.recovery._observe import validate

    listing = {"name": "ListFiles", "tasks_submitted": 1, "tasks_finished": 1,
               "tasks_failed": 0, "active_enrolled_tasks": [], "fixed_r_recovered_tasks": 1}
    read = {**listing, "name": "ReadFilesParquetV2", "fixed_r_recovered_tasks": 0}
    observed = {"executions": {"run": {"state": "finished", "operators": [listing, read]}},
                "trigger": {"execution": "run", "stage": 1}}
    with pytest.raises(ValueError, match="No task replayed"):
        validate(observed, True)
    read["fixed_r_recovered_tasks"] = 1
    validate(observed, True)


def test_launcher_observer_preserves_standard_schema_pickle():
    import pickle
    import pyarrow as pa

    from ray.data.dataset import Schema
    from ray.experimental.recovery._observe import MONITOR_KEY, Observe

    context = configured_context()
    context.set_config(MONITOR_KEY, "test-monitor")
    context.custom_execution_callback_classes.append(Observe)
    schema = Schema(pa.schema([("id", pa.int64())]), data_context=context)
    restored = pickle.loads(pickle.dumps(schema))
    assert restored.names == ["id"]
    assert restored._context.get_config(MONITOR_KEY) == "test-monitor"
    assert Observe in restored._context.custom_execution_callback_classes


@pytest.mark.parametrize("error_type", [ValueError, RuntimeError, RayStateApiException])
def test_recovery_optional_statistics_preserve_strict_default(monkeypatch, error_type):
    from ray.data._internal.scheduling_overhead import collect_scheduling_overhead
    from ray.util.state import api

    calls = []

    def unavailable(**kwargs):
        calls.append(kwargs)
        raise error_type("Dashboard unavailable")

    monkeypatch.setattr(api, "list_tasks", unavailable)
    with pytest.raises(error_type, match="Dashboard unavailable"):
        collect_scheduling_overhead(["ReadRange"])
    assert "timeout" not in calls[-1]
    assert collect_scheduling_overhead(["ReadRange"], best_effort=True) == {}
    assert calls[-1]["timeout"] == 5
