"""Fresh-execution safety and evidence checks for logical worker-node loss."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import ray
from ray.data import DataContext, Dataset
from ray.data._internal.execution import streaming_recovery as adapter


@pytest.fixture
def membership(monkeypatch):
    owner, coordinator, *executors = [ray.NodeID.from_random().hex() for _ in range(6)]
    nodes = [{"NodeID": node, "Alive": True} for node in (owner, coordinator, *executors)]
    monkeypatch.setattr(ray, "nodes", lambda: nodes)
    monkeypatch.setattr(ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: coordinator))
    context = DataContext()
    context.enable_fixed_r_task_recovery = True
    context.fixed_r_task_recovery_output_mode = "streaming"
    context.eager_free = False
    config = adapter.FixedRDataConfig(owner, tuple(executors), {}, dynamic_task_outputs=True)
    context.set_config(adapter.CONFIG_KEY, config)
    return SimpleNamespace(owner=owner, coordinator=coordinator, executors=executors,
                           nodes=nodes, context=context, config=config)


@pytest.mark.parametrize("owner_alive", [True, False])
def test_new_execution_filters_dead_executor_without_retargeting_old_recipe(membership, owner_alive):
    env = membership
    env.nodes[0]["Alive"] = owner_alive
    env.nodes[2]["Alive"] = False
    fresh = adapter.context_for_new_execution(env.context)
    current = adapter.get_config(fresh)
    assert fresh is not env.context
    assert current.owner_node_id == env.owner
    assert current.executor_node_ids == tuple(env.executors[1:])
    assert adapter._owner_alive(current) == owner_alive
    assert env.context.get_config(adapter.CONFIG_KEY) is env.config
    assert env.config.executor_for_task(0) == env.executors[0]
    with pytest.raises(ValueError, match="all configured task executors"):
        adapter._owner_alive(env.config)


@pytest.mark.parametrize("failure", ["unknown", "capacity"])
def test_new_execution_rejects_unknown_membership_and_insufficient_survivors(membership, failure):
    env = membership
    if failure == "unknown":
        env.nodes.pop()
    else:
        for node in env.nodes[2:5]:
            node["Alive"] = False
    with pytest.raises(ValueError, match="unknown|two surviving"):
        adapter.context_for_new_execution(env.context)


def test_explicit_configuration_does_not_silently_change(membership):
    env = membership
    env.context.enable_fixed_r_task_recovery = False
    env.nodes[2]["Alive"] = False
    assert adapter.context_for_new_execution(env.context) is env.context
    assert env.context.get_config(adapter.CONFIG_KEY) is env.config


def test_dataset_creates_private_plan_context_before_new_physical_execution(membership, monkeypatch):
    from ray.data._internal.execution import streaming_executor

    env = membership
    monkeypatch.setattr(streaming_executor, "StreamingExecutor",
                        lambda context, dataset_id: SimpleNamespace(context=context))
    ds = object.__new__(Dataset)
    ds._context = env.context
    ds._current_executor = None
    original_plan = SimpleNamespace(context=env.context)
    ds._logical_plan = original_plan
    ds._run_index = 0
    ds.get_dataset_id = lambda: "fixture"
    previous = ds._create_executor()
    previous_plan = ds._logical_plan
    env.nodes[2]["Alive"] = False
    replacement = ds._create_executor()
    assert ds._logical_plan is not previous_plan
    assert original_plan.context is env.context
    assert ds._logical_plan.context is replacement.context
    assert adapter.get_config(previous.context).executor_node_ids == tuple(env.executors)
    assert adapter.get_config(replacement.context).executor_node_ids == tuple(env.executors[1:])


@pytest.fixture
def comparison(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    monkeypatch.syspath_prepend(str(root / "release/train_tests/xgboost_lightgbm"))
    import train_comparison
    import train_batch_inference_benchmark

    return train_comparison, train_batch_inference_benchmark


@pytest.mark.parametrize("phase", ["training", "prediction"])
def test_execution_callback_keeps_monitor_out_of_serialized_context(comparison, monkeypatch, phase):
    from ray import cloudpickle

    case, _ = comparison
    observations = []
    lookups = []

    class LocalMonitor:
        def __reduce__(self):
            raise AssertionError("The executor-local monitor leaked into DataContext")

        execution = SimpleNamespace(remote=observations.append)

    monitor = LocalMonitor()

    def get_monitor(name, *, namespace):
        lookups.append((name, namespace))
        return monitor

    # Round-trip the actual DataContext payload before resolving a monitor,
    # as happens when submitting read/map tasks to workers.
    context = DataContext()
    context.custom_execution_callback_classes = [case.capture_node_execution("test-monitor", phase)]
    context = cloudpickle.loads(cloudpickle.dumps(context))
    callback = context.custom_execution_callback_classes[0]()
    executor = SimpleNamespace(_data_context=context)
    callback.execution_id = "execution"
    callback.operators = [SimpleNamespace(name="ReadParquet")]
    callback.started_ns = 1
    callback.configured_executors = ["executor-1", "executor-2"]
    monkeypatch.setattr(ray, "get_actor", get_monitor)
    monkeypatch.setattr(ray, "get", lambda ref, **kwargs: ref)
    monkeypatch.setattr(ray, "get_runtime_context",
                        lambda: SimpleNamespace(get_node_id=lambda: "coordinator"))
    callback.publish(executor, "started")
    callback.after_execution_succeeds(executor)
    callback.after_execution_fails(executor, RuntimeError("fixture"))
    assert lookups == [("test-monitor", case.NAMESPACE)]
    assert [item["state"] for item in observations] == ["started", "finished", "failed"]
    assert all(item["phase"] == phase for item in observations)
    assert all(item["configured_executor_node_ids"] == ["executor-1", "executor-2"]
               for item in observations)
    # Execution may continue submitting tasks after the first event. The
    # context must still be serializable without the resolved actor handle.
    cloudpickle.dumps(context)


def test_execution_callback_rejects_monitor_handles(comparison):
    case, _ = comparison
    with pytest.raises(ValueError, match="monitor name"):
        case.capture_node_execution(object(), "training")


def test_input_accounting_detects_same_length_wrong_rows(comparison):
    case, benchmark = comparison
    frame = benchmark.pd.DataFrame({"x": [1., 2., 3., 4.], "labels": [0, 1, 0, 1]})
    expected = benchmark.input_fingerprint(frame)
    groups = [{"workers": [{"worker_id": f"{attempt}-{rank}", "world_rank": rank}
                            for rank in range(2)]} for attempt in range(2)]
    inputs = [{**w, "fingerprint": benchmark.input_fingerprint(frame.iloc[w["world_rank"]::2])}
              for group in groups for w in group["workers"]]
    observed = {"worker_groups": groups, "inputs": inputs}
    case.validate_input_partitions(observed, expected)
    inputs[-1]["fingerprint"] = benchmark.input_fingerprint(frame.iloc[[0, 2]])
    with pytest.raises(ValueError, match="rows/content changed"):
        case.validate_input_partitions(observed, expected)


def node_evidence():
    events = [{"name": "worker_node_confirmed_dead", "at_ns": 10}]
    executions = [{"execution_id": phase, "phase": phase, "state": state,
                   "started_ns": 11, "at_ns": 12, "operators": ["ReadParquet"],
                   "configured_executor_node_ids": ["live-1", "live-2"],
                   "coordinator_node_id": "coordinator"}
                  for phase in ("training", "prediction") for state in ("started", "finished")]
    observation = {"worker_groups": [
        {"workers": [{"node_id": "dead", "pid": 42}]},
        {"workers": [{"node_id": "live-1"}, {"node_id": "live-2"}]},
    ], "events": events, "executions": executions}
    failure = {"node_id": "dead", "training_worker_pid": 42, "node_process_pids": [41, 42],
               "all_node_processes_exited": True, "gcs_marked_dead": True}
    return observation, failure


@pytest.mark.parametrize("gap", ["process", "gcs", "placement", "replacement", "read", "prediction", "finish"])
def test_node_loss_rejects_incomplete_recovery_evidence(comparison, gap):
    case, _ = comparison
    observed, failure = node_evidence()
    options = {"mode": "on", "include_prediction": True}
    live = {"live-1", "live-2", "coordinator"}
    case.validate_worker_node_failure(observed, options, failure, live)
    if gap == "process":
        failure["all_node_processes_exited"] = False
    elif gap == "gcs":
        failure["gcs_marked_dead"] = False
    elif gap == "placement":
        observed["executions"][0]["configured_executor_node_ids"].append("dead")
    elif gap == "replacement":
        observed["worker_groups"][1]["workers"][0]["node_id"] = "dead"
    elif gap == "read":
        for execution in observed["executions"]:
            execution["operators"] = ["ListFiles"]
    elif gap == "prediction":
        observed["executions"] = [e for e in observed["executions"] if e["phase"] != "prediction"]
    else:
        observed["executions"].pop()
    with pytest.raises(ValueError):
        case.validate_worker_node_failure(observed, options, failure, live)


def test_off_node_loss_accepts_native_placement(comparison):
    case, _ = comparison
    observed, failure = node_evidence()
    for execution in observed["executions"]:
        execution["configured_executor_node_ids"] = None
        execution["operators"] = ["ReadParquet->MapBatches(XGBoostPredictor)"]
    case.validate_worker_node_failure(observed, {"mode": "off", "include_prediction": True},
                                      failure, {"live-1", "live-2", "coordinator"})
