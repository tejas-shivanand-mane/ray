"""Full Data executor coverage for the explicit Fixed-R copied-output mode."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import time
from unittest.mock import Mock

import pyarrow as pa
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._raylet import _recovery_stream_return_id
from ray.cluster_utils import Cluster
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.streaming_recovery import (
    CONFIG_KEY,
    FixedRDataConfig,
    StreamingRecoveryDataOpTask,
    get_config,
    validate_execution,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.exceptions import GetTimeoutError, RayActorError


@pytest.fixture(scope="module")
def surviving_data_cluster():
    cluster = Cluster()
    try:
        cluster.add_node(
            num_cpus=1, include_dashboard=False,
            _system_config={
                "enable_recovery_succession": True,
                "enable_recovery_witness_holder_baseline": True,
                "enable_recovery_streaming_fixed_r": True,
                "recovery_succession_target_holder_count": 2,
                "recovery_succession_witness_count": 2,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
                "health_check_initial_delay_ms": 1000,
                "health_check_period_ms": 1000,
                "health_check_timeout_ms": 3000,
                "health_check_failure_threshold": 3,
            },
        )
        executor_node = cluster.add_node(num_cpus=3)
        cluster.add_node(num_cpus=3)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        yield cluster, executor_node.node_id
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.fixture
def data_cluster(surviving_data_cluster):
    cluster, executor_id = surviving_data_cluster
    owner_node = cluster.add_node(num_cpus=0)
    cluster.wait_for_nodes()
    removed = False

    def crash(owners):
        nonlocal removed
        cluster.remove_node(owner_node, allow_graceful=False)
        removed = True

        def owners_stopped():
            for owner in owners:
                try:
                    ray.get(owner.__ray_ready__.remote(), timeout=1)
                except RayActorError:
                    continue
                except GetTimeoutError:
                    return False
                return False
            return any(
                node["NodeID"] == owner_node.node_id and not node["Alive"]
                for node in ray.nodes()
            )

        wait_for_condition(owners_stopped, timeout=60)

    try:
        yield owner_node.node_id, executor_id, crash
    finally:
        if not removed:
            cluster.remove_node(owner_node, allow_graceful=False)


def configured_context(owner_id, executor_id, counts, mode="fixed_r"):
    context = DataContext.get_current().copy()
    context.eager_free = False
    context.retried_map_errors = False
    context.max_errored_blocks = 0
    context.execution_options.preserve_order = True
    context.enable_progress_bars = False
    context._max_num_blocks_in_streaming_gen_buffer = 1
    context.set_config(CONFIG_KEY, FixedRDataConfig(owner_id, executor_id, counts, mode))
    return context


@pytest.mark.parametrize("mode", ["copy", "fixed_r"])
def test_public_dataset_copy_pipeline(data_cluster, mode):
    owner_id, executor_id, _ = data_cluster

    def increment(batch):
        return pa.table({"value": [value + 1 for value in batch["value"].to_pylist()]})

    def double(batch):
        return pa.table({"value": [value * 2 for value in batch["value"].to_pylist()]})

    counts = {"MapBatches(increment)": 1, "MapBatches(double)": 1}
    context = configured_context(owner_id, executor_id, counts, mode)
    with DataContext.current(context):
        ds = ray.data.from_blocks([pa.table({"value": [1, 2, 3]})])
        ds = ds.map_batches(increment, batch_format="pyarrow").map_batches(
            double, batch_format="pyarrow"
        )
        iterator, _, executor = ds._execute_to_iterator()
        bundles = list(iterator)
    refs = [ref for bundle in bundles for ref in bundle.block_refs]
    values = [value for block in ray.get(refs) for value in block["value"].to_pylist()]
    assert values == [4, 6, 8]
    ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs(refs)
    operators = [op for op in executor._topology if isinstance(op, MapOperator)]
    assert {op.name for op in operators} == set(counts)
    key = "fixed_r_enrolled_tasks" if mode == "fixed_r" else "fixed_r_copy_baseline_tasks"
    assert sum(op.metrics.extra_metrics[key] for op in operators) == 2
    assert sum(op.metrics.extra_metrics["fixed_r_copied_blocks"] for op in operators) == 2
    assert sum(op.metrics.extra_metrics["fixed_r_closed_streams"] for op in operators) == 2


def test_executor_owner_loss_with_live_downstream_copy(data_cluster, tmp_path):
    owner_id, executor_id, crash = data_cluster
    context = configured_context(owner_id, executor_id, {"Produce": 3, "Consume": 1})
    source_gate = str(tmp_path / "source_gate")
    sink_gate = str(tmp_path / "sink_gate")
    source_log = str(tmp_path / "source_attempts")
    sink_log = str(tmp_path / "sink_attempts")

    # Local functions are serialized by value; remote workers need not import
    # this pytest module. File gates affect timing only, not the output values.
    def produce(blocks, ctx):
        attempt = ray._private.worker.global_worker.core_worker.get_current_task_attempt_number()
        with Path(source_log).open("a") as log:
            log.write(f"{attempt}\n")
        for _ in blocks:
            for index in range(3):
                if index == 1 and attempt == 0:
                    Path(source_gate + ".blocked").touch()
                    while not Path(source_gate).exists():
                        time.sleep(0.01)
                yield pa.table({"value": [index] * 50_000})

    def consume(blocks, ctx):
        attempt = ray._private.worker.global_worker.core_worker.get_current_task_attempt_number()
        for block in blocks:
            value = block["value"][0].as_py()
            with Path(sink_log).open("a") as log:
                log.write(f"{value}:{attempt}\n")
            if value == 0 and attempt == 0:
                Path(sink_gate + ".blocked").touch()
                while not Path(sink_gate).exists():
                    time.sleep(0.01)
            yield pa.table({"value": [value], "rows": [block.num_rows]})

    block = pa.table({"input": [1]})
    accessor = BlockAccessor.for_block(block)
    inputs = InputDataBuffer(context, [RefBundle(
        [BlockEntry(ray.put(block), accessor.get_metadata())],
        schema=accessor.schema(), owns_blocks=False,
    )])
    source = MapOperator.create(
        MapTransformer([BlockMapTransformFn(produce, disable_block_shaping=True)]),
        inputs, context, name="Produce",
    )
    from ray.data import TaskPoolStrategy

    sink = MapOperator.create(
        MapTransformer([BlockMapTransformFn(consume, disable_block_shaping=True)]),
        source, context, name="Consume", compute_strategy=TaskPoolStrategy(size=1),
    )
    executor = StreamingExecutor(context)
    with ThreadPoolExecutor(max_workers=1) as pool:
        iterator = executor.execute(sink)
        result = pool.submit(list, iterator)
        try:
            wait_for_condition(
                lambda: Path(source_gate + ".blocked").exists()
                and Path(sink_gate + ".blocked").exists(), timeout=60,
            )
            source_task = source.get_active_tasks()[0]
            sink_task = sink.get_active_tasks()[0]
            assert isinstance(source_task, StreamingRecoveryDataOpTask)
            assert isinstance(sink_task, StreamingRecoveryDataOpTask)
            copied_input = sink_task.stream.reader._input_refs[-1]
            copied_id = copied_input.binary()
            assert copied_id != _recovery_stream_return_id(source_task.stream.reader.descriptor, 0)
            ray._private.worker.global_worker.core_worker.validate_streaming_recovery_inputs(
                [copied_input]
            )
            assert len(source_task.stream.reader.consumer._retained) <= 2
            owners = [source_task.stream.owner, sink_task.stream.owner]
            crash(owners)
            # This exported copy is valid while both original streams recover.
            assert ray.get(copied_input, timeout=10).num_rows == 50_000
            Path(source_gate).touch()
            Path(sink_gate).touch()
            outputs = result.result(timeout=120)
        finally:
            Path(source_gate).touch()
            Path(sink_gate).touch()
            executor.shutdown(force=True)

    assert copied_input.binary() == copied_id
    assert ray.get(copied_input)["value"].to_pylist() == [0] * 50_000
    blocks = ray.get([ref for bundle in outputs for ref in bundle.block_refs])
    assert [block.to_pydict() for block in blocks] == [
        {"value": [i], "rows": [50_000]} for i in range(3)
    ]
    assert Path(source_log).read_text().splitlines() == ["0", "1"]
    assert Path(sink_log).read_text().splitlines() == ["0:0", "0:1", "1:0", "2:0"]
    assert source.metrics.extra_metrics["fixed_r_enrolled_tasks"] == 1
    assert source.metrics.extra_metrics["fixed_r_recovered_tasks"] == 1
    assert sink.metrics.extra_metrics["fixed_r_enrolled_tasks"] == 1
    assert sink.metrics.extra_metrics["fixed_r_recovered_tasks"] == 1
    assert sink.metrics.extra_metrics["fixed_r_survivor_tasks"] == 2
    assert source.metrics.extra_metrics["fixed_r_copied_blocks"] == 3
    assert sink.metrics.extra_metrics["fixed_r_copied_blocks"] == 3
    assert source.metrics.extra_metrics["fixed_r_closed_streams"] == 1
    assert sink.metrics.extra_metrics["fixed_r_closed_streams"] == 3


@pytest.mark.parametrize("declared", [0, 2])
def test_dataset_rejects_wrong_physical_count(data_cluster, declared):
    owner_id, executor_id, _ = data_cluster

    def identity(batch):
        return batch

    context = configured_context(owner_id, executor_id, {"MapBatches(identity)": declared})
    with DataContext.current(context):
        ds = ray.data.from_blocks([pa.table({"value": [1]})]).map_batches(
            identity, batch_format="pyarrow"
        )
        with pytest.raises(Exception, match="declared count|objects, expected"):
            list(ds.iter_internal_ref_bundles())


def test_config_rejects_missing_operator_before_dispatch(data_cluster):
    owner_id, executor_id, _ = data_cluster
    context = configured_context(owner_id, executor_id, {"Misspelled": 1})
    inputs = InputDataBuffer(context, [])
    def identity(blocks, ctx):
        yield from blocks

    op = MapOperator.create(
        MapTransformer([BlockMapTransformFn(identity, disable_block_shaping=True)]),
        inputs, context, name="Actual",
    )
    with pytest.raises(ValueError, match="Declare each physical operator"):
        validate_execution(op, context)
    assert op._streaming_recovery_metrics["fixed_r_enrolled_tasks"] == 0


@pytest.mark.parametrize("count", [-1, True, None])
def test_invalid_config_count(count):
    config = FixedRDataConfig(
        ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex(), {"Map": count}
    )
    with pytest.raises(ValueError, match="nonnegative ints"):
        config.validate()


def test_config_rejects_eager_free_and_unsafe_retry_policy():
    context = DataContext.get_current().copy()
    context.set_config(CONFIG_KEY, FixedRDataConfig(
        ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex(), {"Map": 1}
    ))
    context.eager_free = True
    with pytest.raises(ValueError, match="eager_free"):
        get_config(context)
    context.eager_free = False
    context.retried_map_errors = True
    with pytest.raises(ValueError, match="UDF retries"):
        get_config(context)


def test_zero_budget_does_not_read_or_copy(monkeypatch):
    stream = Mock()
    task = StreamingRecoveryDataOpTask(0, stream, Mock(), "test")
    assert task.on_data_ready(0, Mock()) == 0
    stream.poll.assert_not_called()
    stream.release_pair.assert_not_called()
    from ray.data._internal.execution.streaming_executor_state import process_completed_tasks

    task.get_waitable = Mock(side_effect=AssertionError("Read started with zero budget"))
    op = Mock()
    op.get_active_tasks.return_value = [task]
    op.has_next.return_value = False
    state = Mock(op=op)
    policy = Mock()
    policy.max_task_output_bytes_to_read.return_value = 0
    guard = Mock()
    guard.should_unblock.return_value = False
    fetcher = Mock()
    fetcher.emit_ready_and_fire_done_callbacks.return_value = []
    wait = Mock(side_effect=AssertionError("No waitable should have been requested"))
    monkeypatch.setattr(ray, "wait", wait)
    assert process_completed_tasks(
        {op: state}, [policy], 0, output_backpressure_guard=guard,
        metadata_fetcher=fetcher,
    ) == 0
    task.get_waitable.assert_not_called()
    wait.assert_not_called()


def test_empty_map_completes_and_retires_protection(data_cluster):
    owner_id, executor_id, _ = data_cluster
    context = configured_context(owner_id, executor_id, {"Empty": 0})

    def empty(blocks, ctx):
        yield from ()

    block = pa.table({"value": [1]})
    accessor = BlockAccessor.for_block(block)
    inputs = InputDataBuffer(context, [RefBundle(
        [BlockEntry(ray.put(block), accessor.get_metadata())],
        schema=accessor.schema(), owns_blocks=False,
    )])
    op = MapOperator.create(
        MapTransformer([BlockMapTransformFn(empty, disable_block_shaping=True)]),
        inputs, context, name="Empty",
    )
    with StreamingExecutor(context) as executor:
        assert list(executor.execute(op)) == []
    assert op.metrics.extra_metrics["fixed_r_enrolled_tasks"] == 1
    assert op.metrics.extra_metrics["fixed_r_copied_blocks"] == 0
    assert op.metrics.extra_metrics["fixed_r_closed_streams"] == 1


def test_copy_mode_config_is_explicit():
    config = FixedRDataConfig(
        ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex(), {"Map": 1}
    )
    with pytest.raises(ValueError, match="mode"):
        replace(config, mode="automatic").validate()


@pytest.mark.parametrize("invalid", [(), [], None, "duplicate", "owner"])
def test_multi_executor_config_rejects_invalid_nodes(invalid):
    owner = ray.NodeID.from_random().hex()
    executor = ray.NodeID.from_random().hex()
    nodes = (executor, executor) if invalid == "duplicate" else (
        (executor, owner) if invalid == "owner" else invalid
    )
    with pytest.raises(ValueError, match="Executors|unique|separate"):
        FixedRDataConfig(owner, nodes, {"Map": 1}).validate()


def test_multi_executor_selection_is_stable():
    nodes = tuple(ray.NodeID.from_random().hex() for _ in range(3))
    config = FixedRDataConfig(ray.NodeID.from_random().hex(), nodes, {"Map": 1})
    config.validate()
    assert [config.executor_for_task(index) for index in range(7)] == list(nodes * 2) + [nodes[0]]
    assert config.executor_for_task(1) == nodes[1]


def test_multi_executor_requires_every_executor_to_survive(monkeypatch):
    from ray.data._internal.execution.streaming_recovery import _owner_alive

    owner, first, second = (ray.NodeID.from_random().hex() for _ in range(3))
    config = FixedRDataConfig(owner, (first, second), {"Map": 1})
    monkeypatch.setattr(ray, "nodes", lambda: [
        {"NodeID": owner, "Alive": True},
        {"NodeID": first, "Alive": True},
        {"NodeID": second, "Alive": False},
    ])
    with pytest.raises(ValueError, match="all configured task executors"):
        _owner_alive(config)


@pytest.mark.parametrize("mode", ["ordinary", "copy", "fixed_r", "fixed_r_failure"])
def test_backpressure_benchmark_multi_executor(data_cluster, monkeypatch, mode):
    # Exercise the actual benchmark adapters, including its count and placement
    # checks, on small blocks. Do not launch the benchmark's CLI or memory sampler.
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    from streaming_recovery_benchmark import run_controlled

    owner_id, _, crash = data_cluster
    coordinator = ray.get_runtime_context().get_node_id()
    executor_ids = tuple(sorted(
        node["NodeID"] for node in ray.nodes()
        if node["Alive"] and node["NodeID"] not in (owner_id, coordinator)
    ))
    assert len(executor_ids) == 2
    args = SimpleNamespace(
        recovery_mode=mode, owner_node_id=owner_id, executor_node_ids=executor_ids,
        producer_concurrency=2, num_input_blocks=4,
        output_batches_per_input_batch=3, output_batch_rows=4, output_row_bytes=64,
        consumer_sleep_s=0.01, recovery_timeout_s=60,
    )
    result = run_controlled(args, crash_owner=lambda: crash([]))
    assert result["validated_output_blocks"] == 12
    assert result["observed_producer_nodes"] == list(executor_ids)
    assert result["observed_consumer_nodes"] == list(executor_ids)
    source, sink = result["operators"]["Produce"], result["operators"]["Consume"]
    if mode == "ordinary":
        assert "fixed_r_enrolled_tasks" not in source
    elif mode == "copy":
        assert source["fixed_r_copy_baseline_tasks"] == 4
        assert sink["fixed_r_copy_baseline_tasks"] == 12
        assert source["fixed_r_enrolled_tasks"] == sink["fixed_r_enrolled_tasks"] == 0
    elif mode == "fixed_r":
        assert source["fixed_r_enrolled_tasks"] == 4
        assert sink["fixed_r_enrolled_tasks"] == 12
        assert source["fixed_r_recovered_tasks"] == sink["fixed_r_recovered_tasks"] == 0
    else:
        assert result["enrolled_at_failure"] == 3
        assert source["fixed_r_enrolled_tasks"] == source["fixed_r_recovered_tasks"] == 2
        assert sink["fixed_r_enrolled_tasks"] == sink["fixed_r_recovered_tasks"] == 1
        assert source["fixed_r_survivor_tasks"] == 2
        assert sink["fixed_r_survivor_tasks"] == 11
