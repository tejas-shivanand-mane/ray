"""Normal Dataset APIs with opt-in bounded finite-task recovery."""

import pickle
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pyarrow as pa
import pytest
import ray
from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import (
    CONFIG_KEY,
    BufferedRecoveryDataOpTask,
    FixedRDataConfig,
    buffered_map_task,
    get_config,
    new_metrics,
    submit_stream,
)


@pytest.fixture
def benchmark_modules(monkeypatch):
    directory = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(directory))
    import streaming_recovery_worker_dataset as harness

    return harness


def arguments(mode="fixed_r_head_failure", failure_stage="map"):
    return SimpleNamespace(
        worker_type="tasks", num_workers=2, num_operators=2,
        blocks_per_worker=2, num_scalar_cols=4, num_array_cols=2, seed=42,
        recovery_mode=mode, recovery_plan="dataset", recovery_failure_stage=failure_stage,
        local_executor_nodes=2, local_object_store_mb=150, recovery_timeout_s=120,
        recovery_failure_operator=1, owner_node_id=None, executor_node_ids=None,
        producer_concurrency=1,
    )


@pytest.mark.parametrize("failure_point,head_dead", [
    ("startup", True), ("startup", False), ("begin", True),
])
def test_unschedulable_helper_only_fails_over_before_begin(monkeypatch, failure_point, head_dead):
    from ray.data._internal.execution import streaming_recovery as runtime

    config = SimpleNamespace(
        automatic_outputs=False, dynamic_task_outputs=True, mode="fixed_r",
        timeout_s=1, owner_node_id="head", executor_for_task=lambda index: "worker",
    )
    liveness = iter([True, False if head_dead else True])
    monkeypatch.setattr(runtime, "_owner_alive", lambda config: next(liveness))
    # If head loss is not authoritative, exhaust the deadline immediately.
    clock = iter([0, 2])
    monkeypatch.setattr(runtime.time, "monotonic", lambda: next(clock))
    core = SimpleNamespace(validate_streaming_recovery_inputs=lambda refs: None)
    monkeypatch.setattr(ray._private.worker.global_worker, "core_worker", core, raising=False)
    owner = Mock()
    owner.__ray_ready__ = Mock()
    helper_class = Mock()
    helper_class.options.return_value.remote.return_value = owner
    monkeypatch.setattr(ray, "remote", lambda **options: lambda cls: helper_class)
    killed = Mock()
    monkeypatch.setattr(ray, "kill", killed)
    error = ray.exceptions.ActorUnschedulableError("head affinity no longer feasible")

    def ready(ref, **kwargs):
        if failure_point == "startup":
            raise error

    monkeypatch.setattr(ray, "get", ready)
    begin = Mock(side_effect=error)
    monkeypatch.setattr(runtime.StreamingRecoveryReader, "submit", begin)
    if failure_point == "begin":
        # The head is alive through readiness. Once begin may have run, an
        # identical error must not authorize a second submission.
        monkeypatch.setattr(runtime, "_owner_alive", lambda config: True)
    producer = Mock()
    metrics = new_metrics()
    if failure_point == "startup" and head_dead:
        stream = submit_stream(config, producer, (), {}, {}, 1, metrics)
        assert stream.reader is None
        producer.options.return_value.remote.assert_called_once()
        assert metrics["fixed_r_pre_submission_failovers"] == 1
        assert metrics["fixed_r_survivor_tasks"] == 1
        begin.assert_not_called()
    else:
        with pytest.raises(ray.exceptions.ActorUnschedulableError) as caught:
            submit_stream(config, producer, (), {}, {}, 1, metrics)
        assert caught.value is error
        producer.options.assert_not_called()
        assert metrics["fixed_r_pre_submission_failovers"] == 0
    killed.assert_called_once_with(owner, no_restart=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Local GCS RocksDB requires Linux")
@pytest.mark.parametrize("mode,stage", [
    ("copy", "map"), ("fixed_r", "map"),
    ("fixed_r_head_failure", "map"), ("fixed_r_head_failure", "read"),
])
def test_original_range_map_materialize(benchmark_modules, monkeypatch, mode, stage):
    import worker_scaling_benchmark as original

    monkeypatch.setattr(original, "TARGET_BLOCK_SIZE_BYTES", 4096)
    harness = benchmark_modules
    with harness.local_head_failure_cluster(arguments(mode, stage)) as (args, crash):
        result = harness.run_dataset(args, crash, {})
        assert result["validated_output_blocks"] == 4
        assert result["read_tasks_protected"]
        assert not result["user_declared_block_counts"]
        assert not result["calibration_required"]
        assert not result["benchmark_input_copies"]
        assert len(result["operators"]) == 3
        # The unmodified original UDF names repeat in the map chain.
        assert result["physical_operator_names"][1] == result["physical_operator_names"][2]
        if mode == "fixed_r_head_failure":
            target = 0 if stage == "read" else 2
            assert result["operators"][target]["fixed_r_recovered_tasks"] >= 1
            assert result["original_head_processes_exited"]
            assert result["coordinator_node_id"] in result["surviving_node_ids"]


@pytest.mark.skipif(sys.platform != "linux", reason="Local GCS RocksDB requires Linux")
@pytest.mark.parametrize("block_count", [0, 2])
def test_buffered_recovery_after_copy_before_eof(benchmark_modules, block_count):
    from ray._common.test_utils import wait_for_condition
    from ray.data._internal.execution.streaming_recovery import _BUFFERED_TASK_MARKER
    from ray.data.block import BlockExecStats, BlockMetadataWithSchema, TaskExecWorkerStats

    def produce():
        pairs = []
        for index in range(block_count):
            block = pa.table({"value": np.full(128_000, index, dtype=np.int64)})
            metadata = BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=BlockExecStats(wall_time_s=0.1, block_ser_time_s=0.1),
                task_exec_stats=TaskExecWorkerStats(task_wall_time_s=0.2),
            )
            pairs.append((block, pickle.dumps(metadata)))
        yield pairs
        yield _BUFFERED_TASK_MARKER

    with benchmark_modules.local_head_failure_cluster(arguments()) as (_, crash):
        context = DataContext.get_current().copy()
        context.enable_fixed_r_task_recovery = True
        context.fixed_r_task_recovery_output_mode = "buffered"
        metrics = new_metrics()
        stream = submit_stream(
            get_config(context), ray.remote(produce), (), {},
            {"num_returns": "streaming"}, 1, metrics,
        )
        outputs = []
        done = Mock()
        task = BufferedRecoveryDataOpTask(
            0, stream, Mock(), "test", output_ready_callback=outputs.append,
            task_done_callback=done,
        )
        try:
            def copied():
                task.on_data_ready(None, None)
                return stream.next_index == 2 and task._pending_block_ref.is_nil()

            wait_for_condition(copied, timeout=60)
            # Mimic local runtime aliases outliving the copy operation. Keep
            # these original refs local and quiescent throughout adoption.
            assert set(stream.reader.consumer._retained) == {0, 1}
            aliases = tuple(stream.reader.consumer._retained.values())
            crash()

            def finished():
                task.on_data_ready(None, None)
                return task.has_finished

            wait_for_condition(finished, timeout=60)
            assert metrics["fixed_r_recovered_tasks"] == 1
            assert metrics["fixed_r_closed_streams"] == 1
            assert metrics["fixed_r_copied_blocks"] == block_count
            assert not metrics["fixed_r_recovery_errors"]
            assert len(outputs) == block_count
            assert len(aliases) == 2
            assert not stream.reader.consumer._retained
            done.assert_called_once()
            assert done.call_args.args[0] is None
            refs = [ref for bundle in outputs for ref in bundle.block_refs]
            for index, block in enumerate(ray.get(refs)):
                assert block.num_rows == 128_000
                assert block["value"].to_pylist() == [index] * 128_000
        finally:
            stream.close()


@pytest.mark.skipif(sys.platform != "linux", reason="Local GCS RocksDB requires Linux")
@pytest.mark.parametrize("output_mode", ["buffered", "streaming"])
def test_variable_outputs_and_duplicate_names_need_no_declarations(benchmark_modules, output_mode):
    def expand(batch):
        for value in batch["id"]:
            # Data-dependent output size, including no UDF output for id=0.
            yield {"value": np.arange(int(value) * 20, dtype=np.int64)}

    def increment(batch):
        return {"value": batch["value"] + 1}

    with benchmark_modules.local_head_failure_cluster(arguments("fixed_r")):
        context = DataContext.get_current().copy()
        context.enable_fixed_r_task_recovery = True
        context.fixed_r_task_recovery_output_mode = output_mode
        context.target_max_block_size = 64
        context.enable_progress_bars = False
        with DataContext.current(context):
            ds = (ray.data.range(5, override_num_blocks=5)
                  .map_batches(expand)
                  .map_batches(increment)
                  .map_batches(increment)
                  .materialize())
            actual = sorted(row["value"] for row in ds.take_all())
            expected = sorted(value + 2 for i in range(5) for value in range(i * 20))
            assert actual == expected
            # Reuse materialized data through normal APIs and another execution.
            assert ds.map_batches(increment).materialize().count() == len(expected)


@pytest.mark.skipif(sys.platform != "linux", reason="Local GCS RocksDB requires Linux")
def test_buffered_actor_map_is_rejected_before_execution(benchmark_modules):
    class Identity:
        def __call__(self, batch):
            return batch

    with benchmark_modules.local_head_failure_cluster(arguments("fixed_r")):
        context = DataContext.get_current().copy()
        context.enable_fixed_r_task_recovery = True
        context.fixed_r_task_recovery_output_mode = "buffered"
        with DataContext.current(context):
            ds = ray.data.range(4).map_batches(
                Identity, compute=ray.data.ActorPoolStrategy(size=1)
            )
            with pytest.raises(ValueError, match="task-map chains"):
                ds.materialize()


def test_surviving_actor_placement_excludes_head_and_disables_reexecution():
    from ray.data._internal.execution.streaming_recovery import surviving_actor_options
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    head, first, second = [ray.NodeID.from_random().hex() for _ in range(3)]
    config = FixedRDataConfig(head, (first, second), {}, dynamic_task_outputs=True)
    options = {"scheduling_strategy": "SPREAD", "max_restarts": -1, "max_task_retries": -1}
    placed = surviving_actor_options(config, options, 1)
    assert placed["scheduling_strategy"].node_id == second
    assert not placed["scheduling_strategy"].soft
    assert placed["max_restarts"] == placed["max_task_retries"] == 0
    assert placed["lifetime"] == "non_detached"
    assert options["max_restarts"] == -1
    for forbidden in (
        {"scheduling_strategy": NodeAffinitySchedulingStrategy(head, soft=False)},
        {"scheduling_strategy": NodeAffinitySchedulingStrategy(first, soft=True)},
        {"lifetime": "detached"},
        {"get_if_exists": True, "name": "foreign-owner"},
        {"placement_group": object()},
    ):
        with pytest.raises(ValueError):
            surviving_actor_options(config, forbidden, 0)


@pytest.mark.parametrize("state", ["DEAD", "RESTARTING"])
def test_surviving_actor_loss_fails_instead_of_reconstructing(state):
    from ray.core.generated import gcs_pb2
    from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool

    pool = object.__new__(_ActorPool)
    pool._require_actor_survival = True
    actor = Mock()
    actor._get_local_state.return_value = getattr(gcs_pb2.ActorTableData.ActorState, state)
    with pytest.raises(RuntimeError, match="lost a required surviving actor"):
        pool._update_running_actor_state(actor)


def test_side_effecting_survivor_task_is_never_enrolled_even_while_head_alive(monkeypatch):
    from ray.data._internal.execution import streaming_recovery as runtime

    config = SimpleNamespace(
        automatic_outputs=False, dynamic_task_outputs=True, mode="fixed_r",
        executor_for_task=lambda index: ray.NodeID.from_random().hex(),
    )
    monkeypatch.setattr(runtime, "_owner_alive", lambda config: True)
    core = SimpleNamespace(validate_streaming_recovery_inputs=lambda refs: None)
    monkeypatch.setattr(ray._private.worker.global_worker, "core_worker", core, raising=False)
    remote = Mock(side_effect=AssertionError("Writes must not create owner helpers"))
    monkeypatch.setattr(ray, "remote", remote)
    producer = Mock()
    metrics = new_metrics()
    stream = submit_stream(
        config, producer, (), {}, {}, 1, metrics, survivor_only=True,
    )
    assert stream.reader is None
    assert metrics["fixed_r_survivor_tasks"] == 1
    assert metrics["fixed_r_enrolled_tasks"] == 0
    assert producer.options.call_args.kwargs["max_retries"] == 0
    assert producer.options.call_args.kwargs["retry_exceptions"] is False
    remote.assert_not_called()


def test_actor_survival_evidence_rejects_restart_or_head_placement(benchmark_modules):
    identity = {"actor_id": "actor", "worker_id": "process", "node_id": "worker", "pid": 1}
    benchmark_modules.validate_actor_survival([identity], [dict(identity)], ("worker",), 1)
    for changed in ({**identity, "pid": 2}, {**identity, "worker_id": "new-process"}):
        with pytest.raises(ValueError, match="process identity"):
            benchmark_modules.validate_actor_survival([identity], [changed], ("worker",), 1)
    with pytest.raises(ValueError, match="surviving-worker placement"):
        benchmark_modules.validate_actor_survival([identity], [identity], ("other",), 1)


def buffered_context(limit=1024 * 1024):
    context = DataContext.get_current().copy()
    context.eager_free = False
    context.set_config(CONFIG_KEY, FixedRDataConfig(
        ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex(), {},
        buffered_task_outputs=True, max_task_output_bytes=limit,
    ))
    return context


def test_buffering_snapshots_reused_udf_buffers(monkeypatch):
    from ray.data._internal.execution.operators import map_operator

    def task(*args, **kwargs):
        buffer = np.zeros(4, dtype=np.int64)
        for index in range(3):
            buffer[:] = index
            yield pa.table({"value": buffer})
            yield b"metadata"

    monkeypatch.setattr(map_operator, "_map_task", task)
    outputs, _ = list(buffered_map_task(None, buffered_context(), None))
    assert [block["value"].to_pylist() for block, _ in outputs] == [
        [0] * 4, [1] * 4, [2] * 4,
    ]


def test_buffered_blocks_preserve_serialization_stats(monkeypatch):
    from ray.data._internal.execution import streaming_recovery
    from ray.data._internal.execution.operators import map_operator
    from ray.data._internal.execution.util import yield_block_with_stats
    from ray.data.block import BlockAccessor, BlockExecStats, BlockMetadataWithSchema

    def task(*args, **kwargs):
        for index in range(2):
            block = pa.table({"value": [index]})

            def metadata(serialization_time_s):
                stats = BlockExecStats(
                    node_id="unit-test-node", wall_time_s=0.25,
                    block_ser_time_s=serialization_time_s,
                )
                return BlockMetadataWithSchema.from_metadata(
                    replace(BlockAccessor.for_block(block).get_metadata(), exec_stats=stats),
                    schema=block.schema,
                )

            # Use the real protocol helper from _map_task. next() in the
            # envelope wrapper would produce None serialization times here.
            yield from yield_block_with_stats(block, metadata)

    ticks = iter([10.0, 10.125, 20.0, 20.5])
    monkeypatch.setattr(streaming_recovery, "time", SimpleNamespace(
        perf_counter=lambda: next(ticks),
    ))
    monkeypatch.setattr(map_operator, "_map_task", task)
    outputs, _ = list(buffered_map_task(None, buffered_context(), None))
    stats = [pickle.loads(metadata).exec_stats for _, metadata in outputs]
    assert [item.block_ser_time_s for item in stats] == [0.125, 0.5]
    assert [item.wall_time_s for item in stats] == [0.25, 0.25]
    assert [block["value"].to_pylist() for block, _ in outputs] == [[0], [1]]


def test_dataset_failure_diagnostics_preserve_internal_frames(benchmark_modules):
    control, callback = benchmark_modules.execution_control(arguments())

    def fail_inside_executor():
        raise AssertionError("internal output failure")

    try:
        fail_inside_executor()
    except AssertionError as error:
        callback().after_execution_fails(None, error)
        error.with_traceback(None)

    assert "fail_inside_executor" in control.execution_error_traceback
    assert "AssertionError: internal output failure" in control.execution_error_traceback


@pytest.mark.parametrize("kind", ["limit", "udf_error", "incomplete_pair"])
def test_failed_buffered_task_publishes_no_partial_result(monkeypatch, kind):
    from ray.data._internal.execution.operators import map_operator

    def task(*args, **kwargs):
        yield pa.table({"value": [1, 2, 3]})
        if kind == "incomplete_pair":
            return
        yield b"metadata"
        if kind == "udf_error":
            raise RuntimeError("UDF failed after its first block")

    monkeypatch.setattr(map_operator, "_map_task", task)
    context = buffered_context(limit=1 if kind == "limit" else 1024 * 1024)
    generator = buffered_map_task(None, context, None)
    with pytest.raises((ValueError, RuntimeError)):
        next(generator)


def test_empty_task_has_a_valid_envelope(monkeypatch):
    from ray.data._internal.execution.operators import map_operator

    def task(*args, **kwargs):
        yield from ()

    monkeypatch.setattr(map_operator, "_map_task", task)
    outputs = list(buffered_map_task(None, buffered_context(), None))
    assert len(outputs) == 2
    assert outputs[0] == []
