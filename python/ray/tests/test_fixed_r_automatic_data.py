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
def test_variable_outputs_and_duplicate_names_need_no_declarations(benchmark_modules):
    def expand(batch):
        for value in batch["id"]:
            # Data-dependent output size, including no UDF output for id=0.
            yield {"value": np.arange(int(value) * 20, dtype=np.int64)}

    def increment(batch):
        return {"value": batch["value"] + 1}

    with benchmark_modules.local_head_failure_cluster(arguments("fixed_r")):
        context = DataContext.get_current().copy()
        context.enable_fixed_r_task_recovery = True
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
def test_actor_map_is_rejected_before_execution(benchmark_modules):
    class Identity:
        def __call__(self, batch):
            return batch

    with benchmark_modules.local_head_failure_cluster(arguments("fixed_r")):
        context = DataContext.get_current().copy()
        context.enable_fixed_r_task_recovery = True
        with DataContext.current(context):
            ds = ray.data.range(4).map_batches(Identity, compute=ray.data.ActorPoolStrategy(1))
            with pytest.raises(ValueError, match="task-map chains"):
                ds.materialize()


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
