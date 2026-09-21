"""Streaming Dataset regressions, including copied-output lifetime and head loss."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import ray
from ray.data._internal.execution.streaming_recovery import StreamingRecoveryDataOpTask


def test_copied_return_keeps_python_and_native_aliases(monkeypatch):
    core = Mock()
    core.try_release_streaming_recovery_return.return_value = True
    monkeypatch.setattr(ray._private.worker.global_worker, "core_worker", core, raising=False)
    retained = {0: ray.ObjectRef.from_random()}
    consumer = SimpleNamespace(_retained=retained, phase="forwarding",
                               release=lambda index: retained.pop(index))
    task = object.__new__(StreamingRecoveryDataOpTask)
    task.stream = SimpleNamespace(reader=SimpleNamespace(consumer=consumer))
    task._copied_return_indices = {0}
    alias = retained[0]
    task._release_unused_copies()
    assert 0 in retained
    core.try_release_streaming_recovery_return.assert_not_called()
    del alias
    # Use a plain callable: Mock would retain its argument as an extra alias.
    core.try_release_streaming_recovery_return = lambda ref: False
    task._release_unused_copies()
    assert 0 in retained
    core.try_release_streaming_recovery_return = lambda ref: True
    task._release_unused_copies()
    assert not retained
    assert not task._copied_return_indices


@pytest.mark.skipif(sys.platform != "linux", reason="Local GCS RocksDB requires Linux")
@pytest.mark.parametrize("mode,point", [
    ("copy", "none"), ("fixed_r", "none"),
    ("fixed_r_head_failure", "producer_before_output"),
    ("fixed_r_head_failure", "producer_after_output"),
    ("fixed_r_head_failure", "consumer"),
])
def test_original_backpressure_runtime_head_failure(monkeypatch, mode, point):
    monkeypatch.syspath_prepend(str(
        Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    ))
    from ray.data import DataContext
    from streaming_recovery_backpressure_dataset import run_dataset
    from streaming_recovery_head_failure import local_head_failure_cluster

    args = SimpleNamespace(
        case="fast-producer-slow-consumer", recovery_plan="runtime", recovery_mode=mode,
        runtime_failure_point=point, local_executor_nodes=2, local_object_store_mb=150,
        owner_node_id=None, executor_node_ids=None, producer_concurrency=1,
        recovery_timeout_s=120, num_input_blocks=4, output_batches_per_input_batch=4,
        output_batch_rows=4, output_row_bytes=1024, consumer_sleep_s=0.01,
    )
    with local_head_failure_cluster(args) as (selected, crash):
        context = DataContext.get_current().copy()
        context.target_max_block_size = 4096
        with DataContext.current(context):
            result = run_dataset(selected, crash, {})
        assert result["validated_producer_rows"] == 64
        assert not result["whole_task_buffering"]
        assert not result["calibration_required"]
        if mode == "fixed_r_head_failure":
            assert result["original_head_processes_exited"]
            if point == "producer_after_output":
                assert result["consumed_returns_before_failure"] >= 2
