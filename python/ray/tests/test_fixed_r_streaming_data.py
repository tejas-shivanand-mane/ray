"""Streaming Dataset regressions, including copied-output lifetime and head loss."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import ray
from ray.data._internal.execution.streaming_recovery import StreamingRecoveryDataOpTask


def test_copied_return_keeps_python_and_native_aliases(monkeypatch):
    release_attempts = []
    releasable = False

    def try_release(ref):
        # Record only the ID: retaining the ObjectRef would change this test.
        release_attempts.append(ref.hex())
        return releasable

    # ObjectRef construction calls add_object_ref_reference(self). A Mock
    # records that argument and becomes an unintended strong Python alias,
    # even if the release method itself is replaced with a plain callable.
    core = SimpleNamespace(
        add_object_ref_reference=lambda ref: None,
        remove_object_ref_reference=lambda ref: None,
        try_release_streaming_recovery_return=try_release,
    )
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
    assert not release_attempts
    del alias
    task._release_unused_copies()
    assert 0 in retained
    assert len(release_attempts) == 1
    releasable = True
    task._release_unused_copies()
    assert len(release_attempts) == 2
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


def test_wait_state_capture_does_not_poll_or_lock_consumers(monkeypatch):
    import json

    monkeypatch.syspath_prepend(str(
        Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    ))
    import streaming_recovery_backpressure_dataset as harness

    def unexpected_call(*args, **kwargs):
        raise AssertionError("Diagnostics must not poll streams or make Ray calls")

    class Consumer:
        _phase = "replaying"
        _next_index = 2
        _retained = {1: None}
        phase = property(unexpected_call)
        next_index = property(unexpected_call)

    reader = SimpleNamespace(
        consumer=Consumer(), _pending_read=None, _recovery_required=False,
        get_waitable=unexpected_call,
    )
    stream = SimpleNamespace(
        reader=reader, task_id=SimpleNamespace(hex=lambda: "task-id"),
        closed=False, next_index=2,
    )
    task = SimpleNamespace(
        stream=stream, task_index=lambda: 0,
        _pending_block_ref=None, _pending_meta_ref=None,
        _copied_return_indices={0, 1},
        _recovery_wait_reason="waiting_for_metadata_ref",
        get_waitable=unexpected_call,
    )

    class Operator:
        name = "producer"

        def get_active_tasks(self):
            return [task]

    control = SimpleNamespace(operators=[Operator()], executor=None)
    monkeypatch.setattr(harness, "snapshot", lambda control: [{"name": "producer"}])
    monkeypatch.setattr(ray, "wait", unexpected_call)
    monkeypatch.setattr(ray, "get", unexpected_call)
    observation = harness.capture_wait_state(control)
    json.dumps(observation)
    assert observation["captured_before_shutdown"]
    state = observation["operators"][0]["task_wait_states"][0]
    assert state["consumer_phase"] == "replaying"
    assert state["retained_return_indices"] == [1]
    assert state["wait_reason"] == "waiting_for_metadata_ref"
    assert observation["thread_stacks"]
