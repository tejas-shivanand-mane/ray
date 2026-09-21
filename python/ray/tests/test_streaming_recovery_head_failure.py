"""Recovery across head replacement with a surviving driver and GCS storage."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import ray


@pytest.mark.skipif(sys.platform != "linux", reason="RocksDB GCS requires Linux")
@pytest.mark.parametrize("recovery_plan,point", [
    ("physical", "gated"), ("dataset", "gated"),
    ("dataset", "early"), ("dataset", "middle"), ("dataset", "late"),
])
def test_backpressure_survives_head_replacement(monkeypatch, recovery_plan, point):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    from streaming_recovery_benchmark import run_controlled
    from streaming_recovery_head_failure import local_head_failure_cluster

    if point != "gated":
        def unexpected_gate(*args, **kwargs):
            pytest.fail("Progress-triggered failure must not create a gate actor")

        monkeypatch.setattr("streaming_recovery_benchmark._make_gate", unexpected_gate)
    args = SimpleNamespace(
        case="fast-producer-slow-consumer", recovery_mode="fixed_r_head_failure",
        owner_node_id=None, executor_node_ids=None, local_executor_nodes=2,
        local_object_store_mb=150, producer_concurrency=2,
        num_input_blocks=4 if point == "gated" else 8,
        output_batches_per_input_batch=3, output_batch_rows=4, output_row_bytes=64,
        consumer_sleep_s=0.05, recovery_timeout_s=120,
        recovery_plan=recovery_plan, head_failure_point=point,
    )
    with local_head_failure_cluster(args) as (case_args, crash_head):
        driver_job_id = ray.get_runtime_context().get_job_id()
        result = run_controlled(case_args, crash_owner=crash_head)
        assert ray.get_runtime_context().get_job_id() == driver_job_id
        assert result["driver_job_id"] == driver_job_id
        assert result["original_head_processes_exited"]
        assert result["original_gcs_pid"] != result["replacement_gcs_pid"]
        assert result["original_head_node_id"] != result["replacement_head_node_id"]
        assert result["coordinator_node_id"] in result["surviving_node_ids"]
        assert result["original_head_node_id"] not in result["surviving_node_ids"]
        expected = args.num_input_blocks * args.output_batches_per_input_batch
        assert result["validated_output_blocks"] == expected
        if recovery_plan == "dataset":
            assert result["workload_variant"] == "public_dataset_unshaped_map_batches"
            assert result["physical_operator_names"] == [
                "MapBatches(produce)", "MapBatches(consume)",
            ]
        source = result["operators"]["Produce"]
        sink = result["operators"]["Consume"]
        if point == "gated":
            assert result["enrolled_at_failure"] == 3
            assert source["fixed_r_recovered_tasks"] == 2
            assert sink["fixed_r_recovered_tasks"] == 1
            assert source["fixed_r_survivor_tasks"] == 2
            assert sink["fixed_r_survivor_tasks"] == 11
        else:
            assert result["failure_gate_enabled"] is False
            assert result["head_failure_requested"]
            assert result["failure_trigger"] == "validated_output_progress"
            observed = result["observation_before_failure"]["validated_outputs"]
            assert result["target_validated_outputs"] <= observed < expected
            assert source["fixed_r_recovered_tasks"] + sink["fixed_r_recovered_tasks"] > 0
            for op, tasks in ((source, args.num_input_blocks), (sink, expected)):
                assert op["fixed_r_enrolled_tasks"] + op["fixed_r_survivor_tasks"] == tasks
                assert op["fixed_r_closed_streams"] == tasks
                assert len(op["fixed_r_recovered_task_details"]) == op["fixed_r_recovered_tasks"]
        nodes = {node["NodeID"]: node for node in ray.nodes()}
        assert not nodes[result["original_head_node_id"]]["Alive"]
        assert nodes[result["replacement_head_node_id"]]["Alive"]
        assert all(nodes[node]["Alive"] for node in result["surviving_node_ids"])


def test_head_failure_suite_preserves_failure_and_continues(monkeypatch):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    import streaming_recovery_head_failure as harness

    entered, exited, saved = [], [], []

    @contextmanager
    def fake_cluster(args):
        entered.append(args.head_failure_point)
        try:
            yield args, lambda: {}
        finally:
            exited.append(args.head_failure_point)

    def fake_run(args, crash_owner, diagnostics):
        diagnostics["head_failure_requested"] = True
        if args.head_failure_point == "early":
            raise RuntimeError("Enrollment failed during head loss")
        return {"validated_output_blocks": 128}

    class Benchmark:
        def __init__(self):
            self.result = {}

        def run_fn(self, key, fn, *args, **kwargs):
            self.result[key] = fn(*args, **kwargs)

        def write_result(self):
            saved.append(json.loads(json.dumps(self.result)))

    monkeypatch.setattr(harness, "local_head_failure_cluster", fake_cluster)
    monkeypatch.setattr(harness, "run_controlled", fake_run)
    args = SimpleNamespace(
        case="fast-producer-slow-consumer", recovery_plan="dataset",
        head_failure_point="suite", num_input_blocks=16,
        output_batches_per_input_batch=8,
    )
    benchmark = Benchmark()
    with pytest.raises(RuntimeError, match="cases failed: early"):
        harness.run_head_failure_cases(benchmark, args)
    assert entered == exited == ["early", "middle", "late"]
    assert [len(snapshot) for snapshot in saved] == [1, 2, 3]
    results = list(benchmark.result.values())
    assert [result["validation_status"] for result in results] == ["failed", "passed", "passed"]
    assert results[0]["head_failure_requested"]
    assert "Enrollment failed during head loss" in results[0]["traceback"]


def test_head_failure_observation_and_replay_details_are_json_serializable(monkeypatch):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    from streaming_recovery_benchmark import _failure_observation, _OutputProgress
    from ray._private.streaming_recovery import StreamingRecoveryRequired
    from ray.data._internal.execution.streaming_recovery import (
        StreamingRecoveryDataOpTask,
        new_metrics,
    )

    stats = new_metrics()
    task_id = ray.TaskID.for_fake_task(ray.JobID.from_int(1))
    stream = SimpleNamespace(
        task_id=task_id, next_index=0, expected_returns=2, reader=object(),
        closed=False, stats=stats,
        poll=Mock(side_effect=StreamingRecoveryRequired("owner lost")),
        recover=Mock(),
    )
    # A real DataOpTask exposes task_index() as a method. A mock attribute
    # would hide the exact bug in both the snapshot and replay-record paths.
    task = StreamingRecoveryDataOpTask(7, stream, Mock(), "test")
    assert task.on_data_ready(1, Mock()) == 0
    stream.recover.assert_called_once_with()
    op = SimpleNamespace(
        name="MapBatches(consume)", get_active_tasks=lambda: [task],
        metrics=SimpleNamespace(
            num_tasks_submitted=8, num_tasks_finished=7, num_tasks_failed=0,
            num_task_outputs_generated=7, rows_task_outputs_generated=7,
            extra_metrics=stats,
        ),
    )
    progress = _OutputProgress()
    progress.record(7)
    observation = _failure_observation(SimpleNamespace(operators=[op]), progress)
    decoded = json.loads(json.dumps(observation))
    details = decoded["operators"][op.name]
    assert details["active_enrolled_tasks"][0]["task_index"] == 7
    assert details["fixed_r_recovered_task_details"] == [
        {"task_index": 7, "task_id": task_id.hex()},
    ]


def test_benchmark_encoding_error_preserves_previous_report(monkeypatch, tmp_path):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    from benchmark import Benchmark

    report = tmp_path / "result.json"
    previous = '{"early": {"validation_status": "passed"}}'
    report.write_text(previous)
    monkeypatch.setenv("TEST_OUTPUT_JSON", str(report))
    benchmark = Benchmark()
    benchmark.result = {"middle": {"unexpected_method": benchmark.write_result}}
    with pytest.raises(TypeError, match="method"):
        benchmark.write_result()
    assert report.read_text() == previous
