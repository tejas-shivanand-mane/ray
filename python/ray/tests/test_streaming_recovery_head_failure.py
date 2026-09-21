"""Recovery across head replacement with a surviving driver and GCS storage."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import ray


@pytest.mark.skipif(sys.platform != "linux", reason="RocksDB GCS requires Linux")
def test_backpressure_survives_head_replacement(monkeypatch):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    from streaming_recovery_benchmark import run_controlled
    from streaming_recovery_head_failure import local_head_failure_cluster

    args = SimpleNamespace(
        case="fast-producer-slow-consumer", recovery_mode="fixed_r_head_failure",
        owner_node_id=None, executor_node_ids=None, local_executor_nodes=2,
        local_object_store_mb=150, producer_concurrency=2, num_input_blocks=4,
        output_batches_per_input_batch=3, output_batch_rows=4, output_row_bytes=64,
        consumer_sleep_s=0.01, recovery_timeout_s=120,
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
        assert result["validated_output_blocks"] == 12
        assert result["enrolled_at_failure"] == 3
        source = result["operators"]["Produce"]
        sink = result["operators"]["Consume"]
        assert source["fixed_r_recovered_tasks"] == 2
        assert sink["fixed_r_recovered_tasks"] == 1
        assert source["fixed_r_survivor_tasks"] == 2
        assert sink["fixed_r_survivor_tasks"] == 11
        nodes = {node["NodeID"]: node for node in ray.nodes()}
        assert not nodes[result["original_head_node_id"]]["Alive"]
        assert nodes[result["replacement_head_node_id"]]["Alive"]
        assert all(nodes[node]["Alive"] for node in result["surviving_node_ids"])
