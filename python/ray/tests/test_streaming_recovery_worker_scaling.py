"""Task worker workload recovery; run with the compiled Fixed-R fork."""

import copy
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import ray
from ray.data.block import BlockAccessor


@pytest.fixture
def worker_recovery(monkeypatch):
    benchmark_dir = Path(__file__).resolve().parents[3] / "release/nightly_tests/dataset"
    monkeypatch.syspath_prepend(str(benchmark_dir))
    import streaming_recovery_worker_scaling as recovery

    return recovery


def arguments(mode="fixed_r_head_failure", operators=2, failure_operator=1):
    return SimpleNamespace(
        worker_type="tasks", num_workers=2, num_operators=operators,
        blocks_per_worker=2, num_scalar_cols=4, num_array_cols=2, seed=42,
        recovery_mode=mode, local_executor_nodes=2, local_object_store_mb=150,
        recovery_timeout_s=120, recovery_failure_operator=failure_operator,
        owner_node_id=None, executor_node_ids=None, producer_concurrency=1,
    )


@pytest.mark.parametrize("field,value", [
    ("worker_type", "actors"), ("num_workers", 0),
    ("recovery_failure_operator", 2), ("recovery_failure_operator", -1),
    ("num_scalar_cols", -1), ("recovery_timeout_s", float("inf")),
])
def test_reject_unsupported_worker_recovery(worker_recovery, field, value):
    args = arguments()
    setattr(args, field, value)
    with pytest.raises(ValueError):
        worker_recovery.validate_recovery_args(args)


def test_schema_validation_rejects_corrupted_values(worker_recovery):
    import worker_scaling_benchmark as original

    args = arguments()
    args.recovery_rows_per_block = 3
    udf = original.make_realistic_schema_udf(args.seed, 4, 2)
    batch = udf({"id": np.arange(3)})
    good = BlockAccessor.batch_to_arrow_block(batch)
    worker_recovery.validate_block(good, args, good.schema)
    for column in ("scalar_col_0", "array_col_0"):
        bad_batch = copy.deepcopy(batch)
        bad_batch[column][0] += np.float32(1)
        with pytest.raises(ValueError, match="Incorrect"):
            worker_recovery.validate_block(
                BlockAccessor.batch_to_arrow_block(bad_batch), args, good.schema,
            )


def test_replacement_without_target_replay_is_not_success(worker_recovery):
    args = arguments()
    args.recovery_input_blocks = 4
    args.recovery_rows_per_block = 3
    op = dict(
        tasks_submitted=4, tasks_finished=4, tasks_failed=0, output_blocks=4,
        output_rows=12, fixed_r_closed_streams=4, fixed_r_copied_blocks=4,
        fixed_r_enrolled_tasks=2, fixed_r_survivor_tasks=2,
        fixed_r_copy_baseline_tasks=0, fixed_r_recovered_tasks=0,
        fixed_r_recovered_task_details=[], fixed_r_pre_submission_failovers=0,
    )
    metrics = {f"MapBatches(worker_schema_{i})": copy.deepcopy(op) for i in range(2)}
    # Even replay in another stage is not sufficient for the selected task gate.
    metrics["MapBatches(worker_schema_0)"].update(
        fixed_r_recovered_tasks=1,
        fixed_r_recovered_task_details=[{"task_index": 0, "task_id": "first-stage"}],
    )
    with pytest.raises(ValueError, match="selected protected task"):
        worker_recovery.validate_metrics(metrics, args)
    metrics["MapBatches(worker_schema_1)"].update(
        fixed_r_recovered_tasks=1,
        fixed_r_recovered_task_details=[{"task_index": 0, "task_id": "last-stage"}],
    )
    worker_recovery.validate_metrics(metrics, args)
    metrics["MapBatches(worker_schema_1)"]["fixed_r_closed_streams"] -= 1
    with pytest.raises(ValueError, match="retirement mismatch"):
        worker_recovery.validate_metrics(metrics, args)


@pytest.mark.skipif(sys.platform != "linux", reason="RocksDB GCS requires Linux")
@pytest.mark.parametrize("mode,operators,stage", [
    ("copy", 2, 1), ("fixed_r", 2, 1),
    ("fixed_r_head_failure", 1, 0), ("fixed_r_head_failure", 2, 1),
])
def test_worker_schema_original_udf_recovery(worker_recovery, monkeypatch, mode, operators, stage):
    import worker_scaling_benchmark as original

    # Keep regression data small; CLI benchmark retains the original 16 MiB sizing.
    monkeypatch.setattr(original, "TARGET_BLOCK_SIZE_BYTES", 4096)
    args = arguments(mode, operators, stage)
    with worker_recovery.local_head_failure_cluster(args) as (selected, crash_head):
        refs, payload, schema = worker_recovery.prepare_workload(selected)
        job_id = ray.get_runtime_context().get_job_id()
        diagnostics = {}
        result = worker_recovery.run_workload(
            selected, refs, payload, schema, crash_head, diagnostics,
        )
        assert result["validated_output_blocks"] == 4
        assert result["validated_output_rows"] == 4 * selected.recovery_rows_per_block
        assert len(result["operators"]) == operators
        assert result["block_shaping_enabled"]
        assert not result["fusion_enabled"]
        assert ray.get_runtime_context().get_job_id() == job_id
        assert len(ray.get(refs)) == 4  # Retained inputs survive full head replacement.
        if mode == "fixed_r_head_failure":
            assert result["original_head_processes_exited"]
            assert result["driver_job_id"] == job_id
            assert result["coordinator_node_id"] in result["surviving_node_ids"]
            op = result["operators"][f"MapBatches(worker_schema_{stage})"]
            assert any(t["task_index"] == 0 for t in op["fixed_r_recovered_task_details"])
        json.dumps(result)
        assert all(not op["active_enrolled_tasks"] for op in
                   diagnostics["last_observation"]["operators"].values())


def test_worker_suite_saves_failures_and_runs_remaining_modes(worker_recovery, monkeypatch):
    import benchmark

    entered, saved = [], []

    @contextmanager
    def cluster(args):
        entered.append(args.recovery_mode)
        yield args, lambda: {}

    def run(args, *unused):
        if args.recovery_mode == "copy":
            raise RuntimeError("bad output values")
        return {"validated_output_blocks": 4}

    class Benchmark:
        def __init__(self):
            self.result = {}

        def run_fn(self, key, fn, *args):
            self.result[key] = fn(*args)

        def write_result(self):
            saved.append(json.loads(json.dumps(self.result)))

    monkeypatch.setattr(benchmark, "Benchmark", Benchmark)
    monkeypatch.setattr(worker_recovery, "local_head_failure_cluster", cluster)
    monkeypatch.setattr(worker_recovery, "prepare_workload", lambda args: ([], b"", None))
    monkeypatch.setattr(worker_recovery, "run_workload", run)
    with pytest.raises(RuntimeError, match="cases failed: copy"):
        worker_recovery.run_recovery_cases(arguments("suite"))
    assert entered == ["copy", "fixed_r", "fixed_r_head_failure"]
    assert [len(snapshot) for snapshot in saved] == [1, 2, 3]
    assert [case["validation_status"] for case in saved[-1].values()] == [
        "failed", "passed", "passed",
    ]
