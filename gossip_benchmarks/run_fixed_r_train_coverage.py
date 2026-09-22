"""One bounded XGBoost Train-v2 head failure and end-to-end inference run.

Uses the collaborator's original train loop, report callback and predictor.
Only local data/resources/storage, observations and head replacement live here.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import traceback
from types import SimpleNamespace
import uuid

import ray
from ray import cloudpickle
from ray.data import DataContext
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_recovery import get_config
from ray.train.v2._internal.execution.callback import (
    ControllerCallback, ReportCallback, WorkerGroupCallback,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "release/nightly_tests/dataset"))
sys.path.insert(0, str(ROOT / "release/train_tests/xgboost_lightgbm"))
from streaming_recovery_head_failure import local_head_failure_cluster
from streaming_recovery_progress import ProgressTrigger, snapshot, register_for_task_serialization

NAMESPACE = "fixed-r-train-benchmark"


def process_identity():
    runtime = ray.get_runtime_context()
    return {
        "actor_id": runtime.get_actor_id(), "worker_id": runtime.get_worker_id(),
        "node_id": runtime.get_node_id(), "pid": os.getpid(),
    }


def train_worker_identity():
    context = ray.train.get_context()
    return {
        **process_identity(), "world_rank": context.get_world_rank(),
        "world_size": context.get_world_size(),
    }


class Monitor:
    def __init__(self, num_boost_round=10, failure_after_round=None, num_train_workers=1):
        self.executions = {}
        self.lifecycle = {}
        self.trigger = None
        self.num_boost_round = num_boost_round
        self.failure_after_round = failure_after_round
        self.num_train_workers = num_train_workers
        self.training_reports = 0
        self.boosting_rounds = 0
        self.final_checkpoint_reported = False
        self.rounds_at_head_replacement = None

    def training_report(self, report_number, worker_count, has_checkpoint):
        if report_number != self.training_reports + 1 or worker_count != self.num_train_workers:
            raise ValueError("Training reports are missing, repeated, or have the wrong worker count")
        self.training_reports = report_number
        if report_number <= self.num_boost_round:
            if has_checkpoint:
                raise ValueError("Expected the original callback's final-only checkpoint policy")
            self.boosting_rounds = report_number
            if report_number == self.failure_after_round:
                self.trigger = {
                    "failure_phase": "training_boosting",
                    "completed_boosting_rounds": report_number,
                    "reporting_workers": worker_count,
                    "observed_ns": time.monotonic_ns(),
                }
        elif report_number == self.num_boost_round + 1 and has_checkpoint:
            self.final_checkpoint_reported = True
        else:
            raise ValueError("Unexpected report sequence from the original XGBoost callback")

    def head_replaced(self):
        if self.rounds_at_head_replacement is not None:
            raise ValueError("Head replacement was recorded more than once")
        self.rounds_at_head_replacement = self.boosting_rounds
        # The original synchronous report can have one queued result and one
        # iteration blocked on enqueue. Leave room beyond those buffered rounds.
        if not self.failure_after_round <= self.boosting_rounds < self.num_boost_round - 2:
            raise ValueError("Head replacement finished too late to prove continued boosting")
        return self.rounds_at_head_replacement

    def record(self, event, value):
        if event in self.lifecycle:
            raise ValueError(f"Repeated Train lifecycle event: {event}")
        self.lifecycle[event] = value

    def publish(self, phase, observation):
        previous = self.executions.get(phase, {})
        if previous.get("state") in ("finished", "failed") and observation["state"] == "running":
            return
        if observation["observed_ns"] >= previous.get("observed_ns", -1):
            self.executions[phase] = observation
        if observation.get("trigger") is not None and self.trigger is None:
            self.trigger = observation["trigger"]

    def read(self):
        return {
            "executions": self.executions, "lifecycle": self.lifecycle, "trigger": self.trigger,
            "training_progress": {
                "reports": self.training_reports, "boosting_rounds": self.boosting_rounds,
                "final_checkpoint_reported": self.final_checkpoint_reported,
                "rounds_at_head_replacement": self.rounds_at_head_replacement,
            },
        }


class TrainingProgressProbe(ReportCallback):
    """Observe the original callback's per-round, all-worker reports; never gate training."""

    def __init__(self, monitor_name):
        self.monitor_name = monitor_name
        self.monitor = None
        self.reports = 0

    def after_report(self, training_report, metrics):
        if self.monitor is None:
            # Resolve before failure and retain the surviving actor handle.
            self.monitor = ray.get_actor(self.monitor_name, namespace=NAMESPACE)
        self.reports += 1
        ray.get(self.monitor.training_report.remote(
            self.reports, len(metrics), training_report.checkpoint is not None,
        ), timeout=5)


def checkpoint_worker_identity():
    from ray.train.xgboost import RayTrainReportCallback

    checkpoint = ray.train.get_checkpoint()
    return {
        **train_worker_identity(),
        "restored_checkpoint_rounds": (
            RayTrainReportCallback.get_model(checkpoint).num_boosted_rounds() if checkpoint else 0
        ),
        "restored_checkpoint_path": checkpoint.path if checkpoint else None,
    }


def model_tree_digest(model):
    return hashlib.sha256("\n".join(model.get_dump(dump_format="json")).encode()).hexdigest()


class CheckpointMonitor(Monitor):
    def __init__(self, *args):
        super().__init__(*args)
        self.worker_groups = []
        self.checkpoint_reports = []
        self.worker_failure_injected = False
        self.worker_target = None

    def worker_group_started(self, identities, actors):
        if len(self.worker_groups) >= 2:
            raise ValueError("Unexpected extra training worker restart")
        self.worker_groups.append(identities)
        if len(self.worker_groups) == 1:
            self.lifecycle["workers_before"] = identities
            self.worker_target = actors[0]

    def failure_target(self):
        return self.worker_target

    def mark_worker_failure(self):
        if self.worker_failure_injected or self.trigger is None:
            raise ValueError("Worker failure must follow exactly one checkpoint trigger")
        self.worker_failure_injected = True

    def failure_injected(self):
        return self.worker_failure_injected

    def checkpoint_report(self, metrics, checkpoint_info):
        rounds = {m["boosting_rounds"] for m in metrics}
        restored = {m["restored_checkpoint_rounds"] for m in metrics}
        if len(metrics) != self.num_train_workers or len(rounds) != 1 or len(restored) != 1:
            raise ValueError("Workers disagree on model progress or restored checkpoint")
        completed, origin = rounds.pop(), restored.pop()
        attempt = len(self.worker_groups)
        expected_origin = 0 if attempt == 1 else self.failure_after_round
        if origin != expected_origin or not origin < completed <= self.num_boost_round:
            raise ValueError("Training restarted from the wrong checkpoint or added extra rounds")
        prior = [r for r in self.checkpoint_reports if r["attempt"] == attempt]
        if completed != (prior[-1]["boosting_rounds"] if prior else origin) + 1:
            raise ValueError("Training reports skipped or repeated model rounds")
        if checkpoint_info and checkpoint_info["rounds"] != completed:
            raise ValueError("Persisted model does not match the reported training round")
        self.checkpoint_reports.append({
            "attempt": attempt, "boosting_rounds": completed,
            "restored_checkpoint_rounds": origin, "checkpoint": checkpoint_info,
            "reporting_workers": len(metrics),
        })
        if attempt == 1 and completed == self.failure_after_round:
            if not checkpoint_info:
                raise ValueError("Failure point must have a persisted, controller-registered checkpoint")
            self.trigger = {
                "failure_phase": "training_checkpoint_restart",
                "checkpoint_rounds": completed, "checkpoint_path": checkpoint_info["path"],
                "checkpoint_tree_digest": checkpoint_info["tree_digest"],
                "observed_ns": time.monotonic_ns(),
            }
            return True
        return False

    def read(self):
        return {
            **super().read(),
            "checkpoint_recovery": {
                "worker_groups": self.worker_groups, "reports": self.checkpoint_reports,
                "worker_failure_injected": self.worker_failure_injected,
            },
        }


class CheckpointRecoveryProbe(WorkerGroupCallback, ControllerCallback, ReportCallback):
    """Observe actual restored models; gate one report while injecting the faults."""

    def __init__(self, monitor_name):
        self.monitor_name = monitor_name
        self.monitor = None
        self.attempt = 0

    def get_monitor(self):
        if self.monitor is None:
            self.monitor = ray.get_actor(self.monitor_name, namespace=NAMESPACE)
        return self.monitor

    def after_controller_start(self, train_run_context):
        ray.get(self.get_monitor().record.remote("controller_before", process_identity()), timeout=5)

    def after_controller_finish(self, result):
        ray.get(self.get_monitor().record.remote("controller_after", process_identity()), timeout=5)

    def after_worker_group_start(self, worker_group):
        self.attempt += 1
        identities = ray.get(worker_group.execute_async(checkpoint_worker_identity), timeout=15)
        ray.get(self.get_monitor().worker_group_started.remote(
            identities, [worker.actor for worker in worker_group.get_workers()],
        ), timeout=5)

    def before_worker_group_shutdown(self, worker_group):
        # The first group deliberately contains a dead actor; probe only the
        # successful replacement group at its normal shutdown.
        if self.attempt == 2:
            identities = ray.get(worker_group.execute_async(train_worker_identity), timeout=15)
            ray.get(self.get_monitor().record.remote("workers_after", identities), timeout=5)

    def after_report(self, training_report, metrics):
        from ray.train.xgboost import RayTrainReportCallback

        info = None
        if training_report.checkpoint is not None:
            model = RayTrainReportCallback.get_model(training_report.checkpoint)
            info = {"path": training_report.checkpoint.path, "rounds": model.num_boosted_rounds(),
                    "tree_digest": model_tree_digest(model)}
        # Train invokes CheckpointManager before user ReportCallbacks, so this
        # checkpoint is both on storage and registered for the next worker group.
        gate = ray.get(self.get_monitor().checkpoint_report.remote(metrics, info), timeout=5)
        if gate:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if ray.get(self.get_monitor().failure_injected.remote(), timeout=5):
                    return
                time.sleep(0.05)
            raise TimeoutError("Head/worker fault injection did not finish within 60 seconds")


class TrainSurvivalProbe(WorkerGroupCallback, ControllerCallback):
    def __init__(self, monitor_name):
        self.monitor_name = monitor_name

    def record(self, event, value):
        monitor = ray.get_actor(self.monitor_name, namespace=NAMESPACE)
        ray.get(monitor.record.remote(event, value), timeout=5)

    def after_controller_start(self, train_run_context):
        self.record("controller_before", process_identity())

    def after_controller_finish(self, result):
        self.record("controller_after", process_identity())

    def after_worker_group_start(self, worker_group):
        self.record("workers_before", ray.get(worker_group.execute_async(train_worker_identity), timeout=15))

    def before_worker_group_shutdown(self, worker_group):
        try:
            identities = ray.get(worker_group.execute_async(train_worker_identity), timeout=15)
            self.record("workers_after", identities)
        except Exception:
            self.record("worker_probe_error", traceback.format_exc())


def parquet_read_stage(names):
    # V2 lists file manifests before reading rows. Listing progress/replay is
    # not evidence of recovery of the training data itself.
    matches = [index for index, name in enumerate(names)
               if name in ("ReadParquet", "ReadFilesParquetV2")]
    if len(matches) != 1:
        raise ValueError(f"Expected one Parquet ingestion stage, got {names}")
    return matches[0]


def capture_execution(monitor_name, phase, total_rows, *, trigger_ingestion=True):
    class Capture(ExecutionCallback):
        def before_execution_starts(self, executor):
            self.control = SimpleNamespace(
                executor=executor,
                operators=[op for op in executor._topology if isinstance(op, MapOperator)],
            )
            if not self.control.operators:
                return  # Train's materialized shard can execute an InputData-only plan.
            self.monitor = ray.get_actor(monitor_name, namespace=NAMESPACE)
            self.trigger = None
            if phase == "training" and trigger_ingestion:
                stage = parquet_read_stage([op.name for op in self.control.operators])
                self.trigger = ProgressTrigger(total_rows, "early", stage)
            self.last_report = 0
            if self.trigger is not None:
                op = self.control.operators[self.trigger.stage]
                submit = op._submit_data_task

                def submit_and_observe(stream, *args, **kwargs):
                    submit(stream, *args, **kwargs)
                    task = next(task for task in op.get_active_tasks()
                                if getattr(task, "stream", None) is stream)
                    emit = task._emit_copied_pair

                    def emit_and_observe(pair):
                        size = emit(pair)
                        if not self.trigger.ready.is_set():
                            self.trigger.observe(snapshot(self.control))
                            if self.trigger.ready.is_set():
                                self.publish("running")
                        return size

                    task._emit_copied_pair = emit_and_observe

                op._submit_data_task = submit_and_observe
            self.publish("running")

        def publish(self, state, error=None):
            if not getattr(self, "control", None) or not self.control.operators:
                return
            self.last_report = time.monotonic()
            self.monitor.publish.remote(phase, {
                "state": state, "observed_ns": time.monotonic_ns(),
                "operators": snapshot(self.control), "coordinator": process_identity(),
                "trigger": self.trigger.observation if self.trigger is not None else None,
                "error": error,
            })

        def on_execution_step(self, executor):
            if hasattr(self, "last_report") and time.monotonic() - self.last_report >= 0.5:
                self.publish("running")

        def after_execution_succeeds(self, executor):
            self.publish("finished")

        def after_execution_fails(self, executor, error):
            self.publish("failed", "".join(traceback.format_exception(
                type(error), error, error.__traceback__,
            )))

    return Capture


def make_input(directory, blocks=32, rows_per_block=1024):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir()
    rng = np.random.default_rng(42)
    for index in range(blocks):
        values = rng.normal(size=(rows_per_block, 16)).astype(np.float32)
        columns = {f"feature_{i}": values[:, i] for i in range(16)}
        columns["labels"] = (values[:, 0] + values[:, 1] > 0).astype(np.int32)
        pq.write_table(pa.table(columns), directory / f"part-{index:04d}.parquet")
    return blocks * rows_per_block


def validate_observations(
    observation, total_rows, executor_nodes, coordinator_node, num_train_workers=1,
    *, failure_phase="ingestion", num_boost_round=10, failure_after_round=None,
):
    lifecycle = observation["lifecycle"]
    before, after = lifecycle.get("workers_before"), lifecycle.get("workers_after")
    if failure_phase == "checkpoint":
        validate_checkpoint_restart(observation, executor_nodes, num_train_workers,
                                    num_boost_round, failure_after_round)
    elif (not before or len(before) != num_train_workers or before != after
            or any(worker["node_id"] not in executor_nodes for worker in before)
            or len({worker["actor_id"] for worker in before}) != num_train_workers
            or len({worker["worker_id"] for worker in before}) != num_train_workers
            or {worker.get("world_rank") for worker in before} != set(range(num_train_workers))
            or any(worker.get("world_size") != num_train_workers for worker in before)
            or "worker_probe_error" in lifecycle):
        raise ValueError("The original Train worker group did not survive with all expected ranks")
    if len({worker["node_id"] for worker in before}) != num_train_workers:
        raise ValueError("Local Train workers must occupy separate surviving executor nodes")
    controller = lifecycle.get("controller_before")
    if (not controller or controller != lifecycle.get("controller_after")
            or controller["node_id"] != coordinator_node):
        raise ValueError("The Train controller process did not survive")
    if observation.get("trigger") is None:
        raise ValueError("Head failure was not triggered during the selected training phase")
    if failure_phase == "boosting":
        progress = observation["training_progress"]
        trigger = observation["trigger"]
        replaced_at = progress["rounds_at_head_replacement"]
        if (trigger.get("failure_phase") != "training_boosting"
                or trigger.get("completed_boosting_rounds") != failure_after_round
                or trigger.get("reporting_workers") != num_train_workers
                or replaced_at is None
                or not failure_after_round <= replaced_at < num_boost_round - 2
                or progress["boosting_rounds"] != num_boost_round
                or progress["reports"] != num_boost_round + 1
                or not progress["final_checkpoint_reported"]):
            raise ValueError("Training did not demonstrate all-worker progress after head replacement")
    training = observation["executions"].get("training", {})
    inference = observation["executions"].get("inference", {})
    for execution in (training, inference):
        if (execution.get("state") != "finished"
                or execution["coordinator"]["node_id"] != coordinator_node):
            raise ValueError("An original Dataset pipeline did not finish on its surviving coordinator")
        for op in execution["operators"]:
            if (op["tasks_failed"] or op["tasks_submitted"] != op["tasks_finished"]
                    or op["active_enrolled_tasks"] or op.get("fixed_r_recovery_errors")):
                raise ValueError(f"Incomplete or failed operator: {op}")
            if "fixed_r_closed_streams" in op and op["fixed_r_closed_streams"] != op["tasks_submitted"]:
                raise ValueError(f"Unclosed task streams: {op}")
    stage = parquet_read_stage([op["name"] for op in training["operators"]])
    reads = training["operators"][stage]
    if reads["output_rows"] != total_rows:
        raise ValueError("Training Parquet ingestion did not validate all rows")
    if failure_phase == "ingestion" and reads.get("fixed_r_recovered_tasks", 0) < 1:
        raise ValueError("Training Parquet ingestion did not exercise replay")
    actors = [op for op in inference["operators"]
              if op.get("fixed_r_actor_mode") == "surviving_coordinator_owned"]
    writes = [op for op in inference["operators"] if op["name"] == "Write"]
    if len(actors) != 1 or actors[0]["output_rows"] != total_rows:
        raise ValueError("Original actor-based inference did not produce all predictions")
    if (len(writes) != 1 or not writes[0].get("fixed_r_survivor_only")
            or writes[0].get("fixed_r_enrolled_tasks") != 0
            or writes[0].get("fixed_r_recovered_tasks") != 0
            or writes[0].get("fixed_r_survivor_tasks") != writes[0]["tasks_submitted"]):
        raise ValueError("External writes must be coordinator-owned and never enrolled or replayed")


def validate_checkpoint_restart(observation, executor_nodes, num_workers, target_rounds, saved_rounds):
    recovery = observation["checkpoint_recovery"]
    groups = recovery["worker_groups"]
    trigger = observation.get("trigger", {})
    if (len(groups) != 2 or not recovery["worker_failure_injected"]
            or trigger.get("failure_phase") != "training_checkpoint_restart"
            or trigger.get("checkpoint_rounds") != saved_rounds):
        raise ValueError("Expected one checkpoint-triggered failure and one worker group restart")
    for index, group in enumerate(groups):
        if (len(group) != num_workers
                or {w["world_rank"] for w in group} != set(range(num_workers))
                or any(w["world_size"] != num_workers or w["node_id"] not in executor_nodes for w in group)
                or len({w["node_id"] for w in group}) != num_workers
                or len({w["actor_id"] for w in group}) != num_workers
                or len({w["worker_id"] for w in group}) != num_workers
                or any(w["restored_checkpoint_rounds"] != (saved_rounds if index else 0) for w in group)
                or any(w["restored_checkpoint_path"] != (trigger["checkpoint_path"] if index else None)
                       for w in group)):
            raise ValueError("Worker group ranks, placement, or restored checkpoint are incorrect")
    # get_checkpoint() advances when a worker reports a newer checkpoint. Compare
    # only process/rank identity at shutdown, preserving the start-time restore evidence.
    replacement_identities = [
        {k: v for k, v in w.items() if not k.startswith("restored_checkpoint_")}
        for w in groups[1]
    ]
    if ({w["actor_id"] for w in groups[0]} & {w["actor_id"] for w in groups[1]}
            or {w["worker_id"] for w in groups[0]} & {w["worker_id"] for w in groups[1]}
            or groups[0] != observation["lifecycle"].get("workers_before")
            or replacement_identities != observation["lifecycle"].get("workers_after")):
        raise ValueError("Expected new worker processes that survive through completion")
    resumed = [r for r in recovery["reports"] if r["attempt"] == 2]
    if ([r["boosting_rounds"] for r in resumed] != list(range(saved_rounds + 1, target_rounds + 1))
            or any(r["restored_checkpoint_rounds"] != saved_rounds
                   or r["reporting_workers"] != num_workers for r in resumed)
            or not resumed[-1]["checkpoint"]
            or resumed[-1]["checkpoint"]["rounds"] != target_rounds):
        raise ValueError("Replacement workers did not complete exactly the remaining model rounds")


def run_case(args, result_directory, diagnostics):
    import numpy as np
    import pyarrow.parquet as pq
    import xgboost as xgb
    import train_batch_inference_benchmark as original
    from ray.train import FailureConfig, RunConfig
    from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer

    if original.XGBoostTrainer is not XGBoostTrainer:
        raise ValueError("Launch with RAY_TRAIN_V2_ENABLED=1 before importing Ray Train")
    register_for_task_serialization()
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    cloudpickle.register_pickle_by_value(original)
    input_directory = result_directory / "input"
    total_rows = make_input(input_directory)
    checkpoint_recovery = args.failure_phase == "checkpoint"
    monitor_name = f"fixed-r-xgboost-{uuid.uuid4().hex}"
    diagnostics.update(
        input_rows=total_rows, input_blocks=32, input_features=16,
        input_source="local_synthetic_parquet",
        num_train_workers=args.num_train_workers, cpus_per_train_worker=1,
        num_boost_rounds=args.num_boost_round,
        train_placement_strategy="STRICT_SPREAD" if args.num_train_workers > 1 else "PACK",
        recovery_timeout_s=args.recovery_timeout_s,
        failure_phase=("training_checkpoint_restart" if checkpoint_recovery else
                       "training_boosting" if args.failure_phase == "boosting"
                       else "training_data_ingestion"),
        failure_after_round=args.failure_after_round,
        training_state_recovery=checkpoint_recovery,
        checkpoint_frequency=args.checkpoint_frequency,
        worker_failure_retry_budget=1 if checkpoint_recovery else 0,
        controller_failure_retry_budget=0,
        checkpoint_storage_scope="shared_local_disk_survives_injected_process_failures",
        replay_required=args.failure_phase == "ingestion",
        validation_scope=("checkpoint_resume_after_head_replacement_and_worker_loss"
                          if checkpoint_recovery else "same_workers_continue_boosting_after_head_replacement"
                          if args.failure_phase == "boosting" else "training_ingestion_replay"),
        original_train_loop=True, original_predictor=True, ray_train_v2=True,
    )

    cluster_args = argparse.Namespace(**vars(args))
    cluster_args.recovery_timeout_s = min(30, args.recovery_timeout_s)
    with local_head_failure_cluster(cluster_args, coordinator_cpus=0) as (case_args, crash_head):
        coordinator_node = ray.get_runtime_context().get_node_id()
        placement = NodeAffinitySchedulingStrategy(coordinator_node, soft=False)
        monitor_cls = CheckpointMonitor if checkpoint_recovery else Monitor
        monitor = ray.remote(num_cpus=0, max_restarts=0)(monitor_cls).options(
            name=monitor_name, namespace=NAMESPACE, scheduling_strategy=placement,
        ).remote(args.num_boost_round, args.failure_after_round, args.num_train_workers)

        class Job:
            def run(self):
                context = DataContext.get_current().copy()
                context.enable_fixed_r_task_recovery = True
                context.fixed_r_task_recovery_output_mode = "streaming"
                context.fixed_r_task_recovery_timeout_s = (
                    min(30, args.recovery_timeout_s) if args.failure_phase != "ingestion"
                    else args.recovery_timeout_s
                )
                context.enable_progress_bars = False
                # The V2 file partitioner otherwise combines these tiny files
                # up to its minimum size despite override_num_blocks=32.
                context.target_min_block_size = 0
                get_config(context)  # Cache the original head before replacement.
                context.custom_execution_callback_classes = [
                    capture_execution(
                        monitor_name, "training", total_rows,
                        trigger_ingestion=args.failure_phase == "ingestion",
                    ),
                ]
                callbacks = [CheckpointRecoveryProbe(monitor_name) if checkpoint_recovery
                             else TrainSurvivalProbe(monitor_name)]
                if args.failure_phase == "boosting":
                    callbacks.append(TrainingProgressProbe(monitor_name))
                with DataContext.current(context):
                    started = time.monotonic()
                    result = original.train(
                        "xgboost", str(input_directory), args.num_train_workers, 1,
                        num_boost_round=args.num_boost_round,
                        checkpoint_frequency=args.checkpoint_frequency,
                        # Leave one CPU on each training node for read tasks.
                        placement_strategy=("STRICT_SPREAD" if args.num_train_workers > 1 else "PACK"),
                        read_kwargs={"override_num_blocks": 32},
                        run_config=RunConfig(
                            name="fixed_r_xgboost", storage_path=str(result_directory / "checkpoints"),
                            failure_config=FailureConfig(
                                max_failures=1 if checkpoint_recovery else 0,
                                controller_failure_limit=0, max_preemption_failures=0,
                            ),
                            callbacks=callbacks,
                        ),
                    )
                    training_s = time.monotonic() - started
                if result.checkpoint is None:
                    raise ValueError("Original training callback did not produce a checkpoint")
                model = original.XGBoostReportCallback.get_model(result.checkpoint)
                if model.num_boosted_rounds() != args.num_boost_round or model.num_features() != 16:
                    raise ValueError("Checkpoint has the wrong training rounds or feature count")
                checkpoint_validation = {}
                if checkpoint_recovery:
                    observed = ray.get(monitor.read.remote(), timeout=5)
                    trigger = observed["trigger"]
                    prefix_digest = model_tree_digest(model[:args.failure_after_round])
                    if prefix_digest != trigger["checkpoint_tree_digest"]:
                        raise ValueError("Final model did not retain the saved checkpoint's trees")
                    checkpoint_validation = {
                        "restored_checkpoint_rounds": args.failure_after_round,
                        "checkpoint_prefix_preserved": True,
                        "checkpoint_prefix_tree_digest": prefix_digest,
                    }
                prediction_context = context.copy()
                prediction_context.custom_execution_callback_classes = [
                    capture_execution(monitor_name, "inference", total_rows),
                ]
                output = result_directory / "predictions"
                with DataContext.current(prediction_context):
                    started = time.monotonic()
                    original.predict(
                        "xgboost", result, str(input_directory), output_path=str(output),
                        read_kwargs={"override_num_blocks": 32},
                    )
                    prediction_s = time.monotonic() - started
                # Validate persisted predictions against the saved model, without
                # changing the predictor or depending on distributed output order.
                frame = pq.read_table(input_directory).to_pandas()
                expected = model.predict(xgb.DMatrix(frame.drop("labels", axis=1)))
                predicted = pq.read_table(output)
                if predicted.column_names != ["predictions"] or predicted.num_rows != total_rows:
                    raise ValueError("Persisted prediction schema or row count is incorrect")
                values = predicted["predictions"].to_numpy()
                if not np.isfinite(values).all() or np.any(values < 0) or np.any(values > 1):
                    raise ValueError("Persisted predictions contain invalid probabilities")
                np.testing.assert_allclose(np.sort(values), np.sort(expected), rtol=1e-6, atol=1e-7)
                return {
                    **checkpoint_validation,
                    "training_s": training_s, "prediction_s": prediction_s,
                    "checkpoint_path": result.checkpoint.path,
                    "validated_prediction_rows": len(values), "checkpoint_rounds": model.num_boosted_rounds(),
                    "prediction_validation": "unordered_values_match_saved_model",
                    "prediction_directory": str(output), "xgboost_version": xgb.__version__,
                }

        job = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(Job).options(
            scheduling_strategy=placement,
        ).remote()
        started = time.monotonic()
        deadline = started + args.recovery_timeout_s
        crashed_at = None
        future = job.run.remote()
        try:
            while time.monotonic() < deadline:
                observed = ray.get(monitor.read.remote(), timeout=min(5, max(0.01, deadline - time.monotonic())))
                diagnostics["observation"] = observed
                ready, _ = ray.wait([future], timeout=0, fetch_local=False)
                if crashed_at is None and observed["trigger"] is not None:
                    if ready:
                        ray.get(future)
                        raise ValueError("Training/inference finished before head failure was exercised")
                    if args.failure_phase in ("boosting", "checkpoint"):
                        ingestion = observed["executions"].get("training", {})
                        if ingestion.get("state") != "finished":
                            # The ingestion observer and training controller are
                            # different actors; await the terminal ingestion report.
                            time.sleep(0.05)
                            continue
                        if observed["training_progress"]["boosting_rounds"] >= args.num_boost_round:
                            raise ValueError("Boosting finished before head failure was requested")
                    diagnostics.update(observed["trigger"])
                    worker_target = None
                    if checkpoint_recovery:
                        # Borrow the controller-owned target handle before head loss.
                        worker_target = ray.get(monitor.failure_target.remote(), timeout=5)
                    print(f"Requesting head failure during {args.failure_phase}: "
                          f"{observed['trigger']}", flush=True)
                    crashed_at = time.monotonic()
                    cluster_args.recovery_timeout_s = min(30, max(0.01, deadline - crashed_at))
                    diagnostics.update(crash_head())
                    if checkpoint_recovery:
                        ray.kill(worker_target, no_restart=True)
                        diagnostics["worker_failure_requested_after_head_replacement"] = True
                        ray.get(monitor.mark_worker_failure.remote(), timeout=5)
                        print("Head replaced; killed one training worker. "
                              "Waiting for checkpoint restoration and predictions.", flush=True)
                    if args.failure_phase == "boosting":
                        diagnostics["boosting_rounds_at_head_replacement"] = ray.get(
                            monitor.head_replaced.remote(),
                            timeout=min(5, max(0.01, deadline - time.monotonic())),
                        )
                        print("Head replaced; completed rounds observed: "
                              f"{diagnostics['boosting_rounds_at_head_replacement']}. "
                              "Waiting for training and prediction validation.", flush=True)
                if ready:
                    result = ray.get(future)
                    if crashed_at is None:
                        raise ValueError("The selected training phase did not trigger head failure")
                    observed = ray.get(monitor.read.remote(), timeout=min(
                        5, max(0.01, deadline - time.monotonic()),
                    ))
                    diagnostics["observation"] = observed
                    # Reports originate in multiple actors. Allow only the remaining
                    # case deadline for their final messages to reach the monitor.
                    if all(observed["executions"].get(phase, {}).get("state") == "finished"
                           for phase in ("training", "inference")):
                        validate_observations(
                            observed, total_rows, case_args.executor_node_ids,
                            coordinator_node, args.num_train_workers,
                            failure_phase=args.failure_phase, num_boost_round=args.num_boost_round,
                            failure_after_round=args.failure_after_round,
                        )
                        return {
                            **diagnostics, **result, "time": time.monotonic() - started,
                            "failure_request_to_completion_s": time.monotonic() - crashed_at,
                            "validation_status": "passed",
                        }
                time.sleep(0.05)
            raise TimeoutError(f"XGBoost train/inference exceeded the {args.recovery_timeout_s:g}-second case budget")
        finally:
            ray.kill(job, no_restart=True)
            ray.kill(monitor, no_restart=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-directory", type=Path, required=True)
    parser.add_argument("--recovery-timeout-s", type=float, default=120)
    parser.add_argument("--num-train-workers", type=int, choices=(1, 2), default=1)
    parser.add_argument("--num-boost-round", type=int, default=10)
    parser.add_argument("--failure-phase", choices=("ingestion", "boosting", "checkpoint"), default="ingestion")
    parser.add_argument("--checkpoint-frequency", type=int, default=0)
    parser.add_argument("--failure-after-round", type=int,
                        help="Completed all-worker boosting rounds before requesting head failure")
    args = parser.parse_args()
    if args.num_boost_round < 1:
        parser.error("--num-boost-round must be positive")
    if args.failure_phase in ("boosting", "checkpoint"):
        if args.failure_after_round is None:
            args.failure_after_round = args.num_boost_round // 2
        if not 1 <= args.failure_after_round < args.num_boost_round - 2:
            parser.error("Use 1 <= --failure-after-round < --num-boost-round - 2")
    elif args.failure_after_round is not None:
        parser.error("--failure-after-round requires --failure-phase boosting or checkpoint")
    if args.failure_phase == "checkpoint":
        if (args.checkpoint_frequency < 3 or args.failure_after_round % args.checkpoint_frequency
                or args.num_boost_round % args.checkpoint_frequency):
            parser.error("Checkpoint coverage requires frequency >= 3, dividing both failure and final rounds")
    elif args.checkpoint_frequency:
        parser.error("Periodic checkpoints in this harness require --failure-phase checkpoint")
    if not math.isfinite(args.recovery_timeout_s) or args.recovery_timeout_s <= 0:
        parser.error("Use a positive finite case timeout")
    if args.failure_phase == "ingestion" and args.recovery_timeout_s > 120:
        parser.error("Ingestion coverage retains its 120-second maximum case timeout")
    args.result_directory = args.result_directory.resolve()
    args.local_executor_nodes = 4
    args.local_object_store_mb = 512
    args.owner_node_id = args.executor_node_ids = None
    args.producer_concurrency = 4
    output = Path(os.environ["TEST_OUTPUT_JSON"])
    diagnostics = {}
    key = "xgboost/train_v2/local_original_pipeline/head_failure_during_ingestion"
    if args.num_train_workers > 1:
        key = f"xgboost/train_v2/local_{args.num_train_workers}_workers/head_failure_during_ingestion"
    if args.failure_phase == "boosting":
        key = f"xgboost/train_v2/local_{args.num_train_workers}_workers/head_failure_during_boosting"
    elif args.failure_phase == "checkpoint":
        key = f"xgboost/train_v2/local_{args.num_train_workers}_workers/checkpoint_resume_after_head_and_worker_loss"
    try:
        args.result_directory.mkdir(parents=True, exist_ok=True)
        result = run_case(args, args.result_directory, diagnostics)
    except Exception as exc:
        traceback.print_exc()
        result = {
            **diagnostics, "validation_status": "failed", "error_type": type(exc).__name__,
            "error": str(exc), "traceback": traceback.format_exc(),
        }
    output.write_text(json.dumps({key: result}, indent=2))
    print(f"XGBoost recovery result: {output}")
    return 0 if result["validation_status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
