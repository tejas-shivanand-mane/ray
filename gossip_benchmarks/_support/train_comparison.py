"""One observed XGBoost comparison case on a fresh local process cluster.

All event timestamps share the monotonic clock of one Linux host. These are
instrumented comparisons, not observer-free timings or machine-loss tests.
"""

import argparse
import math
from pathlib import Path
import sys
import time
import uuid

import ray
from ray import cloudpickle
from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import clear_config, get_config
from ray.experimental.recovery import system_config
from ray.experimental.recovery._local import local_head_failure_cluster
from ray.train.v2._internal.execution.callback import (
    ControllerCallback, ReportCallback, WorkerGroupCallback,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "gossip_benchmarks"))
import run_fixed_r_train_coverage as coverage

NAMESPACE = "fixed-r-training-comparison"
SCENARIOS = ("none", "worker", "head", "head-worker")


def seconds_between(end, start):
    return None if end is None or start is None else (end - start) / 1e9


class ComparisonMonitor:
    def __init__(self, options):
        self.options = options
        self.events = []
        self.groups = []
        self.reports = []
        self.stages = []
        self.trigger = None
        self.checkpoint = None
        self.fault = None
        self.fault_done = False
        self.target = None
        self.controller = {}
        self.finished_workers = None

    def event(self, name, at_ns=None, **details):
        self.events.append({"name": name, "at_ns": at_ns or time.monotonic_ns(), **details})

    def controller_event(self, name, identity, at_ns):
        if name in self.controller:
            raise ValueError(f"Repeated controller event: {name}")
        self.controller[name] = identity
        self.event(name, at_ns)

    def group_started(self, identities, actors, at_ns):
        if not identities or len(identities) != self.options["num_train_workers"]:
            raise ValueError("Missing training workers")
        attempt = len(self.groups) + 1
        self.groups.append({"attempt": attempt, "at_ns": at_ns, "workers": identities})
        if attempt == 1:
            self.target = actors[0]
        self.event("worker_group_ready", at_ns, attempt=attempt)

    def workers_finished(self, identities, at_ns):
        self.finished_workers = identities
        self.event("workers_finished", at_ns, attempt=len(self.groups))

    def stage(self, observation):
        self.stages.append(observation)

    def report(self, metrics, checkpoint_path, at_ns):
        rounds = {m["boosting_rounds"] for m in metrics}
        restored = {m["restored_checkpoint_rounds"] for m in metrics}
        if (len(metrics) != self.options["num_train_workers"]
                or len(rounds) != 1 or len(restored) != 1 or not self.groups):
            raise ValueError("Training workers disagree on progress")
        completed, origin = rounds.pop(), restored.pop()
        attempt = len(self.groups)
        previous = [r for r in self.reports if r["attempt"] == attempt]
        expected = previous[-1]["rounds"] + 1 if previous else origin + 1
        # Final-only checkpointing emits a second report for the last model.
        final_duplicate = (previous and completed == self.options["num_boost_round"]
                           and completed == previous[-1]["rounds"] and checkpoint_path
                           and previous[-1]["checkpoint_path"] is None)
        group_origin = self.groups[-1]["workers"][0]["restored_checkpoint_rounds"]
        if (completed != expected and not final_duplicate
                or origin != group_origin
                or not origin <= completed <= self.options["num_boost_round"]):
            raise ValueError("Training reports skipped, repeated, or exceeded target rounds")
        self.reports.append({
            "attempt": attempt, "rounds": completed, "restored_rounds": origin,
            "checkpoint_path": checkpoint_path, "at_ns": at_ns,
        })
        if checkpoint_path:
            self.checkpoint = {"rounds": completed, "path": checkpoint_path, "at_ns": at_ns}
        if (self.options["scenario"] != "none" and self.trigger is None
                and attempt == 1 and completed == self.options["failure_after_round"]):
            self.trigger = {"rounds": completed, "checkpoint": self.checkpoint, "at_ns": at_ns}
            return self.options["gated"]
        return False

    def begin_fault(self, at_ns):
        if self.fault is not None or self.trigger is None or not self.reports:
            raise ValueError("Fault must follow exactly one training trigger")
        latest = self.reports[-1]
        if latest["attempt"] != 1 or latest["rounds"] >= self.options["num_boost_round"]:
            raise ValueError("Training finished/restarted before fault injection")
        self.fault = {
            "request_ns": at_ns, "last_reported_round": latest["rounds"],
            "last_registered_checkpoint": self.checkpoint,
            "worker_group_attempt": len(self.groups),
        }
        self.event("fault_requested", at_ns)
        return {"target": self.target, "fault": self.fault}

    def complete_fault(self, at_ns):
        self.fault_done = True
        self.event("fault_actions_completed", at_ns)

    def worker_failure_requested(self, at_ns):
        if self.fault is None or "worker_request_ns" in self.fault:
            raise ValueError("Worker failure requires one active fault")
        self.fault.update(
            worker_request_ns=at_ns,
            last_reported_round_before_worker_failure=self.reports[-1]["rounds"],
            last_checkpoint_before_worker_failure=self.checkpoint,
        )
        self.event("worker_failure_requested", at_ns)

    def injection_ready(self):
        return self.fault_done

    def read(self):
        return {
            "events": self.events, "worker_groups": self.groups, "reports": self.reports,
            "stages": self.stages, "trigger": self.trigger, "fault": self.fault,
            "fault_done": self.fault_done, "controller": self.controller,
            "finished_workers": self.finished_workers,
        }


class ComparisonProbe(WorkerGroupCallback, ControllerCallback, ReportCallback):
    def __init__(self, monitor_name, gate_timeout_s):
        self.monitor_name = monitor_name
        self.gate_timeout_s = gate_timeout_s
        self.monitor = None
        self.attempt = 0

    def control(self):
        if self.monitor is None:
            self.monitor = ray.get_actor(self.monitor_name, namespace=NAMESPACE)
        return self.monitor

    def after_controller_start(self, train_run_context):
        ray.get(self.control().controller_event.remote(
            "controller_started", coverage.process_identity(), time.monotonic_ns(),
        ), timeout=5)

    def after_controller_finish(self, result):
        ray.get(self.control().controller_event.remote(
            "controller_finished", coverage.process_identity(), time.monotonic_ns(),
        ), timeout=5)

    def before_worker_group_start(self, worker_group_context):
        ray.get(self.control().event.remote(
            "worker_group_start_requested", time.monotonic_ns(), attempt=self.attempt + 1,
        ), timeout=5)

    def after_worker_group_start(self, worker_group):
        self.attempt += 1
        identities = ray.get(worker_group.execute_async(coverage.checkpoint_worker_identity), timeout=15)
        ray.get(self.control().group_started.remote(
            identities, [w.actor for w in worker_group.get_workers()], time.monotonic_ns(),
        ), timeout=5)

    def before_worker_group_shutdown(self, worker_group):
        status = worker_group.get_latest_poll_status()
        if status and status.finished and not status.errors:
            identities = ray.get(worker_group.execute_async(coverage.train_worker_identity), timeout=15)
            ray.get(self.control().workers_finished.remote(identities, time.monotonic_ns()), timeout=5)
        ray.get(self.control().event.remote(
            "worker_group_shutdown_requested", time.monotonic_ns(), attempt=self.attempt,
        ), timeout=5)

    def before_controller_execute_failure_decision(self, failure_decision):
        ray.get(self.control().event.remote(
            "controller_failure_detected", time.monotonic_ns(), decision=str(failure_decision),
        ), timeout=5)

    def after_report(self, training_report, metrics):
        gate = ray.get(self.control().report.remote(
            metrics, training_report.checkpoint.path if training_report.checkpoint else None,
            time.monotonic_ns(),
        ), timeout=5)
        if gate:
            deadline = time.monotonic() + self.gate_timeout_s
            while time.monotonic() < deadline:
                if ray.get(self.control().injection_ready.remote(), timeout=5):
                    return
                time.sleep(0.01)
            raise TimeoutError("Comparison fault injection did not release its report gate")


def validate_observation(observation, options, coordinator, executors):
    groups = observation["worker_groups"]
    needs_restart = options["scenario"] in ("worker", "head-worker")
    if len(groups) != (2 if needs_restart else 1):
        raise ValueError("Unexpected number of worker-group attempts")
    controller = observation["controller"]
    if (controller.get("controller_started") != controller.get("controller_finished")
            or not controller.get("controller_started")
            or controller["controller_started"]["node_id"] != coordinator):
        raise ValueError("Train controller did not survive on the coordinator")
    for index, group in enumerate(groups):
        workers = group["workers"]
        if ({w["world_rank"] for w in workers} != set(range(options["num_train_workers"]))
                or len({w["node_id"] for w in workers}) != options["num_train_workers"]
                or any(w["node_id"] not in executors or w["world_size"] != options["num_train_workers"]
                       for w in workers)
                or len({w["worker_id"] for w in workers}) != options["num_train_workers"]):
            raise ValueError("Worker ranks, identities or placement are incorrect")
        if index == 0 and any(w["restored_checkpoint_rounds"] != 0 or w["restored_checkpoint_path"] is not None
                              for w in workers):
            raise ValueError("Fresh comparison started from an existing checkpoint")
    fault = observation["fault"]
    if options["scenario"] == "none":
        if fault is not None or observation["trigger"] is not None:
            raise ValueError("No-failure comparison injected a fault")
    elif not fault or not observation["fault_done"]:
        raise ValueError("Requested fault scenario was not fully exercised")
    if needs_restart:
        if "worker_request_ns" not in fault:
            raise ValueError("Worker failure was not requested")
        if not fault["last_registered_checkpoint"]:
            raise ValueError("Worker recovery has no prior registered checkpoint")
        old = {w["worker_id"] for w in groups[0]["workers"]}
        if old & {w["worker_id"] for w in groups[1]["workers"]}:
            raise ValueError("Expected full Ray Train worker-group replacement")
        # An in-flight report can register a newer checkpoint before restart,
        # especially in ungated cases. Validate the actual restored checkpoint
        # against the controller's history, rather than assuming the trigger's.
        persisted = [r for r in observation["reports"]
                     if r["attempt"] == 1 and r["checkpoint_path"] and r["at_ns"] <= groups[1]["at_ns"]]
        latest = max(persisted, key=lambda r: r["at_ns"]) if persisted else None
        restored = {(w["restored_checkpoint_rounds"], w["restored_checkpoint_path"])
                    for w in groups[1]["workers"]}
        if latest is None or restored != {(latest["rounds"], latest["checkpoint_path"])}:
            raise ValueError("Replacement workers restored the wrong checkpoint")
    final_workers = [{k: v for k, v in w.items() if not k.startswith("restored_checkpoint_")}
                     for w in groups[-1]["workers"]]
    if observation["finished_workers"] != final_workers:
        raise ValueError("Final training workers changed before successful shutdown")
    reports = [r for r in observation["reports"] if r["attempt"] == len(groups)]
    if not reports or reports[-1]["rounds"] != options["num_boost_round"] or not reports[-1]["checkpoint_path"]:
        raise ValueError("Training did not register its final model")
    stage_keys = {(s["worker_id"], s["name"]) for s in observation["stages"]}
    expected = {(w["worker_id"], name) for g in groups for w in g["workers"]
                for name in ("checkpoint_load", "data_ingestion", "dmatrix")}
    if stage_keys != expected or len(observation["stages"]) != len(expected):
        raise ValueError("Missing or duplicated worker stage observations")
    if any(not math.isfinite(s["duration_s"]) or s["duration_s"] < 0
           or s["finished_ns"] < s["started_ns"] for s in observation["stages"]):
        raise ValueError("Invalid worker stage duration")
    identities = {w["worker_id"]: w for g in groups for w in g["workers"]}
    if any(s["world_rank"] != identities[s["worker_id"]]["world_rank"]
           or s["restored_checkpoint_rounds"] != identities[s["worker_id"]]["restored_checkpoint_rounds"]
           or not math.isclose(s["duration_s"], seconds_between(s["finished_ns"], s["started_ns"]), abs_tol=1e-9)
           for s in observation["stages"]):
        raise ValueError("Worker stage identity or duration disagrees with the timeline")


def recovery_metrics(observation):
    fault = observation["fault"]
    if not fault:
        return {}
    events = {e["name"]: e["at_ns"] for e in observation["events"]}
    worker_request = events.get("worker_failure_requested")
    after = worker_request if worker_request is not None else events.get("head_replacement_ready")
    progress = next((r for r in observation["reports"] if after is not None and r["at_ns"] >= after
                     and (r["attempt"] > fault["worker_group_attempt"] if worker_request is not None
                          else r["rounds"] > fault["last_reported_round"])), None)
    first_ns = progress["at_ns"] if progress else None
    saved = fault["last_registered_checkpoint"]
    groups = observation["worker_groups"]
    restored_round = groups[-1]["workers"][0]["restored_checkpoint_rounds"] if worker_request is not None else None
    old_ids = {w["worker_id"] for w in groups[0]["workers"]}
    new_ids = {w["worker_id"] for w in groups[-1]["workers"]}
    worker_round = fault.get("last_reported_round_before_worker_failure")
    detection = events.get("controller_failure_detected")
    replacement = groups[-1]["at_ns"] if worker_request is not None else None
    return {
        "fault_request_to_first_resumed_round_s": seconds_between(first_ns, fault["request_ns"]),
        "worker_failure_to_first_resumed_round_s": seconds_between(first_ns, worker_request),
        "worker_failure_to_detection_s": seconds_between(detection, worker_request),
        "detection_to_worker_group_ready_s": seconds_between(replacement, detection),
        "worker_failure_to_worker_group_ready_s": seconds_between(replacement, worker_request),
        "fault_request_to_training_completion_s": seconds_between(events.get("training_finished"), fault["request_ns"]),
        "fault_request_to_pipeline_completion_s": seconds_between(events.get("pipeline_finished"), fault["request_ns"]),
        "head_replacement_s": seconds_between(events.get("head_replacement_ready"), events.get("head_failure_requested")),
        "first_resumed_round": progress["rounds"] if progress else None,
        "last_reported_round_before_fault": fault["last_reported_round"],
        "last_checkpoint_round_before_fault": saved["rounds"] if saved else None,
        "restored_checkpoint_round": restored_round,
        "last_reported_round_before_worker_failure": worker_round,
        "rollback_reported_rounds": (max(0, worker_round - restored_round)
                                     if worker_round is not None and restored_round is not None else 0),
        "preserved_worker_ids": sorted(old_ids & new_ids),
        "restarted_worker_ids": sorted(old_ids - new_ids),
        "replacement_worker_ids": sorted(new_ids - old_ids),
        "progress_scope": "all-worker controller reports; unreported in-flight computation is not counted",
    }


def run_case(options, directory, diagnostics):
    import numpy as np
    import pyarrow.parquet as pq
    import xgboost as xgb
    import train_batch_inference_benchmark as benchmark
    from ray.train import FailureConfig, RunConfig
    from ray.train.v2.xgboost.xgboost_trainer import XGBoostTrainer

    if benchmark.XGBoostTrainer is not XGBoostTrainer:
        raise ValueError("Training comparison requires RAY_TRAIN_V2_ENABLED=1")
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    cloudpickle.register_pickle_by_value(coverage)
    cloudpickle.register_pickle_by_value(benchmark)
    input_path = directory / "input"
    total_rows = coverage.make_input(input_path)
    enabled = options["mode"] == "on"
    args = argparse.Namespace(
        local_executor_nodes=4, local_object_store_mb=512, owner_node_id=None,
        executor_node_ids=None, producer_concurrency=None, recovery_timeout_s=30,
    )
    with local_head_failure_cluster(
        args, coordinator_cpus=0, recovery_enabled=enabled,
        allow_head_failure=options["scenario"] in ("head", "head-worker"),
    ) as (case_args, crash_head):
        coordinator = ray.get_runtime_context().get_node_id()
        affinity = NodeAffinitySchedulingStrategy(coordinator, soft=False)
        name = f"train-comparison-{uuid.uuid4().hex}"
        monitor = ray.remote(num_cpus=0, max_restarts=0)(ComparisonMonitor).options(
            name=name, namespace=NAMESPACE, scheduling_strategy=affinity,
        ).remote(options)
        diagnostics.update(coordinator_node_id=coordinator, executor_node_ids=case_args.executor_node_ids)
        nodes_before = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        config = ray._private.state.state.get_system_config()
        diagnostics["native_settings"] = {k: config.get(k) for k in system_config()}
        if any(diagnostics["native_settings"][k] != (enabled if k.startswith("enable_") else v)
               for k, v in system_config().items()):
            raise ValueError("Native settings disagree with the comparison mode")

        class Job:
            def run(self):
                context = DataContext.get_current().copy()
                clear_config(context)
                context.enable_fixed_r_task_recovery = enabled
                context.fixed_r_task_recovery_output_mode = "streaming"
                context.fixed_r_task_recovery_timeout_s = min(30, options["timeout_s"])
                context.enable_progress_bars = False
                context.target_min_block_size = 0
                if enabled:
                    get_config(context)
                with DataContext.current(context):
                    started = time.monotonic()
                    result = benchmark.train(
                        "xgboost", str(input_path), options["num_train_workers"], 1,
                        placement_strategy="STRICT_SPREAD", read_kwargs={"override_num_blocks": 32},
                        num_boost_round=options["num_boost_round"],
                        checkpoint_frequency=options["checkpoint_frequency"],
                        _benchmark_stage_observer=monitor,
                        run_config=RunConfig(
                            name="training_comparison", storage_path=str(directory / "checkpoints"),
                            failure_config=FailureConfig(
                                max_failures=options["max_failures"], controller_failure_limit=0,
                                max_preemption_failures=0,
                            ),
                            callbacks=[ComparisonProbe(name, min(60, options["timeout_s"]))],
                        ),
                    )
                    training_s = time.monotonic() - started
                    ray.get(monitor.event.remote("training_finished", time.monotonic_ns()), timeout=5)
                    if result.checkpoint is None:
                        raise ValueError("Training returned no final checkpoint")
                    prediction_s = None
                    if options["include_prediction"]:
                        started = time.monotonic()
                        benchmark.predict(
                            "xgboost", result, str(input_path),
                            output_path=str(directory / "predictions"), read_kwargs={"override_num_blocks": 32},
                        )
                        prediction_s = time.monotonic() - started
                    # Mark workload completion before untimed model/output checks.
                    ray.get(monitor.event.remote("pipeline_finished", time.monotonic_ns()), timeout=5)
                model = benchmark.XGBoostReportCallback.get_model(result.checkpoint)
                if model.num_boosted_rounds() != options["num_boost_round"] or model.num_features() != 16:
                    raise ValueError("Final checkpoint has incorrect model dimensions")
                observed = ray.get(monitor.read.remote(), timeout=5)
                if options["scenario"] in ("worker", "head-worker"):
                    worker = observed["worker_groups"][-1]["workers"][0]
                    checkpoint_model = xgb.Booster()
                    checkpoint_model.load_model(str(Path(worker["restored_checkpoint_path"]) / "model.ubj"))
                    if checkpoint_model.num_boosted_rounds() != worker["restored_checkpoint_rounds"]:
                        raise ValueError("Restored checkpoint model disagrees with its reported round count")
                    if coverage.model_tree_digest(model[:worker["restored_checkpoint_rounds"]]) != coverage.model_tree_digest(checkpoint_model):
                        raise ValueError("Final model does not preserve the saved checkpoint prefix")
                    validation_prefix = {"checkpoint_prefix_preserved": True,
                                         "restored_checkpoint_rounds": worker["restored_checkpoint_rounds"]}
                else:
                    validation_prefix = {}
                validation = {"model_rounds": model.num_boosted_rounds(), "model_features": model.num_features()}
                validation.update(validation_prefix)
                if options["include_prediction"]:
                    frame = pq.read_table(input_path).to_pandas()
                    expected = model.predict(xgb.DMatrix(frame.drop("labels", axis=1)))
                    output = pq.read_table(directory / "predictions")
                    if output.column_names != ["predictions"] or output.num_rows != total_rows:
                        raise ValueError("Prediction output has incorrect schema or row count")
                    values = output["predictions"].to_numpy()
                    if not np.isfinite(values).all() or np.any(values < 0) or np.any(values > 1):
                        raise ValueError("Prediction output contains invalid probabilities")
                    np.testing.assert_allclose(np.sort(values), np.sort(expected), rtol=1e-6, atol=1e-7)
                    validation["prediction_validation"] = "unordered_values_match_saved_model"
                    validation["prediction_rows"] = len(values)
                return {
                    "training_s": training_s, "prediction_s": prediction_s,
                    "pipeline_s": training_s + (prediction_s or 0),
                    "checkpoint_path": result.checkpoint.path, "artifact_validation": validation,
                }

        job = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(Job).options(
            scheduling_strategy=affinity,
        ).remote()
        deadline = time.monotonic() + options["timeout_s"]
        future = job.run.remote()
        injected = False
        try:
            while time.monotonic() < deadline:
                remaining = max(.01, deadline - time.monotonic())
                observed = ray.get(monitor.read.remote(), timeout=min(5, remaining))
                diagnostics["observation"] = observed
                ready, _ = ray.wait([future], timeout=0, fetch_local=False)
                if not injected and observed["trigger"] is not None:
                    if ready:
                        ray.get(future)
                        raise ValueError("Workload finished before the requested fault")
                    fault = ray.get(monitor.begin_fault.remote(time.monotonic_ns()), timeout=5)
                    diagnostics["fault"] = fault["fault"]
                    injected = True
                    if options["scenario"] in ("head", "head-worker"):
                        ray.get(monitor.event.remote("head_failure_requested", time.monotonic_ns()), timeout=5)
                        args.recovery_timeout_s = min(30, max(.01, deadline - time.monotonic()))
                        diagnostics["head_replacement"] = crash_head()
                        ray.get(monitor.event.remote("head_replacement_ready", time.monotonic_ns()), timeout=5)
                    if options["scenario"] in ("worker", "head-worker"):
                        ray.get(monitor.worker_failure_requested.remote(time.monotonic_ns()), timeout=5)
                        ray.kill(fault["target"], no_restart=True)
                    ray.get(monitor.complete_fault.remote(time.monotonic_ns()), timeout=5)
                if ready:
                    result = ray.get(future)
                    # Stage RPCs come from different workers. Drain their bounded
                    # observations before validating complete timeline evidence.
                    observed = ray.get(monitor.read.remote(), timeout=min(5, remaining))
                    expected_stages = len(observed["worker_groups"]) * options["num_train_workers"] * 3
                    if len(observed["stages"]) < expected_stages:
                        time.sleep(.01)
                        continue
                    diagnostics["observation"] = observed
                    validate_observation(observed, options, coordinator, case_args.executor_node_ids)
                    if nodes_before - {n["NodeID"] for n in ray.nodes() if n["Alive"]} != (
                        {case_args.owner_node_id} if options["scenario"] in ("head", "head-worker") else set()
                    ):
                        raise ValueError("An unrequested node failure occurred")
                    recovery = recovery_metrics(observed)
                    if options["scenario"] != "none" and recovery["first_resumed_round"] is None:
                        raise ValueError("No all-worker progress was observed after the injected fault")
                    return {**result, "recovery": recovery, "validation_status": "passed"}
                time.sleep(.005 if not injected and options["scenario"] != "none" else .02)
            raise TimeoutError(f"Training case exceeded {options['timeout_s']:g}s after cluster startup")
        finally:
            ray.kill(job, no_restart=True)
            ray.kill(monitor, no_restart=True)
