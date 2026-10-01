"""Matched XGBoostTrainer retries, using the actual Ray Train controller."""

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import uuid

import numpy as np
import pyarrow.parquet as pq
import ray
import ray.cloudpickle
import ray.data
import ray.train
import xgboost as xgb

from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import (
    clear_config, context_for_new_execution, get_config,
)
from ray.experimental.recovery import system_config
from ray.experimental.recovery._local import local_head_failure_cluster
from ray.experimental.recovery._xgboost_boundary import fingerprint, tree_digest, validate_transition
from ray.train.v2._internal.execution.callback import ReportCallback
from ray.train.v2.xgboost.recovery import get_cached_input
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

NAMESPACE = "xgboost-train-retry"


def write_record(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def read_record(path):
    return json.loads(Path(path).read_text())


class CommitEvidence(ReportCallback):
    def __init__(self, directory):
        self.directory = directory

    def after_report(self, training_report, metrics):
        if training_report.checkpoint is None:
            return
        rounds = metrics[0]["rounds"]
        if any(m["rounds"] != rounds for m in metrics):
            raise ValueError("Ranks reported different model rounds")
        with training_report.checkpoint.as_directory() as path:
            shutil.copyfile(Path(path) / "model.ubj",
                            Path(self.directory) / f"checkpoint-{rounds}.ubj")
        write_record(Path(self.directory) / f"committed-{rounds}.json",
                     {"rounds": rounds, "time_ns": time.monotonic_ns(), "metrics": metrics})


class InputLoader:
    """Surviving input service, identical in both policies; never caches frames."""
    def __init__(self, partitions, context):
        self.partitions = partitions
        self.context = context
        self.events = []

    def load(self, rank):
        started = time.monotonic()
        context = context_for_new_execution(self.context)
        config = get_config(context)
        with DataContext.current(context):
            frame = ray.data.read_parquet(
                self.partitions[rank], override_num_blocks=len(self.partitions[rank])
            ).to_pandas()
        frame = frame.sort_values(list(frame.columns), kind="stable").reset_index(drop=True)
        self.events.append({"rank": rank, "rows": len(frame),
                            "duration_s": time.monotonic() - started,
                            "executor_node_ids": list(config.executor_node_ids) if config else None})
        return frame

    def evidence(self):
        return self.events


def train_loop(config):
    """Ordinary resumable train function plus one explicit immutable input cache."""
    rank = ray.train.get_context().get_world_rank()
    checkpoint = ray.train.get_checkpoint()
    saved = None
    if checkpoint:
        with checkpoint.as_directory() as path:
            saved = xgb.Booster(model_file=str(Path(path) / "model.ubj"))
    start = saved.num_boosted_rounds() if saved is not None else 0
    directory = Path(config["directory"])
    attempt = directory / f"attempt-{start}"

    def load():
        loader = ray.get_actor(config["loader_name"], namespace=NAMESPACE)
        frame = ray.get(loader.load.remote(rank), timeout=30)
        return frame, uuid.uuid4().hex

    frame, token = get_cached_input(config["input_version"], load)
    runtime = ray.get_runtime_context()
    identity = {"rank": rank, "actor_id": runtime.get_actor_id(),
                "worker_id": runtime.get_worker_id(), "node_id": runtime.get_node_id(),
                "pid": os.getpid(), "data_token": token, "loads": 1,
                "input": fingerprint(frame)}
    if identity["input"] != config["expected"][rank]:
        raise ValueError("Input partition changed across Ray Train retries")
    write_record(attempt / f"rank-{rank}-ready.json", {
        "identity": identity, "restored_round": start,
        "restored_digest": tree_digest(saved) if saved is not None else None,
        "time_ns": time.monotonic_ns(),
    })
    # Never cache a DMatrix across collective generations.
    matrix = xgb.DMatrix(frame.drop("labels", axis=1), label=frame["labels"], nthread=1)

    class Observe(xgb.callback.TrainingCallback):
        def after_iteration(self, model, epoch, evals_log):
            rounds = model.num_boosted_rounds()
            if rounds == start + 1:
                write_record(attempt / f"rank-{rank}-progress.json",
                             {"round": rounds, "time_ns": time.monotonic_ns()})
            if (config["inject_failures"] and rounds in (4, 7)
                    and not (directory / f"injected-{rounds}.json").exists()):
                # Complete an uncheckpointed round before a controlled real
                # callback allreduce. The driver kills rank zero's node.
                xgb.collective.allreduce(np.ones(2, dtype=np.float32), xgb.collective.Op.SUM)
                phase = "gated" if rank == 0 else "allreduce-enter"
                write_record(directory / f"fault-{rounds}-rank-{rank}.json",
                             {"identity": identity, "round": rounds, "phase": phase,
                              "time_ns": time.monotonic_ns()})
                if rank == 0:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        time.sleep(.05)
                    raise TimeoutError("Driver missed the active-collective fault gate")
                try:
                    xgb.collective.allreduce(np.ones(1024, dtype=np.float32), xgb.collective.Op.SUM)
                except xgb.core.XGBoostError as exc:
                    write_record(directory / f"fault-{rounds}-error.json",
                                 {"error": str(exc), "time_ns": time.monotonic_ns()})
                    raise
                raise ValueError("Faulted collective unexpectedly succeeded")
            if rounds in (3, 6, 10):
                with tempfile.TemporaryDirectory() as path:
                    if rank == 0:
                        model.save_model(str(Path(path) / "model.ubj"))
                    ray.train.report({"rounds": rounds}, checkpoint=(
                        ray.train.Checkpoint.from_directory(path) if rank == 0 else None))
            return False

    model = xgb.train({"objective": "binary:logistic", "eval_metric": ["logloss", "error"],
                       "tree_method": "hist", "nthread": 1, "seed": 0},
                      matrix, num_boost_round=10 - start, xgb_model=saved, callbacks=[Observe()])
    if saved is not None and tree_digest(model[:start]) != tree_digest(saved):
        raise ValueError("Training changed the restored checkpoint prefix")
    # Local prediction is an integrity probe, not a distributed inference benchmark.
    all_frame = pq.read_table(config["input_path"]).to_pandas()
    predictions = model.predict(xgb.DMatrix(all_frame.drop("labels", axis=1), nthread=1))
    np.save(directory / f"predictions-rank-{rank}.npy", predictions)
    write_record(directory / f"final-rank-{rank}.json", {
        "identity": identity, "rounds": model.num_boosted_rounds(),
        "tree_sha256": tree_digest(model), "time_ns": time.monotonic_ns(),
    })


class Job:
    def run(self, config, selective):
        from ray.train import FailureConfig, RunConfig, ScalingConfig
        from ray.train.xgboost import XGBoostConfig, XGBoostTrainer

        trainer = XGBoostTrainer(
            train_loop, train_loop_config=config,
            xgboost_config=XGBoostConfig(selective_recovery=selective, recovery_timeout_s=20),
            scaling_config=ScalingConfig(num_workers=2, resources_per_worker={"CPU": 1},
                                         placement_strategy="STRICT_SPREAD"),
            run_config=RunConfig(name="train-retry", storage_path=str(Path(config["directory"]) / "storage"),
                                 failure_config=FailureConfig(max_failures=2),
                                 callbacks=[CommitEvidence(config["directory"])]),
        )
        started = time.monotonic()
        result = trainer.fit()
        return {"training_s": time.monotonic() - started, "metrics": result.metrics,
                "checkpoint_path": result.checkpoint.path}


def validate_evidence(directory, policy, faults, expected):
    directory = Path(directory)
    starts = [0, 3, 6] if faults else [0]
    groups, recoveries = [], []
    dead = set()
    for index, start in enumerate(starts):
        ready = [read_record(directory / f"attempt-{start}" / f"rank-{r}-ready.json") for r in range(2)]
        current = [v["identity"] for v in ready]
        if any(v["restored_round"] != start or v["identity"]["input"] != expected[r]
               for r, v in enumerate(ready)):
            raise ValueError("Ray Train restored the wrong checkpoint or partition")
        if index:
            fault = faults[index - 1]
            dead.add(fault["worker_node_failure"]["node_id"])
            checkpoint = xgb.Booster(model_file=str(directory / f"checkpoint-{start}.ubj"))
            if any(v["restored_digest"] != tree_digest(checkpoint) for v in ready):
                raise ValueError("Workers did not restore Ray Train's committed checkpoint")
            transition = validate_transition(groups[-1], current, 0, policy, dead)
            progress = [read_record(directory / f"attempt-{start}" / f"rank-{r}-progress.json") for r in range(2)]
            if any(p["round"] != start + 1 or p["time_ns"] <= fault["request_ns"] for p in progress):
                raise ValueError("Missing new training progress after rollback")
            if policy == "selective":
                error = read_record(directory / f"fault-{start + 1}-error.json")
                if not error["error"] or error["time_ns"] < fault["request_ns"]:
                    raise ValueError("Healthy collective did not fail after node injection")
            recoveries.append({**transition, "checkpoint_round": start,
                               "rolled_back_rounds": 1, "first_resumed_round": start + 1,
                               "reloaded_rows": sum(expected[r]["rows"] for r in transition["replaced_ranks"]),
                               "failure_to_first_resumed_round_s": (
                                   max(p["time_ns"] for p in progress) - fault["request_ns"]) / 1e9})
        groups.append(current)
    final = xgb.Booster(model_file=str(directory / "checkpoint-10.ubj"))
    if final.num_boosted_rounds() != 10:
        raise ValueError("Final round budget is incorrect")
    for rounds in (3, 6):
        saved = xgb.Booster(model_file=str(directory / f"checkpoint-{rounds}.ubj"))
        if tree_digest(final[:rounds]) != tree_digest(saved):
            raise ValueError("Final model changed a committed checkpoint prefix")
    for rank in range(2):
        result = read_record(directory / f"final-rank-{rank}.json")
        if result["rounds"] != 10 or result["tree_sha256"] != tree_digest(final):
            raise ValueError("Final worker models differ")
    return groups, recoveries, final


def run_case(options, directory, diagnostics):
    from run_fixed_r_train_coverage import make_input

    # Module functions/classes must travel with worker payloads.
    ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
    enabled = options["mode"] == "on"
    selective = options["restart_scope"] == "selective"
    inject = options["scenario"] != "none"
    total_rows = make_input(directory / "input", blocks=8)
    files = sorted((directory / "input").glob("*.parquet"))
    partitions = [[str(p) for p in files[rank::2]] for rank in range(2)]
    expected = [fingerprint(pq.read_table(paths).to_pandas()) for paths in partitions]
    diagnostics.update(implementation="Ray_Train_XGBoostTrainer_retry", restart_scope=options["restart_scope"],
                       worker_node_failures=[], faults=[], failure_timing="active" if inject else "none")
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=enabled,
                                    allow_head_failure=False, include_worker_failure=True) as (case, _, crash):
        original_nodes = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        coordinator = ray.get_runtime_context().get_node_id()
        job_id = ray.get_runtime_context().get_job_id()
        native = ray._private.state.state.get_system_config()
        diagnostics["native_settings"] = {k: native.get(k) for k in system_config()}
        if any(native.get(k) != (enabled if k.startswith("enable_") else v)
               for k, v in system_config().items()):
            raise ValueError("Native protection differs from requested mode")
        context = DataContext.get_current().copy()
        clear_config(context)
        context.enable_fixed_r_task_recovery = enabled
        context.fixed_r_task_recovery_output_mode = "streaming"
        context.fixed_r_task_recovery_timeout_s = 30
        context.enable_progress_bars = False
        context.execution_options.preserve_order = True
        context.target_min_block_size = 0
        context.custom_execution_callback_classes = []
        if enabled:
            get_config(context)
        loader_name = "input-" + uuid.uuid4().hex
        placement = NodeAffinitySchedulingStrategy(ray.get_runtime_context().get_node_id(), soft=False)
        loader = ray.remote(num_cpus=0)(InputLoader).options(
            name=loader_name, namespace=NAMESPACE, scheduling_strategy=placement).remote(partitions, context)
        job = ray.remote(num_cpus=0)(Job).options(scheduling_strategy=placement).remote()
        config = {"directory": str(directory), "input_path": str(directory / "input"),
                  "loader_name": loader_name, "expected": expected, "input_version": loader_name,
                  "inject_failures": inject}
        pending = job.run.remote(config, selective)
        deadline = time.monotonic() + options["timeout_s"]

        def wait_for(paths):
            while not all(p.exists() for p in paths):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Ray Train retry observation exceeded deadline")
                ready, _ = ray.wait([pending], timeout=.05)
                if ready:
                    ray.get(pending)
                    raise ValueError("Training ended before the required fault gate")

        try:
            for rounds in ((4, 7) if inject else ()):
                paths = [directory / f"fault-{rounds}-rank-{r}.json" for r in range(2)]
                wait_for([*paths, directory / f"committed-{rounds - 1}.json"])
                gates = [read_record(p) for p in paths]
                if (gates[0]["phase"] != "gated" or gates[1]["phase"] != "allreduce-enter"
                        or any(g["round"] != rounds or g["identity"]["rank"] != r
                               for r, g in enumerate(gates))):
                    raise ValueError("Unexpected collective fault gate")
                ready, _ = ray.wait([pending], timeout=.2)
                if ready or (directory / f"fault-{rounds}-error.json").exists():
                    raise ValueError("Collective completed or failed before injection")
                fault = {"request_ns": time.monotonic_ns(), "interrupted_round": rounds,
                         "checkpoint_round": rounds - 1, "gates": gates}
                diagnostics["faults"].append(fault)
                # Record injection before killing so a fast replacement cannot
                # accidentally enter the same gate for a second time.
                write_record(directory / f"injected-{rounds}.json", fault)
                target = gates[0]["identity"]
                failure = crash(target["node_id"], target["pid"])
                fault["worker_node_failure"] = failure
                diagnostics["worker_node_failures"].append(failure)
                if (not failure["all_node_processes_exited"] or not failure["gcs_marked_dead"]
                        or target["pid"] not in failure["node_process_pids"]):
                    raise ValueError("Logical node death was not confirmed")
            result = ray.get(pending, timeout=max(.1, deadline - time.monotonic()))
            dead = {f["node_id"] for f in diagnostics["worker_node_failures"]}
            alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
            if (len(dead) != (2 if inject else 0) or alive != original_nodes - dead
                    or ray.get_runtime_context().get_node_id() != coordinator
                    or ray.get_runtime_context().get_job_id() != job_id):
                raise ValueError("Unexpected node, coordinator or job loss")
            groups, recoveries, final = validate_evidence(
                directory, options["restart_scope"], diagnostics["faults"], expected)
            diagnostics.update(groups=groups, recoveries=recoveries,
                               ingestion=ray.get(loader.evidence.remote(), timeout=5))
            expected_loads = 2 + (2 if selective else 4) if inject else 2
            if len(diagnostics["ingestion"]) != expected_loads:
                raise ValueError("Unexpected input reload count")
            frame = pq.read_table(directory / "input").to_pandas()
            reference = final.predict(xgb.DMatrix(frame.drop("labels", axis=1), nthread=1))
            for rank in range(2):
                values = np.load(directory / f"predictions-rank-{rank}.npy", allow_pickle=False)
                if len(values) != total_rows or not np.isfinite(values).all():
                    raise ValueError("Invalid final predictions")
                np.testing.assert_allclose(values, reference, rtol=1e-6, atol=1e-7)
            np.save(directory / "predictions.npy", reference)
            return {"validation_status": "passed", "training_s": result["training_s"],
                    "final_tree_sha256": tree_digest(final),
                    "artifact_validation": {"model_rounds": 10, "checkpoint_prefixes_preserved": [3, 6],
                                            "input_rows": total_rows, "predictions_match": True}}
        finally:
            # Capture partial evidence even on failure, in the same JSON report.
            diagnostics["retry_events"] = {str(p.relative_to(directory)): read_record(p)
                                            for p in directory.rglob("*.json")
                                            if "storage" not in p.parts and p.name != "options.json"}
            ray.kill(job, no_restart=True)
            ray.kill(loader, no_restart=True)
