"""Matched checkpoint-boundary prototype, separate from Ray Train retry logic.

Both policies use the same real distributed XGBoost segments, fixed partitions,
checkpoint boundaries and Fixed-R setting. Only worker replacement differs.
"""

import argparse
from concurrent.futures import Future
import os
from pathlib import Path
import threading
import time

import numpy as np
import pyarrow.parquet as pq
import ray
from ray.data import DataContext
from ray.data._internal.execution.streaming_recovery import (
    clear_config, context_for_new_execution, get_config,
)
from ray.experimental.recovery import system_config
from ray.experimental.recovery._local import local_head_failure_cluster
from ray.experimental.recovery._xgboost_boundary import (
    BoundaryWorker, fingerprint, replacement_ranks, tree_digest, validate_transition,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import xgboost as xgb
from packaging.version import Version


def collective_segment(actors, checkpoint, start, end, generation, timeout_s):
    tracker = xgb.RabitTracker(n_workers=2, host_ip=ray.util.get_node_ip_address(),
                              sortby="task", timeout=max(1, int(timeout_s)))
    tracker.start()
    finished = Future()

    def wait_tracker():
        try:
            tracker.wait_for(timeout=max(1, int(timeout_s)))
            finished.set_result(None)
        except BaseException as exc:
            finished.set_exception(exc)

    # XGBoost 2.1 requires wait_for to run before worker_args can return.
    thread = threading.Thread(target=wait_tracker, daemon=True)
    thread.start()
    args = tracker.worker_args()
    results = ray.get([actor.train_segment.remote(args, checkpoint, start, end, generation)
                       for actor in actors], timeout=timeout_s)
    finished.result(timeout=timeout_s)
    thread.join(timeout=1)
    if thread.is_alive():
        raise TimeoutError("Completed collective left its tracker running")
    if (len(results) != 2 or {r["identity"]["rank"] for r in results} != {0, 1}
            or any(not r["collective_finalized"] or r["start_round"] != start
                   or r["end_round"] != end or r["generation"] != generation
                   or r["first_round"] != start + 1 for r in results)
            or len({r["tree_sha256"] for r in results}) != 1):
        raise ValueError("Workers disagree on completed collective/model")
    return results


def run_case(options, directory, diagnostics):
    from run_fixed_r_train_coverage import make_input

    if Version(xgb.__version__) < Version("2.1.0"):
        raise ValueError("Boundary prototype requires XGBoost >= 2.1.0")
    policy = options["restart_scope"]
    replacement_ranks(policy, 0, 2)  # Validate before starting a cluster.
    enabled = options["mode"] == "on"
    input_path = directory / "input"
    total_rows = make_input(input_path, blocks=8)
    files = sorted(input_path.glob("*.parquet"))
    partitions = [[str(p) for p in files[rank::2]] for rank in range(2)]
    expected = [fingerprint(pq.read_table(paths).to_pandas()) for paths in partitions]
    diagnostics.update(restart_scope=policy, groups=[], recoveries=[], segments=[], ingestion=[],
                       worker_node_failures=[], failure_rounds=[3, 6],
                       recovery_scope="checkpoint_boundary_with_finalized_collectives",
                       implementation="experimental_controller_not_Ray_Train_retry")
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=enabled,
                                    allow_head_failure=False, include_worker_failure=True) as (case, _, crash):
        coordinator = ray.get_runtime_context().get_node_id()
        job_id = ray.get_runtime_context().get_job_id()
        original_nodes = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        diagnostics["coordinator_node_id"] = coordinator
        native = ray._private.state.state.get_system_config()
        diagnostics["native_settings"] = {key: native.get(key) for key in system_config()}
        if any(native.get(key) != (enabled if key.startswith("enable_") else value)
               for key, value in system_config().items()):
            raise ValueError("Native protection settings do not match requested mode")
        base_context = DataContext.get_current().copy()
        clear_config(base_context)
        base_context.enable_fixed_r_task_recovery = enabled
        base_context.fixed_r_task_recovery_output_mode = "streaming"
        base_context.fixed_r_task_recovery_timeout_s = 30
        base_context.enable_progress_bars = False
        base_context.execution_options.preserve_order = True
        base_context.target_min_block_size = 0
        base_context.custom_execution_callback_classes = []
        if enabled:
            get_config(base_context)
        worker_cls = ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)(BoundaryWorker)
        actors = [None, None]
        placements = list(case.executor_node_ids[:2])
        dead = set()
        deadline = time.monotonic() + options["timeout_s"]

        def remaining():
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("Boundary prototype exceeded its observation budget")
            return min(30, left)

        def load(rank):
            started = time.monotonic()
            actor = worker_cls.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
                placements[rank], soft=False)).remote(rank)
            actors[rank] = actor
            context = context_for_new_execution(base_context)
            config = get_config(context)
            if enabled and (set(config.executor_node_ids) & dead or len(config.executor_node_ids) < 2):
                raise ValueError("Input loader retained dead executor placement")
            with DataContext.current(context):
                frame = ray.data.read_parquet(partitions[rank], override_num_blocks=len(partitions[rank])).to_pandas()
            # Keep row order identical across reloads; only the actor/cache
            # replacement policy should differ between the two observations.
            frame = frame.sort_values(list(frame.columns), kind="stable").reset_index(drop=True)
            identity = ray.get(actor.prepare.remote(frame, expected[rank]), timeout=remaining())
            if identity["node_id"] != placements[rank]:
                raise ValueError("Training worker did not use its requested live executor")
            diagnostics["ingestion"].append({"rank": rank, "actor_id": identity["actor_id"],
                                              "duration_s": time.monotonic() - started,
                                              "configured_executor_node_ids": list(config.executor_node_ids) if config else None})
            return identity

        started = time.monotonic()
        try:
            current = [load(rank) for rank in range(2)]
            diagnostics["groups"].append(current)
            checkpoint = None
            saved_models = []
            start = 0
            for generation, end in enumerate((3, 6, 10), 1):
                results = collective_segment(actors, checkpoint, start, end, generation, remaining())
                current = [r["identity"] for r in results]
                if current != diagnostics["groups"][-1]:
                    raise ValueError("Worker identity or cached data changed inside a segment")
                if any(w["input"] != expected[w["rank"]] for w in current):
                    raise ValueError("Cached input changed during training")
                if generation > 1:
                    recovery = diagnostics["recoveries"][-1]
                    recovery["first_resumed_round"] = start + 1
                    recovery["failure_to_first_resumed_round_s"] = (
                        max(r["first_round_ns"] for r in results) - recovery["request_ns"]) / 1e9
                diagnostics["segments"].append([{k: v for k, v in r.items() if k != "model"} for r in results])
                checkpoint = results[0]["model"]
                path = directory / f"checkpoint-{end}.ubj"
                temporary = path.with_suffix(".tmp")
                with temporary.open("wb") as handle:
                    handle.write(checkpoint)
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(path)
                saved = xgb.Booster(model_file=str(path))
                if saved.num_boosted_rounds() != end or tree_digest(saved) != results[0]["tree_sha256"]:
                    raise ValueError("Persisted checkpoint differs from collective output")
                saved_models.append((end, saved))
                if end == 10:
                    break
                # All worker RPCs and the tracker have finalized. Failures here
                # cannot leave a healthy rank blocked in the previous collective.
                target = current[0]
                request_ns = time.monotonic_ns()
                args.recovery_timeout_s = remaining()
                failure = crash(target["node_id"], target["pid"])
                if (not failure["all_node_processes_exited"] or not failure["gcs_marked_dead"]
                        or target["pid"] not in failure["node_process_pids"]):
                    raise ValueError("Requested worker-node process loss was not established")
                dead.add(target["node_id"])
                if set(failure["surviving_node_ids"]) != original_nodes - dead:
                    raise ValueError("Unrequested node loss during replacement")
                diagnostics["worker_node_failures"].append(failure)
                recovery = {"request_ns": request_ns, "checkpoint_round": end, "failed_rank": 0,
                            "failed_node_id": target["node_id"], "collective_finalized_before_failure": True}
                diagnostics["recoveries"].append(recovery)
                replace = replacement_ranks(policy, 0, 2)
                if policy == "full":
                    ray.kill(actors[1], no_restart=True)
                placements[0] = next(n for n in case.executor_node_ids if n not in dead and n != placements[1])
                after = list(current)
                for rank in replace:
                    after[rank] = load(rank)
                for rank in set(range(2)) - set(replace):
                    after[rank] = ray.get(actors[rank].identity.remote(), timeout=remaining())
                recovery.update(validate_transition(current, after, 0, policy, dead))
                recovery["failure_to_ready_s"] = (time.monotonic_ns() - request_ns) / 1e9
                recovery["reloaded_rows"] = sum(expected[rank]["rows"] for rank in replace)
                diagnostics["groups"].append(after)
                # Exercise deserialization from the committed file on every
                # generation, even when the healthy actor retains its model.
                checkpoint = path.read_bytes()
                start = end
            training_s = time.monotonic() - started
            final = saved_models[-1][1]
            if final.num_boosted_rounds() != 10 or final.num_features() != 16:
                raise ValueError("Final model has incorrect dimensions")
            for rounds, model in saved_models[:-1]:
                if tree_digest(final[:rounds]) != tree_digest(model):
                    raise ValueError("Final model changed a previously committed checkpoint")
            frame = pq.read_table(input_path).to_pandas()
            reference = final.predict(xgb.DMatrix(frame.drop("labels", axis=1), nthread=1))
            predicted_at = time.monotonic()
            predictions = ray.get([a.predict.remote(frame) for a in actors], timeout=remaining())
            prediction_s = time.monotonic() - predicted_at
            for values in predictions:
                if len(values) != total_rows or not np.isfinite(values).all() or np.any((values < 0) | (values > 1)):
                    raise ValueError("Invalid final predictions")
                np.testing.assert_allclose(values, reference, rtol=1e-6, atol=1e-7)
            np.save(directory / "predictions.npy", predictions[0])
            alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
            if alive != original_nodes - dead or len(dead) != 2:
                raise ValueError("Expected exactly two distinct worker-node losses")
            if (ray.get_runtime_context().get_node_id() != coordinator
                    or ray.get_runtime_context().get_job_id() != job_id):
                raise ValueError("Coordinator/job changed during selective recovery")
            diagnostics["live_node_ids_after"] = sorted(alive)
            return {"validation_status": "passed", "training_s": training_s,
                    "prediction_s": prediction_s, "pipeline_s": training_s + prediction_s,
                    "artifact_validation": {"model_rounds": 10, "checkpoint_prefixes_preserved": [3, 6],
                                            "input_rows": total_rows, "input_verified_each_segment": True,
                                            "prediction_rows": total_rows, "both_worker_predictions_match_checkpoint": True},
                    "final_tree_sha256": tree_digest(final)}
        finally:
            for actor in actors:
                if actor is not None:
                    ray.kill(actor, no_restart=True)
