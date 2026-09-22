"""Original trainer/prefetch workload with a surviving split coordinator.

Actors survive on non-head nodes; only producer tasks use Fixed-R replay.
The harness observes progress and checks consumed batches without changing
the producer, Trainer.train, prefetch implementation, or split algorithm.
"""

import argparse
from dataclasses import replace
from threading import Event, Thread
import time
import traceback
from types import SimpleNamespace
import uuid

import numpy as np
import ray
from ray import cloudpickle
from ray.data import DataContext
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, get_config
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from streaming_recovery_head_failure import local_head_failure_cluster
from streaming_recovery_progress import (
    FRACTIONS, ProgressTrigger, capture_wait_state, register_for_task_serialization, snapshot,
)


def run_dataset(args, crash_head, diagnostics):
    import backpressure_benchmark as original

    register_for_task_serialization()
    total_rows = args.num_input_blocks * args.output_batches_per_input_batch * args.output_batch_rows
    failure = args.recovery_mode == "fixed_r_head_failure"
    phase = args.recovery_head_timing
    timeout = args.recovery_timeout_s
    coordinator_id = ray.get_runtime_context().get_node_id()
    monitor_name = f"fixed-r-prefetch-{uuid.uuid4().hex}"
    namespace = "fixed-r-benchmark"

    class Monitor:
        def __init__(self):
            self.latest = {}
            self.trigger = None
            self.splits = {}
            self.wait_state = None
            self.trainer_progress = {}

        def observe_wait(self, observation):
            self.wait_state = observation

        def progress(self, split, rows, batches):
            self.trainer_progress[split] = {"rows": rows, "batches": batches}

        def report(self, report):
            if report.get("trigger") is not None and self.trigger is None:
                self.trigger = report["trigger"]
            if self.latest.get("state") == "failed" and report["state"] != "failed":
                return
            if self.latest.get("state") == "finished" and report["state"] == "running":
                return
            if report["observed_ns"] >= self.latest.get("observed_ns", -1):
                self.latest = report

        def consumed(self, split, rows, batches):
            if split in self.splits:
                raise ValueError("A trainer consumed its split more than once")
            self.splits[split] = {"rows": rows, "batches": batches}

        def read(self):
            return {
                "latest": self.latest, "trigger": self.trigger, "splits": self.splits,
                "wait_state": self.wait_state, "trainer_progress": self.trainer_progress,
            }

    monitor = ray.remote(num_cpus=0)(Monitor).options(
        name=monitor_name, namespace=namespace,
        scheduling_strategy=NodeAffinitySchedulingStrategy(coordinator_id, soft=False),
    ).remote()

    class Capture(ExecutionCallback):
        def before_execution_starts(self, executor):
            self.control = SimpleNamespace(
                executor=executor,
                operators=[op for op in executor._topology if isinstance(op, MapOperator)],
            )
            if len(self.control.operators) != 1:
                raise ValueError("Expected the original training-prefetch producer map")
            self.monitor = ray.get_actor(monitor_name, namespace=namespace)
            self.trigger = ProgressTrigger(total_rows, phase, 0) if failure else None
            self.last_report = 0
            op = self.control.operators[0]
            submit = op._submit_data_task

            def submit_and_observe(stream, *positional, **kwargs):
                submit(stream, *positional, **kwargs)
                task = next(t for t in op.get_active_tasks() if getattr(t, "stream", None) is stream)
                emit = task._emit_copied_pair

                def emit_and_observe(pair):
                    size = emit(pair)
                    if self.trigger is not None and not self.trigger.ready.is_set():
                        self.trigger.observe(snapshot(self.control))
                        if self.trigger.ready.is_set():
                            self.publish("running")
                    return size

                task._emit_copied_pair = emit_and_observe

            op._submit_data_task = submit_and_observe
            self.publish("running")
            self.stop_observer = Event()

            def observe_waits():
                # A separate thread can capture a blocked executor, where its
                # on_execution_step callback would stop reporting. This never
                # polls streams, acquires consumer locks, or fetches payloads.
                while not self.stop_observer.wait(5):
                    try:
                        observation = capture_wait_state(self.control)
                        observation["observed_ns"] = time.monotonic_ns()
                    except Exception:
                        observation = {"observation_error": traceback.format_exc()}
                    self.monitor.observe_wait.remote(observation)

            Thread(target=observe_waits, name="fixed-r-prefetch-observer", daemon=True).start()

        def publish(self, state, error=None):
            self.last_report = time.monotonic()
            self.monitor.report.remote({
                "observed_ns": time.monotonic_ns(), "state": state,
                "operators": snapshot(self.control),
                "coordinator_node_id": ray.get_runtime_context().get_node_id(),
                "trigger": self.trigger.observation if self.trigger is not None else None,
                "error": error,
            })

        def on_execution_step(self, executor):
            if time.monotonic() - self.last_report >= 0.5:
                self.publish("running")

        def after_execution_succeeds(self, executor):
            self.stop_observer.set()
            self.publish("finished")

        def after_execution_fails(self, executor, error):
            if hasattr(self, "stop_observer"):
                self.stop_observer.set()
            self.publish("failed", "".join(traceback.format_exception(
                type(error), error, error.__traceback__,
            )))

    class ValidatingIterator:
        def __init__(self, inner, split):
            self.inner = inner
            self.split = split

        def iter_batches(self, **kwargs):
            rows = batches = 0
            last_progress = 0
            for batch in self.inner.iter_batches(**kwargs):
                values = batch["data"]
                if (set(batch) != {"data"} or values.dtype != np.uint8
                        or values.ndim != 2 or values.shape[1] != args.output_row_bytes
                        or np.any(values)):
                    raise ValueError("Training-prefetch producer payload changed")
                rows += len(values)
                batches += 1
                if time.monotonic() - last_progress >= 1:
                    monitor.progress.remote(self.split, rows, batches)
                    last_progress = time.monotonic()
                yield batch
            monitor.progress.remote(self.split, rows, batches)
            ray.get(monitor.consumed.remote(self.split, rows, batches), timeout=timeout)

    context = DataContext.get_current().copy()
    context.enable_fixed_r_task_recovery = True
    context.fixed_r_task_recovery_output_mode = "streaming"
    context.fixed_r_task_recovery_timeout_s = timeout
    context.enable_progress_bars = False
    config = get_config(context)
    if args.recovery_mode == "copy":
        context.set_config(CONFIG_KEY, replace(config, mode="copy"))
    context.custom_execution_callback_classes.append(Capture)
    registered = original.__name__ in cloudpickle.list_registry_pickle_by_value()
    if not registered:
        cloudpickle.register_pickle_by_value(original)
    trainers, iterators = [], []
    last_report = {}
    try:
        with DataContext.current(context):
            trainers, iterators = original.build_training_prefetch(args)
        trainer_nodes = ray.get([t.get_node_id.remote() for t in trainers], timeout=timeout)
        if any(node not in args.executor_node_ids for node in trainer_nodes):
            raise ValueError("All trainers must reside on surviving executor nodes")
        if len(set(trainer_nodes)) != len(trainer_nodes):
            raise ValueError("SPREAD did not leave one CPU per worker for protected producers")
        diagnostics["trainer_node_ids"] = trainer_nodes
        pending = [trainer.train.remote(
            ValidatingIterator(iterator, index), batch_size=args.output_batch_rows,
        ) for index, (trainer, iterator) in enumerate(zip(trainers, iterators))]
        deadline = time.monotonic() + timeout
        injected = False
        while True:
            last_report = ray.get(monitor.read.remote(), timeout=timeout)
            latest = last_report["latest"]
            if latest.get("state") == "failed":
                raise RuntimeError(latest["error"])
            if failure and not injected and last_report["trigger"] is not None:
                diagnostics.update(last_report["trigger"])
                started = time.monotonic()
                diagnostics.update(crash_head())
                injected = True
            if pending:
                ready, pending = ray.wait(pending, num_returns=len(pending), timeout=0)
                if ready:
                    ray.get(ready)
            if not pending and latest.get("state") == "finished":
                # Trainers acknowledge their row reports before returning.
                # Refresh after observing completion: the preceding read may
                # have raced the last trainer's report on another sender.
                last_report = ray.get(monitor.read.remote(), timeout=timeout)
                latest = last_report["latest"]
                if latest.get("state") == "failed":
                    raise RuntimeError(latest["error"])
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Training-prefetch did not complete; inspect pre-cleanup observation")
            time.sleep(0.05)
        if failure:
            if not injected:
                raise ValueError("Failure not exercised: training finished before progress trigger")
            diagnostics["failure_request_to_completion_s"] = time.monotonic() - started
        if latest["coordinator_node_id"] != coordinator_id:
            raise ValueError("The split coordinator must reside with the surviving driver")
        operators = latest["operators"]
        op = operators[0]
        if (op["tasks_submitted"] != args.num_input_blocks
                or op["tasks_finished"] != args.num_input_blocks or op["tasks_failed"]
                or op["output_rows"] != total_rows
                or op["fixed_r_closed_streams"] != args.num_input_blocks
                or op["fixed_r_copied_blocks"] != op["output_blocks"]
                or op["active_enrolled_tasks"] or op["fixed_r_recovery_errors"]
                or sum(op[key] for key in ("fixed_r_enrolled_tasks", "fixed_r_survivor_tasks",
                                          "fixed_r_copy_baseline_tasks")) != args.num_input_blocks):
            raise ValueError(f"Incomplete producer execution/recovery: {op}")
        if args.recovery_mode == "copy" and op["fixed_r_copy_baseline_tasks"] != args.num_input_blocks:
            raise ValueError("Copy case unexpectedly enrolled protected tasks")
        if args.recovery_mode == "fixed_r" and op["fixed_r_enrolled_tasks"] != args.num_input_blocks:
            raise ValueError("No-failure case bypassed protection")
        if failure and not op["fixed_r_recovered_task_details"]:
            raise ValueError("Failure not exercised: no protected producer actually replayed")
        splits = last_report["splits"]
        expected_per_split = total_rows // args.num_trainers
        if (set(splits) != set(range(args.num_trainers))
                or any(item["rows"] != expected_per_split for item in splits.values())):
            raise ValueError(f"Trainer row accounting mismatch: {splits}")
        return {
            **vars(args), **diagnostics, "operators": operators,
            "validated_producer_rows": total_rows,
            "validated_consumed_rows": expected_per_split * args.num_trainers,
            "equal_split_dropped_rows": total_rows % args.num_trainers,
            "trainer_splits": splits, "coordinator_node_id": coordinator_id,
            "runtime_recovery": "dynamic_count_streaming",
            "original_udfs": True, "original_trainer_method": True,
            "user_declared_block_counts": False, "calibration_required": False,
            "whole_task_buffering": False, "block_shaping_enabled": True,
            "fusion_enabled": False, "actors_required_to_survive": True,
        }
    except BaseException:
        # The latest remote report is observational, not an atomic live dump.
        diagnostics["observation_before_shutdown"] = last_report
        raise
    finally:
        if iterators:
            coord = iterators[0]._coord_actor
            try:
                ray.get(coord.shutdown_executor.remote(), timeout=5)
            except Exception:
                pass
            ray.kill(coord, no_restart=True)
        for trainer in trainers:
            ray.kill(trainer, no_restart=True)
        ray.kill(monitor, no_restart=True)
        if not registered:
            cloudpickle.unregister_pickle_by_value(original)


def run_recovery_cases(args):
    from benchmark import Benchmark

    if args.recovery_mode not in ("copy", "fixed_r", "fixed_r_head_failure", "suite"):
        raise ValueError("Runtime training-prefetch requires copy/fixed_r/fixed_r_head_failure/suite")
    for name in ("num_input_blocks", "output_batches_per_input_batch", "output_batch_rows",
                 "output_row_bytes", "num_trainers"):
        if getattr(args, name) <= 0:
            raise ValueError(f"{name} must be positive")
    if args.num_trainers > args.local_executor_nodes:
        raise ValueError("Use at most one trainer per two-CPU executor in this local profile")
    timing = getattr(args, "recovery_head_timing", "paused")
    if args.recovery_mode in ("suite", "fixed_r_head_failure") and timing == "paused":
        raise ValueError("Training-prefetch head failure requires --recovery-head-timing early|middle|late|suite")
    phases = tuple(FRACTIONS) if timing == "suite" else (timing,)
    cases = ([("copy", "paused"), ("fixed_r", "paused")]
             if args.recovery_mode == "suite" else [])
    if args.recovery_mode in ("suite", "fixed_r_head_failure"):
        cases += [("fixed_r_head_failure", phase) for phase in phases]
    else:
        cases = [(args.recovery_mode, "paused")]
    benchmark = Benchmark()
    failed = []
    for mode, phase in cases:
        selected = argparse.Namespace(**vars(args))
        selected.recovery_mode = mode
        selected.recovery_head_timing = phase
        diagnostics = {}
        key = f"training_prefetch/original_dataset/{mode}/{phase}"
        try:
            # Keep Trainer's existing SPREAD placement off the coordinator;
            # SplitCoordinator already requests zero CPUs and pins to driver.
            with local_head_failure_cluster(selected, coordinator_cpus=0) as (case_args, crash):
                benchmark.run_fn(key, run_dataset, case_args, crash, diagnostics)
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            traceback.print_exc()
            failed.append(key)
            benchmark.result[key] = {
                **vars(selected), **diagnostics, "validation_status": "failed",
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        finally:
            benchmark.write_result()
    if failed:
        raise RuntimeError(f"Training-prefetch recovery cases failed: {failed}; inspect saved JSON")
