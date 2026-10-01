"""External worker-failure harness for existing CPU TorchTrainer scripts.

The script keeps its own model, optimizer, datasets and train function. This
adapter selects the retry policy, local storage and callbacks at construction.
It kills a worker after the first committed checkpoint and checks actor reuse,
checkpoint delivery and subsequent reports. It does not make an arbitrary
application resumable: that application must save and restore its own state.
"""

import argparse
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import inspect
import json
from pathlib import Path
import runpy
import shutil
import sys
import time
import uuid

import numpy as np
import ray
import ray.cloudpickle
from ray.train.v2._internal.execution.callback import (
    ReportCallback, TrainContextCallback, WorkerGroupCallback,
)


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_record(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex)
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


def checkpoint_files(checkpoint):
    if checkpoint is None:
        return None
    with checkpoint.as_directory() as path:
        return {str(p.relative_to(path)): file_sha256(p)
                for p in sorted(Path(path).rglob("*")) if p.is_file()}


class ReportGate(TrainContextCallback):
    """Keep the first checkpoint from racing ahead of fault injection."""

    def __init__(self, directory):
        self.directory = directory

    @contextmanager
    def on_report(self):
        from ray.train.v2._internal.execution.context import get_train_context

        first = get_train_context().report_call_index == 0
        yield
        if first:
            deadline = time.monotonic() + 30
            release = Path(self.directory) / "release-first-report.json"
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Controller did not commit the first checkpoint")
                time.sleep(.02)


class WorkloadObserver(WorkerGroupCallback, ReportCallback):
    def __init__(self, directory, inject):
        self.directory = Path(directory)
        self.inject = inject
        self.groups = []
        self.reports = []
        self.fault = None
        self.workers = []

    def save(self):
        write_record(self.directory / "timeline.json", {
            "groups": self.groups, "reports": self.reports, "fault": self.fault,
        })

    def after_worker_group_start(self, worker_group):
        self.workers = list(worker_group.get_workers())
        self.groups.append([
            {"rank": w.distributed_context.world_rank,
             "actor_id": w.actor._actor_id.hex(), "pid": w.metadata.pid,
             "node_id": w.metadata.node_id}
            for w in self.workers
        ])
        self.save()

    def after_report(self, training_report, metrics):
        # CheckpointManager is subscribed before user ReportCallbacks.
        record = {"metrics": metrics, "time_ns": time.monotonic_ns(),
                  "checkpoint": checkpoint_files(training_report.checkpoint)}
        if len(self.reports) == 0 and not record["checkpoint"]:
            raise ValueError("First report must contain a synchronous resumable checkpoint")
        self.reports.append(record)
        if training_report.checkpoint:
            with training_report.checkpoint.as_directory() as path:
                shutil.copytree(path, self.directory / "final-checkpoint", dirs_exist_ok=True)
        if len(self.reports) == 1:
            if self.inject:
                self.fault = {"rank": 0, "actor_id": self.groups[0][0]["actor_id"],
                              "request_ns": time.monotonic_ns()}
                ray.kill(self.workers[0].actor, no_restart=True)
            # Healthy workers continue the workload and encounter the dead peer
            # through ordinary Gloo/data/report operations. No injected user exception.
            write_record(self.directory / "release-first-report.json", {"released": True})
        self.save()


def observe_function(function, directory):
    """Record which committed checkpoint Ray supplies to each actual invocation."""
    takes_config = bool(inspect.signature(function).parameters)

    def wrapped(config):
        import os
        import ray.train

        runtime = ray.get_runtime_context()
        checkpoint = ray.train.get_checkpoint()
        identity = {"rank": ray.train.get_context().get_world_rank(),
                    "actor_id": runtime.get_actor_id(), "pid": os.getpid(),
                    "checkpoint": checkpoint_files(checkpoint),
                    "time_ns": time.monotonic_ns()}
        write_record(Path(directory) / "starts" / f"{uuid.uuid4().hex}.json", identity)
        return function(config) if takes_config else function()

    return wrapped


def prepare_regression_input(directory):
    """Local CSV for the existing regression example; no download or new model."""
    import pandas as pd

    rng = np.random.default_rng(0)
    values = rng.normal(size=(256, 100)).astype(np.float32)
    frame = pd.DataFrame(values, columns=[f"x{i:03d}" for i in range(100)])
    frame["y"] = values[:, :4].sum(axis=1)
    path = Path(directory) / "regression.csv"
    frame.to_csv(path, index=False)
    return str(path)


def validate(directory, selective, inject):
    timeline = json.loads((directory / "timeline.json").read_text())
    groups, reports = timeline["groups"], timeline["reports"]
    if len(groups) != (2 if inject else 1):
        raise ValueError("Unexpected retry count; inspect timeline.json")
    if len(reports) < 2 or not reports[-1]["checkpoint"]:
        raise ValueError("Workload must commit a checkpoint and then make further progress")
    starts = [json.loads(p.read_text()) for p in (directory / "starts").glob("*.json")]
    expected_starts = sum(len(g) for g in groups)
    if len(starts) != expected_starts:
        raise ValueError("Missing or unexpected train function invocations")
    recovery = []
    if inject:
        old, new = groups
        if [w["rank"] for w in old] != [w["rank"] for w in new]:
            raise ValueError("Global ranks changed across recovery")
        retained = [w["rank"] for w, n in zip(old, new) if w["actor_id"] == n["actor_id"]]
        if retained != (list(range(1, len(old))) if selective else []):
            raise ValueError(f"Unexpected retained ranks: {retained}; fallback is not selective success")
        resumed = [s for s in starts if s["checkpoint"] is not None]
        if len(resumed) != len(new) or any(s["checkpoint"] != reports[0]["checkpoint"] for s in resumed):
            raise ValueError("Retry did not receive the exact committed checkpoint on every rank")
        if any(s["time_ns"] <= timeline["fault"]["request_ns"] for s in resumed):
            raise ValueError("Recorded retry predates injection")
        recovery.append({"retained_ranks": retained,
                         "replaced_ranks": [w["rank"] for w in old if w["rank"] not in retained],
                         "failure_to_next_report_s": (reports[1]["time_ns"] - timeline["fault"]["request_ns"]) / 1e9})
    elif any(s["checkpoint"] is not None for s in starts):
        raise ValueError("No-failure run unexpectedly restored a checkpoint")
    return {**timeline, "starts": starts, "recoveries": recovery}


def run_case(options, directory, diagnostics):
    import torch
    from ray.data import DataContext
    from ray.data._internal.execution.streaming_recovery import clear_config, get_config
    from ray.experimental.recovery import system_config
    from ray.experimental.recovery._local import local_head_failure_cluster
    from ray.train import FailureConfig, RunConfig
    from ray.train.torch import TorchConfig, TorchTrainer

    script = Path(options["workload"]).resolve()
    script_args = list(options["workload_args"])
    regression = script == Path(__file__).resolve().parents[2] / "python/ray/train/examples/pytorch/torch_regression_example.py"
    if regression and not script_args:
        script_args = ["--num-workers", "2", "--data-path", prepare_regression_input(directory)]
    selective = options["restart_scope"] == "selective"
    inject = options["scenario"] == "worker"
    enabled = options["mode"] == "on"
    if regression and enabled:
        raise ValueError("The regression example uses shuffle/repartition, outside Fixed-R's supported map chains. Use --mode off for the worker-reuse comparison.")
    diagnostics.update(implementation="existing_TorchTrainer_script", restart_scope=options["restart_scope"],
                       workload=str(script), workload_sha256=file_sha256(script),
                       numerical_probe=regression, torch_version=torch.__version__)
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    originals = TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv[:], sys.path[:]
    timings = []

    def configure(trainer, train_loop_per_worker, **kwargs):
        config = kwargs.get("torch_config") or TorchConfig()
        if type(config) is not TorchConfig or config.backend not in (None, "gloo"):
            raise ValueError("Workload harness requires ordinary CPU Gloo TorchTrainer")
        config = replace(config, backend="gloo", timeout_s=10,
                         selective_recovery=selective, recovery_timeout_s=25)
        run = kwargs.get("run_config") or RunConfig()
        run = replace(run, storage_path=str(directory / "storage"), name="workload",
                      failure_config=FailureConfig(max_failures=1), callbacks=[
                          *(run.callbacks or []), ReportGate(str(directory)),
                          WorkloadObserver(str(directory), inject)])
        scaling = kwargs.get("scaling_config")
        if scaling is None or scaling.num_workers < 2 or scaling.use_gpu or scaling.use_tpu:
            raise ValueError("Use at least two CPU workers in the workload")
        kwargs.update(torch_config=config, run_config=run)
        originals[0](trainer, observe_function(train_loop_per_worker, str(directory)), **kwargs)

    def fit(trainer, *a, **kw):
        if timings:
            raise ValueError("Run one Trainer.fit per observation")
        started = time.monotonic()
        result = originals[1](trainer, *a, **kw)
        timings.append(time.monotonic() - started)
        return result

    try:
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=enabled,
                                        allow_head_failure=False, include_worker_failure=True):
            native = ray._private.state.state.get_system_config()
            diagnostics["native_settings"] = {key: native.get(key) for key in system_config()}
            if any(native.get(k) != (enabled if k.startswith("enable_") else v)
                   for k, v in system_config().items()):
                raise ValueError("Native Fixed-R settings do not match requested mode")
            context = DataContext.get_current()
            clear_config(context)
            context.enable_fixed_r_task_recovery = enabled
            context.fixed_r_task_recovery_output_mode = "streaming"
            context.fixed_r_task_recovery_timeout_s = 30
            context.execution_options.preserve_order = True
            context.enable_progress_bars = False
            if enabled:
                get_config(context)
            # The external harness owns this isolated cluster; script ray.init()
            # must attach to it rather than create a different benchmark cluster.
            ray.init = lambda *a, **kw: originals[2](ignore_reinit_error=True)
            TorchTrainer.__init__, TorchTrainer.fit = configure, fit
            sys.argv = [str(script), *script_args]
            sys.path.insert(0, str(script.parent))
            runpy.run_path(str(script), run_name="__main__")
            if len(timings) != 1:
                raise ValueError("The script did not execute exactly one TorchTrainer.fit")
            diagnostics.update(validate(directory, selective, inject))
            if regression:
                reports = diagnostics["reports"]
                if [r["metrics"][0]["epoch"] for r in reports] != [1, 2, 3]:
                    raise ValueError("Regression did not complete each epoch exactly once")
                if any(m["resumed_from_epoch"] != (1 if inject else 0)
                       for r in reports[1:] for m in r["metrics"]):
                    raise ValueError("Regression did not restore epoch progress")
                state = torch.load(directory / "final-checkpoint/model.pt", weights_only=True)
                model = torch.nn.Sequential(torch.nn.Linear(100, 20), torch.nn.ReLU(), torch.nn.Linear(20, 1))
                model.load_state_dict(state)
                with torch.no_grad():
                    probe = torch.from_numpy(np.random.default_rng(1).normal(size=(64, 100)).astype(np.float32))
                    predictions = model(probe).numpy()
                if not np.isfinite(predictions).all():
                    raise ValueError("Final predictions are not finite")
                np.save(directory / "predictions.npy", predictions)
            return {"validation_status": "passed", "training_s": timings[0]}
    finally:
        TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv, sys.path = originals
        ray.cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
