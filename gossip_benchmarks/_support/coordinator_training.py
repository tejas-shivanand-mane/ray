"""External coordinator-process fault harness; CIFAR application is unchanged."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import runpy
import sys
import time
import uuid

import ray
import ray.cloudpickle
from ray.data._internal.execution.execution_callback import ExecutionCallback

import train_workload as workload_support
from train_workload import (
    ReportGate, WorkloadObserver, file_sha256, observe_function,
    validate_active_gates, write_record,
)
from streaming_learning import collect_evidence, input_identity, validate_learning
from coordinator_comparison import validate_progress


def process_identity(actor):
    context = ray.get_runtime_context()
    return {"pid": os.getpid(), "worker_id": context.get_worker_id(),
            "node_id": context.get_node_id(),
            "owner_node_id": getattr(actor, "_owner_node_id", None)}


def probe(actor, timeout=60):
    # Supported actor introspection call, equally applied to both coordinators.
    return ray.get(actor.__ray_call__.remote(process_identity), timeout=timeout)


def await_fault(actor, old, resume, timeout=60, node_loss=False):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Coordinator fault was not observed within its deadline")
        try:
            new = probe(actor, remaining)
        except ray.exceptions.ActorUnavailableError:
            pass
        except ray.exceptions.RayActorError:
            if resume:
                raise
            return None
        else:
            if new["worker_id"] != old["worker_id"]:
                if not resume or ((new["node_id"] == old["node_id"]) == node_loss):
                    raise ValueError("Unexpected coordinator restart/placement")
                return new
        time.sleep(min(.02, max(0, deadline - time.monotonic())))


class CoordinatorRegistry:
    """Records existing handles for injection, without relocating any owner."""
    def __init__(self):
        self.coordinators = {}

    def register(self, rank, coordinator):
        key = coordinator._actor_id.hex()
        if key not in self.coordinators:
            self.coordinators[key] = {"actor": coordinator, "ranks": set(), "identity": probe(coordinator)}
        record = self.coordinators[key]
        record["ranks"].add(rank)

    def initial_identity(self):
        return next(iter(self.coordinators.values()))["identity"]

    def target(self):
        if len(self.coordinators) != 1:
            raise ValueError("Fault must target the initial shared training coordinator")
        key, record = next(iter(self.coordinators.items()))
        if record["ranks"] != {0, 1}:
            raise ValueError("Both ranks must register the same coordinator")
        return key, record["actor"]


class CoordinatorObserver(WorkloadObserver):
    def save(self):
        path = self.directory / "coordinator-fault.json"
        if path.exists():
            self.fault = json.loads(path.read_text())["coordinator_fault"]
        write_record(self.directory / "timeline.json", {
            "groups": self.groups, "reports": self.reports, "fault": self.fault,
        })


def supervise(workload, registry, directory, plan, resume, timeout, *,
              crash_node=None, target_node_id=None, driver_node_id=None):
    node_loss = crash_node is not None
    fault = {"scenario": "coordinator-node" if node_loss else "coordinator-process",
             "scope": "coordinator_node" if node_loss else "coordinator_process_only",
             "failure_timing": "active", "completed": False}
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(workload)
        try:
            deadline = time.monotonic() + timeout
            paths = [directory / "active-checkpoint.json",
                     directory / "active-ready-0.json", directory / "active-ready-1.json"]
            while not all(p.exists() for p in paths):
                if future.done():
                    future.result()
                    raise ValueError("Workload ended before the coordinator fault gate")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Training did not reach the coordinator fault gate")
                time.sleep(.02)
            committed, *gates = [json.loads(p.read_text()) for p in paths]
            actor_id, actor = ray.get(registry.target.remote(), timeout=30)
            old = probe(actor)
            alive = sorted(n["NodeID"] for n in ray.nodes() if n["Alive"])
            fault.update(committed, gates=gates, coordinator_actor_id=actor_id, old=old,
                         alive_nodes_before=alive, request_ns=time.monotonic_ns())
            validate_active_gates(fault, plan)
            if len(fault["groups"]) != 1:
                raise ValueError("Training retried before fault injection")
            write_record(directory / "coordinator-fault.json", {"coordinator_fault": fault})
            if node_loss:
                if (old["node_id"] != target_node_id or old["node_id"] == driver_node_id
                        or not old.get("owner_node_id") or old["owner_node_id"] == target_node_id
                        or any(w["node_id"] == target_node_id for w in fault["groups"][0])):
                    raise ValueError("Node target must exclude the driver, actor owner and training workers")
                fault["driver_node_id"] = driver_node_id
                fault["node_failure"] = crash_node(old["node_id"], old["pid"])
            else:
                ray.kill(actor, no_restart=False)
            fault["new"] = await_fault(actor, old, resume, node_loss=node_loss)
            fault.update(operation_finished_ns=time.monotonic_ns(), completed=True,
                         alive_nodes_after=sorted(n["NodeID"] for n in ray.nodes() if n["Alive"]))
            write_record(directory / "coordinator-fault.json", {"coordinator_fault": fault})
            write_record(directory / "release-active.json", {"released": True})
            future.result()
        except Exception as exc:
            fault.update(error=str(exc))
            write_record(directory / "coordinator-fault.json", {"coordinator_fault": fault})
            raise
        finally:
            write_record(directory / "release-active.json", {"released": True})


def run_case(options, directory, diagnostics):
    import torch
    import torchvision
    from ray.data import DataContext
    from ray.data._internal.execution.streaming_recovery import clear_config
    from ray.data._internal.iterator.resumable_split import CONFIG_KEY
    from ray.experimental.recovery import system_config
    from ray.experimental.recovery._local import local_head_failure_cluster
    from ray.train import FailureConfig, RunConfig
    from ray.train.torch import TorchConfig, TorchTrainer

    directory = Path(directory)
    script = Path(__file__).resolve().parents[1] / "workloads/cifar_streaming.py"
    identity = input_identity(options["data_directory"])
    if identity != options["input_identity"]:
        raise ValueError("CIFAR input changed before the observation")
    mode = options["mode"]
    if mode not in ("ordinary", "resume", "deterministic"):
        raise ValueError("Unknown coordinator comparison mode")
    inject = options["scenario"] != "none"
    node_loss = options.get("failure_scope", "process") == "node"
    if options["scenario"] not in ("none", "coordinator-process", "coordinator-node"):
        raise ValueError("Unsupported coordinator failure scenario")
    if node_loss and mode == "ordinary":
        raise ValueError("Node comparison requires identical prototype sharding and placement")
    if inject and options["scenario"] != ("coordinator-node" if node_loss else "coordinator-process"):
        raise ValueError("Scenario does not match the requested failure scope")
    epoch = options["fault_after_epoch"]
    plan = {"checkpoint_epoch": epoch, "step": options["fault_after_step"],
            "steps_per_epoch": options["steps_per_epoch"]} if inject else None
    diagnostics.update(
        input_identity=identity, workload=str(script), workload_sha256=file_sha256(script),
        torch_version=torch.__version__, torchvision_version=torchvision.__version__,
        training_epochs=options["training_epochs"], batch_size=options["batch_size"],
        steps_per_epoch=options["steps_per_epoch"], train_max_failures=1,
        fixed_r_enabled=False, selective_retry=False, restart_scope="full",
        owner_placement="default", placement_strategy="STRICT_SPREAD",
        sharding="ordinary" if mode == "ordinary" else "deterministic_chunks",
        coordinator_restart_budget=1 if mode == "resume" else 0,
        coordinator_placement="separate_node_soft_affinity" if node_loss else "owner_node_hard_affinity",
        workload_completed=False,
    )
    originals = TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv[:], sys.path[:]
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    timings = []

    class ExecutionRecorder(ExecutionCallback):
        def before_execution_starts(self, executor):
            self.key = uuid.uuid4().hex
            self.record(executor, "running")

        def record(self, executor, state):
            write_record(directory / "data-executions" / f"{self.key}.json", {
                "time_ns": time.monotonic_ns(), "state": state,
                **process_identity(None),
                "operators": [{"operator": op.name, **{
                    k: v for k, v in op.metrics.extra_metrics.items() if k.startswith("fixed_r_")}}
                              for op in executor._topology],
            })

        def after_execution_succeeds(self, executor):
            self.record(executor, "completed")

        def after_execution_fails(self, executor, error):
            self.record(executor, "failed")

    def configure(trainer, train_loop_per_worker, **kwargs):
        config = kwargs.get("torch_config") or TorchConfig()
        if type(config) is not TorchConfig:
            raise ValueError("Expected ordinary TorchConfig")
        kwargs["torch_config"] = replace(config, backend="gloo", timeout_s=120,
                                         selective_recovery=False)
        run = kwargs.get("run_config") or RunConfig()
        kwargs["run_config"] = replace(
            run, storage_path=str(directory / "storage"), name="workload",
            failure_config=FailureConfig(max_failures=1),
            callbacks=[*(run.callbacks or []), ReportGate(str(directory), epoch, 90),
                       CoordinatorObserver(str(directory), False, epoch, active=inject)],
        )
        kwargs["scaling_config"] = replace(kwargs["scaling_config"], placement_strategy="STRICT_SPREAD")
        observed = observe_function(train_loop_per_worker, str(directory), plan)

        def registered(config):
            from ray import train
            shard = train.get_dataset_shard("train")
            expected = "StreamSplitDataIterator" if mode == "ordinary" else "ResumableSplitIterator"
            if type(shard).__name__ != expected:
                raise ValueError("Training received an unexpected input splitter")
            ray.get(registry.register.remote(train.get_context().get_world_rank(), shard._coord_actor))
            return observed(config)

        originals[0](trainer, registered, **kwargs)

    def fit(trainer, *a, **kw):
        if timings:
            raise ValueError("Expected exactly one Trainer.fit")
        started = time.monotonic()
        result = originals[1](trainer, *a, **kw)
        timings.append(time.monotonic() - started)
        return result

    try:
        for module in (sys.modules[__name__], workload_support):
            ray.cloudpickle.register_pickle_by_value(module)
        with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=False,
                                        include_worker_failure=True,
                                        include_data_coordinator=node_loss) as (case_args, _, crash_node):
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
            registry = ray.remote(CoordinatorRegistry).options(
                num_cpus=0, scheduling_strategy=NodeAffinitySchedulingStrategy(
                    case_args.driver_node_id, soft=False,
                ),
            ).remote()
            native = ray._private.state.state.get_system_config()
            diagnostics["native_settings"] = {k: native.get(k) for k in system_config()}
            if any(native.get(k) != (False if k.startswith("enable_") else v)
                   for k, v in system_config().items()):
                raise ValueError("Fixed-R must be disabled in every coordinator comparison arm")
            context = DataContext.get_current()
            clear_config(context)
            context.enable_fixed_r_task_recovery = False
            context.execution_options.preserve_order = True
            context.enable_progress_bars = False
            context.set_config(CONFIG_KEY, None if mode == "ordinary" else {
                "deterministic": True, "rows_per_chunk": options["batch_size"],
                "max_restarts": 1 if mode == "resume" else 0, "timeout_s": 90,
                **({"preferred_node_id": case_args.data_coordinator_node_id,
                    "allow_node_relocation": True} if node_loss else {}),
            })
            context.custom_execution_callback_classes = [*context.custom_execution_callback_classes, ExecutionRecorder]
            TorchTrainer.__init__, TorchTrainer.fit = configure, fit
            ray.init = lambda *a, **kw: originals[2](ignore_reinit_error=True)
            sys.path.insert(0, str(script.parent))
            sys.argv = [str(script), "--data-directory", options["data_directory"],
                        "--epochs", str(options["training_epochs"]), "--batch-size", str(options["batch_size"]),
                        "--telemetry-directory", str(directory / "stream-events")]
            diagnostics["workload_started_ns"] = time.monotonic_ns()
            write_record(directory / "progress.json", diagnostics)
            try:
                workload = lambda: runpy.run_path(str(script), run_name="__main__")
                if inject:
                    supervise(workload, registry, directory, plan, mode == "resume", options["timeout_s"],
                              crash_node=crash_node if node_loss else None,
                              target_node_id=case_args.data_coordinator_node_id,
                              driver_node_id=case_args.driver_node_id)
                else:
                    workload()
                diagnostics["workload_completed"] = True
            finally:
                diagnostics["workload_finished_ns"] = time.monotonic_ns()
                diagnostics["workload_s"] = (diagnostics["workload_finished_ns"] - diagnostics["workload_started_ns"]) / 1e9
                write_record(directory / "progress.json", diagnostics)
                diagnostics.update(collect_evidence(directory))
            diagnostics.update(json.loads((directory / "timeline.json").read_text()))
            diagnostics["starts"] = [json.loads(p.read_text()) for p in sorted((directory / "starts").glob("*.json"))]
            diagnostics["initial_coordinator"] = ray.get(registry.initial_identity.remote(), timeout=30)
            if node_loss:
                initial = diagnostics["initial_coordinator"]
                if (initial["node_id"] != case_args.data_coordinator_node_id
                        or initial.get("owner_node_id") != case_args.driver_node_id):
                    raise ValueError("Initial coordinator/owner placement differs from the controlled topology")
            if inject:
                diagnostics.update(json.loads((directory / "coordinator-fault.json").read_text()))
                validate_active_gates(diagnostics["coordinator_fault"], plan)
            validate_progress(diagnostics, options)
            validate_learning(directory, {**options, "mode": "off", "workload": str(script),
                                          "fault_after_epoch": diagnostics["restored_checkpoint_epoch"]}, diagnostics)
            if diagnostics["fixed_r_enrolled_tasks"]:
                raise ValueError("Unexpected Fixed-R enrollment")
            if len(timings) != 1:
                raise ValueError("Missing completed training timing")
            return {"validation_status": "passed", "training_s": timings[0]}
    finally:
        TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv, sys.path = originals
        for module in (sys.modules[__name__], workload_support):
            ray.cloudpickle.unregister_pickle_by_value(module)
