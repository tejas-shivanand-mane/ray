"""External worker/owner-failure harness for existing CPU TorchTrainer scripts.

The script keeps its own model, optimizer, datasets and train function. This
adapter selects the retry policy, local storage and callbacks at construction.
It kills a worker process or logical node after a selected committed checkpoint.
The optional Fashion active mode first completes real optimizer updates inside
the next unfinished epoch. It checks node/process evidence, actor reuse,
checkpoint delivery, recomputation and subsequent reports. It does not make an arbitrary
application resumable: that application must save and restore its own state.
The data-owner scenario instead holds a random-shuffle map task while the local
harness replaces head processes, then checks end-to-end completion and replay.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
import hashlib
import inspect
import json
from pathlib import Path
import runpy
import shutil
import sys
from threading import current_thread, main_thread
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
    """Hold a selected report until the controller commits it and injects."""

    def __init__(self, directory, report_number=1, timeout_s=30):
        self.directory = directory
        self.report_number = report_number
        self.timeout_s = timeout_s

    @contextmanager
    def on_report(self):
        from ray.train.v2._internal.execution.context import get_train_context

        selected = get_train_context().report_call_index == self.report_number - 1
        yield
        if selected:
            deadline = time.monotonic() + self.timeout_s
            release = Path(self.directory) / f"release-report-{self.report_number}.json"
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Controller did not commit the selected checkpoint")
                time.sleep(.02)


class WorkloadObserver(WorkerGroupCallback, ReportCallback):
    def __init__(self, directory, inject, report_number=1, node_scenario=None, active=False):
        self.directory = Path(directory)
        self.inject = inject
        self.report_number = report_number
        self.node_scenario = node_scenario
        self.active = active
        self.groups = []
        self.reports = []
        self.fault = None
        self.workers = []

    def save(self):
        active_fault = self.directory / "node-fault.json"
        if self.active and active_fault.exists():
            self.fault = json.loads(active_fault.read_text())["node_fault"]
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
        if len(self.reports) == self.report_number:
            if not record["checkpoint"]:
                raise ValueError("Selected failure report has no committed checkpoint")
            if self.active:
                # The step gate will request the fault inside the NEXT epoch.
                # Publish the committed checkpoint without injecting here.
                write_record(self.directory / "active-checkpoint.json", {
                    "report_number": self.report_number, "groups": self.groups,
                    "checkpoint": record["checkpoint"],
                    "checkpoint_committed_ns": record["time_ns"],
                })
            elif self.node_scenario:
                # The controller and workers wait at a committed epoch while
                # the driver's MAIN thread operates the local node supervisor.
                self.fault = {"scenario": self.node_scenario,
                              "report_number": self.report_number,
                              "groups": self.groups,
                              "checkpoint": record["checkpoint"],
                              "checkpoint_committed_ns": record["time_ns"],
                              "request_ns": time.monotonic_ns(), "completed": False}
                self.save()
                write_record(self.directory / "node-fault-request.json", self.fault)
                response = self.directory / "node-fault.json"
                deadline = time.monotonic() + 90
                while not response.exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Node supervisor did not finish the selected failure")
                    time.sleep(.02)
                self.fault = json.loads(response.read_text())["node_fault"]
                self.save()
                if not self.fault.get("completed"):
                    raise RuntimeError(self.fault.get("error", "Node failure did not complete"))
            elif self.inject:
                self.fault = {"rank": 0, "actor_id": self.groups[0][0]["actor_id"],
                              "report_number": self.report_number,
                              "request_ns": time.monotonic_ns()}
                self.save()
                ray.kill(self.workers[0].actor, no_restart=True)
            # Healthy workers continue the workload and encounter the dead peer
            # through ordinary Gloo/data/report operations. No injected user exception.
            write_record(self.directory / f"release-report-{self.report_number}.json", {"released": True})
        self.save()


def observe_function(function, directory, active_plan=None):
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
        gate = (active_optimizer_steps(directory, active_plan, identity, checkpoint)
                if active_plan else nullcontext())
        with gate:
            return function(config) if takes_config else function()

    return wrapped


@contextmanager
def active_optimizer_steps(directory, plan, identity, checkpoint):
    """Instrument the existing Fashion Adam loop without changing its workload.

    The gate follows a real optimizer update, within an uncommitted epoch. This
    is not an arbitrary mid-kernel or in-flight-collective injection.
    """
    import torch

    directory = Path(directory)
    restored_epoch = 0
    if checkpoint:
        with checkpoint.as_directory() as path:
            restored_epoch = torch.load(Path(path) / "training.pt", map_location="cpu", weights_only=True)["epoch"]
    original = torch.optim.Adam.step
    updates = 0
    target = plan["checkpoint_epoch"] * 118 + plan["step"]
    release = directory / "release-active.json"

    def step(optimizer, *args, **kwargs):
        nonlocal updates
        result = original(optimizer, *args, **kwargs)
        updates += 1
        absolute = restored_epoch * 118 + updates
        event = {**identity, "time_ns": time.monotonic_ns(),
                 "restored_epoch": restored_epoch, "optimizer_updates_this_invocation": updates,
                 "absolute_update": absolute, "checkpoint_epoch": plan["checkpoint_epoch"],
                 "step": plan["step"], "epoch": plan["checkpoint_epoch"] + 1}
        if absolute == target:
            if restored_epoch:
                write_record(directory / f"active-recomputed-{identity['rank']}.json", event)
            else:
                write_record(directory / f"active-ready-{identity['rank']}.json", event)
                deadline = time.monotonic() + 90
                while not release.exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Active training fault supervisor did not release the optimizer gate")
                    time.sleep(.02)
        elif absolute == target + 1 and not restored_epoch:
            write_record(directory / f"active-continued-{identity['rank']}.json", event)
        return result

    torch.optim.Adam.step = step
    try:
        yield
    finally:
        torch.optim.Adam.step = original


def active_training_fault(directory, scenario, plan, executor_ids, head_id,
                          crash_head, crash_worker, workload, timeout_s):
    """Operate logical nodes on the main thread while workers are mid-epoch."""
    if current_thread() is not main_thread():
        raise RuntimeError("Active fault supervision must run on the main thread")
    fault = {"scenario": scenario, "report_number": plan["checkpoint_epoch"],
             "failure_timing": "active", "completed": False}
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(workload)
        try:
            deadline = time.monotonic() + timeout_s
            paths = [directory / "active-checkpoint.json", *[
                directory / f"active-ready-{rank}.json" for rank in (0, 1)]]
            while not all(path.exists() for path in paths):
                if future.done():
                    future.result()
                    raise ValueError("Workload ended before the active training gate")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Training did not reach the active optimizer gate")
                time.sleep(.02)
            committed, *gates = [json.loads(path.read_text()) for path in paths]
            fault.update(committed, gates=gates, request_ns=time.monotonic_ns(),
                         original_head_node_id=head_id, executor_node_ids=sorted(executor_ids))
            group = fault["groups"][0]
            if (len(fault["groups"]) != 1 or len(group) != 2
                    or [w["rank"] for w in group] != [0, 1]
                    or len({w["node_id"] for w in group}) != 2
                    or any(w["node_id"] not in executor_ids for w in group)
                    or fault["report_number"] != plan["checkpoint_epoch"]
                    or not fault["checkpoint"]):
                raise ValueError("Active fault requires a committed checkpoint and two separate executor nodes")
            validate_active_gates(fault, plan)
            if scenario == "head-node":
                fault["head_replacement"] = crash_head()
            elif scenario == "worker-node":
                fault["worker_node_failure"] = crash_worker(group[0]["node_id"], group[0]["pid"])
            else:
                raise ValueError("Unsupported active failure scope")
            fault.update(completed=True, operation_finished_ns=time.monotonic_ns())
        except Exception as exc:
            fault.update(error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            write_record(directory / "node-fault.json", {"node_fault": fault})
            write_record(directory / "release-active.json", {"released": True})
        return future.result()


def validate_active_gates(fault, plan):
    group, gates = fault["groups"][0], fault["gates"]
    if len(gates) != 2:
        raise ValueError("Active failure lacks both rank gates")
    for worker, gate in zip(group, gates):
        if (any(worker[k] != gate[k] for k in ("rank", "actor_id", "pid"))
                or gate["restored_epoch"] != 0 or gate["checkpoint"] is not None
                or gate["checkpoint_epoch"] != plan["checkpoint_epoch"]
                or gate["epoch"] != plan["checkpoint_epoch"] + 1
                or gate["step"] != plan["step"] or not 0 < gate["step"] < 118
                or gate["absolute_update"] != plan["checkpoint_epoch"] * 118 + plan["step"]
                or gate["optimizer_updates_this_invocation"] != gate["absolute_update"]
                or not fault["checkpoint_committed_ns"] < gate["time_ns"] <= fault["request_ns"]):
            raise ValueError("Failure did not follow matched uncheckpointed optimizer work on both ranks")


def validate_active_training(directory, diagnostics, plan):
    fault = diagnostics["node_fault"]
    if fault.get("failure_timing") != "active":
        raise ValueError("Expected a mid-epoch fault")
    validate_active_gates(fault, plan)
    retried = len(diagnostics["groups"]) == 2
    label = "recomputed" if retried else "continued"
    events = [json.loads((directory / f"active-{label}-{rank}.json").read_text()) for rank in (0, 1)]
    resumed = [s for s in diagnostics["starts"] if s["checkpoint"] is not None]
    for worker, event in zip(diagnostics["groups"][-1], events):
        expected_epoch = plan["checkpoint_epoch"] if retried else 0
        expected_update = plan["checkpoint_epoch"] * 118 + plan["step"] + (0 if retried else 1)
        if (any(event[k] != worker[k] for k in ("rank", "actor_id", "pid"))
                or event["restored_epoch"] != expected_epoch
                or event["absolute_update"] != expected_update
                or event["optimizer_updates_this_invocation"] != expected_update - expected_epoch * 118
                or event["checkpoint"] != (fault["checkpoint"] if retried else None)
                or not fault["operation_finished_ns"] < event["time_ns"]
                < diagnostics["reports"][plan["checkpoint_epoch"]]["time_ns"]):
            raise ValueError("Missing matched post-failure optimizer progress")
    recovery = diagnostics["recoveries"][0]
    recovery.update(
        failure_timing="active", interrupted_epoch=plan["checkpoint_epoch"] + 1,
        completed_uncheckpointed_steps_per_rank=plan["step"],
        lost_uncommitted_optimizer_steps_per_rank=plan["step"] if retried else 0,
        recomputed_optimizer_steps_per_rank=plan["step"] if retried else 0,
        model_checkpoint_restored=retried, optimizer_progress=events,
        fault_boundary="after an optimizer update inside an unfinished epoch; both ranks gated",
        gate_wait_before_fault_s=(fault["request_ns"] - min(e["time_ns"] for e in fault["gates"])) / 1e9,
        restore_invocation_to_recomputed_step_s=(
            (max(e["time_ns"] for e in events) - max(s["time_ns"] for s in resumed)) / 1e9 if retried else None),
    )


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


def owner_progress_plan(directory):
    path = Path(directory) / "owner-progress-plan.json"
    return json.loads(path.read_text()) if path.exists() else None


def owner_target_index(directory):
    plan = owner_progress_plan(directory)
    return plan["target_index"] if plan else 0


def map_progress(directory):
    return sorted((json.loads(p.read_text()) for p in Path(directory).glob("map-computed-*.json")),
                  key=lambda event: event["index"])


def before_ordered_map(directory, index):
    """A controlled compute order, identical in OFF/ON controls and fault runs.

    Counts completed map computations, not copied outputs or elapsed fractions.
    Later maps wait for injection to finish; replay reuses the same input index.
    """
    plan = owner_progress_plan(directory)
    if plan is None:
        return False
    if not 0 <= index < plan["map_count"]:
        raise ValueError("Unexpected shuffle map count in controlled workload")
    directory = Path(directory)
    deadline = time.monotonic() + 60
    while ((plan["inject"] and index > plan["target_index"]
            and not (directory / "release-data-owner").exists())
           or (index > 0 and not (directory / f"map-computed-{index - 1}.json").exists())):
        if time.monotonic() >= deadline:
            raise TimeoutError("Ordered shuffle map did not become runnable")
        time.sleep(.01)
    if plan["inject"]:
        wait_for_owner_fault(directory, index)
    return True


def record_map_computed(directory, index):
    path = Path(directory) / f"map-computed-{index}.json"
    if not path.exists():
        runtime = ray.get_runtime_context()
        write_record(path, {"index": index, "time_ns": time.monotonic_ns(),
                            "task_id": runtime.get_task_id(), "node_id": runtime.get_node_id()})


def wait_for_owner_fault(directory, task_index):
    """Pause the selected task; legacy experiments select map zero."""
    if task_index != owner_target_index(directory):
        return
    directory = Path(directory)
    release = directory / "release-data-owner"
    if release.exists():
        return
    runtime = ray.get_runtime_context()
    marker = directory / "data-owner-blocked.json"
    if not marker.exists():
        write_record(marker, {
            "task_id": runtime.get_task_id(), "node_id": runtime.get_node_id(),
            "blocked_ns": time.monotonic_ns(), "stage": "RandomShuffle.map",
            "map_index": task_index,
        })
    deadline = time.monotonic() + 60
    while not release.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Owner-failure gate was not released")
        time.sleep(.01)


def owner_gated_map(original, directory, streaming):
    # Ordered experiments gate BEFORE computing the selected map. Legacy
    # experiments pause map zero after computation, before its first export.
    if streaming:
        def produce(*args):
            random_shuffle = args[0]._map_args[2]
            ordered = random_shuffle and before_ordered_map(directory, args[1])
            outputs = original(*args)
            try:
                first = next(outputs)
                if ordered:
                    record_map_computed(directory, args[1])
                elif random_shuffle:
                    wait_for_owner_fault(directory, args[1])
                yield first
                yield from outputs
            finally:
                outputs.close()
    else:
        def produce(*args):
            ordered = args[5] and before_ordered_map(directory, args[0])
            outputs = original(*args)
            # ShuffleTaskSpec.map also implements shuffled repartition. Only
            # random_shuffle=True is the selected failure point.
            if ordered:
                record_map_computed(directory, args[0])
            elif args[5]:
                wait_for_owner_fault(directory, args[0])
            return outputs
    return produce


@contextmanager
def ordered_owner_control(directory, enabled, active):
    """Apply the same map ordering to controls without injecting a fault."""
    if not active:
        yield
        return
    from ray.data._internal.planner.exchange import streaming_recovery as exchange
    from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
    original_map, original_stream = ShuffleTaskSpec.map, exchange._map_outputs
    if enabled:
        exchange._map_outputs = owner_gated_map(original_stream, str(directory), True)
    else:
        ShuffleTaskSpec.map = staticmethod(owner_gated_map(original_map, str(directory), False))
    try:
        yield
    finally:
        ShuffleTaskSpec.map = staticmethod(original_map)
        exchange._map_outputs = original_stream


class OrdinaryShuffleOwner:
    """Benchmark-only owner: submit ordinary tasks and return their nested refs."""

    def submit(self, producer, args, options):
        self.refs = producer.options(**options).remote(*args)
        return self.refs


@contextmanager
def matched_shuffle_ownership(directory, enabled, owner_node_id, executor_ids, diagnostics, inject):
    """Place shuffle-map owners on the head in both arms, without changing the UDF.

    OFF uses ordinary Ray submission through disposable actors. This is an
    explicit ownership experiment, not the default Ray Data placement policy.
    ON observes the existing recovery descriptor without changing submission.
    """
    from ray.core.generated.common_pb2 import Address, RecoveryStreamDescriptor
    from ray.data._internal.planner.exchange import pull_based_shuffle_task_scheduler as pull
    from ray.data._internal.planner.exchange import streaming_recovery as exchange
    from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
    from ray.data._internal.progress.base_progress import BaseProgressBar
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    directory = Path(directory)
    target_index = owner_target_index(directory)
    actors, target_refs = [], []
    original_remote, original_submit = pull.cached_remote_fn, exchange.submit_stream
    original_fetch = BaseProgressBar.fetch_until_complete

    def record(task_id, address, **extra):
        if address.node_id.hex() != owner_node_id:
            raise ValueError("Shuffle output owner is not on the selected head")
        evidence = {"task_id": task_id, "owner_node_id": address.node_id.hex(),
                    "owner_worker_id": address.worker_id.hex(),
                    "recorded_ns": time.monotonic_ns(), **extra}
        diagnostics["shuffle_owner"] = evidence
        write_record(directory / "shuffle-owner.json", evidence)

    class RemoteMap:
        def __init__(self, producer, options=None):
            self.producer, self.remote_options = producer, options or {}

        def options(self, **options):
            return RemoteMap(self.producer, {**self.remote_options, **options})

        def remote(self, *args):
            if not args[5]:  # Keep non-random repartition on its ordinary path.
                return self.producer.options(**self.remote_options).remote(*args)
            owner = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(
                OrdinaryShuffleOwner
            ).options(scheduling_strategy=NodeAffinitySchedulingStrategy(
                owner_node_id, soft=False,
            )).remote()
            actors.append(owner)
            ray.get(owner.__ray_ready__.remote(), timeout=30)
            options = {**self.remote_options, "max_retries": 1, "retry_exceptions": False,
                       "scheduling_strategy": NodeAffinitySchedulingStrategy(
                           executor_ids[args[0] % len(executor_ids)], soft=False)}
            refs = ray.get(owner.submit.remote(self.producer, args, options), timeout=30)
            if args[0] == target_index:
                ref = refs[-1]  # Metadata is fetched by the original shuffle scheduler.
                target_refs.append(ref)
                address = Address.FromString(
                    ray._private.worker.global_worker.core_worker.get_owner_address(ref)
                )
                record(ref.task_id().hex(), address, object_ref_hex=ref.hex())
            return refs

    def remote(function, *args, **kwargs):
        producer = original_remote(function, *args, **kwargs)
        return RemoteMap(producer) if function is ShuffleTaskSpec.map else producer

    def submit(*args, **kwargs):
        stream = original_submit(*args, **kwargs)
        task_args = args[2]
        if (len(task_args) == 4 and type(task_args[1]) is int
                and task_args[1] == target_index and task_args[0]._map_args[2]):
            if stream.reader is None:
                raise ValueError("Target shuffle map was not enrolled before owner loss")
            descriptor = RecoveryStreamDescriptor.FromString(stream.reader.descriptor)
            record(stream.task_id.hex(), descriptor.manifest.succession[0].address)
        return stream

    def fetch(bar, refs):
        targeted = target_refs and target_refs[0] in refs
        if targeted and inject:
            # All ordinary map submissions are complete before metadata fetch.
            # Match ON's settled-batch gate and avoid an actor-RPC failure race.
            (directory / "data-owner-batch-ready").touch()
            deadline = time.monotonic() + 60
            while not (directory / "release-data-owner").exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Owner-failure controller did not release ordinary shuffle")
                time.sleep(.01)
        try:
            return original_fetch(bar, refs)
        except ray.exceptions.OwnerDiedError:
            if targeted and inject:
                # Require Ray's real OwnerDiedError for the exact gated task,
                # not a timeout, actor RPC failure, or arbitrary pipeline error.
                try:
                    ray.get(target_refs[0], timeout=5)
                except ray.exceptions.OwnerDiedError as exc:
                    address = Address.FromString(exc.owner_address)
                    evidence = {"error_type": "OwnerDiedError",
                                "object_ref_hex": exc.object_ref_hex,
                                "owner_node_id": address.node_id.hex(),
                                "owner_worker_id": address.worker_id.hex(),
                                "observed_ns": time.monotonic_ns(),
                                "source": "shuffle_metadata_fetch"}
                    diagnostics["ordinary_owner_loss"] = evidence
                    write_record(directory / "ordinary-owner-loss.json", evidence)
            raise

    if enabled:
        exchange.submit_stream = submit
    else:
        pull.cached_remote_fn = remote
        BaseProgressBar.fetch_until_complete = fetch
    try:
        yield
    finally:
        pull.cached_remote_fn, exchange.submit_stream = original_remote, original_submit
        BaseProgressBar.fetch_until_complete = original_fetch
        for actor in actors:
            ray.kill(actor, no_restart=True)


def data_owner_fault(directory, enabled, crash_head, executor_ids, diagnostics, workload,
                     matched_owner=False, timeout_s=60):
    """Kill/replace head processes after a real shuffle task reaches its gate.

    The default OFF arm retains coordinator ownership. With matched_owner,
    both arms must prove the gated task's owner lives on the failed head.

    Run the workload in a background thread so head replacement can register
    Ray's process shutdown hooks on the main thread.
    """
    from ray.data._internal.planner.exchange import streaming_recovery as exchange
    from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec

    if current_thread() is not main_thread():
        raise RuntimeError("Head replacement must run on the main thread")
    directory = Path(directory)
    target_index = owner_target_index(directory)
    blocked = directory / "data-owner-blocked.json"
    enrolled = directory / "data-owner-enrolled.json"
    batch_ready = directory / "data-owner-batch-ready"
    release = directory / "release-data-owner"
    fault = {"completed": False, "stage": "RandomShuffle.map"}
    diagnostics["data_owner_fault"] = fault
    original_map, original_stream = ShuffleTaskSpec.map, exchange._map_outputs
    original_submit = exchange.submit_stream

    def before_consume(task, *args, **kwargs):
        if enrolled.exists() and not release.exists():
            if json.loads(enrolled.read_text())["task_id"] == task.stream.task_id.hex():
                # The exchange loop consumes only after its submission batch
                # has settled. Pause it here so no ambiguous new enrollment can
                # race head loss. Workers keep the ordinary output gate too.
                batch_ready.touch()
                deadline = time.monotonic() + 60
                while not release.exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Owner-failure controller did not release the consumer")
                    time.sleep(.01)
        return original_consume(task, *args, **kwargs)

    original_consume = exchange._ExchangeTask.on_data_ready

    def capture_submit(*args, **kwargs):
        stream = original_submit(*args, **kwargs)
        task_args = args[2]
        if (len(task_args) == 4 and type(task_args[1]) is int
                and task_args[1] == target_index and task_args[0]._map_args[2]):
            # A running producer alone is not enough: wait for the descriptor
            # and witness enrollment to finish before killing its owner.
            if stream.reader is None:
                raise ValueError("Fault target was not enrolled in Fixed-R")
            write_record(enrolled, {"task_id": stream.task_id.hex()})
        return stream

    def inject(future):
        deadline = time.monotonic() + timeout_s
        try:
            while not (blocked.exists()
                       and (not enabled or (enrolled.exists() and batch_ready.exists()))
                       and (not matched_owner or (
                           (directory / "shuffle-owner.json").exists() and batch_ready.exists()))):
                if future.done():
                    # Preserve the workload's original error if it failed
                    # before reaching the selected fault point.
                    future.result()
                    raise ValueError("Workload ended before the shuffle failure point")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Workload did not reach the shuffle failure point")
                time.sleep(.02)
            target = json.loads(blocked.read_text())
            if target["node_id"] not in executor_ids:
                raise ValueError("Shuffle task must execute on a surviving executor")
            if enabled and json.loads(enrolled.read_text())["task_id"] != target["task_id"]:
                raise ValueError("Blocked task differs from the enrolled recovery task")
            if matched_owner:
                ownership = json.loads((directory / "shuffle-owner.json").read_text())
                if ownership["task_id"] != target["task_id"]:
                    raise ValueError("Blocked task differs from the observed owner task")
                fault["ownership"] = ownership
            fault.update(target=target, request_ns=time.monotonic_ns(),
                         submission_batch_settled=batch_ready.exists(),
                         fixed_r_submission_batch_settled=enabled and batch_ready.exists())
            plan = owner_progress_plan(directory)
            if plan:
                progress = map_progress(directory)
                if ([event["index"] for event in progress] != list(range(target_index))
                        or target.get("map_index") != target_index
                        or any(event["time_ns"] > target["blocked_ns"] for event in progress)):
                    raise ValueError("Failure did not reach the selected shuffle compute prefix")
                fault.update(progress_plan=plan, completed_maps_before_failure=progress)
            write_record(directory / "data-owner-fault.json", fault)
            fault["head_replacement"] = crash_head()
            if matched_owner and ownership["owner_node_id"] != fault["head_replacement"]["original_head_node_id"]:
                raise ValueError("Fault did not kill the observed shuffle owner node")
            fault.update(completed=True, replacement_ready_ns=time.monotonic_ns())
            write_record(directory / "data-owner-fault.json", fault)
        except Exception as exc:
            fault.update(error_type=type(exc).__name__, error=str(exc))
            write_record(directory / "data-owner-fault.json", fault)
            raise
        finally:
            release.touch()

    if enabled:
        exchange._map_outputs = owner_gated_map(original_stream, str(directory), True)
        exchange.submit_stream = capture_submit
        exchange._ExchangeTask.on_data_ready = before_consume
    else:
        ShuffleTaskSpec.map = staticmethod(owner_gated_map(original_map, str(directory), False))
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(workload)
            # inject always releases both gates, including on failure, before
            # the pool waits for the workload to exit. The parent process keeps
            # the existing hard deadline for a workload stuck inside Ray.
            inject(future)
            return future.result()
    finally:
        ShuffleTaskSpec.map = staticmethod(original_map)
        exchange._map_outputs, exchange.submit_stream = original_stream, original_submit
        if enabled:
            del exchange._ExchangeTask.on_data_ready


def training_node_fault(directory, scenario, report_number, executor_ids, head_id,
                        crash_head, crash_worker, workload, timeout_s):
    """Inject one real logical-node failure from the driver's main thread.

    Training runs in a background thread; only the supervisor creates/removes
    Ray nodes, because process startup registers signal handlers. The workers
    remain at the selected committed report until the operation has finished.
    """
    if current_thread() is not main_thread():
        raise RuntimeError("Node failure supervision must run on the main thread")
    if scenario not in ("head-node", "worker-node"):
        raise ValueError("Unsupported training node failure")
    request = directory / "node-fault-request.json"
    response = directory / "node-fault.json"
    release = directory / f"release-report-{report_number}.json"
    fault = {"scenario": scenario, "report_number": report_number, "completed": False}
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(workload)
        try:
            deadline = time.monotonic() + timeout_s
            while not request.exists():
                if future.done():
                    future.result()
                    raise ValueError("Workload ended before the requested node failure")
                if time.monotonic() >= deadline:
                    raise TimeoutError("Training did not reach the requested node failure")
                time.sleep(.02)
            fault = json.loads(request.read_text())
            if (fault.get("scenario") != scenario or fault.get("report_number") != report_number
                    or not fault.get("checkpoint") or len(fault.get("groups", [])) != 1):
                raise ValueError("Unmatched node failure request or premature retry")
            group = fault["groups"][0]
            if ([w["rank"] for w in group] != [0, 1]
                    or len({w["node_id"] for w in group}) != 2
                    or any(w["node_id"] not in executor_ids for w in group)):
                raise ValueError("Node comparison requires two distinct executor nodes")
            fault.update(request_ns=time.monotonic_ns(), original_head_node_id=head_id,
                         executor_node_ids=sorted(executor_ids))
            if fault["checkpoint_committed_ns"] > fault["request_ns"]:
                raise ValueError("Node failure preceded checkpoint commitment")
            if scenario == "head-node":
                fault["head_replacement"] = crash_head()
            else:
                target = group[0]
                fault["worker_node_failure"] = crash_worker(target["node_id"], target["pid"])
            fault.update(completed=True, operation_finished_ns=time.monotonic_ns())
        except Exception as exc:
            fault.update(completed=False, error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            write_record(response, {"node_fault": fault})
            write_record(release, {"released": True})
        return future.result()


def validate_node_evidence(fault, groups, reports):
    if not fault.get("completed") or not groups:
        raise ValueError("Missing completed node-failure evidence")
    epoch = fault["report_number"]
    if (type(epoch) is not int or not 1 <= epoch < len(reports)
            or groups[0] != fault["groups"][0]
            or fault["checkpoint"] != reports[epoch - 1]["checkpoint"]
            or fault["checkpoint_committed_ns"] != reports[epoch - 1]["time_ns"]
            or not fault["checkpoint_committed_ns"] <= fault["request_ns"]
            < fault["operation_finished_ns"] < reports[epoch]["time_ns"]):
        raise ValueError("Node failure lacks matched checkpoint and subsequent progress")
    original = groups[0]
    nodes = {w["node_id"] for w in original}
    executors = set(fault["executor_node_ids"])
    if (len(original) != 2 or [w["rank"] for w in original] != [0, 1]
            or len(nodes) != 2 or not nodes <= executors
            or fault["original_head_node_id"] in nodes):
        raise ValueError("Training workers were not on separate surviving executor nodes")
    if fault["scenario"] == "worker-node":
        loss = fault["worker_node_failure"]
        if (loss.get("failure_scope") != "logical_worker_node_processes_with_surviving_shared_storage"
                or not loss.get("all_node_processes_exited") or not loss.get("gcs_marked_dead")
                or loss["node_id"] != original[0]["node_id"]
                or loss["training_worker_pid"] != original[0]["pid"]
                or loss["training_worker_pid"] not in loss["node_process_pids"]
                or loss["node_id"] in loss["surviving_node_ids"]
                or not (executors - {loss["node_id"]}) <= set(loss["surviving_node_ids"])
                or fault["original_head_node_id"] not in loss["surviving_node_ids"]
                or len(groups) != 2
                or any(w["node_id"] == loss["node_id"] for w in groups[1])):
            raise ValueError("Worker-node loss or replacement was not verified")
    elif fault["scenario"] == "head-node":
        head = fault["head_replacement"]
        if (head.get("failure_scope") != "all_head_processes_with_surviving_gcs_storage"
                or not head.get("original_head_processes_exited")
                or head.get("gcs_storage_backend") != "rocksdb"
                or head["original_head_node_id"] != fault["original_head_node_id"]
                or head["original_head_node_id"] == head["replacement_head_node_id"]
                or head["original_gcs_pid"] == head["replacement_gcs_pid"]
                or not executors <= set(head["surviving_node_ids"])):
            raise ValueError("Head replacement with surviving executors was not verified")
    else:
        raise ValueError("Unknown node-failure scope")


def validate(directory, selective, inject, node_scenario=None):
    timeline = json.loads((directory / "timeline.json").read_text())
    groups, reports = timeline["groups"], timeline["reports"]
    if node_scenario:
        fault = json.loads((directory / "node-fault.json").read_text())["node_fault"]
        if fault["scenario"] != node_scenario:
            raise ValueError("Observed node failure differs from the requested scope")
        validate_node_evidence(fault, groups, reports)
        timeline.update(fault=fault, node_fault=fault)
    retry = inject or node_scenario == "worker-node" or (node_scenario == "head-node" and len(groups) == 2)
    if len(groups) != (2 if retry else 1):
        raise ValueError("Unexpected retry count; inspect timeline.json")
    if len(reports) < 2 or not reports[-1]["checkpoint"]:
        raise ValueError("Workload must commit a checkpoint and then make further progress")
    starts = [json.loads(p.read_text()) for p in (directory / "starts").glob("*.json")]
    expected_starts = sum(len(g) for g in groups)
    if len(starts) != expected_starts:
        raise ValueError("Missing or unexpected train function invocations")
    recovery = []
    if retry:
        fault = timeline["fault"]
        if not fault:
            raise ValueError("Requested worker failure was not injected")
        report_number = fault.get("report_number", 1)
        if type(report_number) is not int or not 1 <= report_number < len(reports):
            raise ValueError("No progress after the selected failure checkpoint")
        committed = reports[report_number - 1]
        if not committed["checkpoint"] or committed["time_ns"] > fault["request_ns"]:
            raise ValueError("Failure does not follow a committed checkpoint")
        old, new = groups
        if [w["rank"] for w in old] != [w["rank"] for w in new]:
            raise ValueError("Global ranks changed across recovery")
        retained = [w["rank"] for w, n in zip(old, new) if w["actor_id"] == n["actor_id"]]
        expected_retained = list(range(1, len(old))) if selective else []
        if ((node_scenario != "head-node" and retained != expected_retained)
                or (node_scenario == "head-node" and not selective and retained)):
            raise ValueError(f"Unexpected retained ranks: {retained}; fallback is not selective success")
        resumed = [s for s in starts if s["checkpoint"] is not None]
        if len(resumed) != len(new) or any(s["checkpoint"] != committed["checkpoint"] for s in resumed):
            raise ValueError("Retry did not receive the exact committed checkpoint on every rank")
        identity = lambda worker: (worker["rank"], worker["actor_id"], worker["pid"])
        if sorted(map(identity, resumed)) != sorted(map(identity, new)):
            raise ValueError("Checkpoint delivery evidence does not match the resumed worker ranks")
        if any(s["time_ns"] <= timeline["fault"]["request_ns"] for s in resumed):
            raise ValueError("Recorded retry predates injection")
        if reports[report_number]["time_ns"] <= max(s["time_ns"] for s in resumed):
            raise ValueError("Post-recovery report predates resumed train functions")
        recovery.append({"retained_ranks": retained,
                         "replaced_ranks": [w["rank"] for w in old if w["rank"] not in retained],
                         "committed_reports_before_failure": report_number,
                         "failure_to_all_workers_invoked_s": (max(s["time_ns"] for s in resumed) - fault["request_ns"]) / 1e9,
                         "failure_to_next_report_s": (reports[report_number]["time_ns"] - fault["request_ns"]) / 1e9})
    elif any(s["checkpoint"] is not None for s in starts):
        raise ValueError("No-failure run unexpectedly restored a checkpoint")
    if node_scenario == "head-node" and not retry:
        epoch = fault["report_number"]
        recovery.append({"retained_ranks": [w["rank"] for w in groups[0]], "replaced_ranks": [],
                         "committed_reports_before_failure": epoch,
                         "failure_to_all_workers_invoked_s": None,
                         "failure_to_next_report_s": (reports[epoch]["time_ns"] - fault["request_ns"]) / 1e9})
    if node_scenario:
        recovery[0].update(worker_retry_occurred=retry,
                           node_operation_s=(fault["operation_finished_ns"] - fault["request_ns"]) / 1e9)
    return {**timeline, "starts": starts, "recoveries": recovery}


def run_case(options, directory, diagnostics, existing_cluster=None):
    import torch
    from ray.data import DataContext
    from ray.data.context import ShuffleStrategy
    from ray.data._internal.execution.streaming_recovery import clear_config, get_config
    from ray.data._internal.execution.execution_callback import ExecutionCallback
    from ray.data._internal.execution.operators.base_physical_operator import AllToAllOperator
    from ray.experimental.recovery import system_config
    from ray.experimental.recovery._local import local_head_failure_cluster
    from ray.train import FailureConfig, RunConfig
    from ray.train.torch import TorchConfig, TorchTrainer

    script = Path(options["workload"]).resolve()
    script_args = list(options["workload_args"])
    regression = script == Path(__file__).resolve().parents[2] / "python/ray/train/examples/pytorch/torch_regression_example.py"
    workloads = Path(__file__).resolve().parents[1] / "workloads"
    fashion = script in (workloads / "fashion_mnist.py", workloads / "fashion_features.py")
    features = script == workloads / "fashion_features.py"
    if features:
        from fashion_comparison import feature_identity
        observed = feature_identity(Path(options["feature_directory"]))
        if observed != options["feature_identity"]:
            raise ValueError("Feature weights changed before observation")
        diagnostics["feature_identity"] = observed
        script_args += ["--validation-output", str(directory / "validation-features.npz")]
    training_epochs = options.get("training_epochs")
    if training_epochs is not None and (
        not (regression or fashion) or type(training_epochs) is not int or training_epochs < 3
    ):
        raise ValueError("Epoch override requires a supported workload and at least three epochs")
    if regression and not script_args:
        script_args = ["--num-workers", "2", "--data-path", prepare_regression_input(directory)]
    selective = options["restart_scope"] == "selective"
    inject = options["scenario"] == "worker"
    node_scenario = options["scenario"] if options["scenario"] in ("head-node", "worker-node") else None
    owner_failure = options["scenario"] == "data-owner"
    enabled = options["mode"] == "on"
    matched_owner = options.get("owner_placement", "default") == "head"
    ordered_plan = options.get("owner_progress_plan")
    if ordered_plan is not None:
        if (not fashion or not matched_owner or selective
                or options["scenario"] not in ("none", "data-owner")
                or ordered_plan.get("map_count") != 4
                or type(ordered_plan.get("target_index")) is not int
                or not 0 <= ordered_plan["target_index"] < 4
                or ordered_plan.get("inject") is not owner_failure):
            raise ValueError("Invalid controlled Fashion-MNIST owner-loss plan")
        write_record(directory / "owner-progress-plan.json", ordered_plan)
        diagnostics["owner_progress_plan"] = ordered_plan
    report_number = options.get("fault_after_epoch", 1) if inject or node_scenario else 1
    if type(report_number) is not int or report_number < 1 or (
        training_epochs is not None and report_number >= training_epochs
    ):
        raise ValueError("Failure must follow a positive epoch with training remaining")
    active_mode = options.get("failure_timing", "boundary") == "active"
    active_plan = None
    if active_mode:
        step = options.get("fault_after_step", 59)
        if (not fashion or options["scenario"] not in ("none", "head-node", "worker-node")
                or type(step) is not int or not 0 < step < 118
                or options.get("placement_strategy") != "STRICT_SPREAD"):
            raise ValueError("Active Fashion faults require spread node cases and a step inside the 118-step epoch")
        if node_scenario:
            active_plan = {"checkpoint_epoch": report_number, "step": step}
    diagnostics.update(failure_timing=options.get("failure_timing", "boundary"),
                       fault_after_step=options.get("fault_after_step", 59) if active_mode else None)
    if fashion:
        from fashion_comparison import input_identity
        identity = input_identity(Path(options["data_directory"]))
        if identity != options["input_identity"]:
            raise ValueError("Fashion-MNIST input changed before observation")
        diagnostics["input_identity"] = identity
    diagnostics.update(implementation="existing_TorchTrainer_script", restart_scope=options["restart_scope"],
                       workload=str(script), workload_sha256=file_sha256(script),
                       numerical_probe=regression or fashion, torch_version=torch.__version__,
                       owner_placement=options.get("owner_placement", "default"),
                       placement_strategy=options.get("placement_strategy", "PACK"),
                       workload_completed=False)
    if regression or fashion:
        # Owner loss can end preprocessing before TorchTrainer is constructed.
        # Keep the planned fixed work available for that failed baseline too.
        diagnostics["training_epochs"] = training_epochs if training_epochs is not None else 3
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    originals = TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv[:], sys.path[:]
    timings = []
    workload_started = None
    context, original_callbacks = None, None

    class ExchangeObserver(ExecutionCallback):
        def record(self, executor):
            exchanges = [
                {"operator": op.name, **op.metrics.extra_metrics}
                for op in executor._topology if isinstance(op, AllToAllOperator)
            ]
            if exchanges:
                write_record(directory / "exchanges" / f"{uuid.uuid4().hex}.json", exchanges)

        def after_execution_succeeds(self, executor):
            self.record(executor)

        def after_execution_fails(self, executor, error):
            self.record(executor)

    def configure(trainer, train_loop_per_worker, **kwargs):
        if regression or fashion:
            loop_config = dict(kwargs.get("train_loop_config") or {})
            if training_epochs is not None:
                loop_config["epochs"] = training_epochs
            diagnostics["training_epochs"] = loop_config.get("epochs", 3)
            kwargs["train_loop_config"] = loop_config
        config = kwargs.get("torch_config") or TorchConfig()
        if type(config) is not TorchConfig or config.backend not in (None, "gloo"):
            raise ValueError("Workload harness requires ordinary CPU Gloo TorchTrainer")
        config = replace(config, backend="gloo", timeout_s=10,
                         selective_recovery=selective, recovery_timeout_s=25)
        run = kwargs.get("run_config") or RunConfig()
        run = replace(run, storage_path=str(directory / "storage"), name="workload",
                      failure_config=FailureConfig(max_failures=1), callbacks=[
                          *(run.callbacks or []), ReportGate(str(directory), report_number, 90 if node_scenario else 30),
                          WorkloadObserver(str(directory), inject, report_number, node_scenario,
                                           active=active_plan is not None)])
        scaling = kwargs.get("scaling_config")
        if scaling is None or scaling.num_workers < 2 or scaling.use_gpu or scaling.use_tpu:
            raise ValueError("Use at least two CPU workers in the workload")
        if options.get("placement_strategy"):
            kwargs["scaling_config"] = replace(scaling, placement_strategy=options["placement_strategy"])
        if node_scenario and options.get("placement_strategy") != "STRICT_SPREAD":
            raise ValueError("Training node failures require STRICT_SPREAD placement")
        kwargs.update(torch_config=config, run_config=run)
        originals[0](trainer, observe_function(train_loop_per_worker, str(directory), active_plan), **kwargs)

    def fit(trainer, *a, **kw):
        if timings:
            raise ValueError("Run one Trainer.fit per observation")
        started = time.monotonic()
        diagnostics["before_trainer_fit_s"] = started - workload_started
        result = originals[1](trainer, *a, **kw)
        timings.append(time.monotonic() - started)
        return result

    try:
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        cluster_context = (nullcontext(existing_cluster) if existing_cluster is not None else
                           local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=enabled,
                                                      allow_head_failure=owner_failure or node_scenario == "head-node",
                                                      include_worker_failure=True))
        with cluster_context as (case_args, crash_head, crash_worker):
            diagnostics["selected_owner_node_id"] = case_args.owner_node_id
            native = ray._private.state.state.get_system_config()
            diagnostics["native_settings"] = {key: native.get(key) for key in system_config()}
            if any(native.get(k) != (enabled if k.startswith("enable_") else v)
                   for k, v in system_config().items()):
                raise ValueError("Native Fixed-R settings do not match requested mode")
            context = DataContext.get_current()
            original_callbacks = context.custom_execution_callback_classes
            clear_config(context)
            context.enable_fixed_r_task_recovery = enabled
            context.fixed_r_task_recovery_output_mode = "streaming"
            context.fixed_r_task_recovery_timeout_s = 30
            context.execution_options.preserve_order = True
            context.enable_progress_bars = False
            if options.get("comparison") in ("fixed-r", "integrated"):
                context.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED
            if enabled:
                get_config(context)
            context.custom_execution_callback_classes = [
                *context.custom_execution_callback_classes, ExchangeObserver
            ]
            # The external harness owns this isolated cluster; script ray.init()
            # must attach to it rather than create a different benchmark cluster.
            ray.init = lambda *a, **kw: originals[2](ignore_reinit_error=True)
            TorchTrainer.__init__, TorchTrainer.fit = configure, fit
            sys.argv = [str(script), *script_args]
            sys.path.insert(0, str(script.parent))

            def workload():
                ownership = (matched_shuffle_ownership(
                    directory, enabled, case_args.owner_node_id,
                    tuple(sorted(case_args.executor_node_ids)), diagnostics, owner_failure,
                ) if matched_owner else nullcontext())
                with ordered_owner_control(directory, enabled, ordered_plan is not None and not owner_failure), \
                        DataContext.current(context), ownership:
                    return runpy.run_path(str(script), run_name="__main__")

            workload_started = time.monotonic()
            diagnostics["workload_started_ns"] = time.monotonic_ns()
            write_record(directory / "progress.json", {
                "workload_started_ns": diagnostics["workload_started_ns"],
            })
            try:
                if owner_failure:
                    data_owner_fault(directory, enabled, crash_head,
                                     case_args.executor_node_ids, diagnostics, workload,
                                     matched_owner=matched_owner, timeout_s=options["timeout_s"])
                elif active_plan is not None:
                    active_training_fault(directory, node_scenario, active_plan,
                                          case_args.executor_node_ids, case_args.owner_node_id,
                                          crash_head, crash_worker, workload, options["timeout_s"])
                elif node_scenario:
                    training_node_fault(directory, node_scenario, report_number,
                                        case_args.executor_node_ids, case_args.owner_node_id,
                                        crash_head, crash_worker, workload, options["timeout_s"])
                else:
                    workload()
                diagnostics["workload_completed"] = True
            finally:
                diagnostics["workload_finished_ns"] = time.monotonic_ns()
                diagnostics["workload_s"] = time.monotonic() - workload_started
                write_record(directory / "progress.json", {
                    key: diagnostics[key] for key in (
                        "workload_started_ns", "workload_finished_ns", "workload_s",
                        "workload_completed",
                    )
                })
                diagnostics["data_exchanges"] = [
                    record for path in sorted((directory / "exchanges").glob("*.json"))
                    for record in json.loads(path.read_text())
                ]
                if ordered_plan is not None:
                    diagnostics["map_progress"] = map_progress(directory)
                if features and (directory / "feature-progress.json").exists():
                    diagnostics["feature_progress"] = json.loads((directory / "feature-progress.json").read_text())
            if ordered_plan is not None and [e["index"] for e in diagnostics["map_progress"]] != list(range(4)):
                raise ValueError("Workload did not compute all four controlled shuffle maps")
            if len(timings) != 1:
                raise ValueError("The script did not execute exactly one TorchTrainer.fit")
            diagnostics.update(validate(directory, selective, inject, node_scenario))
            if active_plan is not None:
                validate_active_training(directory, diagnostics, active_plan)
            exchanges = diagnostics["data_exchanges"]
            if (regression or fashion) and enabled:
                for name in ("Repartition", "RandomShuffle"):
                    if not any(record["operator"].startswith(name)
                               and record.get("fixed_r_enrolled_tasks", 0) > 0
                               and record.get("fixed_r_closed_streams", 0)
                               == (record.get("fixed_r_enrolled_tasks", 0)
                                   + record.get("fixed_r_survivor_tasks", 0))
                               for record in exchanges):
                        raise ValueError(f"Missing completed Fixed-R exchange evidence: {name}")
            if owner_failure and enabled:
                target_id = diagnostics["data_owner_fault"]["target"]["task_id"]
                if not any(detail["task_id"] == target_id
                           for record in exchanges if record["operator"].startswith("RandomShuffle")
                           for detail in record.get("fixed_r_recovered_task_details", [])):
                    raise ValueError("ON completed without replaying the blocked shuffle task")
            if regression:
                reports = diagnostics["reports"]
                if [r["metrics"][0]["epoch"] for r in reports] != list(
                    range(1, diagnostics["training_epochs"] + 1)
                ):
                    raise ValueError("Regression did not complete each epoch exactly once")
                if any(m["resumed_from_epoch"] != (report_number if inject and i >= report_number else 0)
                       for i, r in enumerate(reports) for m in r["metrics"]):
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
            if fashion:
                from fashion_comparison import validate_fashion
                validate_fashion(directory, options, diagnostics)
            return {"validation_status": "passed", "training_s": timings[0]}
    finally:
        if context is not None:
            context.custom_execution_callback_classes = original_callbacks
        TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv, sys.path = originals
        ray.cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
