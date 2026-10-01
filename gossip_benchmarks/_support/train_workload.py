"""External worker/owner-failure harness for existing CPU TorchTrainer scripts.

The script keeps its own model, optimizer, datasets and train function. This
adapter selects the retry policy, local storage and callbacks at construction.
It kills a worker after the first committed checkpoint and checks actor reuse,
checkpoint delivery and subsequent reports. It does not make an arbitrary
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


def wait_for_owner_fault(directory, task_index):
    """Pause task zero before it exports its first shuffle partition."""
    if task_index != 0:
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
        })
    deadline = time.monotonic() + 60
    while not release.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("Owner-failure gate was not released")
        time.sleep(.01)


def owner_gated_map(original, directory, streaming):
    # Both adapters compute the normal partitions, then pause before exporting
    # task zero's first result. They neither change nor regenerate workload data.
    if streaming:
        def produce(*args):
            outputs = original(*args)
            try:
                first = next(outputs)
                if args[0]._map_args[2]:
                    wait_for_owner_fault(directory, args[1])
                yield first
                yield from outputs
            finally:
                outputs.close()
    else:
        def produce(*args):
            outputs = original(*args)
            # ShuffleTaskSpec.map also implements shuffled repartition. Only
            # random_shuffle=True is the selected failure point.
            if args[5]:
                wait_for_owner_fault(directory, args[0])
            return outputs
    return produce


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
            if args[0] == 0:
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
                and task_args[1] == 0 and task_args[0]._map_args[2]):
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
                     matched_owner=False):
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
                and task_args[1] == 0 and task_args[0]._map_args[2]):
            # A running producer alone is not enough: wait for the descriptor
            # and witness enrollment to finish before killing its owner.
            if stream.reader is None:
                raise ValueError("Fault target was not enrolled in Fixed-R")
            write_record(enrolled, {"task_id": stream.task_id.hex()})
        return stream

    def inject(future):
        deadline = time.monotonic() + 60
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
    training_epochs = options.get("training_epochs")
    if training_epochs is not None and (
        not regression or type(training_epochs) is not int or training_epochs < 3
    ):
        raise ValueError("Epoch override requires the regression example and at least three epochs")
    if regression and not script_args:
        script_args = ["--num-workers", "2", "--data-path", prepare_regression_input(directory)]
    selective = options["restart_scope"] == "selective"
    inject = options["scenario"] == "worker"
    owner_failure = options["scenario"] == "data-owner"
    enabled = options["mode"] == "on"
    matched_owner = options.get("owner_placement", "default") == "head"
    diagnostics.update(implementation="existing_TorchTrainer_script", restart_scope=options["restart_scope"],
                       workload=str(script), workload_sha256=file_sha256(script),
                       numerical_probe=regression, torch_version=torch.__version__,
                       owner_placement=options.get("owner_placement", "default"),
                       workload_completed=False)
    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    originals = TorchTrainer.__init__, TorchTrainer.fit, ray.init, sys.argv[:], sys.path[:]
    timings = []
    workload_started = None

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
        if regression:
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
        diagnostics["before_trainer_fit_s"] = started - workload_started
        result = originals[1](trainer, *a, **kw)
        timings.append(time.monotonic() - started)
        return result

    try:
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=enabled,
                                        allow_head_failure=owner_failure,
                                        include_worker_failure=True) as (case_args, crash_head, _):
            diagnostics["selected_owner_node_id"] = case_args.owner_node_id
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
            if options.get("comparison") == "fixed-r":
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
                with DataContext.current(context), ownership:
                    return runpy.run_path(str(script), run_name="__main__")

            workload_started = time.monotonic()
            try:
                if owner_failure:
                    data_owner_fault(directory, enabled, crash_head,
                                     case_args.executor_node_ids, diagnostics, workload,
                                     matched_owner=matched_owner)
                else:
                    workload()
                diagnostics["workload_completed"] = True
            finally:
                diagnostics["workload_s"] = time.monotonic() - workload_started
                diagnostics["data_exchanges"] = [
                    record for path in sorted((directory / "exchanges").glob("*.json"))
                    for record in json.loads(path.read_text())
                ]
            if len(timings) != 1:
                raise ValueError("The script did not execute exactly one TorchTrainer.fit")
            diagnostics.update(validate(directory, selective, inject))
            exchanges = diagnostics["data_exchanges"]
            if regression and enabled:
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
