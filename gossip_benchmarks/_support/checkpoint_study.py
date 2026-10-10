"""CIFAR checkpoint-frequency study using ordinary Ray Data/Train recovery."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import math
from pathlib import Path
import runpy
import shutil
import sys
import time

from train_workload import checkpoint_files, file_sha256, write_record
from streaming_learning import collect_evidence, input_identity


def rank_partition(manifest, batch_size):
    """Map the prepared ordered files to two equal, stable rank inputs."""
    files = sorted(n for n in manifest["files"] if n.startswith("train/"))
    rank_files, rank_ids = [[], []], [[], []]
    offset = 0
    for index, name in enumerate(files):
        count = manifest["files"][name]["rows"]
        rank = index % 2
        rank_files[rank].append(name)
        rank_ids[rank].extend(manifest["selected_ids"]["train"][offset:offset + count])
        offset += count
    if (offset != manifest["training_rows"] or not rank_ids[0]
            or len(rank_ids[0]) != len(rank_ids[1]) or len(rank_ids[0]) % batch_size):
        raise ValueError("Use a prepared subset with equal file stripes divisible by the per-rank batch size")
    return rank_files, rank_ids


def input_suffix(files, file_rows, sample_ids, consumed_rows):
    """Select unread files and the excluded prefix inside the boundary file.

    IDs are unique but deliberately not sorted. Never seek by numeric ID.
    Parquet may still read a boundary row group; decoding sees only the suffix.
    """
    if (not 0 <= consumed_rows < len(sample_ids)
            or len(set(sample_ids)) != len(sample_ids)
            or any(file_rows[name] <= 0 for name in files)
            or sum(file_rows[name] for name in files) != len(sample_ids)):
        raise ValueError("Invalid input cursor or file manifest")
    offset = 0
    for index, name in enumerate(files):
        end = offset + file_rows[name]
        if consumed_rows < end:
            return files[index:], sample_ids[offset:consumed_rows]
        offset = end
    raise ValueError("Input cursor exhausted its epoch")


def restore_cursor(fault_epoch, fault_step, interval):
    if interval < 0 or fault_epoch < 1 or fault_step < 1:
        raise ValueError("Invalid fault/checkpoint configuration")
    if interval and fault_step % interval == 0:
        raise ValueError("Fault must occur between checkpoints, not at a checkpoint boundary")
    return fault_epoch, (fault_step // interval * interval if interval else 0)


def validate_updates(events, rank_ids, epochs, batch_size, fault_epoch, fault_step, interval, inject, input_resume="replay"):
    """Validate all executed updates, including exactly the allowed rollback."""
    if input_resume not in ("replay", "direct"):
        raise ValueError("Unknown input resume policy")
    steps = len(rank_ids[0]) // batch_size
    cursor_epoch, cursor_step = restore_cursor(fault_epoch, fault_step, interval)
    expected_keys = [(e, s) for e in range(epochs) for s in range(1, steps + 1)]
    for rank in (0, 1):
        starts = sorted((e for e in events if e["kind"] == "study_start" and e["rank"] == rank),
                        key=lambda e: e["time_ns"])
        if len(starts) != (2 if inject else 1) or (starts[0]["epoch"], starts[0]["step"]) != (0, 0):
            raise ValueError("Unexpected train invocations or initial cursor")
        if inject and (starts[1]["epoch"], starts[1]["step"]) != (cursor_epoch, cursor_step):
            raise ValueError("Recovery restored the wrong checkpoint cursor")
        for attempt, start in enumerate(starts):
            updates = sorted((e for e in events if e["kind"] == "study_update"
                              and e["rank"] == rank and e["invocation"] == start["invocation"]),
                             key=lambda e: e["time_ns"])
            wanted = expected_keys
            if inject:
                wanted = ([k for k in expected_keys if k <= (fault_epoch, fault_step)] if attempt == 0
                          else [k for k in expected_keys if k > (cursor_epoch, cursor_step)])
            if [(e["epoch"], e["step"]) for e in updates] != wanted:
                raise ValueError("Missing, extra or reordered optimizer updates")
            for event in updates:
                offset = (event["step"] - 1) * batch_size
                if event["sample_ids"] != rank_ids[rank][offset:offset + batch_size]:
                    raise ValueError("Optimizer update consumed the wrong samples")
        skipped = [e for e in events if e["kind"] == "study_input" and e["rank"] == rank and e["skipped"]]
        if len(skipped) != (cursor_step if inject and input_resume == "replay" else 0):
            raise ValueError("Wrong number of reconstructed/skipped batches")
        for index, event in enumerate(sorted(skipped, key=lambda e: e["time_ns"]), 1):
            if (event["epoch"] != cursor_epoch or event["step"] != index
                    or event["sample_ids"] != rank_ids[rank][(index-1)*batch_size:index*batch_size]
                    or event["invocation"] != starts[-1]["invocation"]):
                raise ValueError("Skipped prefix does not match the restored cursor")
    return {"repeated_updates_per_rank": fault_step - cursor_step if inject else 0,
            "verified_skipped_batches_per_rank": cursor_step if inject and input_resume == "replay" else 0,
            "bypassed_batches_per_rank": cursor_step if inject and input_resume == "direct" else 0,
            "restored_epoch": cursor_epoch if inject else None,
            "restored_step": cursor_step if inject else None}


def validate_input_decodes(events, rank_ids, epochs, batch_size, policy, inject):
    """Prove suffix coverage and no prefix decoding on replacement invocations."""
    starts = [e for e in events if e["kind"] == "study_start"]
    known = {e["invocation"]: e for e in starts}
    decode_events = [e for e in events if e["kind"] == "decode" and e["split"] == "train"]
    for event in decode_events:
        start = known.get(event.get("invocation"))
        if start is None or event.get("rank") != start["rank"]:
            raise ValueError("Decode evidence lacks a valid input owner")
        rank = start["rank"]
        if not start["epoch"] <= event["epoch"] < epochs:
            raise ValueError("Decode evidence has an invalid epoch")
        consumed = (start["step"] * batch_size
                    if policy == "direct" and event["epoch"] == start["epoch"] else 0)
        if not set(event["sample_ids"]) <= set(rank_ids[rank][consumed:]):
            raise ValueError("Direct resume decoded a bypassed prefix or wrong rank input")
    for start in starts:
        if start["epoch"] == 0 and inject:
            continue  # Killed in-flight decoding may have no completion event.
        for epoch in range(start["epoch"], epochs):
            consumed = (start["step"] * batch_size
                        if policy == "direct" and epoch == start["epoch"] else 0)
            actual = {i for e in decode_events if e["invocation"] == start["invocation"]
                      and e["epoch"] == epoch for i in e["sample_ids"]}
            if actual != set(rank_ids[start["rank"]][consumed:]):
                raise ValueError("Missing completed decode evidence for resumed input")
    return {str(start["rank"]): sum(len(e["sample_ids"]) for e in decode_events
                if e["invocation"] == start["invocation"])
            for start in starts if start["epoch"] > 0}


def state_digest(value):
    """Digest loaded state, independent of torch.save archive metadata."""
    import torch
    digest = hashlib.sha256()
    def visit(item):
        if isinstance(item, torch.Tensor):
            tensor = item.detach().cpu().contiguous()
            digest.update(str((str(tensor.dtype), tuple(tensor.shape))).encode())
            digest.update(tensor.numpy().tobytes())
        elif isinstance(item, dict):
            for key in sorted(item, key=lambda k: (type(k).__name__, str(k))):
                visit(key)
                visit(item[key])
        elif isinstance(item, (list, tuple)):
            digest.update(str((type(item).__name__, len(item))).encode())
            for entry in item:
                visit(entry)
        else:
            digest.update(repr((type(item).__name__, item)).encode())
    visit(value)
    return digest.hexdigest()


def run_case(options, directory, diagnostics):
    import argparse
    import ray
    import ray.cloudpickle
    import torch
    import torchvision
    from ray.data import DataContext
    from ray.data._internal.execution.streaming_recovery import clear_config
    from ray.experimental.recovery import system_config
    from ray.experimental.recovery._local import local_head_failure_cluster
    from ray.train import CheckpointConfig, FailureConfig, RunConfig
    from ray.train.torch import TorchConfig, TorchTrainer
    from ray.train.v2._internal.execution.callback import ReportCallback, WorkerGroupCallback

    identity = input_identity(options["data_directory"])
    rank_files, rank_ids = rank_partition(identity, options["batch_size"])
    interval = options["checkpoint_every_steps"]
    fault_epoch, fault_step = options["fault_epoch"], options["fault_after_step"]
    expected_cursor = restore_cursor(fault_epoch, fault_step, interval)
    inject = options["scenario"] == "worker-node"
    script = Path(options["workload"])
    gate_dir = directory / "fault-gates"
    gate_dir.mkdir()
    config = {"epochs": options["training_epochs"], "batch_size": options["batch_size"],
              "steps_per_epoch": len(rank_ids[0]) // options["batch_size"],
              "input_identity": hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
              "input_hashes": identity["input_sha256"]["train"], "rank_files": rank_files,
              "rank_sample_ids": rank_ids, "checkpoint_every_steps": interval,
              "fault_epoch": fault_epoch, "fault_step": fault_step,
              "gate_directory": str(gate_dir), "inject": inject,
              "input_resume": options.get("input_resume", "replay"),
              "input_study": options.get("input_study", False),
              "data_directory": options["data_directory"],
              "file_rows": {name: entry["rows"] for name, entry in identity["files"].items()}}
    write_record(directory / "study-config.json", config)
    diagnostics.update(input_identity=identity, workload_sha256=file_sha256(script),
                       torch_version=torch.__version__, torchvision_version=torchvision.__version__,
                       fixed_r_enabled=False, selective_retry=False, restart_scope="full",
                       checkpoint_every_steps=interval, training_epochs=config["epochs"],
                       input_resume=config["input_resume"], input_study=config["input_study"],
                       fault_epoch=fault_epoch, fault_after_step=fault_step,
                       batch_size=config["batch_size"], steps_per_epoch=config["steps_per_epoch"],
                       sharding="ordinary Ray Data iterators over equal deterministic file stripes",
                       workload_completed=False)

    class Observer(WorkerGroupCallback, ReportCallback):
        def __init__(self):
            self.groups, self.reports = [], []
        def save(self):
            write_record(directory / "timeline.json", {"groups": self.groups, "reports": self.reports})
        def after_worker_group_start(self, group):
            self.groups.append([{"rank": w.distributed_context.world_rank,
                                 "actor_id": w.actor._actor_id.hex(), "pid": w.metadata.pid,
                                 "node_id": w.metadata.node_id} for w in group.get_workers()])
            self.save()
        def after_report(self, training_report, metrics):
            cursor = (metrics[0]["cursor_epoch"], metrics[0]["cursor_step"])
            if (len(metrics) != 2 or sorted(m["rank"] for m in metrics) != [0, 1]
                    or any((m["cursor_epoch"], m["cursor_step"]) != cursor for m in metrics)
                    or training_report.checkpoint is None):
                raise ValueError("Ranks did not commit the same checkpoint cursor")
            record = {"metrics": metrics, "cursor": list(cursor), "time_ns": time.monotonic_ns()}
            if cursor == expected_cursor:
                record["checkpoint"] = checkpoint_files(training_report.checkpoint)
            if cursor == (config["epochs"], 0):
                with training_report.checkpoint.as_directory() as source:
                    shutil.copytree(source, directory / "final-checkpoint", dirs_exist_ok=True)
            self.reports.append(record)
            self.save()
            write_record(directory / f"committed-{cursor[0]}-{cursor[1]}.json", record)

    originals = TorchTrainer.__init__, ray.init, sys.argv[:], sys.path[:]
    previous_context = DataContext.get_current()
    def configure(trainer, train_loop_per_worker, **kwargs):
        kwargs["torch_config"] = TorchConfig(backend="gloo", timeout_s=60, selective_recovery=False)
        kwargs["run_config"] = RunConfig(name="checkpoint-study", storage_path=str(directory / "storage"),
                                       failure_config=FailureConfig(max_failures=1),
                                       checkpoint_config=CheckpointConfig(num_to_keep=2), callbacks=[Observer()])
        kwargs["scaling_config"] = replace(kwargs["scaling_config"], placement_strategy="STRICT_SPREAD")
        originals[0](trainer, train_loop_per_worker, **kwargs)

    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    try:
        import train_workload
        ray.cloudpickle.register_pickle_by_value(train_workload)
        ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
        with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=False,
                                        allow_head_failure=False, include_worker_failure=True) as (case, _, crash):
            original_nodes = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
            diagnostics["executor_node_ids"] = sorted(case.executor_node_ids)
            driver_node = ray.get_runtime_context().get_node_id()
            job = ray.get_runtime_context().get_job_id()
            native = ray._private.state.state.get_system_config()
            diagnostics["native_settings"] = {k: native.get(k) for k in system_config()}
            if any(native.get(k) != (False if k.startswith("enable_") else v) for k, v in system_config().items()):
                raise ValueError("Checkpoint study requires Fixed-R OFF")
            context = previous_context.copy()
            clear_config(context)
            context.enable_fixed_r_task_recovery = False
            context.set_config("experimental_resumable_split", None)
            context.enable_progress_bars = False
            context.custom_execution_callback_classes = []
            TorchTrainer.__init__ = configure
            ray.init = lambda *a, **kw: originals[1](ignore_reinit_error=True)
            sys.path.insert(0, str(script.parent))
            sys.argv = [str(script), "--data-directory", options["data_directory"],
                        "--epochs", str(config["epochs"]), "--batch-size", str(config["batch_size"]),
                        "--telemetry-directory", str(directory / "stream-events"),
                        "--checkpoint-study-config", str(directory / "study-config.json")]
            def workload():
                with DataContext.current(context):
                    return runpy.run_path(str(script), run_name="__main__")
            fault = None
            diagnostics["workload_started_ns"] = time.monotonic_ns()
            write_record(directory / "progress.json", {k: diagnostics[k] for k in ("workload_started_ns", "workload_completed")})
            try:
                with ThreadPoolExecutor(1) as pool:
                    pending = pool.submit(workload)
                    if inject:
                        fault = {"completed": False, "scenario": "worker-node"}
                        try:
                            committed_path = directory / f"committed-{expected_cursor[0]}-{expected_cursor[1]}.json"
                            deadline = time.monotonic() + options["timeout_s"]
                            while True:
                                paths = list(gate_dir.glob("*.json"))
                                if len(paths) == 2 and committed_path.exists():
                                    break
                                if pending.done():
                                    pending.result()
                                    raise ValueError("Workload ended before fault gates")
                                if time.monotonic() >= deadline:
                                    raise TimeoutError("Checkpoint/fault gates were not reached")
                                time.sleep(.02)
                            gates = sorted((json.loads(p.read_text()) for p in paths), key=lambda e: e["rank"])
                            timeline = json.loads((directory / "timeline.json").read_text())
                            if len(timeline["groups"]) != 1:
                                raise ValueError("Unexpected recovery before fault")
                            group = sorted(timeline["groups"][0], key=lambda w: w["rank"])
                            if (len(group) != 2 or [w["rank"] for w in group] != [0, 1]
                                    or len({w["node_id"] for w in group}) != 2
                                    or any(w["node_id"] not in case.executor_node_ids for w in group)):
                                raise ValueError("Fault requires separate training executor nodes")
                            for worker, gate in zip(group, gates):
                                if (any(worker[k] != gate[k] for k in worker)
                                        or (gate["epoch"], gate["step"]) != (fault_epoch, fault_step)):
                                    raise ValueError("Fault gates do not match training workers/progress")
                            committed = json.loads(committed_path.read_text())
                            if committed["time_ns"] >= min(g["time_ns"] for g in gates):
                                raise ValueError("Selected checkpoint was not committed before uncheckpointed work")
                            fault.update(request_ns=time.monotonic_ns(), gates=gates,
                                         checkpoint=committed, original_groups=timeline["groups"])
                            fault["worker_node_failure"] = crash(group[0]["node_id"], group[0]["pid"])
                            fault.update(completed=True, finished_ns=time.monotonic_ns())
                        finally:
                            write_record(directory / "node-fault.json", {"node_fault": fault})
                            (gate_dir / "release").touch()
                    pending.result()
                diagnostics["workload_completed"] = True
            finally:
                diagnostics["workload_finished_ns"] = time.monotonic_ns()
                diagnostics["workload_s"] = (diagnostics["workload_finished_ns"] - diagnostics["workload_started_ns"]) / 1e9
                write_record(directory / "progress.json", {k: diagnostics[k] for k in (
                    "workload_started_ns", "workload_finished_ns", "workload_s", "workload_completed")})
                diagnostics.update(collect_evidence(directory))
                if (directory / "timeline.json").exists():
                    diagnostics.update(json.loads((directory / "timeline.json").read_text()))
                diagnostics["node_fault"] = fault
            dead = {fault["worker_node_failure"]["node_id"]} if fault else set()
            alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
            if (alive != original_nodes - dead or ray.get_runtime_context().get_node_id() != driver_node
                    or ray.get_runtime_context().get_job_id() != job):
                raise ValueError("Unexpected node or driver/job loss")
            diagnostics["completion_topology"] = {"original_node_ids": sorted(original_nodes),
                                                   "surviving_node_ids": sorted(alive)}
            validate_case(directory, options, diagnostics, rank_ids)
            return {"validation_status": "passed"}
    finally:
        TorchTrainer.__init__, ray.init, sys.argv, sys.path = originals
        DataContext._set_current(previous_context)
        ray.cloudpickle.unregister_pickle_by_value(sys.modules[__name__])
        ray.cloudpickle.unregister_pickle_by_value(train_workload)


def validate_case(directory, options, diagnostics, rank_ids):
    import torch
    inject = options["scenario"] != "none"
    groups, reports, events = diagnostics["groups"], diagnostics["reports"], diagnostics["stream_events"]
    if len(groups) != (2 if inject else 1):
        raise ValueError("Unexpected worker-group restart count")
    for group in groups:
        if (len(group) != 2 or len({w["node_id"] for w in group}) != 2
                or sorted(w["rank"] for w in group) != [0, 1]
                or any(w["node_id"] not in diagnostics["executor_node_ids"] for w in group)):
            raise ValueError("Workers must occupy distinct nodes")
    recovery = validate_updates(events, rank_ids, options["training_epochs"], options["batch_size"],
                                options["fault_epoch"], options["fault_after_step"],
                                options["checkpoint_every_steps"], inject, options.get("input_resume", "replay"))
    if inject:
        fault = diagnostics["node_fault"]
        loss = fault["worker_node_failure"]
        if (not fault["completed"] or not loss["all_node_processes_exited"] or not loss["gcs_marked_dead"]
                or loss["failure_scope"] != "logical_worker_node_processes_with_surviving_shared_storage"
                or loss["training_worker_pid"] not in loss["node_process_pids"]
                or loss["node_id"] != next(w["node_id"] for w in groups[0] if w["rank"] == 0)
                or any(w["node_id"] not in loss["surviving_node_ids"] for w in groups[1])
                or {w["actor_id"] for w in groups[0]} & {w["actor_id"] for w in groups[1]}):
            raise ValueError("Whole-node death/full-group replacement was not verified")
        starts = [e for e in events if e["kind"] == "study_start" and e["epoch"] > 0]
        if len(starts) != 2:
            raise ValueError("Missing restored worker identities")
        for start in starts:
            worker = next(w for w in groups[1] if w["rank"] == start["rank"])
            if (any(start[k] != worker[k] for k in worker)
                    or start["checkpoint_sha256"] != fault["checkpoint"]["checkpoint"][f"rank-{start['rank']}.pt"]
                    or start["time_ns"] <= fault["request_ns"]):
                raise ValueError("Restored state did not match the committed checkpoint/worker")
        recovery["failure_to_restored_state_ready_s"] = (max(e["time_ns"] for e in starts) - fault["request_ns"])/1e9
        first_updates = [min(e["time_ns"] for e in events if e["kind"] == "study_update"
                            and e["invocation"] == start["invocation"]) for start in starts]
        recovery["failure_to_new_optimizer_update_s"] = (max(first_updates)-fault["request_ns"])/1e9
    interval = options["checkpoint_every_steps"]
    expected_cursors = []
    for epoch in range(options["training_epochs"]):
        if interval:
            expected_cursors.extend([epoch, step] for step in range(interval, diagnostics["steps_per_epoch"], interval))
        expected_cursors.append([epoch + 1, 0])
    if [r["cursor"] for r in reports] != expected_cursors:
        raise ValueError("Checkpoint cadence differs from the requested policy")
    complete = [r for r in reports if r["metrics"][0]["epoch_complete"]]
    if [r["cursor"] for r in complete] != [[e, 0] for e in range(1, options["training_epochs"]+1)]:
        raise ValueError("Missing or repeated committed epochs")
    for report in complete:
        for metric in report["metrics"]:
            ids = rank_ids[metric["rank"]]
            if (metric["sample_ids"] != ids or metric["input_sha256"] != [
                    diagnostics["input_identity"]["input_sha256"]["train"][str(i)] for i in ids]
                    or not math.isfinite(metric["training_loss"])):
                raise ValueError("Committed epoch has incorrect input coverage")
    final_metrics = next(m for m in complete[-1]["metrics"] if m["rank"] == 0)
    if (final_metrics["validation_rows"] != diagnostics["input_identity"]["validation_rows"]
            or not math.isfinite(final_metrics["accuracy"]) or not 0 <= final_metrics["accuracy"] <= 1):
        raise ValueError("Final validation metrics are invalid")
    digests = {}
    for rank in (0, 1):
        state = torch.load(directory / "final-checkpoint" / f"rank-{rank}.pt", map_location="cpu", weights_only=True)
        if (state["epoch"] != options["training_epochs"] or state["step"] != 0
                or not state["optimizer"]["state"]):
            raise ValueError("Final checkpoint lacks completed model/optimizer state")
        digests[str(rank)] = state_digest(state)
    decoded = Counter(i for e in events if e["kind"] == "decode" and e["split"] == "train" for i in e["sample_ids"])
    expected = set(diagnostics["input_identity"]["selected_ids"]["train"])
    if set(decoded) != expected or any(decoded[i] < options["training_epochs"] for i in expected):
        raise ValueError("Missing decoded input evidence")
    if options.get("input_study"):
        diagnostics["resumed_decoded_rows_per_rank"] = validate_input_decodes(
            events, rank_ids, options["training_epochs"], options["batch_size"],
            options["input_resume"], inject)
    checkpoints = [e for e in events if e["kind"] == "study_checkpoint"]
    diagnostics.update(recovery=recovery, final_state_sha256=digests,
                       final_accuracy=next(m["accuracy"] for m in complete[-1]["metrics"] if m["rank"] == 0),
                       decoded_training_rows=sum(decoded.values()),
                       extra_decoded_rows_vs_one_pass_per_epoch=sum(decoded.values())-len(expected)*options["training_epochs"],
                       checkpoint_metrics={"rank_reports": len(checkpoints),
                           "serialized_bytes": sum(e["bytes"] for e in checkpoints),
                           "serialization_rank_seconds": sum(e["serialization_s"] for e in checkpoints),
                           "report_call_rank_seconds": sum(e["report_call_s"] for e in checkpoints)})


def compare(left, right, *, control=False, input_study=False):
    from fashion_comparison import PROVENANCE_KEYS
    for sample in (left, right):
        if (sample["status"] != "passed" or not sample.get("workload_completed") or sample.get("timeout")
                or sample["mode"] != "off" or sample["restart_scope"] != "full"
                or not sample.get("native_settings") or sample.get("fixed_r_enabled") is not False
                or sample.get("selective_retry") is not False
                or any(k.startswith("enable_") and v is not False for k, v in sample.get("native_settings", {}).items())
                or not math.isfinite(sample["workload_s"]) or sample["workload_s"] <= 0):
            raise ValueError("Both arms require completed ordinary recovery with Fixed-R OFF")
    for key in ("input_identity", "workload_sha256", "torch_version", "torchvision_version", "training_epochs",
                "batch_size", "steps_per_epoch", "fault_epoch", "fault_after_step",
                "sharding", "native_settings", "final_state_sha256"):
        if left[key] != right[key]:
            raise ValueError(f"Checkpoint comparison differs in {key}")
    if not left["final_state_sha256"]:
        raise ValueError("Missing final state identity")
    for key in PROVENANCE_KEYS:
        if left["provenance"][key] != right["provenance"][key]:
            raise ValueError(f"Source/environment differs in {key}")
    if left.get("input_study", False) != right.get("input_study", False):
        raise ValueError("Different input construction modes")
    if control:
        if left.get("input_resume", "replay") != right.get("input_resume", "replay"):
            raise ValueError("Wrong input-policy control")
        if right["scenario"] != "none" or left["checkpoint_every_steps"] != right["checkpoint_every_steps"]:
            raise ValueError("Wrong checkpoint-policy control")
    elif input_study:
        if (not left.get("input_study") or left.get("input_resume") != "replay"
                or right.get("input_resume") != "direct"
                or left["checkpoint_every_steps"] <= 0
                or left["checkpoint_every_steps"] != right["checkpoint_every_steps"]
                or left["scenario"] != right["scenario"]):
            raise ValueError("Expected replay versus direct input at the same checkpoint cadence")
    elif (left["checkpoint_every_steps"] != 0 or right["checkpoint_every_steps"] <= 0
          or left["scenario"] != right["scenario"]):
        raise ValueError("Expected epoch-only versus mid-epoch checkpoints in the same scenario")
    return {"left_workload_s": left["workload_s"], "right_workload_s": right["workload_s"],
            "workload_s_change_pct": 100 * (right["workload_s"] / left["workload_s"] - 1),
            "final_model_optimizer_rng_match": True}


def run_comparison(args):
    from run_fixed_r_train_comparison import ROOT, run_observation, write_json
    from training_provenance import source_provenance
    if not (args.data_directory / "manifest.json").is_file():
        raise ValueError("CIFAR preparation manifest missing. Run python gossip_benchmarks/workloads/cifar_streaming.py "
                         "--prepare-data --data-directory DIR --train-rows 2048 --validation-rows 512, "
                         "or pass your existing prepared directory.")
    input_study = args.comparison == "input-resume"
    policies = ("replay", "direct") if input_study else ("epoch", "mid_epoch")
    identity = input_identity(args.data_directory)
    _, rank_ids = rank_partition(identity, args.batch_size)
    steps = len(rank_ids[0]) // args.batch_size
    interval = args.checkpoint_every_steps
    step = args.fault_after_step if args.fault_after_step is not None else steps // 2 + 2
    if not 0 < interval < step < steps:
        raise ValueError("Require 0 < checkpoint interval < fault step < steps per epoch")
    restore_cursor(1, step, interval)
    provenance = source_provenance(ROOT)
    cases = ["none"] if args.controls_only else ["none", "worker-node"]
    report = {"profile": "cifar-input-resume" if input_study else "cifar-checkpoint-frequency", "status": "running", "samples": [], "pairs": [],
              "failed_observations": [], "training_epochs": args.epochs, "steps_per_epoch": steps,
              "checkpoint_every_steps": interval, "fault_epoch": 1, "fault_after_step": step,
              "repeats": args.repeats, "preliminary": args.repeats == 1, "source_provenance": provenance,
              "comparison_axis": ("ordinary full-group retry: prefix replay versus direct input resume" if input_study
                                  else "ordinary full-group retry: epoch versus mid-epoch application checkpoints"),
              "limitations": [
                  "Fixed-R OFF, selective retry OFF, coordinator resume OFF in both arms",
                  "deterministic disjoint file stripes through ordinary Ray Data in both arms; not default streaming_split assignment",
                  "whole logical node process loss on one machine, at a synchronized optimizer boundary in epoch 2",
                  "shared data/checkpoints, head, driver and spare executor capacity survive; no cloud provisioning delay",
                  ("input study uses worker-owned per-epoch Ray Data pipelines in both arms; direct resume bypasses files and filters the boundary prefix before PNG decode" if input_study
                   else "prefix reconstruction verifies and skips already consumed batches; it still reads/decodes them"),
                  "Parquet boundary row groups may still be read; avoided decodes are not avoided physical bytes",
                  "completed decode telemetry only; killed in-flight decoding is uncounted; storage reads/bytes not directly measured",
                  "rank-seconds for checkpoint calls include synchronization and upload; sums overlap across ranks",
                  "correctness telemetry and selected checkpoint hashing are included in workload timings",
                  "small CPU ResNet-18 experiment; no speedup, novelty, convergence or production claim before evidence",
              ]}
    def save():
        write_json(args.output, report)
        write_json(args.result_directory / "comparison.json", report)
    save()
    controls, valid_controls = {}, set()
    for scenario in cases:
        for pair in range(1, args.repeats + 1):
            samples = {}
            for policy in (policies if pair % 2 else policies[::-1]):
                frequency = 0 if policy == "epoch" else interval
                options = {"training_strategy": "cifar-checkpoint-study", "streaming_learning": True,
                           "scenario": scenario, "mode": "off", "restart_scope": "full",
                           "checkpoint_every_steps": frequency, "fault_epoch": 1,
                           "input_study": input_study, "input_resume": policy if input_study else "replay",
                           "fault_after_step": step, "training_epochs": args.epochs,
                           "batch_size": args.batch_size, "timeout_s": args.timeout_s,
                           "data_directory": str(args.data_directory),
                           "workload": str(ROOT / "gossip_benchmarks/workloads/cifar_streaming.py")}
                print(f"{scenario}: {policy}, pair {pair}/{args.repeats}; Fixed-R OFF/full retry", flush=True)
                sample = run_observation(options, pair, args.result_directory / f"{scenario}-{pair}-{policy}", provenance)
                sample.update(policy=policy, checkpoint_every_steps=frequency)
                if scenario == "none":
                    controls[pair, policy] = sample
                elif sample["status"] == "passed":
                    try:
                        if pair not in valid_controls:
                            raise ValueError("No passing paired controls")
                        control = controls[pair, policy]
                        compare(sample, control, control=True)
                        sample["matches_control_state"] = True
                        sample["extra_workload_s_vs_control"] = sample["workload_s"] - control["workload_s"]
                        sample["extra_decoded_rows_vs_control"] = sample["decoded_training_rows"] - control["decoded_training_rows"]
                    except (ValueError, KeyError) as exc:
                        sample.update(status="failed", error=f"Control comparison: {exc}")
                samples[policy] = sample
                report["samples"].append(sample)
                write_json(Path(sample["directory"]) / "sample.json", sample)
                if sample["status"] != "passed":
                    report["failed_observations"].append({"scenario": scenario, "policy": policy,
                                                          "pair": pair, "error": sample.get("error")})
                print(f"  {sample['status']}: {sample.get('error', str(sample.get('workload_s'))+'s')}", flush=True)
                save()
            try:
                values = compare(samples[policies[0]], samples[policies[1]], input_study=input_study)
                report["pairs"].append({"scenario": scenario, "pair": pair, **values})
                if scenario == "none":
                    valid_controls.add(pair)
            except (ValueError, KeyError) as exc:
                report["failed_observations"].append({"scenario": scenario, "pair": pair, "error": str(exc)})
            save()
        if scenario == "none" and len(valid_controls) != args.repeats:
            report["skipped_cases"] = cases[1:]
            break
    report["status"] = "failed" if report["failed_observations"] else "passed"
    save()
    print(f"Report: {args.output}", flush=True)
    return int(report["status"] != "passed")
