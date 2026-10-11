"""Matched CPU MLP comparison: ordinary Ray DDP retry versus TorchFT peer retry.

Synthetic classification isolates training-system cost, not predictive quality.
Both modes write the same scheduled model/Adam/step checkpoints. Input for the
ordinary mode is directly indexed by restored step (no prefix replay penalty).
"""
import concurrent.futures
import hashlib
import importlib.metadata
import json
from pathlib import Path
import tempfile
import time


def run(args, write_json):
    import numpy as np
    import ray
    import ray.data
    import ray.train as train
    import torch
    from torch import nn
    from datetime import timedelta
    from ray.cluster_utils import Cluster
    from ray.train import Checkpoint, RunConfig, ScalingConfig, UserCallback
    from ray.train.torch import TorchConfig, TorchTrainer
    from ray.train.v2.api.config import FailureConfig
    from ray.train.v2._internal.data_integration.step_aligned_input import StepAlignedDataConfig
    from ray.train.v2.torch.torchft_config import TorchftConfig
    from ray.train.v2.examples.pytorch.torchft_linear_example import _FailureAwareDistributedDataParallel
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    aligned = args.mode == "aligned"
    folder = Path(args.sample).parent

    @ray.remote(num_cpus=0)
    class Recorder:
        def __init__(self):
            self.events = []
            self.node = None
            self.finals = {}
            self.checkpoints = {}

        def add(self, event):
            self.events.append(dict(event, time=time.monotonic()))

        def acknowledge(self, step, path):
            self.checkpoints[step] = path
            self.events.append({"kind": "checkpoint_ack", "step": step,
                                "path": path, "time": time.monotonic()})

        def acknowledged(self, step):
            return step in self.checkpoints

        def claim(self, node):
            if self.node is not None:
                return False
            self.node = node
            return True

        def snapshot(self):
            return self.node, self.events, self.finals

        def finish(self, rank, result):
            self.finals[rank] = result

    def batch_values(ids):
        # Stateless deterministic source: cheap random access for ordinary Ray.
        # Both modes receive the exact same features and labels for each step.
        ids = np.asarray(ids, dtype=np.int64)
        features = np.sin(ids[:, None] * .013 + np.arange(256)[None, :] * .071).astype(np.float32)
        labels = ((ids // 7 + ids % 3) % 10).astype(np.int64)
        return {"x": features, "y": labels, "id": ids}

    cluster = Cluster()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        cluster.add_node(num_cpus=2, include_dashboard=False)
        nodes = [cluster.add_node(num_cpus=1, resources={"train_slot": 1}) for _ in range(3)]
        ray.init(address=cluster.address)
        recorder = Recorder.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False)).remote()

        class CheckpointAcknowledgement(UserCallback):
            def after_report(self, run_context, metrics, checkpoint):
                if checkpoint is None:
                    return
                steps = {m["step"] for m in metrics}
                if len(steps) != 1:
                    raise RuntimeError(f"Checkpoint report steps disagree: {steps}")
                step = steps.pop()
                with checkpoint.as_directory() as directory:
                    state = torch.load(Path(directory) / "state.pt",
                                       map_location="cpu", weights_only=True)
                if state["step"] != step:
                    raise RuntimeError(f"Persisted checkpoint is not step {step}")
                # CheckpointManager runs before this callback. A worker-side
                # report return alone does not prove controller registration.
                ray.get(recorder.acknowledge.remote(step, str(checkpoint.path)))

        def training(config):
            rank = train.get_context().get_world_rank()
            # Deliberate simulated initialization, identical per invocation in
            # both modes. Concurrent worker startup delays are not summed.
            ray.get(recorder.add.remote({"kind": "start", "rank": rank}))
            time.sleep(args.startup_delay_s)
            torch.set_num_threads(1)
            torch.manual_seed(100 + args.repeat)
            torch.use_deterministic_algorithms(True)
            base = nn.Sequential(nn.Linear(256, args.model_width), nn.ReLU(),
                nn.Linear(args.model_width, args.model_width), nn.ReLU(),
                nn.Linear(args.model_width, args.model_width), nn.ReLU(),
                nn.Linear(args.model_width, 10))
            raw_optimizer = torch.optim.Adam(base.parameters(), lr=1e-3)
            checkpoint = train.get_checkpoint()
            restored_step = 0
            if checkpoint:
                with checkpoint.as_directory() as path:
                    state = torch.load(Path(path) / "state.pt", map_location="cpu", weights_only=True)
                    base.load_state_dict(state["model"])
                    raw_optimizer.load_state_dict(state["optimizer"])
                    restored_step = state["step"]
            ray.get(recorder.add.remote({"kind": "initialized", "rank": rank, "restored_step": restored_step}))
            manager = None
            if aligned:
                from torchft import Manager, Optimizer, ProcessGroupGloo
                from torchft.checkpointing.pg_transport import PGTransport
                pg = ProcessGroupGloo(timeout=timedelta(seconds=30))

                def state_dict():
                    return {"model": base.state_dict(), "optim": raw_optimizer.state_dict()}

                def load_state_dict(state):
                    base.load_state_dict(state["model"])
                    raw_optimizer.load_state_dict(state["optim"])

                manager = Manager(pg=pg, min_replica_size=2, world_size=1, rank=0,
                    replica_id=f"compare_{rank}", use_async_quorum=False,
                    timeout=timedelta(seconds=60), state_dict=state_dict, load_state_dict=load_state_dict,
                    checkpoint_transport=PGTransport(pg, timeout=timedelta(seconds=30), device=torch.device("cpu")))
                model = _FailureAwareDistributedDataParallel(manager, base)
                optimizer = Optimizer(manager, raw_optimizer)
                shard = train.get_dataset_shard("train")
            else:
                model = train.torch.prepare_model(base)
                optimizer = raw_optimizer
            step = restored_step
            checkpointed_step = restored_step if checkpoint else -1

            def save(step):
                started = time.monotonic()
                with tempfile.TemporaryDirectory() as temporary:
                    cp = None
                    byte_count = 0
                    if rank == 0:
                        path = Path(temporary) / "state.pt"
                        torch.save({"model": base.state_dict(), "optimizer": raw_optimizer.state_dict(),
                                    "step": step}, path)
                        byte_count = path.stat().st_size
                        cp = Checkpoint.from_directory(temporary)
                    train.report({"step": step}, checkpoint=cp)
                    deadline = time.monotonic() + 60
                    while not ray.get(recorder.acknowledged.remote(step)):
                        if time.monotonic() >= deadline:
                            raise TimeoutError(f"Controller did not acknowledge checkpoint {step}")
                        time.sleep(.01)
                    ray.get(recorder.add.remote({"kind": "checkpoint", "rank": rank,
                        "step": step, "seconds": time.monotonic() - started, "bytes": byte_count}))

            try:
                while True:
                    optimizer.zero_grad()
                    if aligned:
                        manager.wait_quorum()
                        step = manager.current_step()
                        if manager.num_participants() != 2:
                            raise RuntimeError("Full membership required")
                    if step >= args.steps:
                        break
                    # Scheduled durable checkpoints in BOTH arms. On replacement
                    # the healed peer step skips already-completed checkpoints.
                    if step % args.checkpoint_every == 0 and step > checkpointed_step:
                        save(step)
                        checkpointed_step = step
                    ids = list(range((step * 2 + rank) * args.batch_size,
                                     (step * 2 + rank + 1) * args.batch_size))
                    batch = shard.batch_for_step(step) if aligned else batch_values(ids)
                    if batch["id"].tolist() != ids:
                        raise RuntimeError("Input does not match the committed model step")
                    if args.fault and rank == args.failed_rank and step == args.fault_step:
                        if ray.get(recorder.claim.remote(ray.get_runtime_context().get_node_id())):
                            while True:
                                time.sleep(.1)
                    compute_started = time.monotonic()
                    loss = nn.functional.cross_entropy(model(torch.tensor(batch["x"])), torch.tensor(batch["y"]))
                    loss.backward()
                    optimizer.step()
                    next_step = manager.current_step() if aligned else step + 1
                    # An aborted update is not a committed step and is not counted
                    # as successful recomputation. Its elapsed work remains timed.
                    if next_step > step:
                        ray.get(recorder.add.remote({"kind": "update", "rank": rank, "step": next_step,
                            "compute_s": time.monotonic() - compute_started}))
                    step = next_step
                flat = np.concatenate([p.detach().cpu().numpy().reshape(-1) for p in base.parameters()])
                model_path = folder / f"model-rank-{rank}.npy"
                np.save(model_path, flat)
                ray.get(recorder.finish.remote(rank, {"step": step, "model_path": str(model_path)}))
                train.report({"step": step, "final": True})
                ray.get(recorder.add.remote({"kind": "finish", "rank": rank}))
            finally:
                if manager is not None:
                    manager.shutdown()

        dataset_args = {}
        if aligned:
            ds = ray.data.range(args.steps * 2 * args.batch_size).map_batches(
                lambda b: batch_values(b["id"]), batch_format="numpy", batch_size=2 * args.batch_size)
            dataset_args = {"datasets": {"train": ds}, "dataset_config": StepAlignedDataConfig(
                args.batch_size, synchronous_full_membership=True)}
        trainer = TorchTrainer(training, scaling_config=ScalingConfig(num_workers=2,
            placement_strategy="STRICT_SPREAD", resources_per_worker={"CPU": 1, "train_slot": 1}),
            torch_config=(TorchftConfig(backend="gloo", lighthouse_kwargs={"min_replicas": 2}) if aligned
                          else TorchConfig(backend="gloo", timeout_s=60)),
            run_config=RunConfig(storage_path=str(folder / "storage"),
                failure_config=FailureConfig(max_failures=1),
                callbacks=[CheckpointAcknowledgement()]), **dataset_args)
        started = time.monotonic()
        future = pool.submit(trainer.fit)
        fault_time = None
        if args.fault:
            while not future.done():
                node, _, _ = ray.get(recorder.snapshot.remote())
                if node is not None:
                    fault_time = time.monotonic()
                    cluster.remove_node(next(n for n in nodes if n.node_id == node), allow_graceful=False)
                    break
                if time.monotonic() - started > args.timeout_s - 30:
                    raise TimeoutError("Fault gate was not reached")
                time.sleep(.1)
            if fault_time is None:
                future.result()
                raise RuntimeError("Requested fault was not injected")
        result = future.result(timeout=args.timeout_s)
        ended = time.monotonic()
        _, events, finals = ray.get(recorder.snapshot.remote())
        if result.error or len(finals) != 2 or any(v["step"] != args.steps for v in finals.values()):
            raise RuntimeError("Training did not finish correctly")
        starts = [e for e in events if e["kind"] == "start"]
        counts = {r: sum(e["rank"] == r for e in starts) for r in (0, 1)}
        expected = {r: 1 + int(args.fault and (not aligned or r == args.failed_rank)) for r in (0, 1)}
        if counts != expected:
            raise RuntimeError(f"Unexpected restart counts: {counts}, expected {expected}")
        arrays = [np.load(finals[r]["model_path"]) for r in (0, 1)]
        if not np.allclose(arrays[0], arrays[1], atol=1e-5, rtol=0):
            raise RuntimeError("Final replicas disagree")
        updates = [e for e in events if e["kind"] == "update"]
        repeated = {r: len([e for e in updates if e["rank"] == r]) - args.steps for r in (0, 1)}
        recovery = None
        if fault_time is not None:
            # Both ranks have advanced past the last pre-failure model step.
            recovery = max(min(e["time"] for e in updates if e["rank"] == r
                               and e["step"] > args.fault_step and e["time"] >= fault_time)
                           for r in (0, 1)) - fault_time
        output = {"status": "completed", "mode": args.mode, "fault": args.fault,
            "repeat": args.repeat, "fit_s": ended - started,
            "loop_span_s": max(e["time"] for e in events if e["kind"] == "finish") - min(e["time"] for e in starts),
            "startup_delay_s": args.startup_delay_s,
            "recovery_to_new_progress_s": recovery, "repeated_updates_per_rank": repeated,
            "fault_time": fault_time, "events": events, "final": finals, "starts": counts,
            "checkpoint_bytes": sum(e["bytes"] for e in events if e["kind"] == "checkpoint"),
            "versions": {"ray": ray.__version__, "torch": torch.__version__,
                         "torchft": importlib.metadata.version("torchft")}}
        write_json(args.sample, output)
    finally:
        ray.shutdown()
        cluster.shutdown()
        pool.shutdown(wait=False, cancel_futures=True)


def recovery_errors(sample, parameters):
    """Audit raw evidence, including old reports that lack acknowledgement events."""
    errors = []
    if sample.get("status") != "completed":
        return ["run did not complete"]
    fault = sample["fault"]
    ordinary = sample["mode"] == "ordinary"
    fault_step = parameters["fault_step"]
    latest = (fault_step // parameters["checkpoint_every"]) * parameters["checkpoint_every"]
    events = sample.get("events", [])
    acknowledgements = [e for e in events if e["kind"] == "checkpoint_ack"]
    for step in range(0, parameters["steps"], parameters["checkpoint_every"]):
        if not any(e["step"] == step for e in acknowledgements):
            errors.append(f"no controller acknowledgement for checkpoint {step}")
    if fault and not any(e["step"] == latest and e["time"] < sample["fault_time"]
                         for e in acknowledgements):
        errors.append(f"checkpoint {latest} was not acknowledged before node loss")
    for rank in (0, 1):
        initializations = [e for e in events if e["kind"] == "initialized" and e["rank"] == rank]
        replaced = fault and (ordinary or rank == parameters["failed_rank"])
        expected_restores = [0, latest] if replaced else [0]
        restores = [e["restored_step"] for e in initializations]
        if restores != expected_restores:
            errors.append(f"rank {rank} restored {restores}, expected {expected_restores}")
        repeated = sum(e["kind"] == "update" and e.get("rank") == rank for e in events) - parameters["steps"]
        expected_repeats = fault_step - latest if fault and ordinary else 0
        if repeated != expected_repeats:
            errors.append(f"rank {rank} repeated {repeated} updates, expected {expected_repeats}")
    return errors


def validate(report):
    import numpy as np
    for sample in report["samples"]:
        sample["recovery_errors"] = recovery_errors(sample, report["parameters"])
        sample["recovery_contract_valid"] = not sample["recovery_errors"]
        if sample["status"] != "completed":
            continue
        reference = next(s for s in report["samples"] if s["repeat"] == sample["repeat"]
                         and s["mode"] == "ordinary" and not s["fault"])
        if reference["status"] != "completed":
            sample["validation"] = "missing healthy ordinary Ray reference"
            continue
        reference_array = np.load(reference["final"]["0"]["model_path"])
        error = max(float(np.max(np.abs(np.load(v["model_path"]) - reference_array)))
                    for v in sample["final"].values())
        sample["max_parameter_error"] = error
        sample["matches_reference"] = error <= 1e-5
    report["pairs"] = []
    for repeat in range(report["parameters"]["repeats"]):
        samples = {(s["mode"], s["fault"]): s for s in report["samples"] if s["repeat"] == repeat}
        if len(samples) != 4 or not all(s.get("matches_reference") and s.get("recovery_contract_valid") for s in samples.values()):
            report["pairs"].append({"repeat": repeat, "valid": False})
            continue
        ordinary, ours = samples[("ordinary", True)], samples[("aligned", True)]
        baseline, healthy = samples[("ordinary", False)], samples[("aligned", False)]
        report["pairs"].append({"repeat": repeat, "valid": True,
            "fault_runtime_reduction_pct": 100 * (ordinary["fit_s"] - ours["fit_s"]) / ordinary["fit_s"],
            "healthy_fit_overhead_pct": 100 * (healthy["fit_s"] / baseline["fit_s"] - 1),
            "healthy_loop_overhead_pct": 100 * (healthy["loop_span_s"] / baseline["loop_span_s"] - 1)})
