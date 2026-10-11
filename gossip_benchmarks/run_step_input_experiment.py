"""Isolate commit-aligned input from TorchFT peer model recovery.

Run in ray-dev with RAY_TRAIN_V2_ENABLED=1. Each observation has a fresh process
and logical cluster. This is a CPU linear-model correctness ablation, not a
production throughput benchmark. Plot the saved JSON separately.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2))
    temporary.replace(path)


def observation(args):
    import concurrent.futures
    import importlib.metadata
    import numpy as np
    import ray
    import ray.data
    import ray.train
    import torch
    from ray.cluster_utils import Cluster
    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2.api.config import FailureConfig
    from ray.train.v2._internal.data_integration.step_aligned_input import StepAlignedDataConfig
    from ray.train.v2.examples.pytorch.torchft_linear_example import train_func
    from ray.train.v2.torch.torchft_config import TorchftConfig
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    @ray.remote(num_cpus=0)
    class Recorder:
        def __init__(self):
            self.events = []
            self.node = None
            self.results = {}

        def record(self, event):
            self.events.append(dict(event, time=time.monotonic()))

        def claim(self, node):
            if self.node is not None:
                return False
            self.node = node
            return True

        def snapshot(self):
            return self.node, self.events, self.results

        def finish(self, rank, result):
            self.results[rank] = result

    # Local class serializes by value, avoiding imports of this script on workers.
    class IndependentInput(ray.train.DataConfig):
        def __init__(self):
            super().__init__(datasets_to_split=[])

        def configure(self, datasets, world_size, worker_handles, worker_node_ids, **kwargs):
            return [{name: ds.filter(
                lambda row, r=rank: (row["id"] // args.batch_size) % world_size == r
            ).iterator() for name, ds in datasets.items()} for rank in range(world_size)]

    cluster = Cluster()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    result = None
    start = time.monotonic()
    try:
        cluster.add_node(num_cpus=2, include_dashboard=False)
        nodes = [cluster.add_node(num_cpus=1, resources={"train_slot": 1}) for _ in range(3)]
        ray.init(address=cluster.address)
        recorder = Recorder.options(scheduling_strategy=NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False)).remote()
        rows = (args.steps + 8) * 2 * args.batch_size
        # Explicit finite ordered data, with varying targets to expose wrong batches.
        ds = ray.data.range(rows, override_num_blocks=1).map(
            lambda row: {"id": row["id"], "x": (row["id"] % 31) / 31,
                         "y": 2 * (row["id"] % 31) / 31 + 0.1 * (row["id"] // 31)}
        )
        ds.context.execution_options.preserve_order = True
        config = (StepAlignedDataConfig(args.batch_size, synchronous_full_membership=True)
                  if args.mode == "aligned" else IndependentInput())

        def train(config):
            torch.set_num_threads(1)
            torch.manual_seed(1234 + args.repeat)
            rank = ray.train.get_context().get_world_rank()
            ray.get(recorder.record.remote({"kind": "start", "rank": rank}))
            shard = ray.train.get_dataset_shard("train")
            if args.mode == "aligned":
                read_batch = shard.batch_for_step
            else:
                batches = iter(shard.iter_batches(batch_size=args.batch_size, prefetch_batches=0))
                # Conventional next(iterator): advances on delivery, restarts at
                # the beginning in a replacement process; no input checkpoint.
                read_batch = lambda step: next(batches)

            def read(step):
                batch = read_batch(step)
                ids = batch["id"].tolist()
                expected = list(range((step * 2 + rank) * args.batch_size,
                                      (step * 2 + rank + 1) * args.batch_size))
                ray.get(recorder.record.remote({"kind": "input", "rank": rank,
                    "step": step, "ids": ids, "matches_committed_step": ids == expected}))
                if args.fault and rank == args.failed_rank and step == args.fault_step:
                    if ray.get(recorder.claim.remote(ray.get_runtime_context().get_node_id())):
                        while True:
                            time.sleep(0.1)
                return batch

            shard.batch_for_step = read
            final = train_func(config)[-1]
            ray.get(recorder.finish.remote(rank, final))
            ray.get(recorder.record.remote({"kind": "finish", "rank": rank}))

        trainer = TorchTrainer(train, train_loop_config={
            "num_steps": args.steps, "num_replicas": 2, "step_aligned_input": True,
            "batch_size": args.batch_size,
        }, datasets={"train": ds}, dataset_config=config,
            scaling_config=ScalingConfig(num_workers=2, placement_strategy="STRICT_SPREAD",
                resources_per_worker={"CPU": 1, "train_slot": 1}),
            torch_config=TorchftConfig(backend="gloo", lighthouse_kwargs={"min_replicas": 2}),
            run_config=RunConfig(storage_path=str(Path(args.sample).parent / "storage"),
                failure_config=FailureConfig(max_failures=1)))
        fit_start = time.monotonic()
        future = pool.submit(trainer.fit)
        fault_time = None
        if args.fault:
            while not future.done():
                node, _, _ = ray.get(recorder.snapshot.remote())
                if node is not None:
                    fault_time = time.monotonic()
                    cluster.remove_node(next(n for n in nodes if n.node_id == node), allow_graceful=False)
                    break
                if time.monotonic() - fit_start > 120:
                    raise TimeoutError("Did not reach fault gate in 120 seconds")
                time.sleep(0.1)
            if fault_time is None:
                future.result()
                raise RuntimeError("Training ended without injecting the requested node failure")
        result = future.result(timeout=180)
        fit_end = time.monotonic()
        _, events, final = ray.get(recorder.snapshot.remote())
        if result.error or result.best_checkpoints or len(final) != 2:
            raise RuntimeError("Missing successful results or unexpected model checkpoints")
        inputs = [e for e in events if e["kind"] == "input"]
        starts = [e for e in events if e["kind"] == "start"]
        expected_starts = {r: 1 + int(args.fault and r == args.failed_rank) for r in (0, 1)}
        actual_starts = {r: sum(e["rank"] == r for e in starts) for r in (0, 1)}
        if actual_starts != expected_starts or any(v["step"] != args.steps for v in final.values()):
            raise RuntimeError(f"Unexpected restart/step counts: {actual_starts}, {final}")
        weights = [np.array(final[r]["weight"] + final[r]["bias"]) for r in (0, 1)]
        if not np.allclose(weights[0], weights[1], atol=1e-5, rtol=0):
            raise RuntimeError("Final replicas disagree")
        write_json(args.sample, {"status": "completed", "mode": args.mode, "fault": args.fault,
            "repeat": args.repeat, "fit_s": fit_end - fit_start,
            "loop_span_s": max(e["time"] for e in events if e["kind"] == "finish") - min(e["time"] for e in starts),
            "fault_time": fault_time, "fit_start": fit_start, "events": events,
            "final": final, "starts": actual_starts, "checkpoints": 0,
            "input_mismatches": sum(not e["matches_committed_step"] for e in inputs),
            "observation_s": time.monotonic() - start,
            "versions": {"ray": ray.__version__, "torch": torch.__version__,
                         "torchft": importlib.metadata.version("torchft")}})
    finally:
        ray.shutdown()
        cluster.shutdown()
        pool.shutdown(wait=False, cancel_futures=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--steps", type=int, default=40)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--fault-step", type=int, default=20)
    parser.add_argument("--failed-rank", type=int, choices=[0, 1], default=0)
    parser.add_argument("--timeout-s", type=int, default=300)
    parser.add_argument("--comparison", choices=["input-ablation", "ordinary-ray"], default="input-ablation")
    parser.add_argument("--checkpoint-every", type=int, default=16)
    parser.add_argument("--model-width", type=int, default=512)
    parser.add_argument("--startup-delay-s", type=float, default=0)
    parser.add_argument("--sample", type=Path)
    parser.add_argument("--mode", choices=["independent", "aligned", "ordinary"])
    parser.add_argument("--repeat", type=int, default=0)
    parser.add_argument("--fault", action="store_true")
    args = parser.parse_args()
    if not 0 < args.fault_step < args.steps or min(args.batch_size, args.repeats, args.timeout_s) <= 0:
        parser.error("Require 0 < fault-step < steps and positive sizes/repeats/timeout")
    if args.checkpoint_every <= 0 or args.model_width <= 0 or args.startup_delay_s < 0:
        parser.error("Checkpoint interval/model width must be positive; startup delay nonnegative")
    if args.comparison == "ordinary-ray" and args.fault_step % args.checkpoint_every == 0:
        parser.error("Choose a failure strictly between scheduled checkpoints")
    if args.comparison != "ordinary-ray" and args.startup_delay_s:
        parser.error("Simulated startup delay is supported by --comparison ordinary-ray")
    os.environ["RAY_TRAIN_V2_ENABLED"] = "1"
    if args.sample:
        if args.comparison == "ordinary-ray":
            from _support.ordinary_ray_comparison import run
            run(args, write_json)
        else:
            observation(args)
        return
    args.output = args.output.expanduser().resolve()
    directory = args.output.parent / (args.output.stem + "-runs")
    if args.output.exists() or directory.exists():
        parser.error("Choose a fresh output name; existing evidence is never overwritten")
    directory.mkdir(parents=True)
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1],
                              capture_output=True, text=True)
    root = Path(__file__).resolve().parents[1]
    source_paths = ["gossip_benchmarks/run_step_input_experiment.py",
        "python/ray/train/v2/examples/pytorch/torchft_linear_example.py",
        "python/ray/train/v2/_internal/data_integration/step_aligned_input.py",
        "python/ray/train/v2/_internal/callbacks/datasets.py",
        "python/ray/train/v2/_internal/execution/worker_group/worker_group.py"]
    if args.comparison == "ordinary-ray":
        source_paths.append("gossip_benchmarks/_support/ordinary_ray_comparison.py")
    source_hashes = {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in source_paths}
    report = {"source_sha256": source_hashes, "source_commit": revision.stdout.strip() if revision.returncode == 0 else None,
        "profile": "ordinary-ray-peer" if args.comparison == "ordinary-ray" else "step-input-ablation", "parameters": {
        "steps": args.steps, "batch_size": args.batch_size, "fault_step": args.fault_step,
        "failed_rank": args.failed_rank, "repeats": args.repeats,
        "checkpoint_every": args.checkpoint_every, "model_width": args.model_width,
        "startup_delay_s": args.startup_delay_s}, "samples": [],
        "scope": "CPU linear model; two logical worker nodes; surviving head/coordinator; no durable checkpoints; Fixed-R not enabled by harness",
        "comparison": "Same TorchFT and failure-aware hook; independent input vs commit-aligned input"}
    if args.comparison == "ordinary-ray":
        report.update(scope="CPU MLP synthetic classification; two logical worker nodes; surviving head/storage; identical scheduled checkpoints; Fixed-R not enabled by harness",
                      comparison="Ordinary Ray DDP full-group checkpoint retry vs TorchFT with commit-aligned input and partial peer recovery",
                      startup_simulation="Each worker invocation sleeps startup_delay_s, including initial startup and restarts in both arms")
    baseline_mode = "ordinary" if args.comparison == "ordinary-ray" else "independent"
    for repeat in range(args.repeats):
        for fault in (False, True):
            for mode in ([baseline_mode, "aligned"] if repeat % 2 == 0 else ["aligned", baseline_mode]):
                folder = directory / f"r{repeat}-{mode}-{'fault' if fault else 'healthy'}"
                folder.mkdir()
                sample = folder / "sample.json"
                command = [sys.executable, str(Path(__file__).resolve()), "--output", str(args.output),
                    "--sample", str(sample), "--mode", mode, "--repeat", str(repeat),
                    "--steps", str(args.steps), "--batch-size", str(args.batch_size),
                    "--fault-step", str(args.fault_step), "--failed-rank", str(args.failed_rank),
                    "--comparison", args.comparison, "--checkpoint-every", str(args.checkpoint_every),
                    "--model-width", str(args.model_width), "--startup-delay-s", str(args.startup_delay_s),
                    "--timeout-s", str(args.timeout_s)]
                if fault:
                    command.append("--fault")
                print(f"repeat {repeat + 1}/{args.repeats}: {mode}, fault={fault}", flush=True)
                with (folder / "run.log").open("w") as log:
                    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    timed_out = False
                    try:
                        code = process.wait(timeout=args.timeout_s)
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        os.killpg(process.pid, signal.SIGINT)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        code = process.returncode
                entry = json.loads(sample.read_text()) if sample.exists() else {}
                if timed_out or code != 0:
                    entry.update(status="timeout" if timed_out else "failed", exit_code=code)
                entry.update(mode=mode, fault=fault, repeat=repeat, log=str(folder / "run.log"))
                report["samples"].append(entry)
                write_json(args.output, report)
                print(f"  {entry['status']}", flush=True)
    if args.comparison == "ordinary-ray":
        from _support.ordinary_ray_comparison import validate
        validate(report)
        report["status"] = "finished"
        write_json(args.output, report)
        print(args.output)
        return
    # Each seed's healthy aligned control is the reference for both arms.
    for sample in report["samples"]:
        if sample["status"] != "completed":
            continue
        reference = next(s for s in report["samples"] if s["repeat"] == sample["repeat"] and s["mode"] == "aligned" and not s["fault"])
        if reference["status"] != "completed":
            sample["validation"] = "missing reference"
            continue
        errors = [abs(a - b) for rank in ("0", "1") for key in ("weight", "bias")
                  for a, b in zip(sample["final"][rank][key], reference["final"][rank][key])]
        sample["max_parameter_error"] = max(errors)
        sample["matches_reference"] = max(errors) <= 1e-5
    report["status"] = "finished"
    write_json(args.output, report)
    print(args.output)


if __name__ == "__main__":
    main()
