"""One fresh Ray Train job. Cluster/VM lifecycle belongs to the external launcher."""

import argparse
import json
import os
import tempfile
import time
import traceback
from datetime import timedelta
from pathlib import Path

from evidence import digest_batch, state_digest, write_json


def run(cfg):
    # Helpers must be available in worker processes even when the image predates them.
    import evidence
    import numpy as np
    import torch
    from torch import nn

    import ray
    import ray.data
    import ray.train as train
    from ray.train import Checkpoint, RunConfig, ScalingConfig, UserCallback
    from ray.train.torch import TorchConfig, TorchTrainer
    from ray.train.v2._internal.data_integration.step_aligned_input import (
        StepAlignedDataConfig,
    )
    from ray.train.v2.api.config import FailureConfig
    from ray.train.v2.examples.pytorch.torchft_linear_example import (
        _FailureAwareDistributedDataParallel,
    )
    from ray.train.v2.torch.torchft_config import TorchftConfig

    ray.cloudpickle.register_pickle_by_value(evidence)
    folder = Path(cfg["folder"])
    attempt = cfg["attempt"]
    out = folder / f"attempt-{attempt}"
    out.mkdir(exist_ok=False)
    aligned = cfg["mode"] == "aligned"
    peer = cfg["mode"] != "ordinary"
    base_step = cfg.get("checkpoint", {}).get("step", 0)
    ray.init(address=cfg["address"])
    alive = [n for n in ray.nodes() if n["Alive"]]
    write_json(out / "cluster.json", alive)

    def batch_values(ids):
        ids = np.asarray(ids, dtype=np.int64)
        return {
            "id": ids,
            "x": np.sin(ids[:, None] * 0.013 + np.arange(256)[None, :] * 0.071).astype(
                np.float32
            ),
            "y": ((ids // 7 + ids % 3) % 10).astype(np.int64),
        }

    class Acknowledge(UserCallback):
        def after_report(self, run_context, metrics, checkpoint):
            if checkpoint is None:
                return
            steps = {m["step"] for m in metrics}
            if len(steps) != 1:
                raise ValueError("Checkpoint ranks disagree")
            step = steps.pop()
            with checkpoint.as_directory() as directory:
                state = torch.load(
                    Path(directory) / "state.pt", map_location="cpu", weights_only=True
                )
            if state["step"] != step:
                raise ValueError("Persisted checkpoint step mismatch")
            write_json(
                folder / "latest-checkpoint.json",
                dict(
                    step=step,
                    path=str(checkpoint.path),
                    state_digest=state_digest(state),
                    attempt=attempt,
                ),
            )

    def training(config):
        rank = train.get_context().get_world_rank()
        torch.set_num_threads(1)
        torch.manual_seed(cfg["seed"])
        torch.use_deterministic_algorithms(True)
        model = nn.Sequential(
            nn.Linear(256, cfg["model_width"]),
            nn.ReLU(),
            nn.Linear(cfg["model_width"], cfg["model_width"]),
            nn.ReLU(),
            nn.Linear(cfg["model_width"], cfg["model_width"]),
            nn.ReLU(),
            nn.Linear(cfg["model_width"], 10),
        )
        raw_optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

        def state(step):
            return dict(
                model=model.state_dict(),
                optimizer=raw_optimizer.state_dict(),
                step=step,
            )

        if attempt:
            saved = torch.load(
                Path(cfg["checkpoint"]["path"]) / "state.pt",
                map_location="cpu",
                weights_only=True,
            )
            if (
                saved["step"] != base_step
                or state_digest(saved) != cfg["checkpoint"]["state_digest"]
            ):
                raise RuntimeError("Selected checkpoint changed")
            model.load_state_dict(saved["model"])
            raw_optimizer.load_state_dict(saved["optimizer"])

        def event(kind, **values):
            row = dict(kind=kind, rank=rank, attempt=attempt, **values)
            with (out / f"rank-{rank}.jsonl").open("a") as stream:
                stream.write(json.dumps(row) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            write_json(out / f"progress-{rank}.json", row)

        event(
            "start",
            base_step=base_step,
            state_digest=state_digest(state(base_step)),
            ip=ray.util.get_node_ip_address(),
            node_id=ray.get_runtime_context().get_node_id(),
            pid=os.getpid(),
            threads=torch.get_num_threads(),
        )
        manager = None
        if peer:
            from torchft import Manager, Optimizer, ProcessGroupGloo
            from torchft.checkpointing.pg_transport import PGTransport

            pg = ProcessGroupGloo(timeout=timedelta(seconds=30))

            def load_peer(value):
                model.load_state_dict(value["model"])
                raw_optimizer.load_state_dict(value["optimizer"])

            manager = Manager(
                pg=pg,
                min_replica_size=2,
                world_size=1,
                rank=0,
                replica_id=f"headloss_{rank}",
                use_async_quorum=False,
                timeout=timedelta(seconds=60),
                state_dict=lambda: state(-1),
                load_state_dict=load_peer,
                checkpoint_transport=PGTransport(
                    pg, timeout=timedelta(seconds=30), device=torch.device("cpu")
                ),
            )
            wrapped = _FailureAwareDistributedDataParallel(manager, model)
            optimizer = Optimizer(manager, raw_optimizer)
        else:
            wrapped = train.torch.prepare_model(model)
            optimizer = raw_optimizer
        shard = train.get_dataset_shard("train") if aligned else None
        step, checkpointed = base_step, base_step if attempt else -1
        try:
            while True:
                optimizer.zero_grad()
                if peer:
                    manager.wait_quorum()
                    if manager.num_participants() != 2:
                        raise RuntimeError("Full membership required")
                    # Fresh TorchFT manager starts at zero after whole-job restart.
                    step = base_step + manager.current_step()
                if step >= cfg["steps"]:
                    break
                if step % cfg["checkpoint_every"] == 0 and step > checkpointed:
                    with tempfile.TemporaryDirectory() as temporary:
                        cp = None
                        if rank == 0:
                            torch.save(state(step), Path(temporary) / "state.pt")
                            cp = Checkpoint.from_directory(temporary)
                        train.report({"step": step}, checkpoint=cp)
                        deadline = time.monotonic() + 60
                        while True:
                            path = folder / "latest-checkpoint.json"
                            if (
                                path.exists()
                                and json.loads(path.read_text())["step"] == step
                            ):
                                break
                            if time.monotonic() > deadline:
                                raise TimeoutError(
                                    "No durable checkpoint acknowledgement"
                                )
                            time.sleep(0.05)
                    checkpointed = step
                # Both ranks gate at an exact committed step. Keep the gate held
                # until fencing; this experiment intentionally does NOT test continuation.
                if cfg["fault"] and not attempt and step == cfg["fault_step"]:
                    write_json(out / f"gate-{rank}.json", dict(step=step, rank=rank))
                    while True:
                        time.sleep(0.1)
                ids = np.arange(
                    (step * 2 + rank) * cfg["batch_size"],
                    (step * 2 + rank + 1) * cfg["batch_size"],
                )
                batch = (
                    shard.batch_for_step(step - base_step)
                    if aligned
                    else batch_values(ids)
                )
                if not np.array_equal(batch["id"], ids):
                    raise RuntimeError("Input not aligned with global model step")
                loss = nn.functional.cross_entropy(
                    wrapped(torch.tensor(batch["x"])), torch.tensor(batch["y"])
                )
                loss.backward()
                optimizer.step()
                following = base_step + manager.current_step() if peer else step + 1
                if following > step:
                    event("update", step=following, input_digest=digest_batch(batch))
                step = following
            torch.save(state(step), out / f"final-{rank}.pt")
            event("finish", step=step)
            train.report({"step": step})
        finally:
            if manager is not None:
                manager.shutdown()

    dataset_args = {}
    if aligned:
        # Regenerate the synthetic suffix; no surviving retained-input claim.
        offset = base_step * 2 * cfg["batch_size"]
        ds = ray.data.range(
            (cfg["steps"] - base_step) * 2 * cfg["batch_size"]
        ).map_batches(
            lambda batch: batch_values(batch["id"] + offset),
            batch_format="numpy",
            batch_size=2 * cfg["batch_size"],
        )
        dataset_args = dict(
            datasets={"train": ds},
            dataset_config=StepAlignedDataConfig(
                cfg["batch_size"], synchronous_full_membership=True
            ),
        )
    trainer = TorchTrainer(
        training,
        scaling_config=ScalingConfig(
            num_workers=2,
            placement_strategy="STRICT_SPREAD",
            resources_per_worker={"CPU": 3, "headloss_train": 1},
        ),
        torch_config=(
            TorchftConfig(
                backend="gloo",
                lighthouse_kwargs={
                    "min_replicas": 2,
                    "bind": ray.util.get_node_ip_address() + ":0",
                },
            )
            if peer
            else TorchConfig(backend="gloo", timeout_s=60)
        ),
        run_config=RunConfig(
            storage_path=str(out / "storage"),
            callbacks=[Acknowledge()],
            failure_config=FailureConfig(max_failures=0),
        ),
        **dataset_args,
    )
    trainer.fit()
    write_json(out / "done.json", {"status": "completed"})
    ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    arguments = parser.parse_args()
    config = json.loads(arguments.config.read_text())
    try:
        run(config)
    except BaseException:
        write_json(
            Path(config["folder"]) / f"error-{config['attempt']}.json",
            {"error": traceback.format_exc()},
        )
        raise
