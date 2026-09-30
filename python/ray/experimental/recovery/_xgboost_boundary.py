"""Experimental worker reuse between completed XGBoost collectives.

The controller must wait for every segment and tracker to finish before it
changes membership. This is not an active-collective failure protocol or a
Ray Train retry implementation. No worker is silently restarted by Ray.
"""

import hashlib
import os
import time
import uuid

import numpy as np
import pandas as pd
import ray
import xgboost as xgb
import xgboost.collective


def fingerprint(frame):
    hashes = pd.util.hash_pandas_object(frame, index=False).to_numpy(dtype=np.uint64)
    return {"rows": len(frame), "columns": list(frame.columns),
            "hash_sum": sum(int(value) for value in hashes) % (1 << 64),
            "hash_xor": int(np.bitwise_xor.reduce(hashes, initial=np.uint64(0)))}


def tree_digest(model):
    return hashlib.sha256("\n".join(model.get_dump(dump_format="json")).encode()).hexdigest()


def replacement_ranks(policy, failed_rank, size):
    if policy not in ("full", "selective") or not 0 <= failed_rank < size:
        raise ValueError("Invalid replacement policy or failed rank")
    return list(range(size)) if policy == "full" else [failed_rank]


def validate_transition(before, after, failed_rank, policy, dead_nodes):
    """Require exact survivor identity and data reuse, not merely job success."""
    if len(before) != len(after) or len(before) < 2:
        raise ValueError("Worker count changed during replacement")
    size = len(before)
    replaced = set(replacement_ranks(policy, failed_rank, size))
    for workers in (before, after):
        if [w["rank"] for w in workers] != list(range(size)):
            raise ValueError("Worker ranks changed during replacement")
        if len({w["actor_id"] for w in workers}) != size or len({w["node_id"] for w in workers}) != size:
            raise ValueError("Worker actors must occupy distinct nodes")
    for rank, (old, new) in enumerate(zip(before, after)):
        if new["node_id"] in dead_nodes or new["input"] != old["input"]:
            raise ValueError("Replacement has a dead node or changed input partition")
        if new["loads"] != 1:
            raise ValueError("Worker reloaded its input unexpectedly")
        identity = ("actor_id", "worker_id", "pid", "node_id", "data_token")
        if rank in replaced:
            if (new["actor_id"] == old["actor_id"] or new["worker_id"] == old["worker_id"]
                    or new["data_token"] == old["data_token"]):
                raise ValueError("Requested replacement did not create a new worker and data cache")
        elif any(new[key] != old[key] for key in identity):
            raise ValueError("Healthy worker or its cached data was not preserved")
    return {"preserved_ranks": [r for r in range(size) if r not in replaced],
            "replaced_ranks": sorted(replaced)}


class BoundaryWorker:
    """One fixed rank with an immutable local partition and committed model."""

    def __init__(self, rank):
        self.rank = rank
        self.frame = None
        self.matrix = None
        self.model = None
        self.loads = 0
        self.generation = 0
        self.data_token = None
        self.matrix_builds = 0

    def prepare(self, frame, expected):
        if self.loads or xgb.collective.is_distributed():
            raise ValueError("Input preparation requires a new worker outside a collective")
        if fingerprint(frame) != expected:
            raise ValueError("Loaded partition does not match its input manifest")
        self.frame = frame
        self.data_token = uuid.uuid4().hex
        self.loads += 1
        return self.identity()

    def identity(self):
        runtime = ray.get_runtime_context()
        return {"rank": self.rank, "actor_id": runtime.get_actor_id(),
                "worker_id": runtime.get_worker_id(), "node_id": runtime.get_node_id(),
                "pid": os.getpid(), "data_token": self.data_token,
                "loads": self.loads, "input": fingerprint(self.frame) if self.frame is not None else None}

    def train_segment(self, tracker_args, checkpoint, start, end, generation):
        if self.frame is None or generation <= self.generation or not 0 <= start < end:
            raise ValueError("Invalid collective generation or round interval")
        saved = None
        if checkpoint is not None:
            saved = xgb.Booster(model_file=bytearray(checkpoint))
            if saved.num_boosted_rounds() != start:
                raise ValueError("Checkpoint does not match segment start")
        elif start:
            raise ValueError("Resumed segment requires a checkpoint")
        if self.model is not None:
            if saved is None or tree_digest(self.model) != tree_digest(saved):
                raise ValueError("Healthy worker disagrees with committed checkpoint")
        else:
            self.model = saved
        started = time.monotonic()
        first_round = []

        class Progress(xgb.callback.TrainingCallback):
            def after_iteration(self, model, epoch, evals_log):
                if not first_round:
                    first_round.append((model.num_boosted_rounds(), time.monotonic_ns()))
                return False

        args = {**tracker_args, "dmlc_task_id": f"rank-{self.rank:08}", "dmlc_timeout": 30}
        with xgb.collective.CommunicatorContext(**args):
            if xgb.collective.get_rank() != self.rank or xgb.collective.get_world_size() != 2:
                raise ValueError("Collective ranks do not match fixed data partitions")
            # SimpleDMatrix caches histogram cuts. Every rank must rebuild
            # after membership changes; mixing cached and fresh matrices can
            # make ranks execute different distributed-sketch collectives.
            self.matrix = xgb.DMatrix(self.frame.drop("labels", axis=1), label=self.frame["labels"], nthread=1)
            self.matrix_builds += 1
            self.model = xgb.train(
                {"objective": "binary:logistic", "eval_metric": ["logloss", "error"],
                 "tree_method": "hist", "nthread": 1, "seed": 0},
                self.matrix, num_boost_round=end - start, xgb_model=self.model, callbacks=[Progress()],
            )
        # The RPC returns only after this rank's communicator has finalized.
        if xgb.collective.is_distributed() or self.model.num_boosted_rounds() != end:
            raise ValueError("Collective did not finish the exact round budget")
        if saved is not None and tree_digest(self.model[:start]) != tree_digest(saved):
            raise ValueError("Training changed the committed checkpoint prefix")
        self.generation = generation
        return {"identity": self.identity(), "start_round": start, "end_round": end,
                "generation": generation, "collective_finalized": True,
                "matrix_builds": self.matrix_builds,
                "first_round": first_round[0][0], "first_round_ns": first_round[0][1],
                "segment_s": time.monotonic() - started, "tree_sha256": tree_digest(self.model),
                "model": bytes(self.model.save_raw(raw_format="ubj"))}

    def predict(self, frame):
        if self.model is None or xgb.collective.is_distributed():
            raise ValueError("Prediction requires a committed model outside a collective")
        return self.model.predict(xgb.DMatrix(frame.drop("labels", axis=1), nthread=1))
