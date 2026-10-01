"""Worker reuse within the normal TrainController retry decision.

No checkpoint or retry policy is implemented here: those remain owned by Ray
Train. Any inability to fence the old generation falls back to full restart.
"""

from dataclasses import replace
import logging
import time

import ray
from ray.exceptions import RayActorError
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.context import DistributedContext
from ray.train.v2._internal.execution.worker_group.execution_group import ReplicaGroup
from ray.train.v2._internal.util import invoke_context_managers

logger = logging.getLogger(__name__)


def assign_preserved_ranks(workers):
    """Refresh local ranks without reordering global ranks/cached partitions."""
    node_ids = list(dict.fromkeys(w.metadata.node_id for w in workers))
    for rank, worker in enumerate(workers):
        peers = [w for w in workers if w.metadata.node_id == worker.metadata.node_id]
        worker.distributed_context = DistributedContext(
            world_rank=rank, world_size=len(workers), local_rank=peers.index(worker),
            local_world_size=len(peers), node_rank=node_ids.index(worker.metadata.node_id),
        )


def try_restart(worker_group, run_attempt_id, timeout_s):
    group = worker_group
    workers = list(group.get_workers())
    deadline = time.monotonic() + timeout_s

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("Selective XGBoost retry deadline expired")
        return value

    try:
        # A collective error on a surviving process must not label that actor
        # dead. Conversely, a missed health check is not proof it can be reused.
        alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        dead = set()
        probes = {rank: w.actor.get_metadata.remote() for rank, w in enumerate(workers)
                  if w.metadata.node_id in alive}
        for rank, worker in enumerate(workers):
            if rank not in probes:
                dead.add(rank)
                continue
            try:
                ray.get(probes[rank], timeout=remaining())
            except RayActorError:
                dead.add(rank)
        if not dead or len(dead) == len(workers):
            return False

        # Fence an unavailable-but-not-yet-dead actor before making a replacement.
        for rank in dead:
            ray.kill(workers[rank].actor, no_restart=True)
        ray.get(group._worker_group_state.sync_actor.reset.remote(), timeout=remaining())
        pending = set(range(len(workers))) - dead
        while pending:
            refs = {rank: workers[rank].actor.prepare_xgboost_retry.remote()
                    for rank in pending}
            for rank, ref in refs.items():
                if ray.get(ref, timeout=remaining()):
                    pending.remove(rank)
            if pending:
                time.sleep(min(.05, remaining()))

        # All surviving training threads (including communicator finalizers)
        # are now stopped. Discard old queues and synchronization state.
        old_context = group._worker_group_context
        for callback in group._callbacks:
            callback.before_worker_group_shutdown(group)
        for callback in group._callbacks:
            callback.after_worker_group_shutdown(old_context)
        group._world_rank_to_ongoing_poll.clear()
        group._latest_poll_status = None
        group._worker_group_context = replace(old_context, run_attempt_id=run_attempt_id)
        state = group._worker_group_state
        ray.kill(state.sync_actor, no_restart=True)
        sync_actor = SynchronizationActor.options(label_selector={
            ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
        }).remote(timeout_s=group._collective_timeout_s,
                  warn_interval_s=group._collective_warn_interval_s)
        group._worker_group_state = replace(state, sync_actor=sync_actor)

        for rank in sorted(dead):
            old_worker = workers[rank]
            new_worker = group._create_workers(
                num_workers=1, placement_group=state.placement_group_handle.placement_group,
                resources_per_worker=old_context.resources_per_worker,
                placement_group_bundle_indices=[old_worker.placement_group_bundle_index],
                starting_world_rank=rank, world_size=len(workers),
                startup_timeout_s=remaining(),
            )[0]
            # Track each replacement immediately so fallback cleans up partially
            # initialized actors as well as the surviving actors.
            group._worker_group_state = group._worker_group_state.replace_workers(
                [old_worker], [new_worker])
            workers[rank] = new_worker
        assign_preserved_ranks(workers)
        group._replica_groups = [ReplicaGroup([w], old_context.resources_per_worker,
                                             group._replica_group_callbacks) for w in workers]
        with invoke_context_managers([c.on_worker_group_start for c in group._callbacks]):
            for callback in group._callbacks:
                callback.before_worker_group_start(group._worker_group_context)
            # CheckpointManager supplies the latest committed checkpoint and
            # report index; every worker receives a fresh execution context.
            group._init_train_context(workers, sync_actor)
            for callback in group._callbacks:
                callback.after_worker_group_start(group)
        ray.get([w.actor.run_train_fn.remote(old_context.train_fn_ref) for w in workers],
                timeout=remaining())
        for callback in group._callbacks:
            callback.after_worker_group_training_start(group)
        logger.info("Selective XGBoost retry replaced ranks %s; preserved ranks %s",
                    sorted(dead), sorted(set(range(len(workers))) - dead))
        return True
    except Exception:
        logger.exception("Selective XGBoost retry unavailable; falling back to full restart")
        return False
