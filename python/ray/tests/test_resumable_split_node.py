"""Run locally: input-prefix replay after actual logical coordinator-node loss."""

import time

import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.cluster_utils import Cluster
from ray.data.context import DataContext
from ray.data._internal.iterator.resumable_split import CONFIG_KEY, EMPTY_DIGEST


@pytest.mark.parametrize("restarts", [0, 1])
def test_node_loss_relocates_with_surviving_owner_or_exhausts_budget(tmp_path, restarts):
    ray.shutdown()
    previous = DataContext.get_current()
    cluster = Cluster()
    try:
        head = cluster.add_node(num_cpus=2, node_ip_address="127.0.0.2", object_store_memory=128 * 1024**2)
        target = cluster.add_node(num_cpus=0, node_ip_address="127.0.0.3", object_store_memory=128 * 1024**2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address, _node_ip_address=head.node_ip_address)
        assert ray.get_runtime_context().get_node_id() == head.node_id
        context = DataContext()
        context.execution_options.preserve_order = True
        context.enable_fixed_r_task_recovery = False
        context.set_config(CONFIG_KEY, {
            "deterministic": True, "rows_per_chunk": 2, "max_restarts": restarts,
            "preferred_node_id": target.node_id, "allow_node_relocation": True,
            "timeout_s": 60,
        })
        DataContext._set_current(context)
        pq.write_table(pa.table({"id": list(range(32))}), tmp_path / "input.parquet")
        splits = ray.data.read_parquet(str(tmp_path / "input.parquet"), concurrency=1).streaming_split(2, equal=True)
        actor = splits[0]._coord_actor
        old = ray.get(actor.identity.remote(), timeout=60)
        assert old["node_id"] == target.node_id
        delivered = ray.get([actor.get.remote(0, rank, 0, EMPTY_DIGEST) for rank in (0, 1)])
        # One consumer has advanced further than its peer at failure time.
        ahead = ray.get(actor.get.remote(0, 0, 1, delivered[0][1]))
        roots = [info.process for infos in target.all_processes.values() for info in infos]
        children = {}
        for root in roots:
            for child in psutil.Process(root.pid).children(recursive=True):
                children[child.pid] = child
        assert old["pid"] in children
        cluster.remove_node(target, allow_graceful=False)
        for child in children.values():
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        deadline = time.monotonic() + 60
        while any(n["NodeID"] == old["node_id"] and n["Alive"] for n in ray.nodes()):
            assert time.monotonic() < deadline, "GCS did not mark target dead"
            time.sleep(.05)
        if not restarts:
            with pytest.raises(ray.exceptions.RayActorError):
                ray.get(actor.get.remote(0, 0, 1, delivered[0][1]), timeout=60)
            return
        new = ray.get(actor.identity.remote(), timeout=60)
        assert new["node_id"] == head.node_id
        assert new["worker_id"] != old["worker_id"]
        assert ray.get(actor.get.remote(0, 0, 1, delivered[0][1]), timeout=60) == ahead
        assert ray.get(actor.get.remote(0, 1, 0, EMPTY_DIGEST), timeout=60) == delivered[1]
        replies = [ahead, ray.get(actor.get.remote(0, 1, 1, delivered[1][1]), timeout=60)]
        for sequence in range(2, 9):
            replies = ray.get([actor.get.remote(0, r, sequence, replies[r][1]) for r in (0, 1)], timeout=60)
            for rank, (payload, _) in enumerate(replies):
                if sequence == 8:
                    assert payload is None
                else:
                    start = sequence * 4 + rank * 2
                    assert pa.ipc.open_stream(payload).read_all()["id"].to_pylist() == [start, start + 1]
    finally:
        ray.shutdown()
        cluster.shutdown()
        DataContext._set_current(previous)
