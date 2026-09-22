"""Local evaluation supervisor with surviving RocksDB storage.

All nodes are processes on one Linux host. This is not a cloud HA supervisor
and does not simulate losing the physical host or its storage.
"""

import argparse
import sys
import tempfile
import time
from contextlib import contextmanager

import ray
from ray.cluster_utils import Cluster
from ray.experimental.recovery import system_config


def local_system_config():
    return {
        **system_config(),
        "health_check_initial_delay_ms": 1000,
        "health_check_period_ms": 1000,
        "health_check_timeout_ms": 3000,
        "health_check_failure_threshold": 3,
    }


@contextmanager
def local_head_failure_cluster(args, *, coordinator_cpus=1):
    if sys.platform != "linux":
        raise ValueError("The local head-failure harness requires Linux RocksDB support")
    if not 2 <= args.local_executor_nodes <= 250:
        raise ValueError("Head failure requires --local-executor-nodes between 2 and 250")
    if args.owner_node_id or args.executor_node_ids:
        raise ValueError("The local head-failure harness selects its own node IDs")
    if ray.is_initialized():
        raise ValueError("Run the head-failure harness in a fresh driver process")

    # Keep the database outside the killed node's session/spill directory.
    # On real machines this must be an independently surviving, reattachable
    # volume (or use external Redis). The local test preserves this directory.
    with tempfile.TemporaryDirectory(prefix="ray-head-recovery-") as storage_path:
        cluster = Cluster()
        try:
            config = local_system_config()
            config.update(
                gcs_storage="rocksdb",
                gcs_storage_path=storage_path,
                gcs_rpc_server_reconnect_timeout_s=300,
            )
            node_options = dict(
                object_store_memory=args.local_object_store_mb * 1024**2,
            )
            head_options = dict(
                num_cpus=0, include_dashboard=False, node_ip_address="127.0.0.2",
                _system_config=config, **node_options,
            )
            head = cluster.add_node(**head_options)
            address = cluster.address
            cluster_id = head.cluster_id.hex()
            session_name = head.session_name
            coordinator = cluster.add_node(
                num_cpus=coordinator_cpus, node_ip_address="127.0.0.3", **node_options,
            )
            executors = [
                cluster.add_node(
                    num_cpus=2, node_ip_address=f"127.0.0.{index + 4}", **node_options,
                )
                for index in range(args.local_executor_nodes)
            ]
            cluster.wait_for_nodes()
            # Connecting through the head endpoint alone selects the head's
            # raylet on a local cluster. Explicitly bind this driver to a worker.
            ray.init(address=address, _node_ip_address=coordinator.node_ip_address)
            runtime = ray.get_runtime_context()
            driver_node_id = runtime.get_node_id()
            driver_job_id = runtime.get_job_id()
            if driver_node_id != coordinator.node_id:
                raise RuntimeError("Benchmark driver must be attached to the surviving coordinator")

            case_args = argparse.Namespace(**vars(args))
            case_args.owner_node_id = head.node_id
            case_args.executor_node_ids = tuple(node.node_id for node in executors)
            if case_args.producer_concurrency is None:
                case_args.producer_concurrency = len(executors)
            survivors = {coordinator.node_id, *case_args.executor_node_ids}
            crashed = False

            def crash_head():
                nonlocal crashed
                if crashed:
                    raise RuntimeError("The head-failure harness permits one failure")
                crashed = True
                processes = [
                    info.process for infos in head.all_processes.values() for info in infos
                ]
                gcs_process = head.all_processes["gcs_server"][0].process
                start = time.monotonic()
                # This kills GCS, the raylet, agents, and the owner actors. It
                # neither disconnects nor reinitializes the surviving driver.
                cluster.remove_node(head, allow_graceful=False)
                if any(process.poll() is None for process in processes):
                    raise RuntimeError("An original head process survived failure injection")
                killed_s = time.monotonic() - start
                replacement = cluster.add_node(
                    gcs_server_port=int(address.rsplit(":", 1)[1]), **head_options,
                )
                if (
                    replacement.cluster_id.hex() != cluster_id
                    or replacement.session_name != session_name
                    or replacement.gcs_address != address
                    or replacement.node_id == head.node_id
                ):
                    raise RuntimeError("Replacement did not restore the original cluster/session")
                deadline = time.monotonic() + args.recovery_timeout_s
                while True:
                    nodes = {node["NodeID"]: node for node in ray.nodes()}
                    if any(node in nodes and not nodes[node]["Alive"] for node in survivors):
                        raise RuntimeError("A required worker died during head replacement")
                    if (
                        all(node in nodes and nodes[node]["Alive"] for node in survivors)
                        and head.node_id in nodes and not nodes[head.node_id]["Alive"]
                        and nodes.get(replacement.node_id, {}).get("Alive", False)
                    ):
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Head replacement did not preserve the surviving workers")
                    time.sleep(0.05)
                if (
                    runtime.get_node_id() != driver_node_id
                    or runtime.get_job_id() != driver_job_id
                ):
                    raise RuntimeError("Head replacement changed the surviving driver/job")
                return {
                    "failure_scope": "all_head_processes_with_surviving_gcs_storage",
                    "gcs_storage_backend": "rocksdb",
                    "cluster_id": cluster_id,
                    "driver_job_id": driver_job_id,
                    "original_head_node_id": head.node_id,
                    "replacement_head_node_id": replacement.node_id,
                    "original_gcs_pid": gcs_process.pid,
                    "replacement_gcs_pid": replacement.all_processes["gcs_server"][0].process.pid,
                    "original_head_processes_exited": True,
                    "head_failure_request_to_process_exit_s": killed_s,
                    "head_failure_request_to_replacement_ready_s": time.monotonic() - start,
                    "surviving_node_ids": sorted(survivors),
                }

            yield case_args, crash_head
        finally:
            ray.shutdown()
            cluster.shutdown()
