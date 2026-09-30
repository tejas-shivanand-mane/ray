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
def local_head_failure_cluster(
    args, *, coordinator_cpus=1, recovery_enabled=True, include_dashboard=False,
    allow_head_failure=None,
    include_worker_failure=False,
):
    # Existing callers retain their injection policy. Comparisons can explicitly
    # apply the same external replacement operation to an OFF cluster as well.
    if allow_head_failure is None:
        allow_head_failure = recovery_enabled
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
            if not recovery_enabled:
                # A fresh disabled native baseline for matched comparisons;
                # retain identical numeric settings and external GCS storage.
                for key in system_config():
                    if key.startswith("enable_"):
                        config[key] = False
            config.update(
                gcs_storage="rocksdb",
                gcs_storage_path=storage_path,
                gcs_rpc_server_reconnect_timeout_s=300,
            )
            node_options = dict(
                object_store_memory=args.local_object_store_mb * 1024**2,
            )
            head_options = dict(
                num_cpus=0, include_dashboard=include_dashboard, node_ip_address="127.0.0.2",
                _system_config=config, **node_options,
            )
            if include_dashboard:
                head_options["dashboard_host"] = "127.0.0.2"
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
            failed_workers = set()

            def crash_worker(node_id, worker_pid):
                """Remove one executor, including its object store and children."""
                import psutil

                if not include_worker_failure:
                    raise RuntimeError("Worker-node failure injection is disabled")
                if node_id in failed_workers:
                    raise ValueError("Target executor was already removed")
                if len(executors) - len(failed_workers) <= 2:
                    raise ValueError("Worker-node failure must leave two executors for recovery")
                node = next((node for node in executors if node.node_id == node_id), None)
                if node is None:
                    raise ValueError("Worker failure must target a known executor, not the head/coordinator")
                before = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
                if node_id not in before:
                    raise ValueError("Target executor is already dead")
                roots = [info.process for infos in node.all_processes.values() for info in infos]
                children = {}
                for root in roots:
                    try:
                        for process in psutil.Process(root.pid).children(recursive=True):
                            children[process.pid] = process
                    except psutil.NoSuchProcess:
                        pass
                if worker_pid not in children:
                    raise ValueError("Selected training process is not a child of the target node")
                failed_workers.add(node_id)
                survivors.discard(node_id)
                started = time.monotonic()
                deadline = started + args.recovery_timeout_s
                cluster.remove_node(node, allow_graceful=False)
                # Ray workers are descendants, not entries in Node.all_processes.
                # Include them explicitly in the process-loss evidence.
                for process in children.values():
                    try:
                        process.kill()
                    except psutil.NoSuchProcess:
                        pass
                while True:
                    live_children = []
                    for process in children.values():
                        try:
                            if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                                live_children.append(process.pid)
                        except psutil.NoSuchProcess:
                            pass
                    if not live_children and all(p.poll() is not None for p in roots):
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Target node processes did not exit")
                    time.sleep(.02)
                processes_exited_s = time.monotonic() - started
                while True:
                    nodes = {n["NodeID"]: n for n in ray.nodes()}
                    alive = {nid for nid, n in nodes.items() if n["Alive"]}
                    if before - {node_id} - alive:
                        raise RuntimeError("An unrequested node died during worker-node failure")
                    if node_id in nodes and not nodes[node_id]["Alive"]:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("GCS did not mark the killed executor dead")
                    time.sleep(.05)
                if runtime.get_node_id() != driver_node_id or runtime.get_job_id() != driver_job_id:
                    raise RuntimeError("Worker failure changed the surviving driver/job")
                return {
                    "failure_scope": "logical_worker_node_processes_with_surviving_shared_storage",
                    "node_id": node_id, "training_worker_pid": worker_pid,
                    "node_process_pids": sorted({p.pid for p in roots} | set(children)),
                    "all_node_processes_exited": True, "gcs_marked_dead": True,
                    "request_to_process_exit_s": processes_exited_s,
                    "request_to_gcs_dead_s": time.monotonic() - started,
                    "surviving_node_ids": sorted(alive),
                }

            def crash_head():
                nonlocal crashed
                if not allow_head_failure:
                    raise RuntimeError("Head failure injection is disabled for this fixture")
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

            if include_worker_failure:
                yield case_args, crash_head, crash_worker
            else:
                yield case_args, crash_head
        finally:
            ray.shutdown()
            cluster.shutdown()
