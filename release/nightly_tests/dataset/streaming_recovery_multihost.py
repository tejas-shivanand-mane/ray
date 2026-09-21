"""Nine-host Fixed-R backpressure suite; run the controller on worker zero.

SSH agents own only the Ray processes they start. A head restart preserves its
GCS RocksDB directory. No cloud provisioning or machine reboot is performed.
See gossip_benchmarks/FIXED_R_EIGHT_WORKER_BACKPRESSURE.md.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import queue
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback


SCRIPT = "release/nightly_tests/dataset/streaming_recovery_multihost.py"
CASES = (
    ("copy", "none"),
    ("fixed_r", "none"),
    ("fixed_r_head_failure", "producer_before_output"),
    ("fixed_r_head_failure", "producer_after_output"),
    ("fixed_r_head_failure", "consumer"),
)


def load_hosts(path):
    manifest = json.loads(Path(path).read_text())
    hosts = [manifest["head"], *manifest["workers"]]
    if len(hosts) != 9 or len(manifest["workers"]) != 8:
        raise ValueError("Supply exactly one head and eight workers")
    for index, host in enumerate(hosts):
        address = ipaddress.IPv4Address(host["ip"])
        if address.is_loopback or address.is_unspecified or address.is_multicast:
            raise ValueError("Use routable private IPv4 addresses, not loopback")
        for key in ("repo", "python"):
            if not Path(host[key]).is_absolute():
                raise ValueError(f"Host {index}: {key} must be an absolute path")
        if index != 1 and (not host.get("ssh") or host["ssh"].startswith("-")):
            raise ValueError(f"Host {index}: supply an SSH destination or config alias")
    if len({host["ip"] for host in hosts}) != 9:
        raise ValueError("Each host must have a distinct IP")
    return hosts


def probe(host):
    import numpy as np
    import pyarrow as pa
    import psutil
    import ray
    from ray._raylet import CoreWorker

    root = Path(host["repo"]).resolve()
    if Path(ray.__file__).resolve() != root / "python/ray/__init__.py":
        raise ValueError("Python must import this checkout's source-built Ray")
    if Path(__file__).resolve() != root / SCRIPT:
        raise ValueError("Run the harness from the configured checkout")
    if not hasattr(CoreWorker, "try_release_streaming_recovery_return"):
        raise ValueError("Native Ray is missing the Fixed-R streaming build")
    if sys.platform != "linux":
        raise ValueError("This harness requires Linux and GCS RocksDB")
    if os.environ.get("RAY_REDIS_ADDRESS"):
        raise ValueError("Unset RAY_REDIS_ADDRESS; this profile owns its GCS RocksDB")
    with socket.socket() as sock:
        sock.bind((host["ip"], 0))
    if psutil.cpu_count() < 8 or psutil.virtual_memory().total < 30 * 1024**3:
        raise ValueError("Each host needs at least 8 vCPUs and approximately 32 GiB RAM")
    if psutil.virtual_memory().available < 20 * 1024**3:
        raise ValueError("Each host needs at least 20 GiB available RAM")
    if shutil.disk_usage("/dev/shm").free < 8 * 1024**3:
        raise ValueError("Each host needs at least 8 GiB free /dev/shm")
    if shutil.disk_usage(tempfile.gettempdir()).free < 128 * 1024**3:
        raise ValueError("Each host needs at least 128 GiB free temporary/spill disk")
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True
    ).strip()
    if dirty:
        raise ValueError("Use a clean tracked checkout on every host")
    # This profile deliberately starts one managed raylet per machine. Do not
    # silently share machines with an existing Ray deployment.
    for process in psutil.process_iter(["name"]):
        if process.info["name"] in ("raylet", "gcs_server"):
            raise ValueError("A Ray cluster already runs on this host")
    return {
        "ip": host["ip"], "revision": revision,
        "machine_id": Path("/etc/machine-id").read_text().strip(),
        "cpus": psutil.cpu_count(), "memory_bytes": psutil.virtual_memory().total,
        "ray_path": ray.__file__, "python_version": list(sys.version_info[:3]),
        "numpy_version": np.__version__, "pyarrow_version": pa.__version__,
        "native_sha256": hashlib.sha256(Path(ray._raylet.__file__).read_bytes()).hexdigest(),
        "spill_disk_free_bytes": shutil.disk_usage(tempfile.gettempdir()).free,
    }


def agent_main():
    # Reserve a clean JSON channel even if native startup writes to stdout.
    protocol = os.fdopen(os.dup(sys.stdout.fileno()), "w", buffering=1)
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    node = None
    head_options = None
    restarted = False

    def stop():
        if node is not None:
            node.kill_all_processes(check_alive=False, allow_graceful=False, wait=True)

    def describe():
        return {
            "node_id": node.node_id, "address": node.gcs_address,
            "cluster_id": node.cluster_id.hex(), "session_name": node.session_name,
            "session_dir": node.get_session_dir_path(),
        }

    def terminate(signum, frame):
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, terminate)
    try:
        for line in sys.stdin:
            try:
                request = json.loads(line)
                action = request["action"]
                if action == "probe":
                    result = probe(request["host"])
                elif action == "start":
                    if node is not None:
                        raise ValueError("This agent already owns a node")
                    from ray._private.node import Node
                    from ray._private.parameter import RayParams
                    from streaming_recovery_benchmark import recovery_system_config

                    options = dict(
                        node_ip_address=request["ip"],
                        num_cpus=0 if request["head"] else 8, num_gpus=0,
                        object_store_memory=8 * 1024**3,
                        temp_dir=tempfile.mkdtemp(prefix="ray-fixed-r-multihost-"),
                        include_dashboard=False, no_monitor=True,
                    )
                    if request["head"]:
                        storage = tempfile.mkdtemp(prefix="ray-fixed-r-gcs-")
                        config = recovery_system_config()
                        config.update(
                            gcs_storage="rocksdb", gcs_storage_path=storage,
                            gcs_rpc_server_reconnect_timeout_s=300,
                        )
                        options.update(_system_config=config, gcs_server_port=0)
                        head_options = options
                    else:
                        options["gcs_address"] = request["address"]
                    node = Node(RayParams(**options), head=request["head"])
                    result = describe()
                elif action == "restart":
                    if node is None or head_options is None or restarted:
                        raise ValueError("Only the original head can be restarted once")
                    restarted = True
                    original = describe()
                    processes = [p.process for entries in node.all_processes.values()
                                 for p in entries]
                    gcs_pid = node.all_processes["gcs_server"][0].process.pid
                    start = time.monotonic()
                    stop()
                    if any(p.poll() is None for p in processes):
                        raise RuntimeError("An original managed head process survived")
                    killed_s = time.monotonic() - start
                    head_options["gcs_server_port"] = int(original["address"].rsplit(":", 1)[1])
                    node = Node(RayParams(**head_options), head=True)
                    replacement = describe()
                    if (any(replacement[k] != original[k] for k in
                            ("address", "cluster_id", "session_name"))
                            or replacement["node_id"] == original["node_id"]):
                        raise RuntimeError("Head replacement lost the cluster/session identity")
                    result = {
                        "failure_scope": "all_head_processes_with_surviving_gcs_storage",
                        "gcs_storage_backend": "rocksdb",
                        "cluster_id": original["cluster_id"],
                        "original_head_node_id": original["node_id"],
                        "replacement_head_node_id": replacement["node_id"],
                        "original_gcs_pid": gcs_pid,
                        "replacement_gcs_pid": node.all_processes["gcs_server"][0].process.pid,
                        "original_head_processes_exited": True,
                        "head_failure_request_to_process_exit_s": killed_s,
                        "replacement": replacement,
                    }
                elif action == "stop":
                    stop()
                    protocol.write(json.dumps({"ok": True, "result": {}}) + "\n")
                    break
                else:
                    raise ValueError(f"Unknown action: {action}")
                protocol.write(json.dumps({"ok": True, "result": result}) + "\n")
            except Exception:
                protocol.write(json.dumps({"ok": False, "error": traceback.format_exc()}) + "\n")
    finally:
        stop()


class Agent:
    def __init__(self, host, local, log_path):
        self.log = open(log_path, "w")
        command = [host["python"], "-u", str(Path(host["repo"]) / SCRIPT), "--agent"]
        if not local:
            command = [
                "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
                "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
                host["ssh"], shlex.join(command),
            ]
        try:
            self.process = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=self.log, text=True, bufsize=1,
            )
        except BaseException:
            self.log.close()
            raise
        self.responses = queue.Queue()
        self.unresponsive = False

        def receive():
            try:
                for line in self.process.stdout:
                    self.responses.put(json.loads(line))
            except Exception as exc:
                self.responses.put({"ok": False, "error": str(exc)})
            finally:
                self.responses.put({"ok": False, "error": f"Agent exited; inspect {log_path}"})

        threading.Thread(target=receive, daemon=True).start()

    def call(self, action, timeout=300, **kwargs):
        if self.unresponsive:
            raise RuntimeError("Agent previously timed out; do not reuse its response channel")
        self.process.stdin.write(json.dumps({"action": action, **kwargs}) + "\n")
        self.process.stdin.flush()
        try:
            response = self.responses.get(timeout=timeout)
        except queue.Empty as exc:
            self.unresponsive = True
            raise TimeoutError(f"Agent {action} timed out; inspect agent log") from exc
        if not response["ok"]:
            raise RuntimeError(response["error"])
        return response["result"]

    def close(self):
        # EOF asks the agent to clean up only its own processes, even after a
        # timed-out RPC. Never use ray stop, killall, or host-wide process kills.
        cleanup_error = None
        try:
            if not self.unresponsive and self.process.poll() is None:
                self.call("stop", timeout=30)
            else:
                raise RuntimeError("Agent stopped responding before cleanup was confirmed")
        except Exception as exc:
            cleanup_error = exc
            print(f"Agent cleanup was not confirmed: {exc}", file=sys.stderr)
        try:
            self.process.stdin.close()
            self.process.wait(timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        finally:
            self.process.stdout.close()
            self.log.close()
        if cleanup_error is not None:
            raise RuntimeError("Remote cleanup was not confirmed; inspect the host before rerunning") from cleanup_error


def collect_all(calls):
    # Observe every outcome and wait for all in-flight starts before cleanup.
    with ThreadPoolExecutor(max_workers=len(calls)) as pool:
        futures = [pool.submit(call) for call in calls]
        results, errors = [], []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("; ".join(str(exc) for exc in errors)) from errors[0]
        return results


def wait_topology(expected, absent, timeout):
    import ray

    deadline = time.monotonic() + timeout
    while True:
        alive = {n["NodeID"] for n in ray.nodes() if n["Alive"]}
        if alive == set(expected) and not alive.intersection(absent):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Cluster membership mismatch: expected={expected}, alive={alive}")
        time.sleep(0.1)


@contextmanager
def remote_cluster(hosts, directory, timeout, diagnostics):
    import ray

    with ExitStack() as stack:
        agents = []
        for index, host in enumerate(hosts):
            agent = Agent(host, index == 1, directory / f"host-{index}.log")
            stack.callback(agent.close)
            agents.append(agent)
        probes = collect_all([
            lambda a=a, h=h: a.call("probe", host=h)
            for a, h in zip(agents, hosts)
        ])
        diagnostics["hosts"] = probes
        if len({p["revision"] for p in probes}) != 1:
            raise ValueError("Hosts have different Git revisions")
        if len({tuple(p["python_version"][:2]) for p in probes}) != 1:
            raise ValueError("Hosts have different Python major/minor versions")
        for dependency in ("numpy_version", "pyarrow_version"):
            if len({p[dependency] for p in probes}) != 1:
                raise ValueError(f"Hosts have different {dependency}")
        if len({p["machine_id"] for p in probes}) != 9:
            raise ValueError("Expected nine distinct machines with unique machine IDs")
        if probe(hosts[1])["revision"] != probes[1]["revision"]:
            raise ValueError("Controller and local worker use different source checkouts")
        head = agents[0].call("start", head=True, ip=hosts[0]["ip"])
        workers = collect_all([
            lambda a=a, h=h: a.call("start", head=False, ip=h["ip"], address=head["address"])
            for a, h in zip(agents[1:], hosts[1:])
        ])
        diagnostics.update(
            runtime_cluster_topology="multihost", original_head=head, workers=workers,
            worker_count=8, worker_cpus=8, object_store_bytes_per_node=8 * 1024**3,
            driver_colocated_with_worker=True,
        )
        try:
            ray.init(address=head["address"], _node_ip_address=hosts[1]["ip"])
            runtime = ray.get_runtime_context()
            driver = runtime.get_node_id()
            job = runtime.get_job_id()
            worker_ids = tuple(w["node_id"] for w in workers)
            if driver != worker_ids[0]:
                raise ValueError("Run the controller on the first worker host")
            wait_topology([head["node_id"], *worker_ids], [], timeout)

            def crash():
                start = time.monotonic()
                result = agents[0].call("restart", timeout=timeout)
                diagnostics.update(result)
                wait_topology(
                    [result["replacement_head_node_id"], *worker_ids],
                    [head["node_id"]], timeout,
                )
                if runtime.get_node_id() != driver or runtime.get_job_id() != job:
                    raise RuntimeError("The surviving driver/job changed")
                result.update(
                    driver_job_id=job,
                    surviving_node_ids=list(worker_ids),
                    head_failure_request_to_replacement_ready_s=time.monotonic() - start,
                )
                diagnostics.update(result)
                return result

            yield head["node_id"], worker_ids, crash
        finally:
            ray.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--output-dir", default="/tmp/fixed-r-eight-worker")
    parser.add_argument("--timeout-s", type=float, default=1800)
    parser.add_argument("--case", choices=["suite", "consumer"], default="suite")
    args = parser.parse_args()
    if not 0 < args.timeout_s < float("inf"):
        parser.error("--timeout-s must be finite and positive")
    hosts = load_hosts(args.hosts)
    directory = Path(args.output_dir).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    # Parse the application's defaults directly, so this profile cannot silently
    # diverge from its input size, UDF batch size, or consumer delay.
    import backpressure_benchmark as original
    from benchmark import Benchmark
    from ray.data._internal.execution.streaming_recovery import FixedRDataConfig
    from streaming_recovery_backpressure_dataset import run_dataset

    old_argv = sys.argv
    try:
        sys.argv = [old_argv[0], "--case", "fast-producer-slow-consumer"]
        workload = original.parse_args()
    finally:
        sys.argv = old_argv
    workload.recovery_plan = "runtime"
    workload.recovery_workload = "original"
    workload.recovery_timeout_s = args.timeout_s
    os.environ["TEST_OUTPUT_JSON"] = str(directory / "backpressure.json")
    benchmark = Benchmark()
    failures = []
    cases = CASES if args.case == "suite" else (CASES[-1],)
    for index, (mode, point) in enumerate(cases):
        selected = argparse.Namespace(**vars(workload))
        selected.recovery_mode = mode
        selected.runtime_failure_point = point
        case_dir = directory / f"{index}-{mode}-{point}"
        case_dir.mkdir()
        diagnostics = {}
        key = f"backpressure/eight_workers/{mode}/{point}"
        try:
            with remote_cluster(hosts, case_dir, args.timeout_s, diagnostics) as (owner, workers, crash):
                selected.owner_node_id = owner
                selected.executor_node_ids = workers
                config = FixedRDataConfig(
                    owner, workers, {}, timeout_s=args.timeout_s, dynamic_task_outputs=True,
                )
                benchmark.run_fn(key, run_dataset, selected, crash, diagnostics, config)
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            failures.append(key)
            traceback.print_exc()
            benchmark.result[key] = {
                **benchmark.result.get(key, {}),
                **vars(selected), **diagnostics, "validation_status": "failed",
                "error_type": type(exc).__name__, "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        finally:
            benchmark.write_result()
        if failures:
            # A failed multi-host setup must be investigated before launching
            # another cluster; retain its logs and its first failure report.
            break
    if failures:
        raise RuntimeError(f"Multi-host suite failed: {failures}; inspect {directory}")


if __name__ == "__main__":
    if sys.argv[1:] == ["--agent"]:
        agent_main()
    else:
        main()
