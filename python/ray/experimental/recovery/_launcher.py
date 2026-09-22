"""Reusable script launcher and bounded local head-replacement supervisor."""

import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback
import uuid


def _stop(process):
    if process is not None and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)


def run_script(script, script_args, *, local=False, address="auto", node_ip_address=None,
               inject_head_failure=False, timeout_s=120, report=None, env=None):
    """Launch the script's normal main. Local mode is a Linux evaluation fixture.

    External mode attaches to an already-configured cluster and never kills or
    replaces its head. Its HA supervisor/storage must be supplied separately.
    Local observations prove execution/replay, not application-specific output
    equivalence; retain the application's own result checks as well.
    """
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("timeout_s must be positive and finite")
    if local and timeout_s > 120:
        raise ValueError("Local evaluation is limited to a 120-second case deadline")
    if inject_head_failure and not local:
        raise ValueError("Head injection is supported only in the isolated local fixture")
    if address.startswith("ray://"):
        raise ValueError("Run the launcher on a surviving worker host using a native Ray address")
    script = Path(script).resolve(strict=True)
    result = {"script": str(script), "argv": list(script_args), "local": local,
              "failure_requested": inject_head_failure, "timeout_s": timeout_s,
              "validation_scope": "normal_entrypoint_completion_and_runtime_replay" if local else "script_exit_only"}
    child_env = {**os.environ, **(env or {})}
    child_env.update(RAY_EXPERIMENTAL_RECOVERY="1", RAY_RECOVERY_TIMEOUT_S=str(timeout_s))
    # Do not inherit another supervisor's observer or driver placement.
    child_env.pop("RAY_RECOVERY_MONITOR", None)
    child_env.pop("RAY_RECOVERY_DRIVER_NODE_IP", None)
    process = None
    child_log = None

    def start(cluster_address, driver_ip=None, monitor_name=None):
        nonlocal child_log
        child_env["RAY_ADDRESS"] = cluster_address
        if driver_ip:
            child_env["RAY_RECOVERY_DRIVER_NODE_IP"] = driver_ip
        if monitor_name:
            child_env["RAY_RECOVERY_MONITOR"] = monitor_name
        child_env["PYTHONUNBUFFERED"] = "1"
        if report:
            log_path = Path(report).resolve().with_suffix(".log")
            log_path.parent.mkdir(parents=True, exist_ok=True)
            child_log = log_path.open("w+b")
            result["application_log"] = str(log_path)
            print(f"Application output: {log_path}", flush=True)
        return subprocess.Popen(
            [sys.executable, "-m", "ray.experimental.recovery._script", str(script), *script_args],
            env=child_env, start_new_session=True,
            stdout=child_log, stderr=subprocess.STDOUT if child_log else None,
        )

    try:
        if local:
            import ray
            from ray.experimental.recovery._local import local_head_failure_cluster
            from ray.experimental.recovery._observe import Monitor, NAMESPACE, validate
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            args = argparse.Namespace(
                local_executor_nodes=4, local_object_store_mb=512,
                owner_node_id=None, executor_node_ids=None, producer_concurrency=None,
                recovery_timeout_s=min(30, timeout_s),
            )
            with local_head_failure_cluster(args, coordinator_cpus=0) as (_, crash_head):
                node = ray.get_runtime_context().get_node_id()
                monitor_name = f"recovery-{uuid.uuid4().hex}"
                monitor = ray.remote(num_cpus=0, max_restarts=0)(Monitor).options(
                    name=monitor_name, namespace=NAMESPACE,
                    scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False),
                ).remote()
                started = time.monotonic()
                deadline = started + timeout_s
                failed_at = None
                process = start(ray.get_runtime_context().gcs_address, "127.0.0.3", monitor_name)
                result["application_driver_pid"] = process.pid
                try:
                    while time.monotonic() < deadline:
                        observation = ray.get(monitor.read.remote(), timeout=min(5, max(.01, deadline-time.monotonic())))
                        result["observation"] = observation
                        status = process.poll()
                        if inject_head_failure and failed_at is None and observation["trigger"]:
                            if status is not None:
                                raise ValueError("Script exited before failure injection")
                            failed_at = time.monotonic()
                            requested_ns = observation["trigger"].get("requested_ns")
                            if requested_ns is not None:
                                result["trigger_to_failure_request_s"] = (
                                    time.monotonic_ns() - requested_ns
                                ) / 1e9
                            args.recovery_timeout_s = min(30, max(.01, deadline-failed_at))
                            failure = crash_head()
                            failure["supervisor_driver_job_id"] = failure.pop("driver_job_id")
                            result.update(failure)
                        if status is not None:
                            observation = ray.get(monitor.read.remote(), timeout=5)
                            result["observation"] = observation
                            result["exit_code"] = status
                            if status:
                                raise RuntimeError(f"Script exited with status {status}; see application_output_tail/application_log")
                            if inject_head_failure and failed_at is None:
                                raise ValueError("Head failure was not exercised")
                            validate(observation, inject_head_failure)
                            result["time"] = time.monotonic() - started
                            if failed_at is not None:
                                result["failure_request_to_completion_s"] = time.monotonic() - failed_at
                            break
                        # Tiny ReadRange tasks can finish within the old 50-ms
                        # polling interval. Poll promptly until failure injection.
                        time.sleep(.005 if inject_head_failure and failed_at is None else .05)
                    else:
                        raise TimeoutError(f"Script exceeded the {timeout_s:g}-second case deadline")
                finally:
                    _stop(process)
        else:
            started = time.monotonic()
            process = start(address, node_ip_address)
            status = process.wait(timeout=timeout_s)
            result.update(exit_code=status, time=time.monotonic()-started)
            if status:
                raise RuntimeError(f"Script exited with status {status}; see application_output_tail/application_log")
        result["validation_status"] = "passed"
    except Exception as exc:
        result.update(validation_status="failed", error_type=type(exc).__name__,
                      error=str(exc), traceback=traceback.format_exc())
        traceback.print_exc()
    finally:
        _stop(process)
        if child_log is not None:
            # Read only after the child stops: seeking a shared file description
            # while it writes could overwrite earlier log output.
            child_log.seek(0, os.SEEK_END)
            size = child_log.tell()
            child_log.seek(max(0, size - 32768))
            tail = child_log.read().decode("utf-8", errors="replace")
            result["application_output_tail"] = tail
            child_log.close()
            if result.get("validation_status") != "passed":
                print(tail, file=sys.stderr)
        if report:
            path = Path(report).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_text(json.dumps(result, indent=2))
            temporary.replace(path)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="Isolated Linux process-cluster evaluation")
    parser.add_argument("--address", default="auto")
    parser.add_argument("--driver-node-ip")
    parser.add_argument("--inject-head-failure", action="store_true")
    parser.add_argument("--timeout-s", type=float, default=120)
    parser.add_argument("--report", type=Path)
    parser.add_argument("script")
    parser.add_argument("script_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    result = run_script(
        args.script, args.script_args, local=args.local, address=args.address,
        node_ip_address=args.driver_node_ip, inject_head_failure=args.inject_head_failure,
        timeout_s=args.timeout_s, report=args.report,
    )
    print(f"Recovery launcher: {result['validation_status']}")
    return 0 if result["validation_status"] == "passed" else 1
