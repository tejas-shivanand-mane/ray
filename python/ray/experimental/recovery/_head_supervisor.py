"""Experimental, off-head supervisor for one fenced head-machine replacement.

Run with --help. Deployment hooks must fence the old machine, start the new
head against the same external Redis, and move a stable GCS endpoint. This
process never calls ray.init() and must survive independently of the head.
"""

import argparse
from dataclasses import dataclass
import ipaddress
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

from ray.experimental.recovery import system_config
from ray.experimental.recovery._launcher import _stop

MODULE = "ray.experimental.recovery._head_supervisor"


@dataclass
class Config:
    address: str
    old_head_ip: str
    replacement_head_ip: str
    redis_address: str
    storage_namespace: str
    survivor_node_ips: list
    fence_command: list
    start_command: list
    switch_command: list
    recovery_timeout_s: float = 120
    probe_timeout_s: float = 5
    poll_interval_s: float = 1
    failure_threshold: int = 3
    reconnect_timeout_s: float = 300
    redis_username_env: str = "RAY_HEAD_REDIS_USERNAME"
    redis_password_env: str = "RAY_HEAD_REDIS_PASSWORD"

    def __post_init__(self):
        # Require a stable routed IPv4 endpoint for this first implementation.
        # DNS updates and client DNS caching add another unverified dependency.
        host, port = self.address.rsplit(":", 1)
        addresses = [host, self.old_head_ip, self.replacement_head_ip,
                     *self.survivor_node_ips]
        for address in addresses:
            parsed = ipaddress.IPv4Address(address)
            if parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast:
                raise ValueError("Use routable machine IPs and a stable GCS IPv4 endpoint")
        if not 1 <= int(port) <= 65535:
            raise ValueError("Invalid GCS port")
        machines = addresses[1:]
        if len(set(machines)) != len(machines) or host in machines:
            raise ValueError("The endpoint and all machine IPs must be distinct")
        if len(self.survivor_node_ips) < 3:
            raise ValueError("Declare the coordinator and at least two executor nodes")
        if not self.storage_namespace or self.storage_namespace == "default":
            raise ValueError("Use a unique, non-default external storage namespace")
        from ray._private.services import get_address

        redis_host, redis_port, _ = get_address(self.redis_address)
        if redis_host in (
            self.old_head_ip, self.replacement_head_ip, host, "localhost", "::1", "0.0.0.0",
        ) or redis_host.startswith("127."):
            raise ValueError("Redis must survive independently of both head machines")
        if not 1 <= int(redis_port) <= 65535:
            raise ValueError("Invalid Redis port")
        for value in (self.recovery_timeout_s, self.probe_timeout_s,
                      self.poll_interval_s, self.reconnect_timeout_s):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Timeouts must be positive and finite")
        if type(self.failure_threshold) is not int or self.failure_threshold < 1:
            raise ValueError("failure_threshold must be a positive integer")
        detection = self.failure_threshold * (
            self.probe_timeout_s + self.poll_interval_s
        )
        if self.reconnect_timeout_s <= detection + self.recovery_timeout_s:
            raise ValueError("Reconnect timeout must exceed detection plus recovery time")
        for command in (self.fence_command, self.start_command, self.switch_command):
            if not isinstance(command, list) or not command or not all(
                isinstance(arg, str) and arg for arg in command
            ):
                raise ValueError("Hooks must be nonempty argv lists; no implicit shell")

    def native_config(self):
        return {
            **system_config(),
            "gcs_storage": "redis",
            "external_storage_namespace": self.storage_namespace,
            "gcs_rpc_server_reconnect_timeout_s": int(self.reconnect_timeout_s),
            "health_check_initial_delay_ms": 1000,
            "health_check_period_ms": 1000,
            "health_check_timeout_ms": 3000,
            "health_check_failure_threshold": 3,
        }

    def redis_client(self):
        import redis
        from ray._private.services import get_address

        host, port, ssl = get_address(self.redis_address)
        return redis.Redis(
            host=host, port=int(port), ssl=ssl,
            username=os.environ.get(self.redis_username_env),
            password=os.environ.get(self.redis_password_env),
            socket_connect_timeout=self.probe_timeout_s,
            socket_timeout=self.probe_timeout_s,
            retry_on_timeout=False,
        )


def _stored_session(config):
    from ray._private.services import get_address, serialize_config
    from ray._raylet import get_session_key_from_storage

    host, port, ssl = get_address(config.redis_address)
    return get_session_key_from_storage(
        host, int(port), os.environ.get(config.redis_username_env, ""),
        os.environ.get(config.redis_password_env, ""), ssl,
        serialize_config(config.native_config()), b"session_name",
    )


def _probe(config, address):
    """Child-only native calls: a stuck GCS/Redis client cannot hang supervision."""
    from ray._private import ray_constants
    from ray._private.state import GlobalState
    from ray._raylet import GcsClient, GcsClientOptions

    client = GcsClient(address=address)
    timeout = config.probe_timeout_s
    session = client.internal_kv_get(
        b"session_name", ray_constants.KV_NAMESPACE_SESSION, timeout=timeout
    )
    stored = _stored_session(config)
    if not session or stored != session:
        raise ValueError("GCS session does not match the configured external Redis")
    state = GlobalState()
    state._initialize_global_state(GcsClientOptions.create(
        address, client.cluster_id.hex(), allow_cluster_id_nil=False,
        fetch_cluster_id_if_nil=False,
    ))
    try:
        native = state.get_system_config()
    finally:
        state.disconnect()
    nodes = client.get_all_node_info(timeout=timeout)
    return {
        "cluster_id": client.cluster_id.hex(), "session_name": session.decode(),
        "native_config": {key: native.get(key) for key in config.native_config()},
        "nodes": [{"id": node_id.hex(), "ip": node.node_manager_address,
                   "head": node.is_head_node, "alive": node.state == 0}
                  for node_id, node in nodes.items()],
    }


def snapshot(config, config_path, address, deadline):
    timeout = min(config.probe_timeout_s, deadline - time.monotonic())
    if timeout <= 0:
        raise TimeoutError("Head supervision deadline reached")
    # Use a file for the result; native libraries can write to stdout.
    with tempfile.TemporaryDirectory(prefix="ray-head-probe-") as temporary:
        output = Path(temporary) / "snapshot.json"
        process = subprocess.Popen(
            [sys.executable, "-m", MODULE, "_probe", "--config", str(config_path),
             "--address", address, "--output", str(output)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            if process.wait(timeout=timeout) != 0:
                return None
            current = json.loads(output.read_text())
            if "error" in current:
                raise ValueError(current["error"])
            return current
        except subprocess.TimeoutExpired:
            return None
        finally:
            _stop(process)


def _check_config(config, current):
    for key, value in config.native_config().items():
        if current["native_config"].get(key) != value:
            raise ValueError(f"Head has incompatible native setting: {key}")


def baseline(config, current):
    if current is None:
        raise ValueError("Arm supervision while the original head is healthy")
    _check_config(config, current)
    live = [node for node in current["nodes"] if node["alive"]]
    heads = [node for node in live if node["head"]]
    if len(heads) != 1 or heads[0]["ip"] != config.old_head_ip:
        raise ValueError("Expected exactly one original head")
    survivors = {}
    for ip in config.survivor_node_ips:
        matching = [node for node in live if node["ip"] == ip and not node["head"]]
        if len(matching) != 1:
            raise ValueError(f"Expected one surviving Ray node at {ip}")
        survivors[ip] = matching[0]["id"]
    return {"cluster_id": current["cluster_id"],
            "session_name": current["session_name"],
            "old_head_node_id": heads[0]["id"], "survivor_node_ids": survivors}


def _same_cluster(config, before, current):
    _check_config(config, current)
    for key in ("cluster_id", "session_name"):
        if current[key] != before[key]:
            raise ValueError(f"Replacement changed {key}; refusing endpoint switch")


def replacement_ready(config, before, current):
    _same_cluster(config, before, current)
    live = {node["id"]: node for node in current["nodes"] if node["alive"]}
    heads = [node for node in live.values() if node["head"]]
    return (
        len(heads) == 1 and heads[0]["ip"] == config.replacement_head_ip
        and before["old_head_node_id"] not in live
        and all(node_id in live and live[node_id]["ip"] == ip
                for ip, node_id in before["survivor_node_ids"].items())
    )


def run_hook(command, before, deadline, log_path):
    # Replace only known tokens, without interpreting shell syntax or braces.
    argv = [arg.replace("{session_name}", before["session_name"])
            .replace("{cluster_id}", before["cluster_id"]) for arg in command]
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Recovery budget exhausted before deployment hook")
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            argv, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            status = process.wait(timeout=remaining)
            if status:
                raise RuntimeError(f"Deployment hook exited {status}; see {log_path}")
        finally:
            _stop(process)


def recover(config, before, read, hook, event, deadline):
    """One promotion only; a failed hook is never retried automatically."""
    event("fencing")
    hook(config.fence_command, before, deadline)
    event("fenced")
    hook(config.start_command, before, deadline)
    event("replacement_started")
    port = config.address.rsplit(":", 1)[1]
    replacement_address = f"{config.replacement_head_ip}:{port}"
    # Check restored identity directly before routing surviving clients to it.
    while time.monotonic() < deadline:
        current = read(replacement_address, deadline)
        if current is not None:
            _same_cluster(config, before, current)
            if any(node["alive"] and node["head"]
                   and node["ip"] == config.replacement_head_ip
                   and node["id"] != before["old_head_node_id"]
                   for node in current["nodes"]):
                break
        time.sleep(min(config.poll_interval_s, max(0, deadline - time.monotonic())))
    else:
        raise TimeoutError("Replacement did not restore the original GCS identity")
    hook(config.switch_command, before, deadline)
    event("endpoint_switched")
    while time.monotonic() < deadline:
        current = read(config.address, deadline)
        if current is not None and replacement_ready(config, before, current):
            return current
        time.sleep(min(config.poll_interval_s, max(0, deadline - time.monotonic())))
    raise TimeoutError("Stable endpoint did not recover all original survivor node IDs")


def _write_report(path, result):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        json.dump(result, output, indent=2)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def start_head(config, config_path, node_ip, expected_session, temp_dir, timeout_s):
    """Start on this host; replacement must find the previously saved session."""
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("timeout_s must be positive and finite")
    expected_ip = config.replacement_head_ip if expected_session else config.old_head_ip
    if node_ip != expected_ip:
        raise ValueError("Initial and replacement head roles must match the configured IPs")
    with tempfile.TemporaryDirectory(prefix="ray-head-storage-") as temporary:
        output = Path(temporary) / "session.json"
        process = subprocess.Popen(
            [sys.executable, "-m", MODULE, "_storage-session", "--config",
             str(config_path), "--output", str(output)], start_new_session=True,
        )
        try:
            if process.wait(timeout=min(timeout_s, config.probe_timeout_s)):
                raise RuntimeError("External Redis preflight failed; head was not started")
            stored = json.loads(output.read_text())
        finally:
            _stop(process)
    if stored != expected_session:
        raise ValueError("External Redis session mismatch; refusing to start a head")
    # CPU-requiring work stays off the head. Zero-CPU controllers still need
    # explicit survivor placement in the application's deployment.
    command = [
        sys.executable, "-m", "ray.scripts.scripts", "start", "--head",
        f"--node-ip-address={node_ip}", f"--port={config.address.rsplit(':', 1)[1]}",
        "--num-cpus=0", "--include-dashboard=false", "--disable-usage-stats",
        "--system-config=" + json.dumps(config.native_config()),
        "--temp-dir=" + str(temp_dir),
    ]
    for flag, variable in (("--redis-username", config.redis_username_env),
                           ("--redis-password", config.redis_password_env)):
        if variable in os.environ:
            command.append(f"{flag}={os.environ[variable]}")
    process = subprocess.Popen(
        command, env={**os.environ, "RAY_REDIS_ADDRESS": config.redis_address},
        start_new_session=True,
    )
    try:
        status = process.wait(timeout=timeout_s)
        if status:
            raise RuntimeError(f"ray start failed with exit code {status}")
    finally:
        _stop(process)


def supervise(config, config_path, report, timeout_s):
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("timeout_s must be positive and finite")
    deadline = time.monotonic() + timeout_s
    result = {"status": "starting", "events": [], "address": config.address,
              "validation_scope": "gcs_identity_and_surviving_node_ids",
              "physical_fencing": "deployment_hook_attestation",
              "application_recovery_validated": False}
    read = lambda address, limit: snapshot(config, config_path, address, limit)
    claim_key = f"ray-fixed-r-head-supervisor:{config.storage_namespace}"
    claim = uuid.uuid4().hex
    claimed = action_started = False
    redis = config.redis_client()

    def event(phase):
        result["status"] = phase
        result["events"].append({"phase": phase, "time_unix_s": time.time()})
        _write_report(report, result)
        print(f"Head supervisor: {phase}", flush=True)

    try:
        before = baseline(config, read(config.address, deadline))
        result["before"] = before
        # A persistent claim prevents another supervisor from promoting again.
        # Never expire it while an old hook might still execute remotely.
        if not redis.set(claim_key, claim, nx=True):
            raise RuntimeError("Namespace already claimed; inspect its prior supervisor")
        claimed = True
        result["claim_key"] = claim_key
        event("armed")
        failures = 0
        while time.monotonic() < deadline:
            current = read(config.address, deadline)
            if current is not None:
                _same_cluster(config, before, current)
                healthy = any(node["id"] == before["old_head_node_id"]
                              and node["alive"] for node in current["nodes"])
            else:
                healthy = False
            failures = 0 if healthy else failures + 1
            if failures >= config.failure_threshold:
                if redis.get(claim_key) != claim.encode():
                    raise RuntimeError("Lost durable Redis claim; refusing replacement")
                action_started = True
                recovery_deadline = min(deadline, time.monotonic() + config.recovery_timeout_s)
                result["after"] = recover(
                    config, before, read,
                    lambda command, initial, limit: run_hook(
                        command, initial, limit, report.with_suffix(".hooks.log")),
                    event, recovery_deadline,
                )
                event("recovered")
                return 0
            time.sleep(min(config.poll_interval_s, max(0, deadline - time.monotonic())))
        event("no_failure_observed")
        return 2
    except (Exception, KeyboardInterrupt) as error:
        result["error"] = str(error)
        result["error_type"] = type(error).__name__
        event("failed")
        return 1
    finally:
        # Once fencing starts, even a timeout leaves the claim in place. A remote
        # start command can outlive a disconnected SSH client. No automatic retry.
        if claimed and not action_started:
            redis.eval(
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                "return redis.call('del', KEYS[1]) else return 0 end",
                1, claim_key, claim,
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=(
        "supervise", "start-head", "system-config", "_probe", "_storage-session",
    ))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--timeout-s", type=float, default=120)
    parser.add_argument("--address")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--node-ip")
    parser.add_argument("--expected-session")
    parser.add_argument("--temp-dir", type=Path)
    args = parser.parse_args()
    config = Config(**json.loads(args.config.read_text()))
    if args.action == "system-config":
        print(json.dumps(config.native_config()))
        return 0
    if args.action == "_storage-session":
        session = _stored_session(config)
        args.output.write_text(json.dumps(session.decode() if session else None))
        return 0
    if args.action == "_probe":
        try:
            current = _probe(config, args.address)
        except ValueError as error:
            current = {"error": str(error)}
        args.output.write_text(json.dumps(current))
        return 0
    if args.action == "start-head":
        if args.node_ip is None or args.temp_dir is None:
            parser.error("start-head requires --node-ip and --temp-dir")
        start_head(config, args.config.resolve(), args.node_ip, args.expected_session,
                   args.temp_dir.expanduser().resolve(), args.timeout_s)
        return 0
    if args.report is None:
        parser.error("supervise requires --report")
    return supervise(config, args.config.resolve(), args.report.resolve(), args.timeout_s)


if __name__ == "__main__":
    sys.exit(main())
