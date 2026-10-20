"""Root-owned forced SSH command; controls only the experiment Ray service."""
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import time
from pathlib import Path

CONFIG = Path("/etc/ray-gcp.json")
UNIT = "ray-exp-node.service"
ENV = "/opt/ray-env/ray-dev/bin"
REPO = Path("/opt/ray-work/ray")
HASH_PATHS = [
    "python/ray/train/v2/_internal/data_integration/step_aligned_input.py",
    "python/ray/train/v2/_internal/execution/worker_group/worker_group.py",
    "python/ray/train/v2/_internal/callbacks/datasets.py",
    "python/ray/train/v2/examples/pytorch/torchft_linear_example.py",
    "python/ray/train/v2/torch/torchft_config.py",
]


def run(command, check=True):
    return subprocess.run(
        command, check=check, text=True, capture_output=True, timeout=60
    )


def active():
    return run(["systemctl", "is-active", UNIT], False).stdout.strip()


def stop():
    status = active()
    if status not in ("inactive", "unknown", "failed", ""):
        run(["systemctl", "stop", UNIT])
    if active() in ("active", "activating", "deactivating"):
        raise RuntimeError("Ray service did not stop")
    run(["systemctl", "reset-failed", UNIT], False)


def main():
    config = json.loads(CONFIG.read_text())
    request = json.load(sys.stdin)
    if request.get("run_id") != config["run_id"]:
        raise ValueError("Experiment identity mismatch")
    action = request["action"]
    if action == "identity":
        import ray
        import ray._raylet

        if not Path(ray.__file__).resolve().is_relative_to(REPO / "python"):
            raise RuntimeError("Ray is not imported from the compiled checkout")
        result = dict(
            name=config["name"],
            ip=config["ip"],
            python=sys.executable,
            ray_file=ray.__file__,
            cpu_count=os.cpu_count(),
            versions={
                p: importlib.metadata.version(p)
                for p in ["ray", "torch", "torchft", "numpy", "pyarrow"]
            },
            git_commit=run(
                [
                    "git",
                    "-c",
                    "safe.directory=" + str(REPO),
                    "-C",
                    str(REPO),
                    "rev-parse",
                    "HEAD",
                ]
            ).stdout.strip(),
            source_sha256={
                p: hashlib.sha256((REPO / p).read_bytes()).hexdigest()
                for p in HASH_PATHS
            },
            native_sha256=hashlib.sha256(
                Path(ray._raylet.__file__).read_bytes()
            ).hexdigest(),
        )
    elif action == "start":
        stop()
        cmd = [
            "systemd-run",
            "--quiet",
            "--collect",
            "--unit=" + UNIT,
            "--uid=raybench",
            "--gid=raybench",
            "--property=KillMode=control-group",
            "--property=TimeoutStopSec=10",
            "--property=Restart=no",
            "--setenv=PATH=" + ENV + ":/usr/bin:/bin",
            "--setenv=RAY_TRAIN_V2_ENABLED=1",
            "--setenv=RAY_USAGE_STATS_ENABLED=0",
            "--setenv=OMP_NUM_THREADS=1",
            "--setenv=MKL_NUM_THREADS=1",
            "--setenv=OPENBLAS_NUM_THREADS=1",
            ENV + "/ray",
            "start",
            "--block",
            "--disable-usage-stats",
            "--node-ip-address=" + config["ip"],
            "--object-store-memory=1073741824",
            "--num-cpus=" + ("2" if config["head"] else "4"),
        ]
        if config["head"]:
            cmd += [
                "--head",
                "--port=6379",
                "--include-dashboard=false",
                "--temp-dir=/tmp/ray-gcp",
            ]
        else:
            cmd += [
                "--address=" + config["head_ip"] + ":6379",
                '--resources={"train_slot":1}',
            ]
        run(cmd)
        result = {"started": True, "name": config["name"]}
    elif action == "kill":
        if config["head"]:
            raise ValueError("Head failure is outside this experiment")
        if active() != "active":
            raise RuntimeError("Cannot inject failure into an inactive service")
        run(["systemctl", "kill", "--kill-whom=all", "--signal=SIGKILL", UNIT])
        deadline = time.monotonic() + 15
        while active() in ("active", "activating", "deactivating"):
            if time.monotonic() > deadline:
                raise RuntimeError("Worker service remained active after SIGKILL")
            time.sleep(0.1)
        result = {"killed": True, "name": config["name"], "active": active()}
    elif action == "stop":
        stop()
        result = {"stopped": True}
    elif action == "logs":
        result = {
            "journal": run(
                ["journalctl", "-u", UNIT, "--no-pager", "-n", "2000"]
            ).stdout
        }
    else:
        raise ValueError("Unsupported action")
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
