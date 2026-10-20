"""Physical GCP transport for the matched upstream benchmark.
The driver and recorder share the surviving head clock. No heartbeat tuning.
"""
import json
import subprocess
from pathlib import Path

CONFIG = Path("/etc/ray-gcp.json")


def command(node, action):
    c = json.loads(CONFIG.read_text())
    payload = json.dumps({"run_id": c["run_id"], "action": action})
    if node["head"]:
        cmd = [
            "sudo",
            "-n",
            "/opt/ray-env/ray-dev/bin/python",
            "/opt/ray-gcp/node_control.py",
        ]
    else:
        cmd = [
            "ssh",
            "-i",
            "/var/lib/raybench/.ssh/id_ed25519",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "UserKnownHostsFile=/var/lib/raybench/.ssh/known_hosts",
            "root@" + node["ip"],
        ]
    p = subprocess.run(cmd, input=payload, capture_output=True, text=True, timeout=120)
    if p.returncode:
        raise RuntimeError(f"{node['name']} {action}: {p.stderr[-3000:]}")
    return json.loads(p.stdout)
