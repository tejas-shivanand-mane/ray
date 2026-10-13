"""Fresh physical Ray cluster per observation inside an existing Slurm allocation.

Only this benchmark's numeric Slurm step IDs are cancelled. No ray stop, SSH,
machine shutdown, heartbeat tuning, or automatic restart of a failed host.
"""
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from types import SimpleNamespace


def cancel_steps(folder):
    job = os.environ["SLURM_JOB_ID"]
    errors = []
    for path in Path(folder).glob("slurm/node-*/identity.json"):
        identity = json.loads(path.read_text())
        step = identity["step"]
        if not re.fullmatch(re.escape(job) + r"\.[0-9]+", step):
            raise RuntimeError(f"Refusing foreign/non-numeric step {step}")
        result = subprocess.run(["scancel", "--signal=KILL", step],
                                capture_output=True, text=True, timeout=20)
        if result.returncode and "Invalid job id" not in result.stderr:
            errors.append(result.stderr.strip())
    if errors:
        raise RuntimeError(f"Slurm cleanup failed: {errors}")


class SlurmCluster:
    def __init__(self, folder):
        self.folder = Path(folder)
        self.processes = []
        self.nodes = []
        self.metadata = {}
        self.job = os.environ["SLURM_JOB_ID"]

    def launch(self, host, index, address=None):
        directory = self.folder / "slurm" / f"node-{index}"
        directory.mkdir(parents=True)
        log = (directory / "ray-start.log").open("w")
        command = ["srun", "--overlap", "--exact", "--cpu-bind=none",
                   "--nodes=1", "--ntasks=1", "--cpus-per-task=4",
                   "--kill-on-bad-exit=0", "--nodelist", host,
                   sys.executable, str(Path(__file__).resolve()), "node",
                   str(directory)]
        if address:
            command += [address]
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append((process, log))
        identity = directory / "identity.json"
        deadline = time.monotonic() + 90
        while not identity.exists():
            if process.poll() is not None:
                raise RuntimeError(f"Node launcher exited; inspect {directory}")
            if time.monotonic() > deadline:
                raise TimeoutError(f"Slurm step did not start on {host}")
            time.sleep(.2)
        result = json.loads(identity.read_text())
        result["host"] = host
        result["process"] = process
        return result

    def start(self):
        import ray
        hosts = subprocess.check_output(
            ["scontrol", "show", "hostnames", os.environ["SLURM_JOB_NODELIST"]],
            text=True).split()
        if not 4 <= len(hosts) <= 6 or int(os.environ.get("SLURM_CPUS_PER_TASK", 0)) != 4:
            raise RuntimeError("Require 4-6 nodes and --cpus-per-task=4")
        if socket.gethostname().split(".")[0] != hosts[0].split(".")[0]:
            raise RuntimeError("Run the driver in the batch script on the first allocated host")
        head = self.launch(hosts[0], 0)
        self.address = head["address"]
        deadline = time.monotonic() + 90
        ip, port = self.address.rsplit(":", 1)
        while True:
            try:
                with socket.create_connection((ip, int(port)), timeout=1):
                    break
            except OSError:
                if head["process"].poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("Head failed to start; inspect node-0/ray-start.log")
                time.sleep(.2)
        ray.init(address=self.address)
        identities = [self.launch(host, i, self.address)
                      for i, host in enumerate(hosts[1:], 1)]
        deadline = time.monotonic() + 90
        while True:
            alive = [n for n in ray.nodes() if n["Alive"]]
            if len(alive) == len(hosts):
                break
            if any(i["process"].poll() is not None for i in identities):
                raise RuntimeError("Worker Ray launcher exited during startup")
            if time.monotonic() > deadline:
                raise TimeoutError("Not all physical workers registered")
            time.sleep(.2)
        for identity in identities:
            matches = [n for n in alive if n["NodeManagerAddress"] == identity["ip"]]
            if len(matches) != 1 or matches[0]["Resources"].get("train_slot") != 1:
                raise RuntimeError(f"Ambiguous/missing worker registration: {identity['host']}")
            self.nodes.append(SimpleNamespace(node_id=matches[0]["NodeID"],
                                              step=identity["step"], host=identity["host"]))
        versions = [{k: v for k, v in identity.items() if k != "process"}
                    for identity in [head] + identities]
        if len({(i["ray_version"], i["ray_file"], i["torch_version"], i["python"])
                for i in versions}) != 1:
            raise RuntimeError("Head/worker Python or Ray/Torch installations differ")
        self.metadata = {"hosts": hosts, "installations": versions, "cpus_per_node": 4,
                         "training_workers": 2, "training_cpus_per_worker": 3,
                         "spare_worker_hosts": len(hosts) - 3,
                         "job_id": self.job, "address": self.address,
                         "node_mapping": [vars(n) for n in self.nodes],
                         "fault_model": "SIGKILL entire Ray worker Slurm step; physical host stays up",
                         "clock": "recorder arrival timestamps and driver on the surviving head"}
        return self.nodes

    def remove_node(self, node, allow_graceful=False):
        if allow_graceful:
            raise ValueError("Only abrupt failures are supported")
        if node not in self.nodes:
            raise ValueError("Unknown worker")
        subprocess.run(["scancel", "--signal=KILL", node.step], check=True, timeout=20)
        self.metadata["killed_node"] = vars(node)

    def shutdown(self):
        cancel_steps(self.folder)
        for process, log in self.processes:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            log.close()


def node_main(directory, address=None):
    directory = Path(directory)
    ip = socket.gethostbyname(socket.gethostname())
    step = os.environ["SLURM_JOB_ID"] + "." + os.environ["SLURM_STEP_ID"]
    if address is None:
        with socket.socket() as sock:
            sock.bind((ip, 0))
            port = sock.getsockname()[1]
        address = f"{ip}:{port}"
    identity = {"step": step, "ip": ip, "address": address,
                "python": sys.executable}
    import ray
    import torch
    import torchft
    from ray.train.v2._internal.data_integration.step_aligned_input import StepAlignedDataConfig
    identity.update(ray_version=ray.__version__, ray_file=ray.__file__,
                    torch_version=torch.__version__, torchft_file=torchft.__file__)
    temporary = directory / "identity.tmp"
    temporary.write_text(json.dumps(identity, indent=2))
    temporary.replace(directory / "identity.json")
    head = directory.name == "node-0"
    command = [str(Path(sys.executable).with_name("ray")), "start",
               "--node-ip-address=" + ip, "--num-cpus=" + ("2" if head else "4"),
               "--object-store-memory=1073741824", "--disable-usage-stats", "--block"]
    if head:
        command += ["--head", "--port=" + address.rsplit(":", 1)[1],
                    "--include-dashboard=false",
                    "--temp-dir=/tmp/ray-ucr-" + step]
    else:
        command += ["--address=" + address, '--resources={"train_slot":1}']
    os.execv(command[0], command)


if __name__ == "__main__":
    if sys.argv[1] == "cleanup":
        cancel_steps(sys.argv[2])
    else:
        node_main(*sys.argv[2:])
