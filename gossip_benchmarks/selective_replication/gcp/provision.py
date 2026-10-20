"""Notebook controller. Requires authenticated gcloud + SSH tools on notebook host.
Never modifies the saved image. Cloud resources are recorded before creation.
"""
import base64
import json
import shlex
import shutil
import subprocess
import time
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from gcp.capabilities import stop_command

SOURCE_COMMIT = "dc4703753c8f3d52e74c389c72449dacc27b41f3"
PYTHON = "/opt/ray-env/ray-dev/bin/python"
ROOT = "/opt/ray-gcp"
SHARED = "/srv/ray-gcp"


def run(cmd, timeout=600, check=True):
    p = subprocess.run(
        [str(c) for c in cmd], capture_output=True, text=True, timeout=timeout
    )
    if check and p.returncode:
        raise RuntimeError(
            shlex.join([str(c) for c in cmd])
            + "\n"
            + p.stdout[-4000:]
            + "\n"
            + p.stderr[-6000:]
        )
    return p


def parallel(function, items, workers=5):
    # Inspect every result, including exceptions, before raising.
    results, errors = {}, []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(function, item): item for item in items}
        for f in as_completed(futures):
            try:
                results[futures[f]] = f.result()
            except Exception as exc:
                errors.append(str(exc))
    if errors:
        raise RuntimeError("\n".join(errors))
    return results


class FreshCluster:
    def __init__(
        self,
        project="research-488322",
        zone="us-central1-a",
        image="ray-built-v1",
        workdir="ray_gcp_results",
    ):
        self.bundle = Path(__file__).parent.resolve() / "bootstrap"
        self.local = Path(workdir).expanduser().resolve()
        self.local.mkdir(parents=True, exist_ok=True)
        token = (
            datetime.now(timezone.utc).strftime("%m%d-%H%M%S")
            + "-"
            + uuid.uuid4().hex[:6]
        )
        ident = "rayexp-" + token
        self.state_path = self.local / (ident + "-state.json")
        self.state = dict(
            project=project,
            zone=zone,
            image=image,
            run_id=ident,
            names=[ident + "-head"] + [ident + f"-w{i}" for i in range(1, 5)],
            network=ident,
            subnet=ident + "-subnet",
            firewall=[ident + "-internal", ident + "-iap"],
            region=zone.rsplit("-", 1)[0],
            cidr="10.90.0.0/24",
            suites={},
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self.save()
        self.project, self.zone = self.state["project"], self.state["zone"]
        self.run_id = self.state["run_id"]
        self.names = self.state["names"]
        self.head = self.names[0]
        self.key = self.local / (self.run_id + "-key")
        print("Experiment:", self.run_id, "\nState file:", self.state_path)

    def save(self):
        temp = self.state_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self.state, indent=2))
        temp.replace(self.state_path)

    def gc(self, *args, timeout=600, check=True):
        return run(
            ["gcloud", *args, "--project=" + self.project, "--quiet"], timeout, check
        )

    def ssh(self, name, command, timeout=600, check=True):
        return self.gc(
            "compute",
            "ssh",
            name,
            "--zone=" + self.zone,
            "--tunnel-through-iap",
            "--command=" + command,
            "--ssh-flag=-oConnectTimeout=15",
            timeout=timeout,
            check=check,
        )

    def scp(self, source, destination, timeout=600):
        return self.gc(
            "compute",
            "scp",
            str(source),
            str(destination),
            "--zone=" + self.zone,
            "--tunnel-through-iap",
            timeout=timeout,
        )

    def _check_owner(self, name):
        if name not in self.names:
            raise ValueError("Unrecorded VM")
        p = self.gc(
            "compute",
            "instances",
            "describe",
            name,
            "--zone=" + self.zone,
            "--format=json",
            check=False,
        )
        if p.returncode:
            if "not found" in p.stderr.lower() or "was not found" in p.stderr.lower():
                return None
            raise RuntimeError(p.stderr)
        instance = json.loads(p.stdout)
        if instance.get("labels", {}).get("ray-experiment") != self.run_id:
            raise RuntimeError("Refusing operation on foreign VM " + name)
        return instance

    def provision(self):
        if self.state.get("provision_started"):
            raise RuntimeError(
                "Provisioning was already attempted. Use setup() if VMs exist, or cleanup before a new experiment."
            )
        for executable in ("gcloud", "ssh", "ssh-keygen"):
            if not shutil.which(executable):
                raise RuntimeError(
                    "Install " + executable + " on the notebook host first"
                )
        self.state["stop_command"] = stop_command()
        image = json.loads(
            self.gc(
                "compute", "images", "describe", self.state["image"], "--format=json"
            ).stdout
        )
        if image.get("status") != "READY":
            raise RuntimeError("Image is not READY")
        self.state["image_id"] = image["id"]
        self.state["image_self_link"] = image["selfLink"]
        self.state["provision_started"] = True
        self.save()
        print("Creating isolated network and five e2-standard-4 VMs ...", flush=True)
        self.gc(
            "compute",
            "networks",
            "create",
            self.state["network"],
            "--subnet-mode=custom",
            "--description=" + self.run_id,
        )
        self.gc(
            "compute",
            "networks",
            "subnets",
            "create",
            self.state["subnet"],
            "--network=" + self.state["network"],
            "--region=" + self.state["region"],
            "--range=" + self.state["cidr"],
        )
        self.gc(
            "compute",
            "firewall-rules",
            "create",
            self.state["firewall"][0],
            "--network=" + self.state["network"],
            "--allow=tcp,udp,icmp",
            "--source-tags=" + self.run_id,
            "--target-tags=" + self.run_id,
        )
        self.gc(
            "compute",
            "firewall-rules",
            "create",
            self.state["firewall"][1],
            "--network=" + self.state["network"],
            "--allow=tcp:22",
            "--source-ranges=35.235.240.0/20",
            "--target-tags=" + self.run_id,
        )
        self.gc(
            "compute",
            "instances",
            "create",
            *self.names,
            "--zone=" + self.zone,
            "--machine-type=e2-standard-4",
            "--provisioning-model=STANDARD",
            "--image=" + self.state["image"],
            "--image-project=" + self.project,
            "--boot-disk-type=pd-balanced",
            "--boot-disk-size=" + str(image["diskSizeGb"]) + "GB",
            "--subnet=" + self.state["subnet"],
            "--tags=" + self.run_id,
            "--labels=ray-experiment=" + self.run_id,
            "--metadata=enable-oslogin=FALSE",
            "--no-service-account",
            "--no-scopes",
            timeout=900,
        )
        nodes = []
        for name in self.names:
            instance = self._check_owner(name)
            nodes.append(
                dict(
                    name=name,
                    ip=instance["networkInterfaces"][0]["networkIP"],
                    head=name == self.head,
                )
            )
        self.state["nodes"] = nodes
        self.save()
        print(
            "VMs created. Preparing shared storage and environment checks ...",
            flush=True,
        )
        self.setup()

    def _wait_ssh(self, name):
        deadline = time.monotonic() + 240
        last = ""
        while time.monotonic() < deadline:
            try:
                p = self.ssh(name, "true", timeout=60, check=False)
                if p.returncode == 0:
                    return
                last = p.stderr
            except subprocess.TimeoutExpired:
                last = "SSH connection attempt timed out"
            time.sleep(5)
        raise RuntimeError(
            name
            + " SSH unavailable. Check IAP Tunnel User / Compute permissions and OS Login policy.\n"
            + last
        )

    def setup(self):
        if "nodes" not in self.state:
            # Recover descriptors after a partial batch create.
            nodes = []
            for name in self.names:
                obj = self._check_owner(name)
                if not obj or obj["status"] != "RUNNING":
                    raise RuntimeError("All five recorded VMs must be running")
                nodes.append(
                    dict(
                        name=name,
                        ip=obj["networkInterfaces"][0]["networkIP"],
                        head=name == self.head,
                    )
                )
            self.state["nodes"] = nodes
            self.save()
        for name in self.names:
            self._check_owner(name)
        # First connection sequentially initializes gcloud's SSH key once.
        self._wait_ssh(self.head)
        parallel(self._wait_ssh, self.names[1:])
        if not self.key.exists():
            run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-N",
                    "",
                    "-f",
                    self.key,
                    "-C",
                    self.run_id,
                ]
            )
        archive = self.local / (self.run_id + "-harness.zip")
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
            for p in self.bundle.rglob("*.py"):
                z.write(p, p.relative_to(self.bundle))
        nodes = self.state["nodes"]
        head_ip = nodes[0]["ip"]
        public_key = self.key.with_suffix(self.key.suffix + ".pub").read_text().strip()
        # Dedicated, identical UID permits NFS access across hosts.
        base_setup = """set -e
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-common unzip
if ! id raybench >/dev/null 2>&1; then
  sudo useradd --uid 25001 --create-home --home-dir /var/lib/raybench --shell /bin/bash raybench
fi
test "$(id -u raybench)" = 25001
sudo mkdir -p /opt/ray-gcp /srv/ray-gcp
mountpoint -q /srv/ray-gcp || sudo chown raybench:raybench /srv/ray-gcp
"""

        def setup_one(name):
            node = next(n for n in nodes if n["name"] == name)
            config = dict(
                node,
                nodes=nodes,
                head_ip=head_ip,
                run_id=self.run_id,
                source_commit=SOURCE_COMMIT,
            )
            config_path = self.local / (name + "-config.json")
            config_path.write_text(json.dumps(config))
            self.ssh(name, base_setup, timeout=600)
            self.scp(archive, name + ":/tmp/" + self.run_id + ".zip")
            self.scp(config_path, name + ":/tmp/" + self.run_id + ".json")
            self.ssh(
                name,
                "set -e\n"
                + f"sudo unzip -oq /tmp/{self.run_id}.zip -d {ROOT}\n"
                + f"sudo install -o root -g root -m 644 /tmp/{self.run_id}.json /etc/ray-gcp.json\n"
                + f"sudo chown -R root:root {ROOT}\n"
                + f"sudo chmod -R a+rX,go-w {ROOT}\n"
                + f"rm /tmp/{self.run_id}.zip /tmp/{self.run_id}.json\n",
            )
            if not node["head"]:
                # Key can only execute the small root-owned node controller,
                # and only from the experiment's head internal IP.
                forced = f'from="{head_ip}",restrict,command="{PYTHON} {ROOT}/node_control.py" {public_key}\n'
                encoded = base64.b64encode(forced.encode()).decode()
                self.ssh(
                    name,
                    "set -e\nsudo mkdir -p /root/.ssh\nsudo chmod 700 /root/.ssh\n"
                    + f"printf %s {shlex.quote(encoded)} | base64 -d | sudo tee -a /root/.ssh/authorized_keys >/dev/null\n"
                    + "sudo chmod 600 /root/.ssh/authorized_keys\n",
                )
            return self.ssh(
                name, "cat /etc/ssh/ssh_host_ed25519_key.pub"
            ).stdout.strip()

        hostkeys = parallel(setup_one, self.names)
        exports = (
            SHARED
            + " "
            + " ".join(
                n["ip"] + "(rw,sync,no_subtree_check,root_squash,fsid=0)"
                for n in nodes[1:]
            )
            + "\n"
        )
        encoded = base64.b64encode(exports.encode()).decode()
        sudoers = f"raybench ALL=(root) NOPASSWD: {PYTHON} {ROOT}/node_control.py\n"
        encoded_sudo = base64.b64encode(sudoers.encode()).decode()
        self.ssh(
            self.head,
            "set -e\nsudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server\nsudo install -d -m 755 /etc/exports.d\n"
            + f"printf %s {shlex.quote(encoded)} | base64 -d | sudo tee /etc/exports.d/ray-gcp.exports >/dev/null\n"
            + "sudo exportfs -ra\nsudo systemctl enable --now nfs-server\n"
            + f"printf %s {shlex.quote(encoded_sudo)} | base64 -d | sudo tee /etc/sudoers.d/ray-gcp >/dev/null\n"
            + "sudo chmod 440 /etc/sudoers.d/ray-gcp\nsudo visudo -cf /etc/sudoers.d/ray-gcp\n"
            + "sudo install -d -o raybench -g raybench -m 700 /var/lib/raybench/.ssh\n",
        )
        self.scp(self.key, self.head + ":/tmp/" + self.run_id + "-key")
        known = self.local / (self.run_id + "-known-hosts")
        known.write_text(
            "".join(n["ip"] + " " + hostkeys[n["name"]] + "\n" for n in nodes[1:])
        )
        self.scp(known, self.head + ":/tmp/" + self.run_id + "-known")
        self.ssh(
            self.head,
            "set -e\n"
            + f"sudo install -o raybench -g raybench -m 600 /tmp/{self.run_id}-key /var/lib/raybench/.ssh/id_ed25519\n"
            + f"sudo install -o raybench -g raybench -m 600 /tmp/{self.run_id}-known /var/lib/raybench/.ssh/known_hosts\n"
            + f"rm /tmp/{self.run_id}-key /tmp/{self.run_id}-known\n",
        )

        def mount(name):
            self.ssh(
                name,
                "set -e\n"
                + f"printf '%s\\n' '{head_ip}:/ {SHARED} nfs4 vers=4.2,hard,_netdev 0 0' | sudo tee -a /etc/fstab >/dev/null\n"
                + f"mountpoint -q {SHARED} || sudo mount {SHARED}\n"
                + f"sudo -u raybench touch {SHARED}/write-check-{name}\n",
            )

        parallel(mount, self.names[1:])
        # Direct SSH must work before any long-running job or billable sweep.
        check_code = (
            "from _support.gcp_cluster import command; import json; "
            + 'c=json.load(open("/etc/ray-gcp.json")); '
            + 'print(json.dumps([command(n,"identity") for n in c["nodes"]]))'
        )
        result = self.ssh(
            self.head,
            f"cd {ROOT} && sudo -u raybench {PYTHON} -c " + shlex.quote(check_code),
            timeout=600,
        )
        identities = json.loads(result.stdout)
        if any(i["git_commit"] != SOURCE_COMMIT for i in identities):
            raise RuntimeError(
                "Image checkout differs from the inspected benchmark commit; inspect identity report"
            )
        self.state["identities"] = identities
        self.state["ready"] = True
        self.save()
        print(
            "Five VMs ready. Ray/TorchFT imports, direct SSH and shared storage passed.",
            flush=True,
        )

    def cleanup(self, confirm=None, delete_network=True, allow_uncollected=False):
        """Delete ONLY manifest-named, label-matching VMs and their boot disks."""
        if confirm != self.run_id:
            raise ValueError(
                "Pass confirm=cluster.run_id to explicitly delete this experiment"
            )
        if not allow_uncollected:
            raise RuntimeError(
                "Download selective suite evidence first; pass allow_uncollected=True to acknowledge deletion of head storage"
            )
        existing = [n for n in self.names if self._check_owner(n) is not None]
        if existing:
            self.gc(
                "compute",
                "instances",
                "delete",
                *existing,
                "--zone=" + self.zone,
                "--delete-disks=boot",
                timeout=900,
            )
        if delete_network:
            # Names are unique and stored before creation; absence is harmless.
            for firewall in self.state["firewall"]:
                p = self.gc(
                    "compute",
                    "firewall-rules",
                    "describe",
                    firewall,
                    "--format=json",
                    check=False,
                )
                if p.returncode == 0:
                    obj = json.loads(p.stdout)
                    if obj["network"].rsplit("/", 1)[-1] != self.state["network"]:
                        raise RuntimeError("Foreign firewall rule")
                    self.gc("compute", "firewall-rules", "delete", firewall)
                elif "not found" not in p.stderr.lower():
                    raise RuntimeError(p.stderr)
            p = self.gc(
                "compute",
                "networks",
                "subnets",
                "describe",
                self.state["subnet"],
                "--region=" + self.state["region"],
                "--format=json",
                check=False,
            )
            if p.returncode == 0:
                if (
                    json.loads(p.stdout)["network"].rsplit("/", 1)[-1]
                    != self.state["network"]
                ):
                    raise RuntimeError("Foreign subnet")
                self.gc(
                    "compute",
                    "networks",
                    "subnets",
                    "delete",
                    self.state["subnet"],
                    "--region=" + self.state["region"],
                )
            elif "not found" not in p.stderr.lower():
                raise RuntimeError(p.stderr)
            p = self.gc(
                "compute",
                "networks",
                "describe",
                self.state["network"],
                "--format=json",
                check=False,
            )
            if p.returncode == 0:
                if json.loads(p.stdout).get("description") != self.run_id:
                    raise RuntimeError("Foreign network")
                self.gc("compute", "networks", "delete", self.state["network"])
            elif "not found" not in p.stderr.lower():
                raise RuntimeError(p.stderr)
        self.state["deleted_at"] = datetime.now(timezone.utc).isoformat()
        self.save()
        print(
            "Experiment VMs, boot disks and requested network resources deleted. Image retained:",
            self.state["image"],
        )
