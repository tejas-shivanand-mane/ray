"""External GCP supervisor: reuse five recorded VMs; never create/delete a VM."""

import hashlib
import json
import re
import shlex
import subprocess
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from evidence import write_json

PYTHON = "/opt/ray-env/ray-dev/bin/python"
BIN = "/opt/ray-env/ray-dev/bin"
ROOT = "/opt/ray-headloss"
SHARED = "/srv/ray-headloss"
NODE_UNIT = "ray-headloss-node.service"
DRIVER_UNIT = "ray-headloss-driver.service"
SOURCE = "dc4703753c8f3d52e74c389c72449dacc27b41f3"


def run(command, timeout=180, check=True):
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(
            shlex.join(command) + "\n" + result.stderr[-4000:] + result.stdout[-4000:]
        )
    return result


def parallel(function, items):
    # Wait for and inspect every operation, even when one host fails.
    errors = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(function, item) for item in items]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                errors.append(str(exc))
    if errors:
        raise RuntimeError("\n".join(errors))


class HeadVMLoss:
    def __init__(self, state_file, workdir="ray_gcp_results/head-vm-loss"):
        if not state_file:
            raise ValueError(
                "An existing STATE_FILE is required; this runner never provisions"
            )
        self.state = json.loads(Path(state_file).read_text())
        self.project, self.zone = self.state["project"], self.state["zone"]
        self.run_id = self.state["run_id"]
        if not re.fullmatch(r"rayexp-[a-z0-9-]+", self.run_id):
            raise ValueError("Invalid experiment identity")
        self.names = self.state["names"]
        if self.names != [self.run_id + "-head"] + [
            self.run_id + f"-w{i}" for i in range(1, 5)
        ]:
            raise ValueError("Expected the original five-VM layout")
        self.head, *workers = self.names
        self.trainers, self.replacement, self.support = (
            workers[:2],
            workers[2],
            workers[3],
        )
        self.local = Path(workdir).resolve() / self.run_id
        self.local.mkdir(parents=True, exist_ok=True)
        self.bundle = Path(__file__).parent.resolve()
        self.nodes = {}
        self.ready = False

    def gc(self, *args, **kwargs):
        return run(["gcloud", *args, "--project=" + self.project, "--quiet"], **kwargs)

    def ssh(self, name, command, **kwargs):
        if name not in self.names:
            raise ValueError("Unknown experiment VM")
        return self.gc(
            "compute",
            "ssh",
            name,
            "--zone=" + self.zone,
            "--tunnel-through-iap",
            "--ssh-flag=-oConnectTimeout=10",
            "--command=" + command,
            **kwargs,
        )

    def scp(self, source, target, recursive=False):
        return self.gc(
            "compute",
            "scp",
            str(source),
            str(target),
            "--zone=" + self.zone,
            "--tunnel-through-iap",
            *(["--recurse"] if recursive else []),
            timeout=600,
        )

    def describe(self, name):
        if name not in self.names:
            raise ValueError("Unrecorded VM")
        obj = json.loads(
            self.gc(
                "compute",
                "instances",
                "describe",
                name,
                "--zone=" + self.zone,
                "--format=json",
            ).stdout
        )
        if obj.get("labels", {}).get("ray-experiment") != self.run_id:
            raise RuntimeError("Foreign VM: " + name)
        if name in self.nodes and str(obj["id"]) != str(self.nodes[name]["id"]):
            raise RuntimeError("Instance identity changed: " + name)
        return obj

    def inventory(self):
        # Includes any earlier cluster; no creation/deletion follows this query.
        result = json.loads(
            self.gc("compute", "instances", "list", "--format=json").stdout
        )
        return [
            {
                "name": v["name"],
                "status": v["status"],
                "zone": v["zone"].split("/")[-1],
                "experiment": v.get("labels", {}).get("ray-experiment"),
            }
            for v in result
            if v.get("labels", {}).get("ray-experiment")
        ]

    def _remote_json(self, path):
        command = f"if test -f {shlex.quote(str(path))}; then cat {shlex.quote(str(path))}; else printf null; fi"
        return json.loads(self.ssh(self.support, command, timeout=45).stdout)

    def _upload_json(self, value, remote):
        path = self.local / "upload.json"
        write_json(path, value)
        self.scp(path, self.support + ":/tmp/ray-headloss-config.json")
        self.ssh(
            self.support,
            "sudo install -o raybench -g raybench -m 644 "
            "/tmp/ray-headloss-config.json " + shlex.quote(str(remote)),
        )

    def prepare(self):
        """Move storage/supervision off head without editing the original harness."""
        print(json.dumps(self.inventory(), indent=2), flush=True)
        for name in self.names:
            obj = self.describe(name)
            if obj["status"] != "RUNNING":
                raise RuntimeError(
                    "Preparation requires all five recorded VMs RUNNING: " + name
                )
            self.nodes[name] = dict(
                id=obj["id"], ip=obj["networkInterfaces"][0]["networkIP"]
            )
            active = self.ssh(
                name,
                "systemctl list-units 'ray-exp-driver-*' 'ray-headloss-driver*' "
                "--state=active,activating --no-legend",
            ).stdout
            if active.strip():
                raise RuntimeError("A suite is active on " + name)
        if self.ssh(
            self.support, "test ! -d /var/lib/raybench/ray-headloss-lock", check=False
        ).returncode:
            raise RuntimeError(
                "A head-loss supervisor owns the cluster; inspect its report before cleanup"
            )
        # Check CLI support; never silently downgrade to a service kill or graceful stop.
        help_text = run(["gcloud", "compute", "instances", "stop", "--help"]).stdout
        if "--no-graceful-shutdown" not in help_text:
            raise RuntimeError("Update gcloud: VM loss requires --no-graceful-shutdown")
        identities = []
        for name in self.names:
            request = shlex.quote(
                json.dumps(dict(run_id=self.run_id, action="identity"))
            )
            result = self.ssh(
                name,
                f"printf %s {request} | sudo {PYTHON} /opt/ray-gcp/node_control.py",
            )
            identities.append(json.loads(result.stdout))
        comparable = [
            {k: v for k, v in item.items() if k not in ("name", "ip")}
            for item in identities
        ]
        if any(item != comparable[0] for item in comparable) or any(
            i["git_commit"] != SOURCE or i["cpu_count"] != 4 for i in identities
        ):
            raise RuntimeError(
                "Image/source/native build/package identities differ from the expected setup"
            )
        self.identities = identities
        # Cleanly fence the old, idle harness before unmounting its head-hosted NFS.
        def stop_old(name):
            request = shlex.quote(json.dumps(dict(run_id=self.run_id, action="stop")))
            self.ssh(
                name,
                f"printf %s {request} | sudo {PYTHON} /opt/ray-gcp/node_control.py",
            )
            self._stop_node(name)
            if name != self.head:
                self.ssh(
                    name,
                    "if mountpoint -q /srv/ray-gcp; then sudo umount /srv/ray-gcp; fi",
                )

        parallel(stop_old, self.names)
        # Different mount path/export; old checkpoints and suites are untouched.
        self.ssh(
            self.support,
            "set -e\nsudo apt-get update -qq\n"
            "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nfs-kernel-server\n"
            f"sudo install -d -o raybench -g raybench {SHARED}\n",
            timeout=600,
        )
        clients = " ".join(
            self.nodes[n]["ip"] + "(rw,sync,no_subtree_check,root_squash)"
            for n in self.names
            if n != self.support
        )
        exports = shlex.quote(SHARED + " " + clients + "\n")
        self.ssh(
            self.support,
            f"set -e\nprintf %s {exports} | sudo tee /etc/exports.d/ray-headloss.exports >/dev/null\n"
            "sudo exportfs -ra\nsudo systemctl start nfs-server",
        )

        def mount(name):
            self.ssh(
                name,
                f"set -e\nsudo mkdir -p {SHARED}\n"
                f"if mountpoint -q {SHARED}; then sudo umount {SHARED}; fi\n"
                f"sudo mount -t nfs -o vers=3,proto=tcp,hard {self.nodes[self.support]['ip']}:{SHARED} {SHARED}\n"
                f"sudo -u raybench touch {SHARED}/write-check-{name}",
            )

        parallel(mount, [n for n in self.names if n != self.support])
        self.ssh(self.support, f"sudo mkdir -p {ROOT}")
        for name in ("train.py", "evidence.py"):
            self.scp(self.bundle / name, self.support + ":/tmp/headloss-" + name)
            self.ssh(
                self.support, f"sudo install -m 644 /tmp/headloss-{name} {ROOT}/{name}"
            )
        self.ready = True
        write_json(
            self.local / "environment.json",
            dict(
                nodes=self.nodes,
                identities=identities,
                roles=dict(
                    original_head=self.head,
                    replacement_head=self.replacement,
                    support=self.support,
                    trainers=self.trainers,
                ),
                source_sha256={
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in self.bundle.glob("*.py")
                },
            ),
        )

    def _stop_node(self, name):
        # Unknown/inactive transient units are harmless; inability to stop active ones is not.
        self.ssh(
            name,
            f"set -e\nfor unit in {DRIVER_UNIT} {NODE_UNIT}; do\n"
            "if systemctl is-active --quiet $unit; then sudo systemctl stop $unit; fi\n"
            "sudo systemctl reset-failed $unit >/dev/null 2>&1 || true\ndone",
        )

    def _start_cluster(self, head):
        ip = self.nodes[head]["ip"]
        hosts = [head, self.support, *self.trainers]

        def start(name):
            is_head = name == head
            cmd = [
                "sudo",
                "systemd-run",
                "--quiet",
                "--collect",
                "--unit=" + NODE_UNIT,
                "--uid=raybench",
                "--gid=raybench",
                "--property=KillMode=control-group",
                "--property=TimeoutStopSec=10",
                "--property=Restart=no",
                "--setenv=PATH=" + BIN + ":/usr/bin:/bin",
                "--setenv=RAY_TRAIN_V2_ENABLED=1",
                "--setenv=RAY_USAGE_STATS_ENABLED=0",
                "--setenv=OMP_NUM_THREADS=1",
                "--setenv=MKL_NUM_THREADS=1",
                "--setenv=OPENBLAS_NUM_THREADS=1",
                BIN + "/ray",
                "start",
                "--block",
                "--disable-usage-stats",
                "--node-ip-address=" + self.nodes[name]["ip"],
                "--object-store-memory=1073741824",
                "--num-cpus=" + ("4" if name in self.trainers else "2"),
            ]
            if is_head:
                # Fresh GCS session on a DIFFERENT VM. No old disk/Redis/RocksDB reuse.
                cmd += [
                    "--head",
                    "--port=6379",
                    "--include-dashboard=false",
                    "--temp-dir=/tmp/ray-headloss",
                ]
            else:
                cmd += ["--address=" + ip + ":6379"]
                if name in self.trainers:
                    cmd += ['--resources={"headloss_train":1}']
            self.ssh(name, shlex.join(cmd))

        start(head)
        # Actual readiness, not just systemd-run dispatch.
        probe = (
            "import socket,time\nfor _ in range(90):\n try:\n  socket.create_connection(("
            + repr(ip)
            + ",6379),1).close();break\n except OSError: time.sleep(1)\nelse: raise TimeoutError('GCS unavailable')"
        )
        self.ssh(self.support, PYTHON + " -c " + shlex.quote(probe), timeout=110)
        parallel(start, hosts[1:])
        code = (
            "import ray,time,json\nray.init(address=" + repr(ip + ":6379") + ")\n"
            "for _ in range(90):\n nodes=[n for n in ray.nodes() if n['Alive']]\n"
            " if len(nodes)==4: break\n time.sleep(1)\nelse: raise TimeoutError('Nodes did not register')\n"
            "assert {n['NodeManagerAddress'] for n in nodes} == "
            + repr({self.nodes[n]["ip"] for n in hosts})
            + "\n"
            "print(json.dumps(nodes))\nray.shutdown()"
        )
        self.ssh(
            self.support,
            "sudo -u raybench " + PYTHON + " -c " + shlex.quote(code),
            timeout=140,
        )
        return ip + ":6379"

    def _launch(self, cfg):
        config_path = cfg["folder"] + f"/config-{cfg['attempt']}.json"
        self._upload_json(cfg, config_path)
        log = cfg["folder"] + f"/driver-{cfg['attempt']}.log"
        cmd = [
            "sudo",
            "systemd-run",
            "--quiet",
            "--collect",
            "--unit=" + DRIVER_UNIT,
            "--uid=raybench",
            "--gid=raybench",
            "--property=KillMode=control-group",
            "--property=TimeoutStopSec=10",
            "--property=RuntimeMaxSec=" + str(cfg["timeout_s"]),
            "--property=Restart=no",
            "--setenv=RAY_TRAIN_V2_ENABLED=1",
            "--setenv=RAY_USAGE_STATS_ENABLED=0",
            "--setenv=OMP_NUM_THREADS=1",
            "--setenv=MKL_NUM_THREADS=1",
            "--setenv=OPENBLAS_NUM_THREADS=1",
            "--property=StandardOutput=append:" + log,
            "--property=StandardError=append:" + log,
            PYTHON,
            ROOT + "/train.py",
            config_path,
        ]
        self.ssh(self.support, shlex.join(cmd))

    def _wait(self, cfg, gate=False):
        deadline = time.monotonic() + cfg["timeout_s"]
        while time.monotonic() < deadline:
            if self._remote_json(cfg["folder"] + f"/error-{cfg['attempt']}.json"):
                raise RuntimeError("Training failed; collect driver log and error JSON")
            base = cfg["folder"] + f"/attempt-{cfg['attempt']}"
            if gate:
                rows = [self._remote_json(base + f"/gate-{r}.json") for r in (0, 1)]
                if all(row and row["step"] == cfg["fault_step"] for row in rows):
                    return
            elif self._remote_json(base + "/done.json"):
                return
            time.sleep(1)
        raise TimeoutError("Training gate/completion timed out")

    def _stop_vm(self, record, save):
        instance = self.describe(self.head)
        if instance["status"] != "RUNNING":
            raise RuntimeError("Head must be RUNNING immediately before injection")
        record["instance_before"] = instance
        record["stop_requested_monotonic"] = time.monotonic()
        record["stop_requested_unix"] = time.time()
        save()  # Persist intent BEFORE an uncertain cloud API outcome.
        result = self.gc(
            "compute",
            "instances",
            "stop",
            self.head,
            "--zone=" + self.zone,
            "--no-graceful-shutdown",
            "--async",
            "--format=json",
        )
        record["stop_operation"] = json.loads(result.stdout)
        save()
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            obj = self.describe(self.head)
            if obj["status"] == "TERMINATED":
                record["instance_after"] = obj
                record["vm_terminated_monotonic"] = time.monotonic()
                save()
                return
            time.sleep(2)
        raise TimeoutError(
            "Head VM did not reach TERMINATED; no recovery claim permitted"
        )

    def _head_off(self):
        if self.describe(self.head)["status"] != "TERMINATED":
            raise RuntimeError("Original head became available during recovery")

    def download(self, suite):
        self._check_name(suite)
        destination = self.local / suite / "download"
        destination.mkdir(parents=True, exist_ok=True)
        remote = f"{SHARED}/{self.run_id}/{suite}"
        archive = f"{SHARED}/{self.run_id}/{suite}-evidence.tar.gz"
        # Read even private Ray checkpoint directories as their owning account.
        self.ssh(
            self.support,
            f"sudo -u raybench tar -czf {archive} -C {remote} .",
            timeout=600,
        )
        local_archive = destination / "evidence.tar.gz"
        self.scp(self.support + ":" + archive, local_archive)
        with tarfile.open(local_archive) as stream:
            for member in stream.getmembers():
                target = (destination / member.name).resolve()
                if not target.is_relative_to(destination) or not (
                    member.isfile() or member.isdir()
                ):
                    raise ValueError("Unexpected archive member: " + member.name)
            stream.extractall(destination)
        return destination

    @staticmethod
    def _check_name(name):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,45}", name):
            raise ValueError("Use a short lowercase suite name")

    def run_suite(
        self,
        name,
        steps=60,
        fault_step=49,
        checkpoint_every=30,
        batch_size=32,
        model_width=512,
        repeats=1,
        timeout_s=1200,
        modes=("ordinary", "aligned"),
    ):
        self._check_name(name)
        if not self.ready:
            raise RuntimeError("Run prepare() first")
        if (
            min(steps, checkpoint_every, batch_size, model_width, repeats, timeout_s)
            <= 0
        ):
            raise ValueError("Positive parameters required")
        if not 0 < fault_step < steps or fault_step % checkpoint_every == 0:
            raise ValueError("Gate must fall between checkpoints and before completion")
        if (
            not modes
            or len(set(modes)) != len(modes)
            or any(m not in ("ordinary", "indexed", "aligned") for m in modes)
        ):
            raise ValueError("Choose distinct ordinary/indexed/aligned modes")
        local = self.local / name
        local.mkdir(exist_ok=False)
        remote = f"{SHARED}/{self.run_id}/{name}"
        report = dict(
            status="preparing",
            samples=[],
            fault_model="GCP head VM stop --no-graceful-shutdown",
            recovery="fresh cluster + whole-job checkpoint restart",
            fixed_r=False,
            worker_continuation="not tested: both ranks held at gate until fenced",
            control_plane="fresh GCS, not restored GCS metadata",
            input="synthetic suffix regenerated",
            timing="external supervisor monotonic clock; progress is polling upper bound",
            environment=json.loads((self.local / "environment.json").read_text()),
        )

        def save():
            write_json(local / "report.json", report)

        save()
        lock = "/var/lib/raybench/ray-headloss-lock"
        self.ssh(self.support, f"sudo -u raybench mkdir {lock}")
        try:
            self.ssh(
                self.support,
                f"sudo -u raybench mkdir -p {SHARED}/{self.run_id}\n"
                f"sudo -u raybench mkdir {remote}",
            )
            for repeat in range(repeats):
                order = list(modes) if repeat % 2 == 0 else list(reversed(modes))
                # All healthy arms precede loss observations. Same seed/reference.
                baseline = "ordinary" if "ordinary" in modes else modes[0]
                for fault in (False, True):
                    healthy_order = [baseline] + [m for m in order if m != baseline]
                    for mode in order if fault else healthy_order:
                        print(
                            f"{name}: repeat={repeat} mode={mode} head_vm_loss={fault}",
                            flush=True,
                        )
                        if self.describe(self.head)["status"] == "TERMINATED":
                            # ONLY between completed observations, never for recovery.
                            self.gc(
                                "compute",
                                "instances",
                                "start",
                                self.head,
                                "--zone=" + self.zone,
                                timeout=300,
                            )
                            deadline = time.monotonic() + 180
                            while self.ssh(
                                self.head, "true", check=False, timeout=30
                            ).returncode:
                                if time.monotonic() > deadline:
                                    raise TimeoutError("Original head boot timeout")
                                time.sleep(3)
                            # Mounts are not persisted in fstab.
                            self.ssh(
                                self.head,
                                f"sudo mount -t nfs -o vers=3,proto=tcp,hard {self.nodes[self.support]['ip']}:{SHARED} {SHARED}",
                            )
                        parallel(self._stop_node, self.names)
                        address = self._start_cluster(self.head)
                        folder = (
                            remote
                            + f"/r{repeat}-{mode}-{'fault' if fault else 'healthy'}"
                        )
                        self.ssh(self.support, "sudo -u raybench mkdir " + folder)
                        cfg = dict(
                            folder=folder,
                            address=address,
                            mode=mode,
                            fault=fault,
                            attempt=0,
                            steps=steps,
                            fault_step=fault_step,
                            checkpoint_every=checkpoint_every,
                            batch_size=batch_size,
                            model_width=model_width,
                            seed=100 + repeat,
                            timeout_s=timeout_s,
                            training_ips=[self.nodes[n]["ip"] for n in self.trainers],
                        )
                        sample = dict(
                            mode=mode,
                            fault=fault,
                            repeat=repeat,
                            status="running",
                            parameters=dict(cfg),
                            started_monotonic=time.monotonic(),
                        )
                        report["samples"].append(sample)
                        save()
                        self._launch(cfg)
                        if fault:
                            self._wait(cfg, gate=True)
                            checkpoint = self._remote_json(
                                folder + "/latest-checkpoint.json"
                            )
                            if (
                                not checkpoint
                                or checkpoint["step"]
                                != fault_step // checkpoint_every * checkpoint_every
                            ):
                                raise RuntimeError(
                                    "Expected pre-fault durable checkpoint was not acknowledged"
                                )
                            sample["selected_checkpoint"] = checkpoint
                            self._upload_json(
                                checkpoint, folder + "/selected-checkpoint.json"
                            )
                            self._stop_vm(sample, save)
                            # Actual head loss is confirmed before intentionally fencing
                            # the old job. This is a full restart policy in BOTH arms.
                            parallel(
                                self._stop_node,
                                [self.support, *self.trainers, self.replacement],
                            )
                            sample["old_job_fenced_monotonic"] = time.monotonic()
                            self._head_off()
                            cfg.update(
                                address=self._start_cluster(self.replacement),
                                attempt=1,
                                checkpoint=checkpoint,
                            )
                            sample["new_cluster_ready_monotonic"] = time.monotonic()
                            self._head_off()
                            self._launch(cfg)
                            sample["driver_relaunch_monotonic"] = time.monotonic()
                            save()
                            deadline = time.monotonic() + timeout_s
                            while True:
                                if self._remote_json(folder + "/error-1.json"):
                                    raise RuntimeError("Restarted training failed")
                                rows = [
                                    self._remote_json(
                                        folder + f"/attempt-1/progress-{r}.json"
                                    )
                                    for r in (0, 1)
                                ]
                                if all(
                                    r and r.get("step", 0) > fault_step for r in rows
                                ):
                                    break
                                if time.monotonic() > deadline:
                                    raise TimeoutError(
                                        "No new progress beyond pre-loss step"
                                    )
                                time.sleep(1)
                            sample["new_progress_observed_monotonic"] = time.monotonic()
                            sample["recovery_to_new_progress_upper_bound_s"] = (
                                sample["new_progress_observed_monotonic"]
                                - sample["stop_requested_monotonic"]
                            )
                        self._wait(cfg)
                        sample["completed_monotonic"] = time.monotonic()
                        if fault:
                            self._head_off()
                            sample["original_head_still_terminated"] = True
                        reference = (
                            remote
                            + f"/r{repeat}-{'ordinary' if 'ordinary' in modes else modes[0]}-healthy"
                        )
                        # Validation occurs on support (torch installed), with all
                        # evidence on support-backed NFS, before original head restart.
                        code = (
                            "import json; from evidence import validate; print(json.dumps(validate("
                            + repr(folder)
                            + ","
                            + repr(reference)
                            + ","
                            + repr(cfg)
                            + ","
                            + repr(fault)
                            + ")))"
                        )
                        validation = self.ssh(
                            self.support,
                            f"cd {ROOT} && {PYTHON} -c " + shlex.quote(code),
                        )
                        sample["validation"] = json.loads(validation.stdout)
                        sample["status"] = (
                            "passed" if sample["validation"]["valid"] else "invalid"
                        )
                        save()
                        if sample["status"] != "passed":
                            raise RuntimeError("Model/input/restart validation failed")
            report["status"] = "passed"
        except BaseException as exc:
            report.update(
                status="interrupted"
                if isinstance(exc, KeyboardInterrupt)
                else "failed",
                error=str(exc),
            )
            if report["samples"] and report["samples"][-1]["status"] == "running":
                report["samples"][-1]["status"] = report["status"]
            raise
        finally:
            save()
            # Do not revive the lost head to collect evidence. Failures remain visible.
            cleanup_errors = []
            for node in self.names:
                try:
                    if self.describe(node)["status"] == "RUNNING":
                        journal = self.ssh(
                            node,
                            f"sudo journalctl -u {NODE_UNIT} -u {DRIVER_UNIT} --no-pager -n 3000",
                        ).stdout
                        (local / (node + ".log")).write_text(journal)
                        self._stop_node(node)
                except Exception as exc:
                    cleanup_errors.append(str(exc))
            try:
                self.download(name)
            except Exception as exc:
                cleanup_errors.append("Collection failed: " + str(exc))
            if not cleanup_errors:
                self.ssh(self.support, f"sudo -u raybench rmdir {lock}")
            else:
                report["cleanup_errors"] = cleanup_errors
                report["status"] = "cleanup_failed"
            save()
        return report

    def restore_worker_layout(self):
        """Explicit between-suite reset; never called during fault recovery."""
        if self.ssh(
            self.support, "test ! -d /var/lib/raybench/ray-headloss-lock", check=False
        ).returncode:
            raise RuntimeError(
                "Head-loss lock remains; inspect and collect the interrupted run first"
            )
        if self.describe(self.head)["status"] == "TERMINATED":
            self.gc(
                "compute",
                "instances",
                "start",
                self.head,
                "--zone=" + self.zone,
                timeout=300,
            )
        deadline = time.monotonic() + 180
        while self.ssh(self.head, "true", check=False, timeout=30).returncode:
            if time.monotonic() > deadline:
                raise TimeoutError("Original head boot timeout")
            time.sleep(3)
        parallel(self._stop_node, self.names)
        head_ip = self.describe(self.head)["networkInterfaces"][0]["networkIP"]

        def mount_original(name):
            self.ssh(
                name,
                f"set -e\nif mountpoint -q {SHARED}; then sudo umount {SHARED}; fi\n"
                f"if ! mountpoint -q /srv/ray-gcp; then sudo mount -t nfs4 -o vers=4.2,hard {head_ip}:/ /srv/ray-gcp; fi",
            )

        parallel(mount_original, self.names[1:])
        self.ready = False
        print(
            "Original worker-harness mounts restored. Existing notebook settings are unchanged."
        )
