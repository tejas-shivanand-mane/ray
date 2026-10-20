"""Notebook-host supervisor for the fresh cluster recorded by provision.py."""

import copy
import hashlib
import json
import re
import shlex
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Reuse only the existing ownership-checked gcloud/SSH/SCP wrappers.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "head_vm_loss"))
from gcp.capabilities import stop_command  # noqa: E402
from gcp.suite import assess, jobs, smoke_jobs  # noqa: E402
from launcher import PYTHON, HeadVMLoss, parallel  # noqa: E402
from workload import write_json  # noqa: E402

SHARED = "/srv/ray-gcp/selective-vm"
UNIT = "ray-selective-driver.service"
LOCK = "/var/lib/raybench/ray-selective-lock"


class GCPStudy(HeadVMLoss):
    def __init__(
        self, state_file, workdir="ray_gcp_results/selective-vm", failure_mode="process"
    ):
        if failure_mode not in ("process", "vm"):
            raise ValueError("failure_mode must be process or vm")
        self.failure_mode = failure_mode
        super().__init__(state_file, workdir)
        self.mapping = dict(zip(("safe", "spot_a", "spot_b"), self.names[1:4]))
        self.source = Path(__file__).resolve().parents[1]
        self.code = None
        self.prepared = False

    def node(self, name, action):
        request = shlex.quote(json.dumps(dict(run_id=self.run_id, action=action)))
        return self.ssh(
            name, f"printf %s {request} | sudo {PYTHON} /opt/ray-gcp/node_control.py"
        )

    def put(self, value, remote):
        local = self.local / "transfer.json"
        write_json(local, value)
        self.scp(local, self.head + ":/tmp/ray-selective-transfer.json")
        self.ssh(
            self.head,
            "sudo install -o raybench -g raybench -m 644 "
            "/tmp/ray-selective-transfer.json " + shlex.quote(remote),
        )

    def get(self, remote):
        return json.loads(self.ssh(self.head, "cat " + shlex.quote(remote)).stdout)

    def driver_state(self):
        """An SSH error is not evidence that the controller has exited."""
        for attempt in range(3):
            result = self.ssh(self.head, "systemctl is-active " + UNIT, check=False)
            state = result.stdout.strip()
            if result.returncode == 0 and state in (
                "active",
                "activating",
                "reloading",
            ):
                return "active"
            if result.returncode in (3, 4) and state in (
                "inactive",
                "failed",
                "unknown",
            ):
                return state
            if attempt < 2:
                time.sleep(1)
        raise RuntimeError(
            f"Cannot determine driver state: SSH/systemctl exit={result.returncode}; "
            f"stdout={result.stdout!r}; stderr={result.stderr!r}"
        )

    def archive_interrupted(self, name, job_id):
        """Preserve a failed attempt so the same matrix can resume explicitly."""
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name) or not re.fullmatch(
            r"run-[0-9]+", job_id
        ):
            raise ValueError("Invalid suite/job name")
        local = self.local / name
        if (local / (job_id + ".json")).exists():
            raise RuntimeError("Completed local result exists; do not retry")
        control = SHARED + "/" + name + "/" + job_id
        if self.driver_state() == "active":
            raise RuntimeError("Driver is still running; cannot archive")
        self.ssh(self.head, "sudo mkdir " + LOCK)
        try:
            if self.driver_state() == "active":
                raise RuntimeError("Driver started; cannot archive")
            status = json.loads(
                self.ssh(
                    self.head,
                    f"if test -f {control}/status.json; then cat {control}/status.json; else printf null; fi",
                ).stdout
            )
            if status and status.get("ok"):
                raise RuntimeError(
                    "Remote attempt succeeded; collect its result instead of repeating it"
                )
            archive = (
                SHARED
                + "/"
                + name
                + "/interrupted-"
                + job_id
                + "-"
                + str(time.time_ns())
            )
            output = SHARED + "/" + name + "/study/" + job_id
            self.ssh(
                self.head,
                f"set -e\nsudo -u raybench mkdir {archive}\n"
                f"sudo -u raybench mv {control} {archive}/control\n"
                f"if test -d {output}; then sudo -u raybench mv {output} {archive}/output; fi",
            )
            write_json(
                local / (job_id + "-retry.json"),
                dict(
                    job_id=job_id,
                    archived=archive,
                    reason="Explicit retry after interrupted controller; original evidence retained",
                ),
            )
            return archive
        finally:
            self.ssh(self.head, "sudo rmdir " + LOCK)

    def fence_driver(self):
        # --collect can remove a successfully completed transient unit. A missing
        # unit is safe; a failed SSH call or a still-running service is not.
        self.ssh(self.head, "sudo systemctl stop " + UNIT, check=False)
        state = self.ssh(self.head, "systemctl is-active " + UNIT, check=False)
        return state.returncode in (3, 4) and state.stdout.strip() in (
            "inactive",
            "unknown",
            "failed",
        )

    def prepare(self, install_missing_dependencies=False):
        print(json.dumps(self.inventory(), indent=2), flush=True)
        self.stop_command = stop_command() if self.failure_mode == "vm" else None
        # Refresh the process-fencing helper even when reusing an active session.
        for name in self.names:
            self.describe(name)
            self.scp(
                self.source / "gcp/bootstrap/node_control.py",
                name + ":/tmp/ray-node-control.py",
            )
            self.ssh(
                name,
                "sudo install -o root -g root -m 644 /tmp/ray-node-control.py /opt/ray-gcp/node_control.py",
            )
        identities = []
        for name in self.names:
            obj = self.describe(name)
            if obj["status"] != "RUNNING":
                raise RuntimeError(
                    f"Inspect/restore the recorded VM before preparation: {name} {obj['status']}"
                )
            self.nodes[name] = dict(
                id=str(obj["id"]), ip=obj["networkInterfaces"][0]["networkIP"]
            )
            active = self.ssh(
                name,
                "systemctl list-units 'ray-exp-driver-*' 'ray-headloss-*' 'ray-selective-driver*' --state=active,activating --no-legend",
            ).stdout
            if active.strip():
                raise RuntimeError("Another experiment is active: " + name)
            for lock in (LOCK, "/var/lib/raybench/ray-headloss-lock"):
                if self.ssh(name, "test ! -d " + lock, check=False).returncode:
                    raise RuntimeError(
                        "Inspect the outstanding experiment lock: " + lock
                    )
            identities.append(json.loads(self.node(name, "identity").stdout))
        if (
            len(
                {
                    json.dumps(
                        {k: v for k, v in i.items() if k not in ("name", "ip")},
                        sort_keys=True,
                    )
                    for i in identities
                }
            )
            != 1
        ):
            raise RuntimeError("Image/package/source identities differ")
        # The earlier worker harness already owns this NFS export and raybench UID.
        # Fail before running if it is not the same writable storage on all VMs.
        self.ssh(self.head, f"sudo install -d -o raybench -g raybench {SHARED}")
        sentinel = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()
        self.put(dict(sentinel=sentinel), SHARED + "/mount-check.json")
        for name in self.names[1:]:
            check = self.ssh(
                name,
                f"mountpoint -q /srv/ray-gcp && sudo -u raybench test -w {SHARED} && cat {SHARED}/mount-check.json",
            ).stdout
            if json.loads(check) != dict(sentinel=sentinel):
                raise RuntimeError("Wrong shared checkpoint mount: " + name)
        bundle = self.local / "helpers.tar.gz"
        paths = sorted(
            p for p in self.source.rglob("*.py") if "__pycache__" not in p.parts
        )
        hashes = {
            str(p.relative_to(self.source)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in paths
        }
        digest = hashlib.sha256(
            json.dumps(hashes, sort_keys=True).encode()
        ).hexdigest()[:16]
        self.code = "/opt/ray-selective/" + digest
        with tarfile.open(bundle, "w:gz") as archive:
            for path in paths:
                archive.add(path, arcname=str(path.relative_to(self.source)))
        self.scp(bundle, self.head + ":/tmp/ray-selective-helpers.tar.gz")
        self.ssh(
            self.head,
            f"sudo mkdir -p {self.code} && sudo tar -xzf /tmp/ray-selective-helpers.tar.gz -C {self.code}",
        )
        # Use only the image's virtual environment; never replace Ray or Torch.
        for name, identity in zip(self.names, identities):
            check = self.ssh(name, PYTHON + " -c 'import sklearn'", check=False)
            if check.returncode and install_missing_dependencies:
                self.ssh(
                    name,
                    "sudo " + PYTHON + " -m pip install scikit-learn==1.7.2",
                    timeout=600,
                )
            versions = "import json,importlib.metadata as m; print(json.dumps({p:m.version(p) for p in ['ray','torch','numpy','scikit-learn']}))"
            identity["selective_versions"] = json.loads(
                self.ssh(name, PYTHON + " -c " + shlex.quote(versions)).stdout
            )
        if (
            len(
                {
                    json.dumps(i["selective_versions"], sort_keys=True)
                    for i in identities
                }
            )
            != 1
        ):
            raise RuntimeError(
                "Selective experiment package versions differ across VMs"
            )
        self.identities = identities
        self.manifest = dict(
            nodes=self.nodes,
            identities=identities,
            helper_sha256=hashes,
            stop_command=self.stop_command,
            failure_mode=self.failure_mode,
        )
        write_json(self.local / "environment.json", self.manifest)
        self.prepared = True

    def reset(self):
        """Between observations only: restore stopped workers and start fresh Ray."""
        for name in self.mapping.values():
            obj = self.describe(name)
            if obj["status"] == "TERMINATED":
                self.gc(
                    "compute",
                    "instances",
                    "start",
                    name,
                    "--zone=" + self.zone,
                    timeout=300,
                )
            elif obj["status"] != "RUNNING":
                raise RuntimeError("Unexpected VM state: " + obj["status"])

        def wait_guest(name):
            deadline = time.monotonic() + 180
            while True:
                try:
                    self.node(name, "stop")
                    if name != self.head:
                        self.ssh(
                            name,
                            "mountpoint -q /srv/ray-gcp || sudo mount /srv/ray-gcp",
                        )
                    return
                except Exception:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(2)

        parallel(wait_guest, self.names)
        self.node(self.head, "start")
        # Wait for GCS before starting workers (node_control does not wait).
        ip = self.nodes[self.head]["ip"]
        probe = (
            "import socket,time\nfor _ in range(60):\n try:\n  socket.create_connection(("
            + repr(ip)
            + ",6379),1).close();break\n except OSError: time.sleep(1)\nelse: raise TimeoutError('GCS')"
        )
        self.ssh(self.head, PYTHON + " -c " + shlex.quote(probe), timeout=90)
        parallel(lambda n: self.node(n, "start"), self.mapping.values())
        probe = (
            "import ray,time\nray.init(address='auto')\nexpected="
            + repr({self.nodes[n]["ip"] for n in [self.head, *self.mapping.values()]})
            + "\nfor _ in range(90):\n alive={n['NodeManagerAddress'] for n in ray.nodes() if n['Alive']}\n if alive==expected: break\n time.sleep(1)\nelse: raise TimeoutError(str(alive))\nray.shutdown()"
        )
        self.ssh(
            self.head,
            "sudo -u raybench " + PYTHON + " -c " + shlex.quote(probe),
            timeout=120,
        )

    def kill_worker(self, machine, request, control):
        if self.failure_mode == "vm":
            return self.stop_vm(machine, request, control)
        name = self.mapping[machine]
        record = dict(machine=machine, vm=name, request=request, failure_mode="process")
        before = time.monotonic()
        try:
            obj = self.describe(name)
            if obj["status"] != "RUNNING":
                raise RuntimeError("Process-loss target VM must be running")
            record["instance_id"] = str(obj["id"])
            record["dispatch_unix_s"] = time.time()
            evidence = json.loads(self.node(name, "kill").stdout)
            if not evidence.get("unit_empty") or evidence.get("signal") != "SIGKILL":
                raise RuntimeError("Missing process-group kill evidence")
            record.update(
                status="PROCESSES_KILLED",
                unit_empty=True,
                signal="SIGKILL",
                process_evidence=evidence,
                kill_and_confirm_s=time.monotonic() - before,
            )
        except Exception as exc:
            record["error"] = str(exc)
        write_json(
            self.local / (Path(control).name + "-" + machine + "-process.json"), record
        )
        return record

    def stop_vm(self, machine, request, control):
        """Executed concurrently for correlated loss; save evidence on the host first."""
        name = self.mapping[machine]
        before = time.monotonic()
        record = dict(machine=machine, vm=name, request=request)
        try:
            obj = self.describe(name)
            if obj["status"] != "RUNNING":
                raise RuntimeError("Fault target was not RUNNING")
            record["instance_id"] = str(obj["id"])
            record["dispatch_unix_s"] = time.time()
            operation = self.gc(
                *self.stop_command,
                name,
                "--zone=" + self.zone,
                "--no-graceful-shutdown",
                "--format=json",
                timeout=180,
            )
            record["operation_stdout"] = operation.stdout
            record["operation_stderr"] = operation.stderr
            after = self.describe(name)
            record["status"] = after["status"]
            record["stop_and_confirm_s"] = time.monotonic() - before
            if after["status"] != "TERMINATED":
                raise RuntimeError("VM did not terminate")
        except Exception as exc:
            record["error"] = str(exc)
        local = self.local / (Path(control).name + "-" + machine + "-cloud.json")
        write_json(local, record)
        return record

    def execute(self, request):
        request = dict(request, failure_mode=self.failure_mode)
        free = int(
            self.ssh(
                self.head,
                PYTHON
                + " -c \"import shutil; print(shutil.disk_usage('/srv/ray-gcp').free)\"",
            ).stdout
        )
        if free < 5 * 1024**3:
            raise RuntimeError(
                "Less than 5 GiB free on checkpoint storage; download evidence and inspect storage before continuing"
            )
        control = request["control"]
        self.ssh(
            self.head, "sudo install -d -o raybench -g raybench " + shlex.quote(control)
        )
        self.put(request, control + "/request.json")
        command = [
            "sudo",
            "systemd-run",
            "--quiet",
            "--collect",
            "--unit=" + UNIT,
            "--uid=raybench",
            "--gid=raybench",
            "--property=KillMode=control-group",
            "--property=TimeoutStopSec=20",
            "--property=Restart=no",
            "--working-directory=" + self.code,
            "--setenv=OMP_NUM_THREADS=1",
            "--setenv=MKL_NUM_THREADS=1",
            "--setenv=OPENBLAS_NUM_THREADS=1",
            PYTHON,
            "-m",
            "gcp.controller",
            control + "/request.json",
        ]
        self.ssh(self.head, shlex.join(command))
        pending, sent = {}, set()
        timeout = request.get("case", {}).get("horizon_s", 21600) + 900
        deadline = time.monotonic() + timeout
        read_code = (
            "import pathlib,json; p=pathlib.Path("
            + repr(control)
            + "); print(json.dumps({f.name:json.loads(f.read_text()) for f in p.glob('*.json') if f.name!='request.json'}))"
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            while True:
                files = json.loads(
                    self.ssh(
                        self.head, PYTHON + " -c " + shlex.quote(read_code), timeout=60
                    ).stdout
                )
                for machine in self.mapping:
                    key = f"request-{machine}.json"
                    if key in files and machine not in sent:
                        if machine not in request["scenario"]["failures"]:
                            raise RuntimeError("Unexpected fault request")
                        sent.add(machine)
                        pending[machine] = executor.submit(
                            self.kill_worker, machine, files[key], control
                        )
                for machine, future in list(pending.items()):
                    if future.done():
                        self.put(future.result(), control + f"/response-{machine}.json")
                        del pending[machine]
                status = files.get("status.json")
                if status:
                    if pending:
                        raise RuntimeError(
                            "Controller exited with cloud operations outstanding"
                        )
                    if not status["ok"]:
                        raise RuntimeError(status["traceback"])
                    return files.get("result.json")
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        "Controller exceeded observation limit; inspect service journal"
                    )
                active = self.driver_state()
                if active != "active":
                    time.sleep(1)
                    status_path = shlex.quote(control + "/status.json")
                    final = json.loads(
                        self.ssh(
                            self.head,
                            f"if test -f {status_path}; then cat {status_path}; else printf null; fi",
                        ).stdout
                    )
                    if final is None:
                        journal = self.ssh(
                            self.head,
                            "sudo journalctl -u " + UNIT + " --no-pager -n 100",
                            check=False,
                        )
                        log = self.local / (Path(control).name + "-driver.log")
                        log.write_text(journal.stdout + "\n" + journal.stderr)
                        raise RuntimeError(
                            f"Driver is {active} without status.json. Journal saved to {log}"
                        )
                    if not final["ok"]:
                        raise RuntimeError(final["traceback"])
                time.sleep(1)

    def prepare_suite(self, name, config):
        if not self.prepared:
            raise RuntimeError("Run prepare() and inspect inventory first")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("Use a simple unique suite name")
        root = SHARED + "/" + name
        local = self.local / name
        if local.exists():
            raise FileExistsError("Suite exists; use download/resume, not overwrite")
        self.ssh(self.head, "sudo mkdir " + LOCK)
        try:
            # Exclusive remote mkdir protects results even from another notebook host.
            self.ssh(self.head, "sudo -u raybench mkdir " + root)
            local.mkdir()
            cfg = copy.deepcopy(config)
            for machine, vm in self.mapping.items():
                cfg["machines"][machine]["node_ip"] = self.nodes[vm]["ip"]
            self.put(
                dict(config=cfg, environment=self.manifest), root + "/manifest.json"
            )
            self.reset()
            self.execute(
                dict(
                    action="profile",
                    config=cfg,
                    folder=root + "/study",
                    control=root + "/profile-control",
                )
            )
            measured = self.get(root + "/study/profile.json")
            searched = self.get(root + "/study/search.json")
            write_json(local / "profile.json", measured)
            write_json(local / "search.json", searched)
            write_json(
                local / "manifest.json", dict(config=cfg, environment=self.manifest)
            )
            return searched
        finally:
            if self.fence_driver():
                self.ssh(self.head, "sudo rmdir " + LOCK)

    def preview(self, name, repeats=3, smoke=False, case_indices=None, policies=None):
        searched = json.loads((self.local / name / "search.json").read_text())
        planned = (
            smoke_jobs(searched)
            if smoke
            else jobs(searched, repeats, False, policies, case_indices)
        )
        # A conservative limit, not an invoice or expected runtime. Stop/start,
        # Python cold start and validation are outside the profiled workflow time.
        seconds = sum(j["case"]["horizon_s"] + 900 for j in planned)
        print(
            f"{len(planned)} observations; conservative runtime limit {seconds/3600:.1f} hours."
        )
        return planned

    def run_suite(self, name, planned, smoke_gate=None):
        if not self.prepared:
            raise RuntimeError("Run prepare() first")
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name) or not planned:
            raise ValueError(
                "Need a valid suite name and a nonempty observation matrix"
            )
        if len({j["id"] for j in planned}) != len(planned) or any(
            not re.fullmatch(r"run-[0-9]+", j["id"]) for j in planned
        ):
            raise ValueError("Observation IDs must be unique run-N identifiers")
        if smoke_gate is not None:
            gate = json.loads((self.local / smoke_gate / "summary.json").read_text())
            if gate.get("failure_modes") != [self.failure_mode]:
                raise RuntimeError(
                    "Smoke failure mode differs: run smoke for the current mode"
                )
            if not gate["valid"]:
                raise RuntimeError("Smoke suite did not pass the evidence gates")
        root, local = SHARED + "/" + name, self.local / name
        manifest = json.loads((local / "manifest.json").read_text())
        if manifest["environment"]["helper_sha256"] != self.manifest["helper_sha256"]:
            raise RuntimeError("Helpers changed: create a fresh suite/profile")
        if manifest["environment"].get("failure_mode", "vm") != self.failure_mode:
            raise RuntimeError("Failure mode changed: create a new suite")
        plan_path = local / "jobs.json"
        if plan_path.exists() and json.loads(plan_path.read_text()) != planned:
            raise RuntimeError("Resume requires the identical matrix")
        write_json(plan_path, planned)
        self.ssh(self.head, "sudo mkdir " + LOCK)
        self.put(planned, root + "/jobs.json")
        rows = []
        try:
            for job in planned:
                path = local / (job["id"] + ".json")
                if path.exists():
                    rows.append(json.loads(path.read_text()))
                    continue
                control = root + "/" + job["id"]
                # Never replay an interrupted observation under the same ID.
                if self.ssh(self.head, "test ! -d " + control, check=False).returncode:
                    raise RuntimeError(
                        "Interrupted observation: download evidence; use a new suite name"
                    )
                before = time.monotonic()
                self.reset()
                row = self.execute(
                    dict(
                        action="replay",
                        folder=root + "/study",
                        control=control,
                        output=root + "/study/" + job["id"],
                        **job,
                    )
                )
                row.update(
                    case_index=job["case_index"],
                    policies=job["policies"],
                    repeat=job["repeat"],
                    observation_wall_s=time.monotonic() - before,
                    all_five_vm_hours_upper_bound=5
                    * (time.monotonic() - before)
                    / 3600,
                )
                for machine in job["scenario"]["failures"]:
                    if self.failure_mode == "process":
                        evidence = json.loads(
                            self.node(self.mapping[machine], "process_status").stdout
                        )
                        if not evidence["unit_empty"]:
                            raise AssertionError(
                                "Killed worker rejoined before observation ended"
                            )
                    elif self.describe(self.mapping[machine])["status"] != "TERMINATED":
                        raise AssertionError(
                            "Failed VM restarted before observation ended"
                        )
                row["failure_mode"] = self.failure_mode
                write_json(path, row)
                rows.append(row)
                write_json(local / "replay.json", dict(rows=rows))
                summary = assess(
                    rows, len(planned), require_mechanisms=smoke_gate is None
                )
                write_json(local / "summary.json", summary)
                print(
                    f"{job['id']}: correct={row['correct']} deadline={row['met_deadline']} {row['elapsed_s']:.1f}s",
                    flush=True,
                )
                if not row["correct"] or not row["fault_coverage_complete"]:
                    raise RuntimeError(
                        "Correctness/fault coverage failed; inspect evidence before continuing"
                    )
            summary = assess(rows, len(planned), require_mechanisms=smoke_gate is None)
            write_json(local / "summary.json", summary)
            self.put(summary, root + "/summary.json")
            return summary
        finally:
            if self.fence_driver():
                self.ssh(self.head, "sudo rmdir " + LOCK)
            # Never restart a faulted VM as exception cleanup.

    def download(self, name):
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError("Invalid suite name")
        destination = self.local / (name + "-evidence")
        if destination.exists():
            raise FileExistsError(destination)
        self.scp(self.head + ":" + SHARED + "/" + name, destination, recursive=True)
        return destination
