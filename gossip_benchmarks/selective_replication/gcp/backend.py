"""Ray transports tasks; the notebook host injects actual GCP VM stops."""

import importlib.metadata
import json
import os
import queue
import threading
import time
import traceback
from collections import deque
from pathlib import Path

from workload import train, write_json


class TrainingActor:
    def __init__(self, data_path):
        import numpy as np
        import torch

        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
        torch.use_deterministic_algorithms(True)
        with np.load(data_path) as archive:
            self.data = dict(archive)
        torch.optim.SGD(torch.nn.Linear(1, 1).parameters(), lr=0.1)
        self.events = queue.Queue()
        self.busy = False
        self.cancelled = threading.Event()

    def identity(self):
        import torch

        import ray

        return dict(
            node_id=ray.get_runtime_context().get_node_id(),
            hostname=os.uname().nodename,
            cpu_capability=torch.backends.cpu.get_cpu_capability(),
            torch_build=torch.__config__.show(),
            threads=torch.get_num_threads(),
            versions={
                p: importlib.metadata.version(p)
                for p in ("ray", "torch", "numpy", "scikit-learn")
            },
        )

    def submit(self, job):
        if self.busy:
            raise RuntimeError("Worker reused before cancellation acknowledgement")
        self.busy = True
        self.cancelled = threading.Event()
        cancelled = self.cancelled

        def notify(event):
            self.events.put(
                {**event, "generation": job["generation"], "task": job["name"]}
            )

        def work():
            try:
                train(
                    job["task"],
                    self.data,
                    job["resume"],
                    job["folder"],
                    notify,
                    cancelled.is_set,
                    job["parents"],
                )
            except Exception:
                notify(dict(kind="error", traceback=traceback.format_exc()))
            finally:
                self.busy = False
                notify(dict(kind="idle"))

        threading.Thread(target=work, daemon=True).start()

    def cancel(self):
        self.cancelled.set()

    def drain(self):
        events = []
        while True:
            try:
                events.append(self.events.get_nowait())
            except queue.Empty:
                return events


class Connection:
    def __init__(self, actor):
        self.actor, self.pending = actor, None
        self.submission = None
        self.events = deque()

    def poll(self, timeout=0):
        import ray

        end = time.monotonic() + timeout
        while True:
            if self.submission is not None:
                submitted, _ = ray.wait([self.submission], timeout=0)
                if submitted:
                    try:
                        ray.get(self.submission)
                    except ray.exceptions.RayActorError as exc:
                        raise EOFError("Training VM lost during dispatch") from exc
                    finally:
                        self.submission = None
            if self.events:
                return True
            if self.pending is None:
                self.pending = self.actor.drain.remote()
            ready, _ = ray.wait([self.pending], timeout=min(max(timeout, 0), 0.05))
            if ready:
                try:
                    self.events.extend(ray.get(self.pending))
                except ray.exceptions.RayActorError as exc:
                    raise EOFError("Training VM/actor lost") from exc
                finally:
                    self.pending = None
            if self.events or time.monotonic() >= end:
                return bool(self.events)
            time.sleep(0.01)

    def recv(self):
        return self.events.popleft()


class ProcessStatus:
    # Runtime compatibility: actual loss is verified through the cloud response,
    # not inferred from a local SSH or actor process exit code.
    def join(self, timeout=0):
        pass

    def is_alive(self):
        return True


class RayWorkers:
    def __init__(self, machines, data_path):
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        self.actors, self.connections, self.ready, self.processes = {}, {}, {}, {}
        self.generations = dict.fromkeys(machines, 0)
        nodes = {
            n["NodeManagerAddress"]: n["NodeID"] for n in ray.nodes() if n["Alive"]
        }
        actor_type = ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)(
            TrainingActor
        )
        try:
            for machine, spec in machines.items():
                node = nodes[spec["node_ip"]]
                actor = actor_type.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(node, soft=False)
                ).remote(data_path)
                self.actors[machine] = actor
                identity = ray.get(actor.identity.remote(), timeout=90)
                if identity["node_id"] != node or identity["threads"] != 1:
                    raise AssertionError("Wrong training placement/thread count")
                self.ready[machine] = identity
                self.connections[machine] = Connection(actor)
                self.processes[machine] = ProcessStatus()
            if len({v["node_id"] for v in self.ready.values()}) != len(machines):
                raise AssertionError("Workers must occupy distinct VMs")
            if (
                len(
                    {
                        json.dumps(v["versions"], sort_keys=True)
                        for v in self.ready.values()
                    }
                )
                != 1
            ):
                raise AssertionError("Worker packages differ")
        except Exception:
            self.close()
            raise

    def submit(self, machine, name, task, resume, folder, parents=None):
        self.generations[machine] += 1
        token = self.generations[machine]
        self.connections[machine].submission = self.actors[machine].submit.remote(
            dict(
                generation=token,
                name=name,
                task=task,
                resume=resume,
                folder=str(folder),
                parents=parents or [],
            )
        )
        return token

    def cancel(self, machine):
        self.actors[machine].cancel.remote()

    def close(self):
        import ray

        for actor in self.actors.values():
            ray.kill(actor, no_restart=True)


class VMFaultInjector:
    """File handoff to external gcloud supervisor; no cloud keys on guests.

    Recovery is deliberately gated on TERMINATED proof. This measures an
    externally supervised policy, not Ray's native failure detection latency.
    """

    def __init__(self, workers, failures, begin, schedule, folder):
        self.failures, self.begin, self.schedule = failures, begin, schedule
        self.folder = Path(folder)
        self.requested, self.records, self.detected = {}, {}, {}
        self.error = None
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        try:
            for machine, delay in sorted(self.failures.items(), key=lambda p: p[1]):
                if self.stop.wait(max(0, self.begin + delay - time.monotonic())):
                    return
                request = dict(
                    machine=machine,
                    requested_s=delay,
                    handoff_s=time.monotonic() - self.begin,
                    active_at_request=self.schedule.active.get(machine),
                )
                with self.lock:
                    self.requested[machine] = request
                write_json(self.folder / f"request-{machine}.json", request)
        except Exception as exc:
            self.error = exc

    def observe_loss(self, machine):
        self.detected.setdefault(
            machine,
            dict(
                actor_loss_observed_s=time.monotonic() - self.begin,
                active_at_actor_loss=self.schedule.active.get(machine),
            ),
        )

    def snapshot(self):
        if self.error:
            raise RuntimeError("Fault handoff failed") from self.error
        with self.lock:
            requested = dict(self.requested)
        for machine, request in requested.items():
            path = self.folder / f"response-{machine}.json"
            if machine in self.records or not path.exists():
                continue
            response = json.loads(path.read_text())
            if response.get("status") != "TERMINATED" or response.get("error"):
                raise RuntimeError(f"VM stop failed: {response}")
            self.records[machine] = {
                **request,
                **response,
                **self.detected.get(machine, {}),
                "observed_s": time.monotonic() - self.begin,
                "active_task": self.schedule.active.get(machine),
            }
        return dict(self.records)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        deadline = time.monotonic() + 240
        while set(self.snapshot()) != set(self.requested):
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "External fault supervisor did not acknowledge VM stop"
                )
            time.sleep(0.2)

    def verify(self, evidence):
        for machine, record in evidence.items():
            if record["status"] != "TERMINATED" or not record.get("instance_id"):
                raise AssertionError("Missing whole-VM-loss evidence")
