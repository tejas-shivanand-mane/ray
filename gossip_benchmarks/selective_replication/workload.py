"""Real CPU learning trials and a small local process pool for fault injection."""

import hashlib
import json
import multiprocessing as mp
import os
import time
import traceback
from pathlib import Path


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def prepare_data(folder, seed=123):
    import numpy as np
    from sklearn.datasets import load_digits
    from sklearn.model_selection import train_test_split

    digits = load_digits()
    ids = np.arange(len(digits.target))
    train, test = train_test_split(
        ids, test_size=0.2, random_state=seed, stratify=digits.target
    )
    values = dict(
        x=(digits.data[train] / 16).astype("float32"),
        y=digits.target[train].astype("int64"),
        ids=train.astype("int64"),
        test_x=(digits.data[test] / 16).astype("float32"),
        test_y=digits.target[test].astype("int64"),
    )
    path = Path(folder) / "digits.npz"
    np.savez(path, **values)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return str(path.resolve()), digest


def train(task, data, resume, folder, notify, cancelled, parents=None):
    import numpy as np
    import torch
    from torch import nn

    # Apply inside the actual training thread, including Ray's background thread.
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    precision = task.get("dtype", "float32")
    if precision not in ("float32", "float64"):
        raise ValueError("dtype must be float32 or float64")
    dtype = getattr(torch, precision)
    start = time.monotonic()
    if task.get("shard") is not None:
        index, count = task["shard"]
        data = {**data, **{k: data[k][index::count] for k in ("x", "y", "ids")}}
    torch.manual_seed(task["seed"])
    width = task["width"]
    model = nn.Sequential(
        nn.Linear(64, width),
        nn.ReLU(),
        nn.Linear(width, width),
        nn.ReLU(),
        nn.Linear(width, 10),
    ).to(dtype=dtype)
    if parents and not resume:
        # One local-SGD round: average completed shard models in stable parent
        # order, reset momentum, then fine-tune on the full training split.
        states = [
            torch.load(path, map_location="cpu", weights_only=True)["model"]
            for path in parents
        ]
        model.load_state_dict(
            {k: torch.stack([state[k] for state in states]).mean(0) for k in states[0]}
        )
    optimizer = torch.optim.SGD(model.parameters(), lr=task["lr"], momentum=0.9)
    # Checkpoint cadence is a protection choice, not part of the learned state.
    # All data/model/optimizer/epoch settings remain in the identity.
    identity = fingerprint({k: v for k, v in task.items() if k != "checkpoint_every"})
    completed, input_history = 0, []
    if resume:
        state = torch.load(resume, map_location="cpu", weights_only=True)
        if state["identity"] != identity:
            raise ValueError("Checkpoint belongs to a different trial or dataset")
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        completed, input_history = state["epoch"], state["input_history"]
    x, y = torch.from_numpy(data["x"]).to(dtype=dtype), torch.from_numpy(data["y"])
    init_s = time.monotonic() - start
    checkpoint_load_s = init_s if resume else 0.0
    notify(
        {
            "kind": "start",
            "restored_epoch": completed,
            "initialization_s": init_s,
            "restore_init_s": checkpoint_load_s,
            "parent_count": len(parents or []),
            "dtype": precision,
            "threads": torch.get_num_threads(),
            "deterministic_algorithms": torch.are_deterministic_algorithms_enabled(),
            "cpu_capability": torch.backends.cpu.get_cpu_capability(),
        }
    )
    epoch_times, save_times = [], []
    path = Path(resume) if resume else None
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=False)
    for epoch in range(completed, task["epochs"]):
        before = time.monotonic()
        order = np.random.default_rng(task["seed"] + epoch).permutation(len(x))
        for offset in range(0, len(x), task["batch_size"]):
            if cancelled():
                return
            indices = order[offset : offset + task["batch_size"]]
            optimizer.zero_grad()
            nn.functional.cross_entropy(model(x[indices]), y[indices]).backward()
            optimizer.step()
        input_history.append(hashlib.sha256(data["ids"][order].tobytes()).hexdigest())
        epoch_times.append(time.monotonic() - before)
        if (epoch + 1) % task["checkpoint_every"] == 0 or epoch + 1 == task["epochs"]:
            before = time.monotonic()
            state = dict(
                model=model.state_dict(),
                optimizer=optimizer.state_dict(),
                epoch=epoch + 1,
                input_history=input_history,
                identity=identity,
            )
            path = folder / f"epoch-{epoch + 1}.pt"
            temporary = path.with_suffix(".tmp")
            with temporary.open("wb") as stream:
                torch.save(state, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            save_times.append(time.monotonic() - before)
            notify({"kind": "checkpoint", "epoch": epoch + 1, "path": str(path)})
    with torch.no_grad():
        accuracy = float(
            (
                model(torch.from_numpy(data["test_x"]).to(dtype=dtype)).argmax(1)
                == torch.from_numpy(data["test_y"])
            )
            .float()
            .mean()
        )
    notify(
        dict(
            kind="result",
            path=str(path),
            epoch=task["epochs"],
            input_history=input_history,
            accuracy=accuracy,
            dtype=precision,
            threads=torch.get_num_threads(),
            cpu_capability=torch.backends.cpu.get_cpu_capability(),
            init_s=init_s,
            epoch_times=epoch_times,
            save_times=save_times,
            training_s=time.monotonic() - start,
        )
    )


def worker(connection, generation, data_path, cpu):
    import numpy as np
    import torch

    try:
        if cpu is not None:
            os.sched_setaffinity(0, {cpu})
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
        torch.use_deterministic_algorithms(True)
        with np.load(data_path) as archive:
            data = dict(archive)
        # Exclude interpreter/library cold start equally for all policies. Pay
        # model/optimizer initialization and checkpoint restoration per attempt.
        torch.optim.SGD(torch.nn.Linear(1, 1).parameters(), lr=0.1)
        connection.send(
            {
                "kind": "ready",
                "pid": os.getpid(),
                "cpu": cpu,
                "threads": torch.get_num_threads(),
            }
        )
        while True:
            job = connection.recv()
            if job is None:
                return
            token = job["generation"]

            def notify(event):
                connection.send({**event, "generation": token, "task": job["name"]})

            train(
                job["task"],
                data,
                job["resume"],
                job["folder"],
                notify,
                lambda: generation.value != token,
                job.get("parents"),
            )
            notify({"kind": "idle"})
    except (EOFError, BrokenPipeError):
        return
    except Exception:
        connection.send({"kind": "error", "traceback": traceback.format_exc()})


class Workers:
    def __init__(self, machines, data_path):
        context = mp.get_context("spawn")
        cpus = (
            sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
        )
        if cpus and len(cpus) < len(machines):
            raise ValueError("Need one available CPU per logical machine")
        self.processes, self.connections, self.generations, self.ready = {}, {}, {}, {}
        try:
            for index, machine in enumerate(machines):
                parent, child = context.Pipe()
                generation = context.Value(
                    "i", 0, lock=False
                )  # Single writer; avoid killing a lock holder.
                process = context.Process(
                    target=worker,
                    args=(child, generation, data_path, cpus[index] if cpus else None),
                )
                process.start()
                child.close()
                self.processes[machine] = process
                self.connections[machine] = parent
                self.generations[machine] = generation
            for machine, connection in self.connections.items():
                if not connection.poll(90):
                    raise TimeoutError(f"Worker {machine} did not initialize")
                event = connection.recv()
                if event["kind"] != "ready":
                    raise RuntimeError(event)
                self.ready[machine] = event
        except Exception:
            self.close()
            raise

    def submit(self, machine, name, task, resume, folder, parents=None):
        self.generations[machine].value += 1
        token = self.generations[machine].value
        self.connections[machine].send(
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
        self.generations[machine].value += 1

    def kill(self, machine):
        process = self.processes[machine]
        process.kill()
        process.join(timeout=10)
        if process.is_alive():
            raise RuntimeError(f"Failed to fence {machine}")
        return {"pid": process.pid, "exitcode": process.exitcode}

    def close(self):
        for process in self.processes.values():
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        for connection in self.connections.values():
            connection.close()


def validate_state(actual, expected, tolerance=1e-5):
    import torch

    left = torch.load(actual, map_location="cpu", weights_only=True)
    right = torch.load(expected, map_location="cpu", weights_only=True)
    maximum = 0.0

    def compare(a, b, path="state"):
        nonlocal maximum
        if isinstance(a, torch.Tensor):
            if (
                a.shape != b.shape
                or a.dtype != b.dtype
                or not torch.isfinite(a).all()
                or not torch.isfinite(b).all()
            ):
                raise AssertionError(
                    f"{path}: invalid tensor shape, dtype, or finite state"
                )
            delta = float((a - b).abs().max()) if a.numel() else 0.0
            maximum = max(maximum, delta)
            if delta > tolerance:
                raise AssertionError(
                    f"{path}: State error {delta} exceeds absolute {tolerance} ({a.dtype})"
                )
        elif isinstance(a, dict):
            if a.keys() != b.keys():
                raise AssertionError("State keys differ")
            for key in a:
                compare(a[key], b[key], f"{path}.{key}")
        elif isinstance(a, (list, tuple)):
            if len(a) != len(b):
                raise AssertionError("State lengths differ")
            for index, (x, y) in enumerate(zip(a, b)):
                compare(x, y, f"{path}[{index}]")
        elif a != b:
            raise AssertionError("State metadata/input alignment differs")

    compare(left, right)
    return maximum
