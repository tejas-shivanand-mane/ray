#!/usr/bin/env python3
"""Linux process/thread CPU attribution for both methods at the selected K.

Run through 03_profile.py. Native profiling is OFF. Endpoint snapshots report
CPU, context switches, role/thread groups and endpoint churn. Ended processes
and threads can be missed; affected estimates are explicitly lower bounds."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
BENCH59_PATH = HERE / "comparison.py"
PROFILE_K = int(os.environ.get("RECOVERY_PROFILE_K", "1"))
if PROFILE_K not in (1, 2, 4, 8, 16, 32):
    raise ValueError("unsupported profiling K")
VARIANTS = [("fixed_r" if PROFILE_K == 1 else f"fixed_k{PROFILE_K}"), f"succession_k{PROFILE_K}"]
CLK_TCK = int(os.sysconf("SC_CLK_TCK"))


def _load_benchmark59():
    spec = importlib.util.spec_from_file_location("recovery_k1_system_cpu_b59", BENCH59_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH59_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b59 = _load_benchmark59()
b58 = b59.b58

import ray  # noqa: E402
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: E402
from common import safe_shutdown, wait_for_cluster  # noqa: E402


@dataclass(frozen=True)
class CpuPoint:
    user_ticks: int
    system_ticks: int
    voluntary_ctx: int = 0
    involuntary_ctx: int = 0


@dataclass(frozen=True)
class ProcInfo:
    pid: int
    ppid: int
    start_ticks: int
    comm: str
    cmdline: str
    point: CpuPoint


def _read_stat(path: Path) -> tuple[int, int, int, int, str] | None:
    try:
        raw = path.read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    close = raw.rfind(")")
    open_ = raw.find("(")
    if open_ < 0 or close < 0:
        return None
    fields = raw[close + 2 :].split()
    if len(fields) < 20:
        return None
    return int(fields[1]), int(fields[11]), int(fields[12]), int(fields[19]), raw[open_ + 1 : close]


def _read_ctx(path: Path) -> tuple[int, int]:
    voluntary = involuntary = 0
    try:
        for line in path.read_text().splitlines():
            if line.startswith("voluntary_ctxt_switches:"):
                voluntary = int(line.split(":", 1)[1])
            elif line.startswith("nonvoluntary_ctxt_switches:"):
                involuntary = int(line.split(":", 1)[1])
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
        pass
    return voluntary, involuntary


def _read_cmdline(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(
            "utf-8", "replace"
        ).strip()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return ""


def _process_table() -> dict[int, ProcInfo]:
    out: dict[int, ProcInfo] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        stat = _read_stat(entry / "stat")
        if stat is None:
            continue
        ppid, user, system, start, comm = stat
        out[pid] = ProcInfo(
            pid=pid,
            ppid=ppid,
            start_ticks=start,
            comm=comm,
            # Command lines are read lazily only for selected descendants.
            # Reading every system process's cmdline on every sample would add
            # avoidable profiler contention to the benchmark.
            cmdline="",
            point=CpuPoint(user, system),
        )
    return out


def _descendants(table: dict[int, ProcInfo], root_pids: set[int]) -> set[int]:
    selected = set(root_pids)
    changed = True
    while changed:
        changed = False
        for pid, info in table.items():
            if pid not in selected and info.ppid in selected:
                selected.add(pid)
                changed = True
    return selected


def _service_process_class(process_type: str) -> str:
    lower = process_type.lower()
    if "gcs_server" in lower:
        return "gcs_server"
    if "raylet" in lower:
        return "raylet"
    if "dashboard" in lower:
        return "dashboard"
    if "runtime_env" in lower:
        return "runtime_env_agent"
    if "log_monitor" in lower:
        return "log_monitor"
    if "monitor" in lower or "autoscaler" in lower:
        return "autoscaler_monitor"
    if "reaper" in lower:
        return "reaper"
    return "ray_auxiliary"


def _cluster_service_classes(cluster: Any) -> dict[int, str]:
    """Return authoritative PIDs for services started by Cluster/Node."""
    out: dict[int, str] = {}
    for node in cluster.list_all_nodes():
        for process_type, infos in node.all_processes.items():
            for info in infos:
                process = getattr(info, "process", None)
                pid = getattr(process, "pid", None)
                poll = getattr(process, "poll", None)
                alive = poll is None or poll() is None
                if pid is not None and alive:
                    out[int(pid)] = _service_process_class(str(process_type))
    return out


def _process_class(
    info: ProcInfo,
    root_pid: int,
    borrower_pids: set[int],
    service_classes: dict[int, str],
) -> str:
    if info.pid == root_pid:
        return "owner_driver_coreworker"
    if info.pid in borrower_pids:
        return "borrower_coreworker"
    if info.pid in service_classes:
        return service_classes[info.pid]
    text = f"{info.comm} {info.cmdline}".lower()
    if "gcs_server" in text:
        return "gcs_server"
    if "default_worker.py" in text or info.comm.startswith("ray::"):
        return "other_coreworker"
    first_arg = info.cmdline.split(" ", 1)[0]
    if info.comm == "raylet" or Path(first_arg).name == "raylet":
        return "raylet"
    if "dashboard" in text:
        return "dashboard"
    if "runtime_env" in text:
        return "runtime_env_agent"
    if "log_monitor" in text:
        return "log_monitor"
    if "monitor.py" in text or "autoscaler" in text:
        return "autoscaler_monitor"
    if "reaper" in text:
        return "reaper"
    return "ray_auxiliary"


def _thread_group(comm: str, is_main: bool) -> str:
    lower = comm.lower()
    if (
        "grpc" in lower
        or "event_engine" in lower
        or "completion" in lower
        or lower == "nexting_thread"
        or lower.startswith("client.poll")
        or lower.startswith("server.poll")
    ):
        return "grpc_cq"
    if is_main:
        return "main"
    if lower == "worker.io":
        return "coreworker_io"
    if "io_service" in lower or "event_loop" in lower or "event-loop" in lower:
        return "io_event_loop"
    if lower.startswith("ray::"):
        return "ray_internal"
    return comm[:48] or "unnamed"


class ProcTreeSampler:
    def __init__(
        self,
        borrower_pids: set[int],
        service_classes: dict[int, str],
    ):
        self.root_pid = os.getpid()
        self.borrower_pids = set(borrower_pids)
        self.service_classes = dict(service_classes)
        self.root_pids = {self.root_pid, *self.service_classes}
        self.initialized = False
        self.samples = 0
        self.start_proc_keys: set[tuple[int, int]] = set()
        self.start_thread_keys: set[tuple[int, int, int]] = set()
        self.proc_first: dict[tuple[int, int], CpuPoint] = {}
        self.proc_last: dict[tuple[int, int], CpuPoint] = {}
        self.proc_class: dict[tuple[int, int], str] = {}
        self.thread_first: dict[tuple[int, int, int], CpuPoint] = {}
        self.thread_last: dict[tuple[int, int, int], CpuPoint] = {}
        self.thread_label: dict[tuple[int, int, int], tuple[str, str]] = {}

    def _sample(self) -> tuple[set[tuple[int, int]], set[tuple[int, int, int]]]:
        table = _process_table()
        selected = _descendants(table, self.root_pids)
        current_proc_keys: set[tuple[int, int]] = set()
        current_thread_keys: set[tuple[int, int, int]] = set()
        for pid in selected:
            info = table.get(pid)
            if info is None:
                continue
            pkey = (pid, info.start_ticks)
            current_proc_keys.add(pkey)
            if pkey not in self.proc_first:
                self.proc_first[pkey] = info.point if not self.initialized else CpuPoint(0, 0)
                classified = ProcInfo(
                    pid=info.pid,
                    ppid=info.ppid,
                    start_ticks=info.start_ticks,
                    comm=info.comm,
                    cmdline=_read_cmdline(pid),
                    point=info.point,
                )
                self.proc_class[pkey] = _process_class(
                    classified,
                    self.root_pid,
                    self.borrower_pids,
                    self.service_classes,
                )
            self.proc_last[pkey] = info.point

            task_dir = Path(f"/proc/{pid}/task")
            try:
                tids = list(task_dir.iterdir())
            except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
                continue
            for task in tids:
                if not task.name.isdigit():
                    continue
                tid = int(task.name)
                stat = _read_stat(task / "stat")
                if stat is None:
                    continue
                _, user, system, thread_start, comm = stat
                voluntary, involuntary = _read_ctx(task / "status")
                point = CpuPoint(user, system, voluntary, involuntary)
                tkey = (pid, tid, thread_start)
                current_thread_keys.add(tkey)
                if tkey not in self.thread_first:
                    self.thread_first[tkey] = point if not self.initialized else CpuPoint(0, 0)
                    self.thread_label[tkey] = (
                        self.proc_class[pkey], _thread_group(comm, tid == pid)
                    )
                self.thread_last[tkey] = point
        self.initialized = True
        self.samples += 1
        return current_proc_keys, current_thread_keys

    def start(self) -> None:
        self.start_proc_keys, self.start_thread_keys = self._sample()

    @staticmethod
    def _delta(first: CpuPoint, last: CpuPoint) -> CpuPoint:
        return CpuPoint(
            max(0, last.user_ticks - first.user_ticks),
            max(0, last.system_ticks - first.system_ticks),
            max(0, last.voluntary_ctx - first.voluntary_ctx),
            max(0, last.involuntary_ctx - first.involuntary_ctx),
        )

    def stop(self) -> dict[str, Any]:
        end_proc_keys, end_thread_keys = self._sample()

        proc_ticks: dict[str, list[int]] = {}
        for key, last in self.proc_last.items():
            delta = self._delta(self.proc_first[key], last)
            row = proc_ticks.setdefault(self.proc_class[key], [0, 0])
            row[0] += delta.user_ticks
            row[1] += delta.system_ticks

        thread_ticks: dict[str, list[int]] = {}
        total_voluntary = total_involuntary = 0
        for key, last in self.thread_last.items():
            delta = self._delta(self.thread_first[key], last)
            process_class, group = self.thread_label[key]
            label = f"{process_class}/{group}"
            row = thread_ticks.setdefault(label, [0, 0, 0, 0])
            row[0] += delta.user_ticks
            row[1] += delta.system_ticks
            row[2] += delta.voluntary_ctx
            row[3] += delta.involuntary_ctx
            total_voluntary += delta.voluntary_ctx
            total_involuntary += delta.involuntary_ctx

        return {
            "process_ticks": proc_ticks,
            "thread_ticks": thread_ticks,
            "voluntary_ctx": total_voluntary,
            "involuntary_ctx": total_involuntary,
            "samples": self.samples,
            "processes_seen": len(self.proc_first),
            "threads_seen": len(self.thread_first),
            "processes_started": len(end_proc_keys - self.start_proc_keys),
            "processes_ended": len(self.start_proc_keys - end_proc_keys),
            "threads_started": len(end_thread_keys - self.start_thread_keys),
            "threads_ended": len(self.start_thread_keys - end_thread_keys),
        }


def _ticks_seconds(ticks: int) -> float:
    return ticks / CLK_TCK


def single_profile(args: argparse.Namespace) -> dict[str, Any]:
    if not Path("/proc/self/stat").exists():
        raise RuntimeError("System profiling requires Linux /proc")
    cluster = None
    sampler: ProcTreeSampler | None = None
    try:
        cluster, producer_node = b58.start_cluster(args, args.single_variant, False)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + args.holders + args.witness_count, args.cluster_timeout_seconds)
        produce, Borrower = b58.remote_types()
        borrowers = [
            Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote()
            for i in range(args.holders)
        ]
        borrower_pids = {int(pid) for pid in ray.get([b.ping.remote() for b in borrowers])}
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = b58.build_padding(args.single_padding_bytes, args.inline_chunk_bytes)

        if args.warmup_seconds > 0:
            b58.run_window(
                produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
                payload_bytes=args.payload_bytes, duration_s=args.warmup_seconds,
                inflight=args.inflight_tasks, burst=args.burst_size,
                wait_timeout=args.wait_timeout_seconds,
                drain_timeout=args.drain_timeout_seconds, request_base=1_000_000,
            )
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        service_classes = _cluster_service_classes(cluster)
        if not any(value == "raylet" for value in service_classes.values()):
            raise RuntimeError("Cluster.all_processes did not expose a live Raylet PID")
        sampler = ProcTreeSampler(borrower_pids, service_classes)
        sampler.start()
        started = time.perf_counter()
        perf = b58.run_window(
            produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
            payload_bytes=args.payload_bytes, duration_s=args.duration_seconds,
            inflight=args.inflight_tasks, burst=args.burst_size,
            wait_timeout=args.wait_timeout_seconds,
            drain_timeout=args.drain_timeout_seconds, request_base=10_000_000,
        )
        elapsed = time.perf_counter() - started
        sampled = sampler.stop()
        sampler = None

        process_cpu: dict[str, dict[str, float]] = {}
        for process_class, ticks in sampled["process_ticks"].items():
            process_cpu[process_class] = {
                "user_seconds": _ticks_seconds(ticks[0]),
                "system_seconds": _ticks_seconds(ticks[1]),
            }
        thread_cpu: dict[str, dict[str, float | int]] = {}
        for label, ticks in sampled["thread_ticks"].items():
            thread_cpu[label] = {
                "cpu_seconds": _ticks_seconds(ticks[0] + ticks[1]),
                "voluntary_ctx": ticks[2],
                "involuntary_ctx": ticks[3],
            }

        required_classes = {"owner_driver_coreworker", "borrower_coreworker", "raylet"}
        missing = required_classes - process_cpu.keys()
        if missing:
            raise RuntimeError(f"/proc attribution missed required process classes: {sorted(missing)}")

        return {
            "variant": args.single_variant,
            "repetition": args.single_repetition,
            "profiling_enabled": 0,
            "holders": args.holders,
            "witness_count": args.witness_count,
            "task_spec_padding_bytes": args.single_padding_bytes,
            "payload_bytes": args.payload_bytes,
            "burst_size": args.burst_size,
            "inflight_tasks": args.inflight_tasks,
            "sample_count": sampled["samples"],
            "processes_seen": sampled["processes_seen"],
            "threads_seen": sampled["threads_seen"],
            "processes_started": sampled["processes_started"],
            "processes_ended": sampled["processes_ended"],
            "threads_started": sampled["threads_started"],
            "threads_ended": sampled["threads_ended"],
            "measured_elapsed_seconds": elapsed,
            "voluntary_ctx": sampled["voluntary_ctx"],
            "involuntary_ctx": sampled["involuntary_ctx"],
            "process_cpu": process_cpu,
            "thread_cpu": thread_cpu,
            **perf,
        }
    finally:
        if sampler is not None:
            sampler.stop()
        safe_shutdown(ray, cluster)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.unlink(missing_ok=True)
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _order(rep: int, seed: int) -> list[str]:
    base = list(VARIANTS)
    random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def _child_cmd(args: argparse.Namespace, variant: str, rep: int, temp: Path) -> list[str]:
    return [
        sys.executable, str(Path(__file__).resolve()), "_single-profile",
        "--single-variant", variant, "--single-repetition", str(rep),
        "--single-padding-bytes", str(args.task_spec_padding_bytes),
        "--single-output-json", str(temp),
        "--holders", str(args.holders), "--witness-count", str(args.witness_count),
        "--payload-bytes", str(args.payload_bytes),
        "--inline-chunk-bytes", str(args.inline_chunk_bytes),
        "--burst-size", str(args.burst_size), "--inflight-tasks", str(args.inflight_tasks),
        "--warmup-seconds", str(args.warmup_seconds),
        "--settle-seconds", str(args.settle_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--cpus-per-node", str(args.cpus_per_node),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
    ]


def _mean(rows: list[dict[str, Any]], variant: str, key: str) -> float:
    return statistics.fmean(float(r[key]) for r in rows if r["variant"] == variant)


def run(args: argparse.Namespace) -> None:
    if args.repetitions < 2:
        raise ValueError("--repetitions must be >= 2")
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("System profiling requires R=2 and W=2")

    out = Path(args.output_dir)
    runs_path = out / "system_cpu_runs.csv"
    process_path = out / "system_cpu_process_classes.csv"
    thread_path = out / "system_cpu_thread_classes.csv"
    if args.overwrite:
        for path in (runs_path, process_path, thread_path):
            path.unlink(missing_ok=True)
    runs: list[dict[str, Any]] = [dict(r) for r in _read_csv(runs_path)]
    process_rows: list[dict[str, Any]] = [dict(r) for r in _read_csv(process_path)]
    thread_rows: list[dict[str, Any]] = [dict(r) for r in _read_csv(thread_path)]
    completed = {(str(r["variant"]), int(r["repetition"])) for r in runs}
    cases = [
        (rep, variant, pos)
        for rep in range(1, args.repetitions + 1)
        for pos, variant in enumerate(_order(rep, args.seed), 1)
    ]
    pending = [c for c in cases if (c[1], c[0]) not in completed]
    print(
        "system CPU differential: "
        f"reps={args.repetitions} warmup={args.warmup_seconds:.1f}s "
        f"timed={args.duration_seconds:.1f}s snapshots=start/end"
    )
    print(
        "  Fixed-R vs Succession; "
        "R=2 W=2 profiling=OFF; fresh cluster per case"
    )
    print("  endpoint snapshots do not run during the workload; all submitted work is drained")

    for i, (rep, variant, pos) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} "
            f"position={pos}/2 variant={variant}",
            flush=True,
        )
        temp = out / f".system_cpu_{variant}_{rep}.json"
        temp.parent.mkdir(parents=True, exist_ok=True)
        temp.unlink(missing_ok=True)
        proc = subprocess.run(
            _child_cmd(args, variant, rep, temp),
            env=b58.child_env(profiling=False),
        )
        if proc.returncode != 0 or not temp.exists():
            _write_csv(runs_path, runs)
            _write_csv(process_path, process_rows)
            _write_csv(thread_path, thread_rows)
            raise SystemExit(proc.returncode or 1)
        raw = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        process_cpu = raw.pop("process_cpu")
        thread_cpu = raw.pop("thread_cpu")
        pipelines = int(raw["total_pipeline_submitted"])
        raw["block_position"] = pos
        total_cpu = 0.0
        cluster_cpu = 0.0
        for process_class, values in process_cpu.items():
            cpu = float(values["user_seconds"]) + float(values["system_seconds"])
            total_cpu += cpu
            if process_class != "owner_driver_coreworker":
                cluster_cpu += cpu
            process_rows.append({
                "variant": variant, "repetition": rep, "block_position": pos,
                "process_class": process_class,
                "user_seconds": values["user_seconds"],
                "system_seconds": values["system_seconds"],
                "cpu_seconds": cpu,
                "cpu_us_per_pipeline": 1e6 * cpu / pipelines,
            })
        grpc_cpu = sum(
            float(values["cpu_seconds"])
            for label, values in thread_cpu.items()
            if label.endswith("/grpc_cq")
        )
        coreworker_io_cpu = sum(
            float(values["cpu_seconds"])
            for label, values in thread_cpu.items()
            if label.endswith("/coreworker_io")
        )
        for label, values in thread_cpu.items():
            process_class, thread_group = label.split("/", 1)
            thread_rows.append({
                "variant": variant, "repetition": rep, "block_position": pos,
                "process_class": process_class, "thread_group": thread_group,
                "cpu_seconds": values["cpu_seconds"],
                "cpu_us_per_pipeline": 1e6 * float(values["cpu_seconds"]) / pipelines,
                "voluntary_ctx": values["voluntary_ctx"],
                "involuntary_ctx": values["involuntary_ctx"],
            })
        raw.update({
            "total_cpu_seconds": total_cpu,
            "cluster_cpu_seconds": cluster_cpu,
            "total_cpu_us_per_pipeline": 1e6 * total_cpu / pipelines,
            "cluster_cpu_us_per_pipeline": 1e6 * cluster_cpu / pipelines,
            "grpc_cq_cpu_us_per_pipeline": 1e6 * grpc_cpu / pipelines,
            "coreworker_io_cpu_us_per_pipeline": 1e6 * coreworker_io_cpu / pipelines,
            "context_switches_per_pipeline": (
                int(raw["voluntary_ctx"]) + int(raw["involuntary_ctx"])
            ) / pipelines,
            "average_total_cores": total_cpu / float(raw["measured_elapsed_seconds"]),
        })
        runs.append(raw)
        _write_csv(runs_path, runs)
        _write_csv(process_path, process_rows)
        _write_csv(thread_path, thread_rows)
        print(
            f"  throughput={float(raw['throughput_rps']):.1f} rps "
            f"CPU={float(raw['total_cpu_us_per_pipeline']):.1f} us/pipeline "
            f"ctx={float(raw['context_switches_per_pipeline']):.2f}/pipeline "
            f"churn=proc+{int(raw['processes_started'])}/-{int(raw['processes_ended'])} "
            f"thread+{int(raw['threads_started'])}/-{int(raw['threads_ended'])}"
        )

    print("\nFinal system CPU profile:")
    for variant in VARIANTS:
        print(
            f"  {variant:16s} thr={_mean(runs, variant, 'throughput_rps'):8.1f} rps  "
            f"total CPU={_mean(runs, variant, 'total_cpu_us_per_pipeline'):8.1f} us/task  "
            f"Ray children={_mean(runs, variant, 'cluster_cpu_us_per_pipeline'):8.1f} us/task  "
            f"gRPC/CQ={_mean(runs, variant, 'grpc_cq_cpu_us_per_pipeline'):7.1f} us/task  "
            f"CoreWorker-I/O={_mean(runs, variant, 'coreworker_io_cpu_us_per_pipeline'):7.1f} us/task  "
            f"ctx={_mean(runs, variant, 'context_switches_per_pipeline'):6.2f}/task"
        )

    by_case = {(str(r["variant"]), int(r["repetition"])): r for r in runs}
    paired: dict[str, list[float]] = {
        "throughput_pct": [], "total_cpu_us": [], "total_cpu_pct": [],
        "cluster_cpu_us": [], "grpc_cpu_us": [], "ctx": [],
        "coreworker_io_us": [],
    }
    for rep in range(1, args.repetitions + 1):
        fixed = by_case.get((VARIANTS[0], rep))
        succession = by_case.get((VARIANTS[1], rep))
        if fixed is None or succession is None:
            continue
        fthr, sthr = float(fixed["throughput_rps"]), float(succession["throughput_rps"])
        fcpu, scpu = float(fixed["total_cpu_us_per_pipeline"]), float(succession["total_cpu_us_per_pipeline"])
        paired["throughput_pct"].append(100.0 * (sthr - fthr) / fthr)
        paired["total_cpu_us"].append(scpu - fcpu)
        paired["total_cpu_pct"].append(100.0 * (scpu - fcpu) / fcpu)
        paired["cluster_cpu_us"].append(
            float(succession["cluster_cpu_us_per_pipeline"]) - float(fixed["cluster_cpu_us_per_pipeline"])
        )
        paired["grpc_cpu_us"].append(
            float(succession["grpc_cq_cpu_us_per_pipeline"]) - float(fixed["grpc_cq_cpu_us_per_pipeline"])
        )
        paired["coreworker_io_us"].append(
            float(succession["coreworker_io_cpu_us_per_pipeline"])
            - float(fixed["coreworker_io_cpu_us_per_pipeline"])
        )
        paired["ctx"].append(
            float(succession["context_switches_per_pipeline"]) - float(fixed["context_switches_per_pipeline"])
        )

    print("\nPaired Succession minus Fixed-R:")
    print(f"  throughput                         = {statistics.fmean(paired['throughput_pct']):+8.2f}%")
    print(
        f"  measured total CPU                = {statistics.fmean(paired['total_cpu_us']):+8.1f} us/task "
        f"({statistics.fmean(paired['total_cpu_pct']):+7.2f}%)"
    )
    print(f"  Ray child-process CPU             = {statistics.fmean(paired['cluster_cpu_us']):+8.1f} us/task")
    print(f"  gRPC/CQ-thread CPU                = {statistics.fmean(paired['grpc_cpu_us']):+8.1f} us/task")
    print(
        "  CoreWorker I/O-thread CPU        = "
        f"{statistics.fmean(paired['coreworker_io_us']):+8.1f} us/task"
    )
    print(f"  context switches                  = {statistics.fmean(paired['ctx']):+8.2f} /task")

    class_means: dict[tuple[str, str], float] = {}
    classes = sorted({str(r["process_class"]) for r in process_rows})
    for variant in VARIANTS:
        for process_class in classes:
            by_rep = {
                int(r["repetition"]): float(r["cpu_us_per_pipeline"])
                for r in process_rows
                if r["variant"] == variant and r["process_class"] == process_class
            }
            class_means[(variant, process_class)] = statistics.fmean(
                by_rep.get(rep, 0.0) for rep in range(1, args.repetitions + 1)
            )
    deltas = [
        (class_means[(VARIANTS[1], c)] - class_means[(VARIANTS[0], c)], c)
        for c in classes
    ]
    print("\nProcess-class CPU deltas (Succession - Fixed-R):")
    for delta, process_class in sorted(deltas, reverse=True):
        print(f"  {process_class:31s} {delta:+8.1f} us/task")

    thread_means: dict[tuple[str, str, str], float] = {}
    labels = sorted({(str(r["process_class"]), str(r["thread_group"])) for r in thread_rows})
    for variant in VARIANTS:
        for process_class, group in labels:
            by_rep = {
                int(r["repetition"]): float(r["cpu_us_per_pipeline"])
                for r in thread_rows
                if r["variant"] == variant and r["process_class"] == process_class
                and r["thread_group"] == group
            }
            thread_means[(variant, process_class, group)] = statistics.fmean(
                by_rep.get(rep, 0.0) for rep in range(1, args.repetitions + 1)
            )
    thread_deltas = [
        (
            thread_means[(VARIANTS[1], pc, group)] - thread_means[(VARIANTS[0], pc, group)],
            pc, group,
        )
        for pc, group in labels
    ]
    print("\nLargest positive thread-group CPU deltas:")
    for delta, process_class, group in sorted(thread_deltas, reverse=True)[:12]:
        print(f"  {process_class + '/' + group:47s} {delta:+8.1f} us/task")

    print("\nDecision signal:")
    started_processes = sum(int(r["processes_started"]) for r in runs)
    ended_processes = sum(int(r["processes_ended"]) for r in runs)
    started_threads = sum(int(r["threads_started"]) for r in runs)
    ended_threads = sum(int(r["threads_ended"]) for r in runs)
    print(
        "  endpoint churn                   = "
        f"process+{started_processes}/-{ended_processes} "
        f"thread+{started_threads}/-{ended_threads}"
    )
    print(
        "  endpoint coverage                = "
        + (
            "complete for pre-existing processes/threads"
            if ended_processes == 0 and ended_threads == 0
            else (
                f"LOWER BOUND: {ended_processes} processes and "
                f"{ended_threads} threads ended between snapshots"
            )
        )
    )
    print("  A material Raylet/gRPC concentration supports a transport redesign.")
    print("  R=2, W=2, selected K semantics, and the witness durability boundary are unchanged.")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-profile"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default=str(HERE.parent / "results" / "profile_system"),
    )
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warmup-seconds", type=float, default=1.0)
    p.add_argument("--settle-seconds", type=float, default=0.25)
    p.add_argument("--duration-seconds", type=float, default=6.0)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--single-variant")
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-output-json")
    return p


def main() -> None:
    args = parser().parse_args()
    if args.command == "_single-profile":
        if args.single_variant not in VARIANTS:
            raise ValueError("invalid system-profile variant")
        if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
            raise ValueError("system-profile child requires RAY_RECOVERY_PROFILING=0")
        Path(args.single_output_json).write_text(json.dumps(single_profile(args), allow_nan=True))
    else:
        run(args)


if __name__ == "__main__":
    main()
