#!/usr/bin/env python3
"""Benchmark 72: differential system CPU profile for Fixed-R vs Succession K=1.

This runs Benchmark 69's exact profiling-OFF workload in fresh local Ray clusters
and samples Linux /proc during each timed-and-drained window. CPU is normalized
by every submitted pipeline because run_window drains all submitted work.

The sampler reports the owner driver/CoreWorker, borrower workers, other Ray
workers, Raylets, GCS/auxiliary processes, gRPC/CQ thread groups, and context
switches. Its own thread CPU is measured and subtracted. No recovery protocol,
R/W value, admission order, or Recovery Frontier behavior is changed.

Run:
  python gossip_benchmarks/72_recovery_succession_k1_system_cpu_profile.py
"""
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
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
BENCH59_PATH = HERE / "59_recovery_frontier_fixed_vs_succession_performance.py"
VARIANTS = ["fixed_r", "succession_k1"]
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
from _benchmark_common import safe_shutdown, wait_for_cluster  # noqa: E402


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
                if pid is not None and process.poll() is None:
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
    if "grpc" in lower or "event_engine" in lower or "completion" in lower:
        return "grpc_cq"
    if is_main:
        return "main"
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
        interval_s: float,
    ):
        self.root_pid = os.getpid()
        self.borrower_pids = set(borrower_pids)
        self.service_classes = dict(service_classes)
        self.root_pids = {self.root_pid, *self.service_classes}
        self.interval_s = interval_s
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.sampler_tid: int | None = None
        self.initialized = False
        self.samples = 0
        self.proc_first: dict[tuple[int, int], CpuPoint] = {}
        self.proc_last: dict[tuple[int, int], CpuPoint] = {}
        self.proc_class: dict[tuple[int, int], str] = {}
        self.thread_first: dict[tuple[int, int, int], CpuPoint] = {}
        self.thread_last: dict[tuple[int, int, int], CpuPoint] = {}
        self.thread_label: dict[tuple[int, int, int], tuple[str, str]] = {}

    def _sample(self) -> None:
        table = _process_table()
        selected = _descendants(table, self.root_pids)
        for pid in selected:
            info = table.get(pid)
            if info is None:
                continue
            pkey = (pid, info.start_ticks)
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
                if tkey not in self.thread_first:
                    self.thread_first[tkey] = point if not self.initialized else CpuPoint(0, 0)
                    self.thread_label[tkey] = (
                        self.proc_class[pkey], _thread_group(comm, tid == pid)
                    )
                self.thread_last[tkey] = point
        self.initialized = True
        self.samples += 1

    def _run(self) -> None:
        self.sampler_tid = threading.get_native_id()
        self._sample()
        while not self.stop_event.wait(self.interval_s):
            self._sample()

    def start(self) -> None:
        self._sample()
        self.thread = threading.Thread(target=self._run, name="b72-proc-sampler", daemon=True)
        self.thread.start()

    @staticmethod
    def _delta(first: CpuPoint, last: CpuPoint) -> CpuPoint:
        return CpuPoint(
            max(0, last.user_ticks - first.user_ticks),
            max(0, last.system_ticks - first.system_ticks),
            max(0, last.voluntary_ctx - first.voluntary_ctx),
            max(0, last.involuntary_ctx - first.involuntary_ctx),
        )

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, self.interval_s * 4))
            if self.thread.is_alive():
                raise RuntimeError("/proc sampler thread did not stop")
        self._sample()

        proc_ticks: dict[str, list[int]] = {}
        for key, last in self.proc_last.items():
            delta = self._delta(self.proc_first[key], last)
            row = proc_ticks.setdefault(self.proc_class[key], [0, 0])
            row[0] += delta.user_ticks
            row[1] += delta.system_ticks

        thread_ticks: dict[str, list[int]] = {}
        total_voluntary = total_involuntary = 0
        sampler_user = sampler_system = sampler_voluntary = sampler_involuntary = 0
        for key, last in self.thread_last.items():
            delta = self._delta(self.thread_first[key], last)
            process_class, group = self.thread_label[key]
            if key[0] == self.root_pid and key[1] == self.sampler_tid:
                sampler_user += delta.user_ticks
                sampler_system += delta.system_ticks
                sampler_voluntary += delta.voluntary_ctx
                sampler_involuntary += delta.involuntary_ctx
            else:
                label = f"{process_class}/{group}"
                row = thread_ticks.setdefault(label, [0, 0, 0, 0])
                row[0] += delta.user_ticks
                row[1] += delta.system_ticks
                row[2] += delta.voluntary_ctx
                row[3] += delta.involuntary_ctx
                total_voluntary += delta.voluntary_ctx
                total_involuntary += delta.involuntary_ctx

        owner = proc_ticks.get("owner_driver_coreworker", [0, 0])
        owner[0] = max(0, owner[0] - sampler_user)
        owner[1] = max(0, owner[1] - sampler_system)

        return {
            "process_ticks": proc_ticks,
            "thread_ticks": thread_ticks,
            "voluntary_ctx": total_voluntary,
            "involuntary_ctx": total_involuntary,
            "sampler_cpu_ticks": sampler_user + sampler_system,
            "sampler_ctx": sampler_voluntary + sampler_involuntary,
            "samples": self.samples,
            "processes_seen": len(self.proc_first),
            "threads_seen": len(self.thread_first),
        }


def _ticks_seconds(ticks: int) -> float:
    return ticks / CLK_TCK


def single_profile(args: argparse.Namespace) -> dict[str, Any]:
    if not Path("/proc/self/stat").exists():
        raise RuntimeError("Benchmark 72 requires Linux /proc")
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
        sampler = ProcTreeSampler(
            borrower_pids,
            service_classes,
            args.sample_interval_ms / 1000.0,
        )
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
            "sample_interval_ms": args.sample_interval_ms,
            "sample_count": sampled["samples"],
            "processes_seen": sampled["processes_seen"],
            "threads_seen": sampled["threads_seen"],
            "measured_elapsed_seconds": elapsed,
            "voluntary_ctx": sampled["voluntary_ctx"],
            "involuntary_ctx": sampled["involuntary_ctx"],
            "sampler_cpu_seconds": _ticks_seconds(sampled["sampler_cpu_ticks"]),
            "sampler_ctx": sampled["sampler_ctx"],
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
        "--sample-interval-ms", str(args.sample_interval_ms),
    ]


def _mean(rows: list[dict[str, Any]], variant: str, key: str) -> float:
    return statistics.fmean(float(r[key]) for r in rows if r["variant"] == variant)


def run(args: argparse.Namespace) -> None:
    if args.repetitions < 2:
        raise ValueError("--repetitions must be >= 2")
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")
    if args.sample_interval_ms < 20:
        raise ValueError("--sample-interval-ms must be >= 20")
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("Benchmark 72 requires R=2 and W=2")

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
        "K=1 system CPU differential: "
        f"reps={args.repetitions} warmup={args.warmup_seconds:.1f}s "
        f"timed={args.duration_seconds:.1f}s sample={args.sample_interval_ms:.0f}ms"
    )
    print(
        "  Fixed-R vs ordinary Succession; "
        "R=2 W=2 profiling=OFF; fresh cluster per case"
    )
    print("  /proc sampler CPU is measured and subtracted; all submitted work is drained")

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
            f"ctx={float(raw['context_switches_per_pipeline']):.2f}/pipeline"
        )

    print("\nFinal K=1 system CPU profile:")
    for variant in VARIANTS:
        print(
            f"  {variant:16s} thr={_mean(runs, variant, 'throughput_rps'):8.1f} rps  "
            f"total CPU={_mean(runs, variant, 'total_cpu_us_per_pipeline'):8.1f} us/task  "
            f"Ray children={_mean(runs, variant, 'cluster_cpu_us_per_pipeline'):8.1f} us/task  "
            f"gRPC/CQ={_mean(runs, variant, 'grpc_cq_cpu_us_per_pipeline'):7.1f} us/task  "
            f"ctx={_mean(runs, variant, 'context_switches_per_pipeline'):6.2f}/task"
        )

    by_case = {(str(r["variant"]), int(r["repetition"])): r for r in runs}
    paired: dict[str, list[float]] = {
        "throughput_pct": [], "total_cpu_us": [], "total_cpu_pct": [],
        "cluster_cpu_us": [], "grpc_cpu_us": [], "ctx": [],
    }
    for rep in range(1, args.repetitions + 1):
        fixed = by_case.get(("fixed_r", rep))
        succession = by_case.get(("succession_k1", rep))
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
        (class_means[("succession_k1", c)] - class_means[("fixed_r", c)], c)
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
            thread_means[("succession_k1", pc, group)] - thread_means[("fixed_r", pc, group)],
            pc, group,
        )
        for pc, group in labels
    ]
    print("\nLargest positive thread-group CPU deltas:")
    for delta, process_class, group in sorted(thread_deltas, reverse=True)[:12]:
        print(f"  {process_class + '/' + group:47s} {delta:+8.1f} us/task")

    print("\nDecision signal:")
    print("  A material Raylet/gRPC concentration supports a transport redesign.")
    print("  Otherwise stop K=1 micro-optimization and treat the remaining gap as structural.")
    print("  R=2, W=2, ordinary K=1 semantics, and the witness durability boundary are unchanged.")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-profile"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/72_recovery_succession_k1_system_cpu_profile",
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
    p.add_argument("--sample-interval-ms", type=float, default=100.0)
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
