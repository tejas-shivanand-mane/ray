#!/usr/bin/env python3
"""Benchmark 73: native stack attribution for Fixed-R vs Succession K=1.

Linux perf launches each benchmark child and inherits sampling into its Ray
process tree. Events remain disabled through cluster startup and warmup; FIFO
control with acknowledgements enables them immediately before the measured
window and disables them immediately after its drain. This avoids attaching a
child profiler back to the owner and retains samples from short-lived threads.

The diagnostic reports owner, borrower, and producer-worker user-space stacks
for Benchmark 69's profiling-OFF workload shape. Categories are inclusive and
may overlap. Recovery profiling remains OFF. R=2, W=2, ordinary K=1 ordering,
the witness durability boundary, Fixed-R, and Recovery Frontier are unchanged.

Run:
  python gossip_benchmarks/73_recovery_succession_k1_native_stack_profile.py \
      --overwrite
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import re
import select
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
BENCH72_PATH = HERE / "72_recovery_succession_k1_system_cpu_profile.py"
VARIANTS = ["fixed_r", "succession_k1"]
ROLES = ["owner", "borrowers", "producer_workers"]

CATEGORY_PATTERNS: dict[str, tuple[str, ...]] = {
    "candidate_report": (
        "candidate",
        "recoveryholdercandidate",
        "recovery_holder_candidate",
        "reportrecoveryholder",
    ),
    "witness_update": (
        "witness",
        "updaterecoverywitness",
        "update_recovery_witness",
    ),
    "grpc_cq": (
        "grpc",
        "completionqueue",
        "completion_queue",
        "clientcall",
        "client_call",
        "polleventsfromcompletionqueue",
        "poll_events_from_completion_queue",
        "nexting_thread",
        "client.poll",
        "server.poll",
    ),
    "coreworker_scheduling": (
        "coreworkerprocess",
        "core_worker_process",
        "coreworker.io",
        "coreworker_io",
        "worker.io",
        "instrumentedio",
        "instrumented_io",
        "asio::io_context",
        "io_context::",
    ),
    "task_objectref": (
        "objectref",
        "object_ref",
        "referencecounter",
        "reference_counter",
        "taskmanager",
        "task_manager",
        "task_event",
        "submitter",
    ),
}


def _load_benchmark72():
    spec = importlib.util.spec_from_file_location("recovery_k1_native_stack_b72", BENCH72_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH72_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b72 = _load_benchmark72()
b58 = b72.b58

import ray  # noqa: E402
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: E402
from _benchmark_common import safe_shutdown, wait_for_cluster  # noqa: E402


def _perf_or_raise() -> str:
    perf = shutil.which("perf")
    if perf is None:
        raise RuntimeError(
            "Linux perf is not installed. On Ubuntu, install the linux-tools "
            "package matching the running kernel, then rerun Benchmark 73."
        )
    return perf


def _perf_failure(prefix: str, stderr: str) -> RuntimeError:
    detail = stderr.strip() or "perf returned no diagnostic text"
    return RuntimeError(
        f"{prefix}: {detail}\n"
        "Benchmark 73 does not use sudo automatically. If access was denied, inspect "
        "/proc/sys/kernel/perf_event_paranoid and grant user-space perf access before rerunning."
    )


def _open_control_fifos(control_path: Path, ack_path: Path) -> tuple[int, int]:
    for path in (control_path, ack_path):
        path.unlink(missing_ok=True)
        os.mkfifo(path, 0o600)
    # Opening both ends keeps perf and its command child from deadlocking while
    # they independently open the named pipes.
    control_keepalive = os.open(control_path, os.O_RDWR | os.O_NONBLOCK)
    try:
        ack_keepalive = os.open(ack_path, os.O_RDWR | os.O_NONBLOCK)
    except Exception:
        os.close(control_keepalive)
        raise
    return control_keepalive, ack_keepalive


def _close_control_fifos(
    keepalive: tuple[int, int] | None,
    control_path: Path,
    ack_path: Path,
) -> None:
    if keepalive is not None:
        for fd in keepalive:
            try:
                os.close(fd)
            except OSError:
                pass
    control_path.unlink(missing_ok=True)
    ack_path.unlink(missing_ok=True)


def _controlled_perf_command(
    perf: str,
    data_path: Path,
    control_path: Path,
    ack_path: Path,
    frequency: int,
    call_graph: str,
    command: list[str],
) -> list[str]:
    return [
        perf,
        "record",
        "--quiet",
        "--event", "cpu-clock:u",
        "--freq", str(frequency),
        "--call-graph", call_graph,
        "--inherit",
        "--delay=-1",
        "--control", f"fifo:{control_path},{ack_path}",
        "--output", str(data_path),
        "--",
        *command,
    ]


def _run_controlled_perf(
    perf: str,
    data_path: Path,
    stderr_path: Path,
    frequency: int,
    call_graph: str,
    command: list[str],
    env: dict[str, str] | None,
) -> subprocess.CompletedProcess[Any]:
    control_path = data_path.with_suffix(data_path.suffix + ".control.fifo")
    ack_path = data_path.with_suffix(data_path.suffix + ".ack.fifo")
    data_path.unlink(missing_ok=True)
    data_path.with_name(data_path.name + ".old").unlink(missing_ok=True)
    stderr_path.unlink(missing_ok=True)
    keepalive: tuple[int, int] | None = None
    try:
        keepalive = _open_control_fifos(control_path, ack_path)
        child_command = [
            *command,
            "--single-control-fifo", str(control_path),
            "--single-ack-fifo", str(ack_path),
        ]
        perf_command = _controlled_perf_command(
            perf,
            data_path,
            control_path,
            ack_path,
            frequency,
            call_graph,
            child_command,
        )
        with stderr_path.open("w") as stderr_file:
            return subprocess.run(perf_command, env=env, stderr=stderr_file)
    finally:
        _close_control_fifos(keepalive, control_path, ack_path)


def _open_child_control(args: argparse.Namespace) -> tuple[int, int]:
    if not args.single_control_fifo or not args.single_ack_fifo:
        raise ValueError("perf control FIFO paths are required")
    control_fd = os.open(args.single_control_fifo, os.O_WRONLY)
    try:
        ack_fd = os.open(args.single_ack_fifo, os.O_RDONLY)
    except Exception:
        os.close(control_fd)
        raise
    return control_fd, ack_fd


def _control_perf(
    control_fd: int,
    ack_fd: int,
    command: str,
    timeout_seconds: float,
) -> None:
    os.write(control_fd, f"{command}\n".encode())
    readable, _, _ = select.select([ack_fd], [], [], timeout_seconds)
    if not readable:
        raise RuntimeError(f"timed out waiting for perf '{command}' acknowledgement")
    # perf versions differ in whether FIFO acknowledgements include a trailing
    # NUL after the documented ``ack\n`` payload.
    reply = os.read(ack_fd, 64).decode("utf-8", "replace").strip(" \t\r\n\x00")
    if reply != "ack":
        raise RuntimeError(f"unexpected perf '{command}' acknowledgement: {reply!r}")


def _control_smoke(args: argparse.Namespace) -> None:
    control_fd, ack_fd = _open_child_control(args)
    try:
        _control_perf(control_fd, ack_fd, "enable", args.perf_control_timeout_seconds)
        deadline = time.perf_counter() + 0.25
        value = 1
        while time.perf_counter() < deadline:
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
        if value < 0:  # keep the loop observable without printing
            raise AssertionError(value)
        _control_perf(control_fd, ack_fd, "disable", args.perf_control_timeout_seconds)
    finally:
        os.close(ack_fd)
        os.close(control_fd)


def _preflight(perf: str, out: Path, args: argparse.Namespace) -> None:
    data_path = out / ".perf_preflight.data"
    stderr_path = out / ".perf_preflight.stderr.txt"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "_control-smoke",
        "--perf-control-timeout-seconds", str(args.perf_control_timeout_seconds),
    ]
    try:
        proc = _run_controlled_perf(
            perf,
            data_path,
            stderr_path,
            19,
            args.call_graph,
            command,
            os.environ.copy(),
        )
        ok = proc.returncode == 0 and data_path.exists() and data_path.stat().st_size > 0
        if not ok:
            detail = stderr_path.read_text() if stderr_path.exists() else ""
            raise _perf_failure(
                "perf preflight failed; Benchmark 73 requires perf FIFO control support",
                detail,
            )
    finally:
        data_path.unlink(missing_ok=True)
        data_path.with_name(data_path.name + ".old").unlink(missing_ok=True)
        stderr_path.unlink(missing_ok=True)


def _run_perf_text(
    cmd: list[str],
    label: str,
    timeout_seconds: float,
) -> str:
    env = os.environ.copy()
    # Ubuntu may configure remote debuginfod servers. Native Ray symbols are
    # already available locally, so network lookup only makes analysis hang
    # when those servers are unreachable.
    env["DEBUGINFOD_URLS"] = ""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{label} exceeded {timeout_seconds:.0f}s; "
            "the perf analysis subprocess was terminated"
        ) from exc
    if proc.returncode != 0:
        raise _perf_failure(label, proc.stderr)
    return proc.stdout


def _leaf_symbol(block: str) -> str | None:
    lines = block.splitlines()[1:]
    for line in lines:
        stripped = line.strip()
        match = re.match(r"^[0-9a-f]+\s+(.+?)\s+\([^()]+\)$", stripped)
        if match is None:
            continue
        symbol = match.group(1).strip()
        if symbol and "unknown" not in symbol.lower():
            return symbol
    return None


def _analyze_script(script: str) -> dict[str, Any]:
    blocks = [block.strip() for block in re.split(r"\n\s*\n", script) if block.strip()]
    samples = [block for block in blocks if "cpu-clock" in block.splitlines()[0].lower()]
    if not samples:
        # Some perf versions omit the event when the input contains only one.
        samples = blocks
    category_counts = {name: 0 for name in CATEGORY_PATTERNS}
    symbolized = 0
    leaf_counts: Counter[str] = Counter()
    matched = 0
    for block in samples:
        lower = block.lower()
        block_matched = False
        for name, patterns in CATEGORY_PATTERNS.items():
            if any(pattern in lower for pattern in patterns):
                category_counts[name] += 1
                block_matched = True
        if block_matched:
            matched += 1
        leaf = _leaf_symbol(block)
        if leaf is not None:
            symbolized += 1
            leaf_counts[leaf] += 1
    return {
        "samples": len(samples),
        "symbolized_samples": symbolized,
        "matched_samples": matched,
        "category_counts": category_counts,
        "top_leaf_symbols": [
            {"symbol": symbol, "samples": count}
            for symbol, count in leaf_counts.most_common(12)
        ],
    }


def _analyze_role(
    perf: str,
    data_path: Path,
    case_dir: Path,
    role: str,
    pids: list[int],
    timeout_seconds: float,
) -> dict[str, Any]:
    script_path = case_dir / f"{role}.perf.script.txt"
    script = _run_perf_text(
        [
            perf,
            "script",
            "--pid", ",".join(str(pid) for pid in pids),
            "--input", str(data_path),
        ],
        f"perf script failed for {role}",
        timeout_seconds,
    )
    script_path.write_text(script)
    analysis = _analyze_script(script)
    if int(analysis["samples"]) == 0:
        raise RuntimeError(
            f"perf captured no samples for {role} PIDs {pids}; "
            "increase --duration-seconds before interpreting this profile"
        )
    analysis.update(
        {
            "role": role,
            "pids": pids,
            "perf_data": str(data_path),
            "perf_script": str(script_path),
            "perf_data_bytes": data_path.stat().st_size,
        }
    )
    return analysis


def _coreworker_roles(cluster: Any, borrower_pids: set[int]) -> dict[str, list[int]]:
    service_classes = b72._cluster_service_classes(cluster)
    table = b72._process_table()
    selected = b72._descendants(table, {os.getpid(), *service_classes})
    producer_workers: set[int] = set()
    for pid in selected:
        info = table.get(pid)
        if info is None or pid == os.getpid() or pid in borrower_pids:
            continue
        classified = b72.ProcInfo(
            pid=info.pid,
            ppid=info.ppid,
            start_ticks=info.start_ticks,
            comm=info.comm,
            cmdline=b72._read_cmdline(pid),
            point=info.point,
        )
        process_class = b72._process_class(
            classified,
            os.getpid(),
            borrower_pids,
            service_classes,
        )
        if process_class == "other_coreworker":
            producer_workers.add(pid)
    return {
        "owner": [os.getpid()],
        "borrowers": sorted(borrower_pids),
        "producer_workers": sorted(producer_workers),
    }


def single_profile(args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    control_fd: int | None = None
    ack_fd: int | None = None
    sampling_enabled = False
    try:
        print(f"  {args.single_variant}: starting Ray cluster", flush=True)
        cluster, producer_node = b58.start_cluster(args, args.single_variant, False)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(
            ray,
            1 + 1 + args.holders + args.witness_count,
            args.cluster_timeout_seconds,
        )
        print(f"  {args.single_variant}: cluster ready; starting warmup", flush=True)
        produce, Borrower = b58.remote_types()
        borrowers = [
            Borrower.options(
                resources={f"borrower_node_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(args.holders)
        ]
        borrower_pids = {int(pid) for pid in ray.get([b.ping.remote() for b in borrowers])}
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = b58.build_padding(args.single_padding_bytes, args.inline_chunk_bytes)

        if args.warmup_seconds > 0:
            b58.run_window(
                produce=produce,
                borrowers=borrowers,
                strategy=strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight=args.inflight_tasks,
                burst=args.burst_size,
                wait_timeout=args.wait_timeout_seconds,
                drain_timeout=args.drain_timeout_seconds,
                request_base=1_000_000,
            )
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        role_pids = _coreworker_roles(cluster, borrower_pids)
        if len(role_pids["borrowers"]) != 2:
            raise RuntimeError(f"expected two borrower CoreWorkers, got {role_pids['borrowers']}")
        if not role_pids["producer_workers"]:
            raise RuntimeError("warmup exposed no producer-side CoreWorker PID")
        print(
            f"  {args.single_variant}: warmup drained; "
            f"owner={len(role_pids['owner'])} borrowers={len(role_pids['borrowers'])} "
            f"producer_workers={len(role_pids['producer_workers'])}",
            flush=True,
        )

        control_fd, ack_fd = _open_child_control(args)
        _control_perf(control_fd, ack_fd, "enable", args.perf_control_timeout_seconds)
        sampling_enabled = True
        print(
            f"  {args.single_variant}: sampling enabled; running timed window",
            flush=True,
        )
        started = time.perf_counter()
        measured = b58.run_window(
            produce=produce,
            borrowers=borrowers,
            strategy=strategy,
            padding=padding,
            payload_bytes=args.payload_bytes,
            duration_s=args.duration_seconds,
            inflight=args.inflight_tasks,
            burst=args.burst_size,
            wait_timeout=args.wait_timeout_seconds,
            drain_timeout=args.drain_timeout_seconds,
            request_base=10_000_000,
        )
        elapsed = time.perf_counter() - started
        _control_perf(control_fd, ack_fd, "disable", args.perf_control_timeout_seconds)
        sampling_enabled = False
        print(
            f"  {args.single_variant}: timed window drained in {elapsed:.2f}s; "
            "sampling disabled",
            flush=True,
        )
        return {
            "variant": args.single_variant,
            "profiling_enabled": 0,
            "holders": args.holders,
            "witness_count": args.witness_count,
            "frequency_hz": args.frequency,
            "call_graph": args.call_graph,
            "measured_elapsed_seconds": elapsed,
            "role_pids": role_pids,
            **measured,
        }
    finally:
        if sampling_enabled and control_fd is not None and ack_fd is not None:
            try:
                _control_perf(
                    control_fd,
                    ack_fd,
                    "disable",
                    args.perf_control_timeout_seconds,
                )
            except Exception:
                pass
        if ack_fd is not None:
            os.close(ack_fd)
        if control_fd is not None:
            os.close(control_fd)
        print(f"  {args.single_variant}: shutting down Ray cluster", flush=True)
        safe_shutdown(ray, cluster)
        print(f"  {args.single_variant}: Ray shutdown complete", flush=True)


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


def _summary_row(
    variant: str,
    analysis: dict[str, Any],
    pipelines: int,
    frequency: int,
) -> dict[str, Any]:
    samples = int(analysis["samples"])
    sample_us = 1e6 / frequency
    row: dict[str, Any] = {
        "variant": variant,
        "role": analysis["role"],
        "pids": json.dumps(analysis["pids"]),
        "samples": samples,
        "symbolized_samples": analysis["symbolized_samples"],
        "symbolized_pct": 100.0 * int(analysis["symbolized_samples"]) / samples,
        "matched_samples": analysis["matched_samples"],
        "matched_pct": 100.0 * int(analysis["matched_samples"]) / samples,
        "estimated_cpu_us_per_task": sample_us * samples / pipelines,
        "perf_data": analysis["perf_data"],
        "perf_script": analysis["perf_script"],
        "perf_data_bytes": analysis["perf_data_bytes"],
        "top_leaf_symbols": json.dumps(analysis["top_leaf_symbols"]),
    }
    for category, count in analysis["category_counts"].items():
        row[f"{category}_samples"] = count
        row[f"{category}_pct"] = 100.0 * int(count) / samples
        row[f"{category}_estimated_cpu_us_per_task"] = sample_us * int(count) / pipelines
    return row


def _child_cmd(
    args: argparse.Namespace,
    variant: str,
    case_dir: Path,
    result_path: Path,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-profile",
        "--single-variant", variant,
        "--single-padding-bytes", str(args.task_spec_padding_bytes),
        "--single-case-dir", str(case_dir),
        "--single-output-json", str(result_path),
        "--holders", "2",
        "--witness-count", "2",
        "--payload-bytes", str(args.payload_bytes),
        "--inline-chunk-bytes", str(args.inline_chunk_bytes),
        "--burst-size", str(args.burst_size),
        "--inflight-tasks", str(args.inflight_tasks),
        "--warmup-seconds", str(args.warmup_seconds),
        "--settle-seconds", str(args.settle_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--cpus-per-node", str(args.cpus_per_node),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
        "--frequency", str(args.frequency),
        "--call-graph", args.call_graph,
        "--perf-control-timeout-seconds", str(args.perf_control_timeout_seconds),
    ]


def _clean_case(case_dir: Path) -> None:
    for path in [
        case_dir / "profile.perf.data",
        case_dir / "profile.perf.data.old",
        case_dir / "perf.record.stderr.txt",
        *(case_dir / f"{role}.perf.report.txt" for role in ROLES),
        *(case_dir / f"{role}.perf.script.txt" for role in ROLES),
    ]:
        path.unlink(missing_ok=True)


def _run_variant(
    perf: str,
    args: argparse.Namespace,
    variant: str,
    case_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    data_path = case_dir / "profile.perf.data"
    stderr_path = case_dir / "perf.record.stderr.txt"
    result_path = case_dir / ".result.json"
    result_path.unlink(missing_ok=True)
    proc = _run_controlled_perf(
        perf,
        data_path,
        stderr_path,
        args.frequency,
        args.call_graph,
        _child_cmd(args, variant, case_dir, result_path),
        b58.child_env(profiling=False),
    )
    if proc.returncode != 0 or not result_path.exists():
        detail = stderr_path.read_text() if stderr_path.exists() else ""
        raise _perf_failure(f"native stack child failed for {variant}", detail)
    if not data_path.exists() or data_path.stat().st_size == 0:
        detail = stderr_path.read_text() if stderr_path.exists() else ""
        raise _perf_failure(f"perf produced no data for {variant}", detail)

    result = json.loads(result_path.read_text())
    result_path.unlink(missing_ok=True)
    role_pids = result.pop("role_pids")
    analyses = []
    print(f"  {variant}: recording finalized; analyzing native stacks", flush=True)
    for role in ROLES:
        print(f"  {variant}: analyzing {role}", flush=True)
        analyses.append(
            _analyze_role(
                perf,
                data_path,
                case_dir,
                role,
                role_pids[role],
                args.perf_analysis_timeout_seconds,
            )
        )
    return result, analyses


def run(args: argparse.Namespace) -> None:
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")
    if args.frequency < 19 or args.frequency > 999:
        raise ValueError("--frequency must be between 19 and 999 Hz")
    if args.perf_analysis_timeout_seconds <= 0:
        raise ValueError("--perf-analysis-timeout-seconds must be positive")
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("Benchmark 73 requires R=2 and W=2")

    perf = _perf_or_raise()
    out = Path(args.output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    _preflight(perf, out, args)

    summary_path = out / "native_stack_summary.csv"
    runs_path = out / "native_stack_runs.csv"
    if args.overwrite:
        summary_path.unlink(missing_ok=True)
        runs_path.unlink(missing_ok=True)

    run_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    print(
        "K=1 native stack profile: "
        f"frequency={args.frequency}Hz call_graph={args.call_graph} "
        f"warmup={args.warmup_seconds:.1f}s timed={args.duration_seconds:.1f}s"
    )
    print("  Fixed-R vs ordinary Succession; R=2 W=2 profiling=OFF; fresh cluster per case")
    print("  throughput observed under perf is diagnostic, not an acceptance measurement")
    print("  one inherited perf recording retains exited-thread samples")
    print("  stack categories are inclusive and may overlap")

    for index, variant in enumerate(VARIANTS, 1):
        print(f"[{index}/2] variant={variant}", flush=True)
        case_dir = out / variant
        case_dir.mkdir(parents=True, exist_ok=True)
        if args.overwrite:
            _clean_case(case_dir)
        try:
            result, analyses = _run_variant(perf, args, variant, case_dir)
        except Exception:
            _write_csv(runs_path, run_rows)
            _write_csv(summary_path, summary_rows)
            raise
        run_rows.append(result)
        for analysis in analyses:
            summary_rows.append(
                _summary_row(
                    variant,
                    analysis,
                    int(result["total_pipeline_submitted"]),
                    int(result["frequency_hz"]),
                )
            )
        _write_csv(runs_path, run_rows)
        _write_csv(summary_path, summary_rows)
        print(
            f"  throughput={float(result['throughput_rps']):.1f} rps "
            f"pipelines={int(result['total_pipeline_submitted'])}"
        )

    print("\nFinal K=1 native stack profile:")
    for variant in VARIANTS:
        print(f"\n  {variant}:")
        for role in ROLES:
            row = next(
                item
                for item in summary_rows
                if item["variant"] == variant and item["role"] == role
            )
            print(
                f"    {role:18s} samples={int(row['samples']):6d} "
                f"estCPU={float(row['estimated_cpu_us_per_task']):7.1f}us/task "
                f"symbolized={float(row['symbolized_pct']):5.1f}% "
                f"candidate={float(row['candidate_report_pct']):5.1f}% "
                f"witness={float(row['witness_update_pct']):5.1f}% "
                f"gRPC/CQ={float(row['grpc_cq_pct']):5.1f}% "
                f"CoreWorker={float(row['coreworker_scheduling_pct']):5.1f}% "
                f"task/ref={float(row['task_objectref_pct']):5.1f}%"
            )
            top = json.loads(str(row["top_leaf_symbols"]))[:5]
            for item in top:
                print(f"      {int(item['samples']):5d}  {item['symbol']}")

    print("\nSuccession minus Fixed-R inclusive stack estimate:")
    for role in ROLES:
        fixed = next(
            row
            for row in summary_rows
            if row["variant"] == "fixed_r" and row["role"] == role
        )
        succession = next(
            row
            for row in summary_rows
            if row["variant"] == "succession_k1" and row["role"] == role
        )
        deltas = []
        for category in CATEGORY_PATTERNS:
            key = f"{category}_estimated_cpu_us_per_task"
            delta = float(succession[key]) - float(fixed[key])
            deltas.append(f"{category}={delta:+.1f}us/task")
        print(f"  {role:18s} " + "  ".join(deltas))

    print("\nDecision signal:")
    print("  Use the detailed *.perf.script.txt files to inspect concrete hot call paths.")
    print("  Require a recovery-RPC-specific stack concentration before changing transport.")
    print("  If stacks remain generic, stop ordinary K=1 transport experimentation.")
    print("  R=2, W=2, K=1 ordering, and witness-backed durability remain unchanged.")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "command",
        choices=["run", "_single-profile", "_control-smoke"],
        nargs="?",
        default="run",
    )
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/73_recovery_succession_k1_native_stack_profile",
    )
    p.add_argument("--warmup-seconds", type=float, default=1.0)
    p.add_argument("--settle-seconds", type=float, default=0.25)
    p.add_argument("--duration-seconds", type=float, default=10.0)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=90.0)
    p.add_argument("--frequency", type=int, default=99)
    p.add_argument("--call-graph", default="dwarf,8192")
    p.add_argument("--perf-control-timeout-seconds", type=float, default=10.0)
    p.add_argument("--perf-analysis-timeout-seconds", type=float, default=120.0)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--single-variant")
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-case-dir")
    p.add_argument("--single-output-json")
    p.add_argument("--single-control-fifo")
    p.add_argument("--single-ack-fifo")
    return p


def main() -> None:
    args = parser().parse_args()
    if args.command == "_control-smoke":
        _control_smoke(args)
        return
    if args.command == "_single-profile":
        if args.single_variant not in VARIANTS:
            raise ValueError("invalid native-stack variant")
        if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
            raise ValueError("native stack child requires RAY_RECOVERY_PROFILING=0")
        result = single_profile(args)
        Path(args.single_output_json).write_text(json.dumps(result, allow_nan=True))
        return
    run(args)


if __name__ == "__main__":
    main()
