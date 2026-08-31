#!/usr/bin/env python3
"""Shared helpers for Fixed-R correctness benchmarks 62-65."""
from __future__ import annotations

import os
import signal
import time
from typing import Any, Iterable

from ray._raylet import compute_task_id
from ray._private import ray_constants


def _node_id_hex(node: Any) -> str:
    node_id = node.node_id
    if isinstance(node_id, bytes):
        return node_id.hex()
    return str(node_id)


def stable_witness_score(task_id_binary: bytes, node_id_binary: bytes) -> int:
    """Bit-for-bit match for CoreWorker StableWitnessScoreOptimized (FNV-1a)."""
    value = 1469598103934665603
    prime = 1099511628211
    mask = (1 << 64) - 1
    for byte in task_id_binary + node_id_binary:
        value ^= byte
        value = (value * prime) & mask
    return value


def fixed_r_witness_order(object_ref: Any, nodes: Iterable[Any], count: int) -> list[Any]:
    """Reconstruct the ordered Fixed-R witness selection for one task."""
    task_id_binary = compute_task_id(object_ref).binary()
    scored: list[tuple[int, bytes, Any]] = []
    for node in nodes:
        node_binary = bytes.fromhex(_node_id_hex(node))
        scored.append(
            (stable_witness_score(task_id_binary, node_binary), node_binary, node)
        )
    scored.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in scored[:count]]


def node_id_hex(node: Any) -> str:
    return _node_id_hex(node)


def same_node(left: Any, right: Any) -> bool:
    return _node_id_hex(left) == _node_id_hex(right)


def raylet_pid(node: Any) -> int:
    infos = node.all_processes[ray_constants.PROCESS_TYPE_RAYLET]
    if len(infos) != 1:
        raise RuntimeError(f"Expected one raylet process, found {len(infos)}")
    process = infos[0].process
    if process.poll() is not None:
        raise RuntimeError(f"Raylet {_node_id_hex(node)} is already dead")
    return int(process.pid)


def stop_raylet(node: Any) -> int:
    pid = raylet_pid(node)
    os.kill(pid, signal.SIGSTOP)
    return pid


def continue_raylet(node: Any) -> int:
    pid = raylet_pid(node)
    os.kill(pid, signal.SIGCONT)
    return pid


def wait_for_node_state(
    ray_module: Any,
    node_id: str,
    *,
    alive: bool,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        for row in ray_module.nodes():
            if row.get("NodeID") == node_id:
                last = bool(row.get("Alive"))
                if last is alive:
                    return
                break
        time.sleep(0.05)
    raise TimeoutError(
        f"Node {node_id} did not reach Alive={alive}; last observed={last}"
    )


def assert_node_alive(ray_module: Any, node_id: str) -> None:
    for row in ray_module.nodes():
        if row.get("NodeID") == node_id:
            if not row.get("Alive"):
                raise AssertionError(f"Node {node_id} became authoritatively dead")
            return
    raise AssertionError(f"Node {node_id} disappeared from ray.nodes()")
