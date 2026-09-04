#!/usr/bin/env python3
"""Benchmark 71: profiling-only ordinary Succession K=1 CPU service attribution.

This reuses Benchmark 70's exact R=2, W=2, two-borrower, 32-task control
workload and adds synchronous CPU service measurements for the owner,
RayletClient witness batching, logical witness callbacks, and manifest commit.

It is a diagnostic, not a throughput benchmark. Profiling is ON and no recovery
protocol, holder count, witness count, admission order, or Frontier behavior is
changed.

Run:
  python gossip_benchmarks/71_recovery_succession_k1_cpu_service_profile.py
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH70_PATH = HERE / "70_recovery_succession_k1_quick_control_profile.py"


def _load_benchmark70():
    spec = importlib.util.spec_from_file_location(
        "recovery_k1_cpu_service_b70", BENCH70_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH70_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b70 = _load_benchmark70()
b70.REPORT_TITLE = "Succession K=1 CPU service profile:"


if __name__ == "__main__":
    b70.run(b70.parser().parse_args())
