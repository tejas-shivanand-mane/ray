#!/usr/bin/env python3
'''
Patch 4K: make 4J no-piggyback the default and physically batch H1 admission.

Apply on top of Patch 4J + buildfix1.

Production/full mode:
  - keeps Patch 4J task-centric state
  - does NOT place the full producer TaskSpec on downstream PushTask
  - H1 sends the same logical ReportRecoveryCandidate as before
  - H1 candidate reports from independent tasks use the existing Patch-4E
    500-us / 64-item batcher
  - the owner's existing batch candidate handler automatically groups and
    batches the resulting InstallRecoveryHolder RPCs
  - witness publication and ordered commit semantics are unchanged

Benchmark control:
  - mode=no_piggyback preserves the old single-item H1 fast path, so
    NoPiggybackLegacyH1FastPath vs Full4K isolates H1 physical batching.
  - piggyback_no_candidate keeps the old 4F/4J piggyback diagnostic.
  - candidate_rpc_no_admit now exercises the batched H1 candidate path.

No protobuf changes.
'''

from __future__ import annotations

import datetime as dt
import shutil
import subprocess
import sys
from pathlib import Path

VERSION = "4K-v1-batched-h1-no-piggyback"
print(f"Patch 4K patcher version: {VERSION}")

ROOT = Path.cwd()
CORE = ROOT / "src/ray/core_worker/core_worker.cc"
MGR = ROOT / "src/ray/core_worker/recovery_succession_manager.cc"
BENCH = ROOT / "gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py"

for path in (CORE, MGR, BENCH):
    if not path.exists():
        raise SystemExit(f"Patch 4K: missing required file: {path}")

texts = {
    "core": CORE.read_text(),
    "mgr": MGR.read_text(),
    "bench": BENCH.read_text(),
}

requirements = {
    "core": [
        "owner_recovery_task_specs",
        "owner_lazy_task_spec_copies_avoided",
        "Patch 4E-1: also fast-path the first real holder (H1).",
    ],
    "mgr": [
        "Patch 4J: task-centric recovery state.",
        "owner_task_specs",
        "task_centric_metadata_builds",
    ],
    "bench": [
        'Case("full", "Full4J", True, "full")',
        '"borrower_candidate_rpc_reports_per_pipeline"',
    ],
}
path_by_key = {"core": CORE, "mgr": MGR, "bench": BENCH}
for key, needles in requirements.items():
    for needle in needles:
        if needle not in texts[key]:
            raise SystemExit(
                f"Patch 4K: {path_by_key[key]} is not the expected post-4J tree; "
                f"missing {needle!r}"
            )

if "Patch 4K: batched H1 candidate/install path." in texts["core"]:
    raise SystemExit("Patch 4K: already appears to be applied")


def replace_once(text: str, old: str, new: str, desc: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"Patch 4K: {desc}: expected exactly one anchor, found {count}"
        )
    return text.replace(old, new, 1)


# ---------------------------------------------------------------------------
# core_worker.cc
# ---------------------------------------------------------------------------
core = texts["core"]

core = replace_once(
    core,
    "// Patch 4H: compact task-argument recovery metadata.\n",
    "// Patch 4H: compact task-argument recovery metadata.\n"
    "// Patch 4I: TaskSpec-level recovery argument sidecar.\n"
    "// Patch 4J: task-centric recovery state.\n"
    "// Patch 4K: batched H1 candidate/install path.\n",
    "core patch comment",
)

old_prefetch = '''    if (!recovery_witness_holder_baseline_enabled_ &&
        (recovery_mode == "full" ||
         recovery_mode == "piggyback_no_candidate")) {
'''
new_prefetch = '''    // Patch 4K: normal/full mode uses ordinary async holder installation.
    // Only the explicit piggyback diagnostic needs a TaskManager TaskSpec
    // during downstream TaskSpec construction.
    if (!recovery_witness_holder_baseline_enabled_ &&
        recovery_mode == "piggyback_no_candidate") {
'''
core = replace_once(
    core, old_prefetch, new_prefetch, "disable full-mode TaskSpec prefetch"
)

old_fastpath = '''  // Preserve deterministic failure-injection semantics. A batch has one gRPC
  // status for all logical items, so the post-witness/pre-commit test continues
  // to use the original single-item RPC path.
  //
  // Patch 4E-1: also fast-path the first real holder (H1). When the cached
  // succession contains only the owner, delaying this report for the 4E
  // coalescing window adds latency/control-path pressure without batching
  // multiple holders for this task. H2+ still use the normal 4E batching path.
  const bool first_holder_candidate =
      !request.has_cached_manifest() ||
      request.cached_manifest().succession_size() <= 1;

  if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack() ||
      coordinator_address.worker_id().empty() ||
      first_holder_candidate) {
'''
new_fastpath = '''  // Preserve deterministic failure-injection semantics. A batch has one gRPC
  // status for all logical items, so the post-witness/pre-commit test continues
  // to use the original single-item RPC path.
  //
  // Patch 4K: H1 now uses the same physical coalescing path as H2+ in full
  // mode. This does NOT change logical admission: every real borrower still
  // contributes exactly one candidate report, and the owner performs the same
  // reservation -> install -> witness -> ordered commit sequence. It only
  // coalesces independent candidate RPCs, which also lets the existing batch
  // server path coalesce the corresponding holder installs.
  //
  // Keep Patch-4E-1's old single-H1 behavior only in the explicit
  // no_piggyback benchmark control so we can isolate the batching effect.
  const bool first_holder_candidate =
      !request.has_cached_manifest() ||
      request.cached_manifest().succession_size() <= 1;
  const bool preserve_legacy_h1_fast_path =
      first_holder_candidate &&
      RecoveryBenchmarkAblationMode() == "no_piggyback";

  if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack() ||
      coordinator_address.worker_id().empty() ||
      preserve_legacy_h1_fast_path) {
'''
core = replace_once(
    core, old_fastpath, new_fastpath, "replace H1 fast path with 4K batching"
)

texts["core"] = core


# ---------------------------------------------------------------------------
# recovery_succession_manager.cc
# ---------------------------------------------------------------------------
mgr = texts["mgr"]

mgr = replace_once(
    mgr,
    "// Patch 4J: task-centric recovery state.\n",
    "// Patch 4J: task-centric recovery state.\n"
    "// Patch 4K: full mode uses async holder install; no H1 TaskSpec piggyback.\n",
    "manager patch comment",
)

old_skip = '''    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
        patch4g_mode == "no_piggyback") {
      return;
    }
'''
new_skip = '''    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
        patch4g_mode == "no_piggyback" ||
        patch4g_mode == "full") {
      return;
    }
'''
mgr = replace_once(
    mgr, old_skip, new_skip, "make full mode no-piggyback"
)

texts["mgr"] = mgr


# ---------------------------------------------------------------------------
# Benchmark 16
# ---------------------------------------------------------------------------
bench = texts["bench"]

bench = bench.replace(
    "  NoPiggyback               full recovery; H1 uses InstallRecoveryHolder\n"
    "  Full4J                    ordinary recovery with Patch-4I TaskSpec sidecar\n",
    "  NoPiggybackLegacyH1FastPath  4J no-piggyback with old single H1 candidate RPC\n"
    "  Full4K                       4J task-centric + no piggyback + batched H1 admission\n",
)

bench = replace_once(
    bench,
    '        Case("no_piggyback", "NoPiggyback", True, "no_piggyback"),\n'
    '        Case("full", "Full4J", True, "full"),\n',
    '        Case("no_piggyback", "NoPiggybackLegacyH1FastPath", True, "no_piggyback"),\n'
    '        Case("full", "Full4K", True, "full"),\n',
    "benchmark case labels",
)

old_derived = '''    row["borrower_candidate_reports_per_pipeline"] = float(borrower["candidate_reports_built"]) / tasks
    row["borrower_candidate_rpc_reports_per_pipeline"] = float(borrower["candidate_rpc_logical_reports_sent"]) / tasks
    row["owner_piggyback_copies_per_pipeline"] = float(owner["first_holder_piggyback_copies_sent"]) / tasks
'''
new_derived = '''    row["borrower_candidate_reports_per_pipeline"] = float(borrower["candidate_reports_built"]) / tasks
    row["borrower_candidate_rpc_reports_per_pipeline"] = float(borrower["candidate_rpc_logical_reports_sent"]) / tasks
    row["borrower_candidate_physical_rpcs_per_pipeline"] = float(borrower["candidate_rpc_physical_rpcs_sent"]) / tasks
    physical_candidate_rpcs = int(borrower["candidate_rpc_physical_rpcs_sent"])
    row["borrower_candidate_mean_batch_width"] = (
        float(borrower["candidate_rpc_logical_reports_sent"]) / physical_candidate_rpcs
        if physical_candidate_rpcs > 0
        else math.nan
    )
    row["owner_piggyback_copies_per_pipeline"] = float(owner["first_holder_piggyback_copies_sent"]) / tasks
'''
bench = replace_once(
    bench, old_derived, new_derived, "candidate physical-RPC derived metrics"
)

old_summary_fields = '''        "borrower_candidate_queue_cpu_us_per_pipeline",
        "borrower_candidate_reports_per_pipeline",
        "borrower_candidate_rpc_reports_per_pipeline",
        "owner_piggyback_copies_per_pipeline",
'''
new_summary_fields = '''        "borrower_candidate_queue_cpu_us_per_pipeline",
        "borrower_candidate_reports_per_pipeline",
        "borrower_candidate_rpc_reports_per_pipeline",
        "borrower_candidate_physical_rpcs_per_pipeline",
        "borrower_candidate_mean_batch_width",
        "owner_piggyback_copies_per_pipeline",
'''
bench = replace_once(
    bench, old_summary_fields, new_summary_fields, "summary physical-RPC metrics"
)

texts["bench"] = bench


stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
backup_root = ROOT / ".patch4k_backups" / stamp

for path in (CORE, MGR, BENCH):
    dst = backup_root / path.relative_to(ROOT)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dst)

CORE.write_text(texts["core"])
MGR.write_text(texts["mgr"])
BENCH.write_text(texts["bench"])

subprocess.run([sys.executable, "-m", "py_compile", str(BENCH)], check=True)
subprocess.run(["git", "diff", "--check"], check=True)

print("Patch 4K applied successfully.")
print(f"Backups: {backup_root}")
print("Modified:")
print(f"  {CORE.relative_to(ROOT)}")
print(f"  {MGR.relative_to(ROOT)}")
print(f"  {BENCH.relative_to(ROOT)}")
print()
print("No protobuf changes.")
print()
print("Rebuild:")
print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")
print()
print("Benchmark:")
print("  python gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py run-and-plot \\")
print("    --repetitions 2 \\")
print("    --warmup-seconds 3 \\")
print("    --duration-seconds 15 \\")
print("    --inflight 64 \\")
print("    --payload-bytes 1024 \\")
print("    --output-dir gossip_benchmarks/results/16_patch4k_b1")
