#!/usr/bin/env python3
"""Apply the Benchmark 14 test-only candidate-commit fault injection.

Run from the root of the Ray repository:

    python gossip_benchmarks/apply_candidate_commit_fault.py

The script:
- edits only src/ray/common/ray_config_def.h and src/ray/core_worker/core_worker.cc
- checks exact structural anchors before writing
- refuses ambiguous edits
- is idempotent
"""

from pathlib import Path
import sys

ROOT = Path.cwd()
CONFIG = ROOT / "src/ray/common/ray_config_def.h"
CORE = ROOT / "src/ray/core_worker/core_worker.cc"

CONFIG_KEY = "recovery_succession_test_fail_after_witness_ack"
FAULT_LOG = (
    "TEST ONLY: injected recovery succession failure after "
    "witness ACK before candidate commit"
)

CONFIG_ANCHOR = """/// Enables lightweight profiling of recovery-succession holder formation.
/// Intended only for experiments/debugging. When false, no timing or
/// protobuf-size measurements are performed.
RAY_CONFIG(bool, enable_recovery_succession_profiling, false)
"""

CONFIG_INSERT = """/// Enables lightweight profiling of recovery-succession holder formation.
/// Intended only for experiments/debugging. When false, no timing or
/// protobuf-size measurements are performed.
RAY_CONFIG(bool, enable_recovery_succession_profiling, false)

/// TEST ONLY: deterministically expose the crash window after a compact
/// witness has acknowledged the proposed holder manifest but before the
/// candidate has received/applied CommitRecoveryManifest.
///
/// When enabled, FinishRecoveryHolderAdmission returns an injected RPC error
/// immediately after successful witness publication, before owner-side
/// CommitHolderAdmission and before the candidate commit RPC. The benchmark
/// then hard-kills the owner node. Default false; never enable in production.
RAY_CONFIG(bool, recovery_succession_test_fail_after_witness_ack, false)
"""

FAULT_BLOCK = """        // TEST ONLY.
        //
        // At this point a real witness ACK has already made
        // proposed_manifest discoverable during recovery, while a newly
        // installed candidate is still provisional
        // (manifest_committed == false). Inject a failure here so the
        // correctness benchmark can deterministically kill the owner in the
        // exact post-witness / pre-candidate-commit window.
        if (candidate_needs_commit_rpc &&
            RayConfig::instance()
                .recovery_succession_test_fail_after_witness_ack()) {
          RAY_LOG(WARNING).WithField(task_id)
              << "TEST ONLY: injected recovery succession failure after "
                 "witness ACK before candidate commit";

          send_reply_callback(
              Status::IOError(
                  "Injected recovery succession failure after witness ACK "
                  "before candidate commit"),
              nullptr,
              nullptr);
          return;
        }

"""


def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def read(path: Path) -> str:
    if not path.exists():
        fail(f"Missing {path}. Run this script from the Ray repository root.")
    return path.read_text()


def patch_config(text: str) -> tuple[str, bool]:
    if CONFIG_KEY in text:
        return text, False

    count = text.count(CONFIG_ANCHOR)
    if count != 1:
        fail(
            f"Expected exactly one profiling-config anchor in {CONFIG}, "
            f"found {count}. No changes written."
        )

    return text.replace(CONFIG_ANCHOR, CONFIG_INSERT, 1), True


def patch_core(text: str) -> tuple[str, bool]:
    if FAULT_LOG in text:
        return text, False

    start_token = "void CoreWorker::FinishRecoveryHolderAdmission("
    end_token = "\nvoid CoreWorker::HandleReportRecoveryCandidate("

    start = text.find(start_token)
    if start < 0:
        fail(f"Could not find FinishRecoveryHolderAdmission in {CORE}")

    end = text.find(end_token, start)
    if end < 0:
        fail(f"Could not find the end of FinishRecoveryHolderAdmission in {CORE}")

    func = text[start:end]
    anchor = "        rpc::RecoveryManifest committed_manifest;\n"

    count = func.count(anchor)
    if count != 1:
        fail(
            "Expected exactly one committed_manifest anchor inside "
            f"FinishRecoveryHolderAdmission, found {count}. No changes written."
        )

    func = func.replace(anchor, FAULT_BLOCK + anchor, 1)
    return text[:start] + func + text[end:], True


def main() -> None:
    config_original = read(CONFIG)
    core_original = read(CORE)

    # Compute both replacements first so an anchor failure cannot leave a
    # partially modified repository.
    config_new, config_changed = patch_config(config_original)
    core_new, core_changed = patch_core(core_original)

    if config_changed:
        CONFIG.write_text(config_new)
        print(f"[patched] {CONFIG}")
    else:
        print(f"[already applied] {CONFIG}")

    if core_changed:
        CORE.write_text(core_new)
        print(f"[patched] {CORE}")
    else:
        print(f"[already applied] {CORE}")

    print()
    if config_changed or core_changed:
        print("Fault injection applied successfully.")
    else:
        print("Fault injection was already applied; no files changed.")

    print("Review with:")
    print(
        "  git diff -- src/ray/common/ray_config_def.h "
        "src/ray/core_worker/core_worker.cc"
    )


if __name__ == "__main__":
    main()
