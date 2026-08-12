\
#!/usr/bin/env python3
"""Apply Benchmark 15's test-only holder-confirmation suppression hook.

Run from the root of the Ray repository:

    python gossip_benchmarks/apply_benchmark15_confirmation_fault.py

This adds a default-off test hook that is consulted only when a provisional
recovery holder has already received RecoverTaskOutput and would otherwise
query its compact witnesses for confirmation.

It is intentionally separate from the Benchmark 14 owner-side fault hook.
"""

from pathlib import Path
import sys

ROOT = Path.cwd()
CONFIG = ROOT / "src/ray/common/ray_config_def.h"
CORE = ROOT / "src/ray/core_worker/core_worker.cc"

KEY = "recovery_succession_test_fail_holder_witness_confirmation"
LOG = "TEST ONLY: suppressing provisional holder witness confirmation"

CONFIG_ANCHOR = (
    "RAY_CONFIG(bool, recovery_succession_test_fail_after_witness_ack, false)\n"
)

CONFIG_INSERT = """RAY_CONFIG(bool, recovery_succession_test_fail_after_witness_ack, false)

/// TEST ONLY: when a provisionally installed recovery holder receives a
/// recovery request, suppress its independent compact-witness confirmation.
///
/// This is used by Benchmark 15 to prove that a requester's cached manifest
/// cannot by itself promote a provisional holder. Default false.
RAY_CONFIG(bool, recovery_succession_test_fail_holder_witness_confirmation, false)
"""

CORE_ANCHOR = """    rpc::RecoveryManifest provisional_manifest;
    provisional_manifest.CopyFrom(latest_manifest);

    LookupRecoveryManifestFromWitnesses(
"""

CORE_INSERT = """    rpc::RecoveryManifest provisional_manifest;
    provisional_manifest.CopyFrom(latest_manifest);

    // TEST ONLY: deterministically make the holder's own witness
    // confirmation unavailable. The requester may still have obtained the
    // holder manifest from a real witness; this hook verifies that the
    // requester alone cannot vouch for a provisional holder.
    if (RayConfig::instance()
            .recovery_succession_test_fail_holder_witness_confirmation()) {
      RAY_LOG(WARNING)
          .WithField(TaskID::FromBinary(request.task_id()))
          << "TEST ONLY: suppressing provisional holder witness confirmation";

      reply->set_result(
          rpc::RecoverTaskOutputReply::TASK_NOT_FOUND);

      send_reply_callback(
          Status::OK(),
          nullptr,
          nullptr);
      return;
    }

    LookupRecoveryManifestFromWitnesses(
"""


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def read(path: Path) -> str:
    if not path.exists():
        fail(f"Missing {path}. Run from the Ray repository root.")
    return path.read_text()


def patch_config(text: str) -> tuple[str, bool]:
    if KEY in text:
        return text, False

    count = text.count(CONFIG_ANCHOR)
    if count != 1:
        fail(
            "Expected exactly one Benchmark 14 config anchor "
            f"in {CONFIG}, found {count}. No files changed."
        )

    return text.replace(CONFIG_ANCHOR, CONFIG_INSERT, 1), True


def patch_core(text: str) -> tuple[str, bool]:
    if LOG in text:
        return text, False

    # Restrict the search to HandleRecoverTaskOutput so another similarly
    # formatted manifest lookup elsewhere cannot be modified accidentally.
    start_token = "void CoreWorker::HandleRecoverTaskOutput("
    end_token = "\nstd::optional<rpc::ObjectReference>\nCoreWorker::StartRecoveryReplay("

    start = text.find(start_token)
    if start < 0:
        fail("Could not find CoreWorker::HandleRecoverTaskOutput.")

    end = text.find(end_token, start)
    if end < 0:
        fail("Could not find the end of HandleRecoverTaskOutput.")

    function_text = text[start:end]
    count = function_text.count(CORE_ANCHOR)

    if count != 1:
        fail(
            "Expected exactly one provisional-holder witness lookup anchor "
            f"inside HandleRecoverTaskOutput, found {count}. No files changed."
        )

    function_text = function_text.replace(CORE_ANCHOR, CORE_INSERT, 1)
    return text[:start] + function_text + text[end:], True


def main() -> None:
    config_original = read(CONFIG)
    core_original = read(CORE)

    # Compute all modifications before writing either file.
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
    print("Review with:")
    print(
        "  git diff -- "
        "src/ray/common/ray_config_def.h "
        "src/ray/core_worker/core_worker.cc"
    )
    print("  git diff --check")


if __name__ == "__main__":
    main()
