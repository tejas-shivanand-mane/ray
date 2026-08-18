#!/usr/bin/env python3
"""Hotfix Patch 4D SIGSEGV caused by moving `state` during function-argument evaluation.

Run from the root of the custom Ray repository:

    python gossip_benchmarks/fix_patch4d_sigsegv.py

Root cause
----------
Patch 4D generated this call:

    PublishRecoveryManifestToWitnesses(
        state->proposed_manifest,
        [this, manager, state = std::move(state), ...](...) { ... });

The C++ evaluation order of function arguments is not left-to-right. The lambda
capture may move `state` first, leaving the local shared_ptr empty before
`state->proposed_manifest` is evaluated. That can dereference a null shared_ptr
and SIGSEGV.

The fix captures the shared_ptr by value instead. This keeps the same lifetime
semantics without invalidating `state` while another argument still uses it.
"""

from pathlib import Path
import shutil
import sys

ROOT = Path.cwd()
CORE = ROOT / "src/ray/core_worker/core_worker.cc"

OLD = """  PublishRecoveryManifestToWitnesses(
      state->proposed_manifest,
      [this, manager, state = std::move(state), witness_publish_start_ns](
"""

NEW = """  PublishRecoveryManifestToWitnesses(
      state->proposed_manifest,
      [this, manager, state, witness_publish_start_ns](
"""

MARKER = "Patch 4D: pipelined holder admission"


def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    if not CORE.exists():
        fail(f"Missing {CORE}. Run this script from the Ray repository root.")

    text = CORE.read_text()

    if MARKER not in text:
        fail("Patch 4D marker is not present in core_worker.cc.")

    if NEW in text and OLD not in text:
        print("Patch 4D SIGSEGV hotfix is already applied; nothing to do.")
        return

    count = text.count(OLD)
    if count != 1:
        fail(f"Expected exactly one unsafe Patch-4D capture, found {count}.")

    backup = CORE.with_suffix(CORE.suffix + ".pre4d_sigsegv_fix")
    if not backup.exists():
        shutil.copy2(CORE, backup)

    text = text.replace(OLD, NEW, 1)
    CORE.write_text(text)

    print("Applied Patch 4D SIGSEGV hotfix.")
    print(f"Changed: {CORE.relative_to(ROOT)}")
    print(f"Backup:  {backup.relative_to(ROOT)}")
    print()
    print("Changed capture:")
    print("  state = std::move(state)  ->  state")
    print()
    print("Next:")
    print("  git diff --check")
    print("  rebuild Ray")
    print("  rerun the 1-repetition concurrent Benchmark 21 smoke test")


if __name__ == "__main__":
    main()
