#!/usr/bin/env python3
# Patch the lazy witness-holder baseline's stale-install race.
#
# Run from the Ray repo root or gossip_benchmarks/:
#   python patch_baseline_tombstone_supersession.py

from __future__ import annotations

import shutil
import sys
from pathlib import Path


REL = Path("src/ray/core_worker/core_worker.cc")


def find_root() -> Path:
    cwd = Path.cwd().resolve()
    for root in (cwd, cwd.parent):
        if (root / REL).exists() and (root / "gossip_benchmarks").is_dir():
            return root
    print(
        "ERROR: run from the Ray repo root or its gossip_benchmarks/ directory.",
        file=sys.stderr,
    )
    raise SystemExit(1)


def main() -> None:
    root = find_root()
    path = root / REL
    text = path.read_text(encoding="utf-8")

    old = '''          if (!stored) {
            RAY_LOG(FATAL)
                .WithField(task_id)
                << "Lazy witness-holder baseline failed to install "
                << "the full TaskSpec on every configured holder."
                << (newer_manifest.has_value()
                        ? " A newer witness manifest was observed."
                        : "");
          }
'''

    new = '''          if (!stored) {
            // A completed task may legitimately leave application scope while
            // this baseline's R full-TaskSpec witness-holder writes are still
            // in flight. Patch 4L cleanup then publishes a newer tombstone.
            // Any witness that sees that tombstone first must reject this older
            // install. That is cancellation/supersession, not a durability
            // failure, because the object no longer needs protection.
            //
            // Keep every other failure fatal so this does NOT weaken the
            // live-reference requirement that all R baseline holders store the
            // full TaskSpec while the producer ObjectRef remains live.
            if (newer_manifest.has_value() &&
                newer_manifest->tombstoned()) {
              manager->ApplyRecoveryTombstone(
                  newer_manifest.value());

              RAY_LOG(DEBUG)
                  .WithField(task_id)
                  << "Lazy witness-holder baseline install was superseded by "
                     "a newer tombstone at generation "
                  << newer_manifest->version().generation();
              return;
            }

            RAY_LOG(FATAL)
                .WithField(task_id)
                << "Lazy witness-holder baseline failed to install "
                << "the full TaskSpec on every configured holder."
                << (newer_manifest.has_value()
                        ? " A newer non-tombstone witness manifest was observed."
                        : "");
          }
'''

    if new in text:
        print("Patch already applied; nothing to do.")
        return

    count = text.count(old)
    if count != 1:
        print(
            f"ERROR: expected exactly one baseline fatal block, found {count}. "
            "No file was changed.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    backup = path.with_name(path.name + ".pre_baseline_tombstone_supersession.bak")
    if not backup.exists():
        shutil.copy2(path, backup)
        print(f"Backup: {backup}")

    text = text.replace(old, new, 1)
    path.write_text(text, encoding="utf-8")

    print(f"Patched: {path}")
    print()
    print("Behavior after patch:")
    print("  newer tombstone       -> expected supersession; no crash")
    print("  newer non-tombstone   -> FATAL")
    print("  no newer manifest     -> FATAL")


if __name__ == "__main__":
    main()
