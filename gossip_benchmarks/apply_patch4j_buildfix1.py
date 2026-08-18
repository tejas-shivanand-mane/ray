#!/usr/bin/env python3
"""
Patch 4J build fix 1.

Fixes:
1. Capture owner_task_specs in PopulateTaskArgumentMetadata's populate_one lambda.
2. Use Abseil-compatible erase iteration for borrowed_objects_.

Apply AFTER apply_patch4j_task_centric_state.py.
No rollback and no protobuf rebuild changes are required.
"""

from pathlib import Path
import shutil
import datetime as dt
import subprocess

ROOT = Path.cwd()
CC = ROOT / "src/ray/core_worker/recovery_succession_manager.cc"

print("Patch 4J build fix: 4J-buildfix1")

if not CC.exists():
    raise SystemExit(f"Missing {CC}")

text = CC.read_text()

old_capture = (
    "  auto populate_one = [this, task_spec, &attached_object_ids](\n"
)
new_capture = (
    "  auto populate_one = [this, task_spec, &attached_object_ids, owner_task_specs](\n"
)

count = text.count(old_capture)
if count != 1:
    raise SystemExit(
        f"Expected exactly one 4J populate_one lambda capture anchor; found {count}. "
        "Make sure Patch 4J is already applied."
    )
text = text.replace(old_capture, new_capture, 1)

old_erase = """  for (auto it = borrowed_objects_.begin(); it != borrowed_objects_.end();) {
    if (it->second.task_id == task_id) {
      object_recovery_metadata_.erase(it->first);
      it = borrowed_objects_.erase(it);
    } else {
      ++it;
    }
  }
"""
new_erase = """  for (auto it = borrowed_objects_.begin(); it != borrowed_objects_.end();) {
    if (it->second.task_id == task_id) {
      object_recovery_metadata_.erase(it->first);
      const auto erase_it = it++;
      borrowed_objects_.erase(erase_it);
    } else {
      ++it;
    }
  }
"""

count = text.count(old_erase)
if count != 1:
    raise SystemExit(
        f"Expected exactly one Abseil erase-loop anchor; found {count}. "
        "Make sure Patch 4J is already applied and has not been manually edited there."
    )
text = text.replace(old_erase, new_erase, 1)

stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
backup = ROOT / ".patch4j_buildfix1_backups" / stamp / CC.relative_to(ROOT)
backup.parent.mkdir(parents=True, exist_ok=True)
shutil.copy2(CC, backup)

CC.write_text(text)

subprocess.run(["git", "diff", "--check"], check=True)

print("Patch 4J build fix applied successfully.")
print(f"Backup: {backup}")
print("Now rerun:")
print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")
