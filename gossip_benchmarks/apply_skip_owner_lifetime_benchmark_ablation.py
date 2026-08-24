#!/usr/bin/env python3
"""
Add benchmark-only flag:
  enable_recovery_succession_skip_owner_lifetime_for_benchmark

This intentionally disables ALL owner-side dormant TaskSpec/lifetime retention
for eligible tasks. It is NOT correctness-preserving and must be used only with
the dormant_only benchmark ablation.

Purpose:
  dormant_only                         = feature shell + owner lifetime tracking
  dormant_only + skip_owner_lifetime  = feature shell only

This distinguishes whether the ~16% dormant penalty is caused by the lifetime
requirement itself or by broader always-on Recovery Succession hooks.

Env:
  RAY_RECOVERY_SKIP_OWNER_LIFETIME=1
"""

from __future__ import annotations
import argparse
from pathlib import Path


def replace_once(path: Path, old: str, new: str, label: str):
    text = path.read_text()
    if new in text:
        print(f"[already] {label}")
        return
    n = text.count(old)
    if n != 1:
        raise RuntimeError(f"{label}: expected one match in {path}, found {n}")
    path.write_text(text.replace(old, new, 1))
    print(f"[patched] {label}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    root = Path(ap.parse_args().repo).resolve()

    cfg = root / "src/ray/common/ray_config_def.h"
    core = root / "src/ray/core_worker/core_worker.cc"
    common = root / "gossip_benchmarks/_benchmark_common.py"
    for p in (cfg, core, common):
        if not p.exists():
            raise FileNotFoundError(p)

    # Config flag.
    text = cfg.read_text()
    if "enable_recovery_succession_skip_owner_lifetime_for_benchmark" not in text:
        anchor = "RAY_CONFIG(bool, enable_recovery_succession_task_manager_lifetime, false)"
        if anchor not in text:
            # Fallback for a tree without 4Q.
            anchor = "RAY_CONFIG(bool, enable_recovery_succession_task_manager_pin, false)"
        if anchor not in text:
            raise RuntimeError("Could not find Recovery Succession config anchor")
        text = text.replace(
            anchor,
            anchor
            + """

/// BENCHMARK ONLY. Intentionally removes all owner-side dormant lifetime
/// retention. This breaks late-borrow correctness and must never be enabled
/// outside the dormant_only diagnostic experiment.
RAY_CONFIG(bool,
           enable_recovery_succession_skip_owner_lifetime_for_benchmark,
           false)""",
            1,
        )
        cfg.write_text(text)
        print("[patched] add benchmark-only lifetime-skip config")
    else:
        print("[already] add benchmark-only lifetime-skip config")

    # Benchmark env -> system config.
    text = common.read_text()
    if "RAY_RECOVERY_SKIP_OWNER_LIFETIME" not in text:
        marker = "    config: dict[str, Any] = {\n"
        if text.count(marker) != 1:
            raise RuntimeError("Could not uniquely locate benchmark config dict")
        text = text.replace(
            marker,
            """    skip_owner_lifetime = (
        os.environ.get("RAY_RECOVERY_SKIP_OWNER_LIFETIME", "0") == "1"
        and method.recovery_enabled
    )
""" + marker,
            1,
        )

        # Put it next to the task-manager lifetime flag if present, otherwise pin.
        key = '        "enable_recovery_succession_task_manager_lifetime": task_manager_lifetime,\n'
        if key in text:
            text = text.replace(
                key,
                key
                + '        "enable_recovery_succession_skip_owner_lifetime_for_benchmark": skip_owner_lifetime,\n',
                1,
            )
        else:
            key = '        "enable_recovery_succession_task_manager_pin": task_manager_pin,\n'
            if key not in text:
                raise RuntimeError("Could not find recovery config key insertion point")
            text = text.replace(
                key,
                key
                + '        "enable_recovery_succession_skip_owner_lifetime_for_benchmark": skip_owner_lifetime,\n',
                1,
            )

        common.write_text(text)
        print("[patched] benchmark env/config plumbing")
    else:
        print("[already] benchmark env/config plumbing")

    # Skip the entire owner-lifetime branch. Match the stable outer condition,
    # which is unchanged by 4Q because 4Q branches inside it.
    old = """  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
"""
    new = """  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !RayConfig::instance()
           .enable_recovery_succession_skip_owner_lifetime_for_benchmark() &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
"""
    replace_once(core, old, new, "skip all dormant owner lifetime tracking")

    print()
    print("Benchmark-only owner-lifetime skip added.")
    print("Rebuild Ray.")
    print("Use ONLY with RAY_RECOVERY_ABLATION_MODE=dormant_only.")


if __name__ == "__main__":
    main()
