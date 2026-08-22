#!/usr/bin/env python3
"""
Add benchmark-only Recovery Succession ablation: metadata_no_transport.

This mode performs sender-side lazy activation and metadata construction exactly
like metadata_only, then strips all recovery metadata from the outgoing TaskSpec
before normal task serialization/transport.

Benchmark-only; this mode provides no recovery durability.
"""

from __future__ import annotations
import argparse
from pathlib import Path


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text()
    if new in text:
        print(f"[already] {label}")
        return
    n = text.count(old)
    if n != 1:
        raise RuntimeError(f"{label}: expected 1 match in {path}, found {n}")
    path.write_text(text.replace(old, new, 1))
    print(f"[patched] {label}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    root = Path(ap.parse_args().repo).resolve()

    core = root / "src/ray/core_worker/core_worker.cc"
    rsm = root / "src/ray/core_worker/recovery_succession_manager.cc"
    config = root / "src/ray/common/ray_config_def.h"
    for p in (core, rsm, config):
        if not p.exists():
            raise FileNotFoundError(p)

    old_check = '''  RAY_CHECK(mode == "full" || mode == "no_piggyback" ||
            mode == "metadata_only" || mode == "metadata_no_receiver" ||
            mode == "piggyback_no_candidate" ||
            mode == "candidate_rpc_no_admit")
'''
    new_check = '''  RAY_CHECK(mode == "full" || mode == "no_piggyback" ||
            mode == "metadata_only" || mode == "metadata_no_receiver" ||
            mode == "metadata_no_transport" ||
            mode == "piggyback_no_candidate" ||
            mode == "candidate_rpc_no_admit")
'''
    replace_once(core, old_check, new_check, "allow mode in core_worker.cc")
    replace_once(rsm, old_check, new_check, "allow mode in recovery manager")

    replace_once(
        rsm,
        '''    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "metadata_no_receiver" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
''',
        '''    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "metadata_no_receiver" ||
        patch4g_mode == "metadata_no_transport" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
''',
        "metadata-only sender behavior",
    )

    replace_once(
        rsm,
        '''  if (patch4g_mode == "metadata_only" ||
      patch4g_mode == "metadata_no_receiver" ||
      patch4g_mode == "piggyback_no_candidate") {
''',
        '''  if (patch4g_mode == "metadata_only" ||
      patch4g_mode == "metadata_no_receiver" ||
      patch4g_mode == "metadata_no_transport" ||
      patch4g_mode == "piggyback_no_candidate") {
''',
        "no candidate reports",
    )

    replace_once(
        core,
        '''    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());

    absl::flat_hash_map<TaskID, rpc::TaskSpec> owner_recovery_task_specs;
''',
        '''    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());

    if (RecoveryBenchmarkAblationMode() == "metadata_no_transport") {
      rpc::TaskSpec *outgoing_task_spec = builder.MutableMessage();

      // Patch 4I primary sidecars.
      outgoing_task_spec->clear_recovery_argument_metadata();

      // Backward-compatible embedded metadata paths, if any were populated.
      for (rpc::TaskArg &arg : *outgoing_task_spec->mutable_args()) {
        if (arg.has_object_ref()) {
          arg.mutable_object_ref()->clear_recovery_metadata();
        }
        for (rpc::ObjectReference &nested_ref :
             *arg.mutable_nested_inlined_refs()) {
          nested_ref.clear_recovery_metadata();
        }
      }
    }

    absl::flat_hash_map<TaskID, rpc::TaskSpec> owner_recovery_task_specs;
''',
        "strip recovery metadata before TaskSpec transport",
    )

    replace_once(
        core,
        '''      RecoveryBenchmarkAblationMode() != "metadata_no_receiver" &&
      RecoverySuccessionManager::CarriesRecoveryMetadata(
''',
        '''      RecoveryBenchmarkAblationMode() != "metadata_no_receiver" &&
      RecoveryBenchmarkAblationMode() != "metadata_no_transport" &&
      RecoverySuccessionManager::CarriesRecoveryMetadata(
''',
        "skip receiver processing",
    )

    replace_once(
        config,
        '''///   metadata_no_receiver    - compact metadata transported, but receiver recovery processing is skipped
///   piggyback_no_candidate  - metadata + H1 TaskSpec sidecar; no candidate report
''',
        '''///   metadata_no_receiver    - compact metadata transported, but receiver recovery processing is skipped
///   metadata_no_transport   - sender builds metadata, then strips it before TaskSpec transport
///   piggyback_no_candidate  - metadata + H1 TaskSpec sidecar; no candidate report
''',
        "document benchmark mode",
    )

    print()
    print("metadata_no_transport ablation added.")
    print("Rebuild Ray before running it.")


if __name__ == "__main__":
    main()
