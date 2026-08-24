#!/usr/bin/env python3
"""Freeze resolved fixed-R witness-baseline optimizations into the baseline.

Targets the current main branch inspected on 2026-08-24.

Removes independent flags for:
  compact metadata, witness batching, elide TaskSpec copy, separate manifest,
  fast receiver, fast manifest validation, move witness TaskSpec, batch Swap,
  top-k witness selection.

Keeps only enable_recovery_baseline_serialize_task_spec_once as an experimental
switch until the 64--128 KiB crossover policy is finalized.

Usage:
  python cleanup_witness_baseline.py /path/to/ray --check
  python cleanup_witness_baseline.py /path/to/ray
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path


class PatchError(RuntimeError):
    pass


FILES = [
    "src/ray/common/ray_config_def.h",
    "gossip_benchmarks/_benchmark_common.py",
    "src/ray/core_worker/recovery_succession_manager.cc",
    "src/ray/core_worker/core_worker.cc",
    "src/ray/raylet/node_manager.cc",
    "src/ray/raylet_rpc_client/raylet_client.cc",
]

REMOVED_FLAGS = [
    "enable_recovery_baseline_compact_argument_metadata",
    "enable_recovery_baseline_witness_batching",
    "enable_recovery_baseline_elide_task_spec_copy",
    "enable_recovery_baseline_separate_manifest_storage",
    "enable_recovery_baseline_fast_receiver",
    "enable_recovery_baseline_fast_manifest_validation",
    "enable_recovery_baseline_move_witness_task_spec",
    "enable_recovery_baseline_batch_request_swap",
    "enable_recovery_baseline_topk_witness_selection",
]


def replace_once(files, rel, old, new, label):
    n = files[rel].count(old)
    if n != 1:
        raise PatchError(f"{label}: expected one match in {rel}, found {n}")
    files[rel] = files[rel].replace(old, new, 1)
    print(f"[stage] {label}")


def regex_once(files, rel, pattern, repl, label, flags=0):
    out, n = re.subn(pattern, repl, files[rel], count=1, flags=flags)
    if n != 1:
        raise PatchError(f"{label}: expected one match in {rel}, found {n}")
    files[rel] = out
    print(f"[stage] {label}")


def remove_config(files, name):
    rel = "src/ray/common/ray_config_def.h"
    # Remove adjacent /// comment block plus declaration when possible.
    pat = rf"(?m)(?:^///[^\n]*\n)+^RAY_CONFIG\(bool,\s*{re.escape(name)},\s*false\)\n\n?"
    out, n = re.subn(pat, "", files[rel], count=1)
    if n == 0:
        decl = f"RAY_CONFIG(bool, {name}, false)\n"
        if files[rel].count(decl) != 1:
            raise PatchError(f"cannot uniquely remove config {name}")
        out = files[rel].replace(decl, "", 1)
    files[rel] = out
    print(f"[stage] remove config {name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    root = Path(args.repo).resolve()

    files = {}
    original = {}
    for rel in FILES:
        p = root / rel
        if not p.exists():
            raise PatchError(f"missing expected file: {p}")
        files[rel] = p.read_text()
        original[rel] = files[rel]

    # 1) Delete resolved baseline optimization flags. Serialize-once remains.
    for name in REMOVED_FLAGS:
        remove_config(files, name)

    # 2) Benchmark plumbing: only serialize-once remains a baseline experiment.
    rel = "gossip_benchmarks/_benchmark_common.py"
    start = files[rel].find("    baseline_all = (")
    end = files[rel].find("    config: dict[str, Any] = {", start)
    if start < 0 or end < 0:
        raise PatchError("cannot locate baseline env/config block")
    block = '''    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
    # Keep this switch only for non-baseline Succession experiments.
    task_manager_pin = (
        os.environ.get("RAY_RECOVERY_TASKMANAGER_PIN", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )

'''
    files[rel] = files[rel][:start] + block + files[rel][end:]
    for name in REMOVED_FLAGS:
        pat = rf'(?m)^\s*"{re.escape(name)}":\s*[^,\n]+,\n'
        out, n = re.subn(pat, "", files[rel], count=1)
        if n != 1:
            raise PatchError(f"cannot remove benchmark config key {name}: {n}")
        files[rel] = out
    print("[stage] simplify benchmark optimization plumbing")

    # 3) Baseline TaskManager pin is permanent.
    rel = "src/ray/core_worker/recovery_succession_manager.cc"
    replace_once(
        files, rel,
        '''  const bool task_manager_pin =
      RayConfig::instance().enable_recovery_succession_task_manager_pin();
''',
        '''  const bool task_manager_pin =
      RayConfig::instance().enable_recovery_witness_holder_baseline() ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();
''',
        "baseline always uses TaskManager pin")

    rel = "src/ray/core_worker/core_worker.cc"
    replace_once(
        files, rel,
        '''    if (RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
      RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
          << "Eligible recovery task disappeared before TaskManager pin: "
          << task_spec.TaskId();
    }
''',
        '''    if (recovery_witness_holder_baseline_enabled_ ||
        RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
      RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
          << "Eligible recovery task disappeared before TaskManager pin: "
          << task_spec.TaskId();
    }
''',
        "pin baseline TaskManager entry")
    replace_once(
        files, rel,
        '''      if (final_return_deleted &&
          RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
        task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
      }
''',
        '''      if (final_return_deleted &&
          (recovery_witness_holder_baseline_enabled_ ||
           RayConfig::instance().enable_recovery_succession_task_manager_pin())) {
        task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
      }
''',
        "release baseline TaskManager pin")

    # 4) Compact dependency metadata is permanent for baseline (Succession already uses it).
    rel = "src/ray/core_worker/recovery_succession_manager.cc"
    replace_once(
        files, rel,
        '''    const bool baseline_enabled =
        RayConfig::instance().enable_recovery_witness_holder_baseline();
    const bool compact_allowed =
        !baseline_enabled ||
        RayConfig::instance().enable_recovery_baseline_compact_argument_metadata();

    if (compact_allowed && entry->has_owner_address()) {
''',
        '''    if (entry->has_owner_address()) {
''',
        "compact baseline dependency metadata")
    replace_once(
        files, rel,
        '''      } else if (
          !RayConfig::instance().enable_recovery_witness_holder_baseline() ||
          RayConfig::instance().enable_recovery_baseline_compact_argument_metadata()) {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
''',
        '''      } else {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
''',
        "compact metadata fallback accounting")

    # 5) Fast receiver: remove it entirely. Its measured contribution was noise-level,
    #    while it duplicates borrowed-object state update logic and the ordinary path
    #    is already correctness-tested for the baseline.
    regex_once(
        files, rel,
        r'''(?s)  if \(RayConfig::instance\(\)\.enable_recovery_witness_holder_baseline\(\) &&\n      RayConfig::instance\(\)\.enable_recovery_baseline_fast_receiver\(\) &&\n      !should_store_task\) \{\n.*?\n    return reports;\n  \}\n\n  std::vector<CandidateReport> reports;''',
        "  std::vector<CandidateReport> reports;",
        "remove noise-level fast receiver branch",
        flags=re.S)

    # 6) Optimized deterministic witness scoring/top-k is permanent for baseline.
    rel = "src/ray/core_worker/core_worker.cc"
    replace_once(
        files, rel,
        '''  const bool optimized_baseline_selection =
      recovery_witness_holder_baseline_enabled_ &&
      RayConfig::instance().enable_recovery_baseline_topk_witness_selection();
''',
        '''  const bool optimized_baseline_selection =
      recovery_witness_holder_baseline_enabled_;
''',
        "optimized baseline witness score")
    replace_once(
        files, rel,
        '''  if (recovery_witness_holder_baseline_enabled_ &&
      RayConfig::instance().enable_recovery_baseline_topk_witness_selection() &&
      selected_count < candidates.size()) {
''',
        '''  if (recovery_witness_holder_baseline_enabled_ &&
      selected_count < candidates.size()) {
''',
        "baseline top-k witness selection")

    # 7) Sender: permanently elide the intermediate owner TaskSpec copy.
    old = '''    const bool separate_manifest_storage =
        RayConfig::instance().enable_recovery_baseline_separate_manifest_storage();
    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();
    const bool elide_intermediate_copy =
        RayConfig::instance().enable_recovery_baseline_elide_task_spec_copy();

    rpc::TaskSpec baseline_task_spec;
    std::string serialized_baseline_task_spec;
    const rpc::TaskSpec *publish_task_spec = nullptr;
    const std::string *publish_serialized_task_spec = nullptr;

    if (serialize_task_spec_once) {
      // Transport remains identical to the original baseline contract: every
      // holder receives a complete replayable TaskSpec with the authoritative
      // RecoveryManifest embedded. Serialize that representation only once.
      baseline_task_spec.CopyFrom(task_proto);
      baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
      serialized_baseline_task_spec = baseline_task_spec.SerializeAsString();
      publish_serialized_task_spec = &serialized_baseline_task_spec;
    } else if (separate_manifest_storage || elide_intermediate_copy) {
      // Avoid the intermediate owner copy. Publication attaches the authoritative
      // manifest directly to each outgoing request copy.
      publish_task_spec = &task_proto;
    } else {
      // Original fixed-R baseline control.
      baseline_task_spec.CopyFrom(task_proto);
      baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
      publish_task_spec = &baseline_task_spec;
    }
'''
    new = '''    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();

    rpc::TaskSpec serialized_task_spec_proto;
    std::string serialized_baseline_task_spec;
    const rpc::TaskSpec *publish_task_spec = nullptr;
    const std::string *publish_serialized_task_spec = nullptr;

    if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
      // replayable TaskSpec with the authoritative RecoveryManifest embedded.
      serialized_task_spec_proto.CopyFrom(task_proto);
      serialized_task_spec_proto.mutable_recovery_manifest()->CopyFrom(manifest);
      serialized_baseline_task_spec = serialized_task_spec_proto.SerializeAsString();
      publish_serialized_task_spec = &serialized_baseline_task_spec;
    } else {
      // Frozen baseline: publication copies directly into each outgoing request.
      publish_task_spec = &task_proto;
    }
'''
    replace_once(files, rel, old, new, "remove elide/separate sender flag interaction")
    replace_once(
        files, rel,
        '''      if (recovery_witness_holder_baseline_enabled_ &&
          (RayConfig::instance()
               .enable_recovery_baseline_separate_manifest_storage() ||
           RayConfig::instance()
               .enable_recovery_baseline_elide_task_spec_copy())) {
''',
        '''      if (recovery_witness_holder_baseline_enabled_) {
''',
        "always attach manifest to baseline wire TaskSpec")

    # 8) Baseline lineage publication always uses existing per-raylet batching.
    rel = "src/ray/raylet_rpc_client/raylet_client.cc"
    regex_once(
        files, rel,
        r'''(?s)  const bool baseline_lineage_request =\n      request\.has_task_spec\(\) \|\| !request\.serialized_task_spec\(\)\.empty\(\);\n\n  // Original baseline control: one physical RPC per logical witness update\.\n  // The optimized baseline feeds full-lineage updates through the existing\n  // per-raylet batcher without changing logical callbacks or all-R durability\.\n  if \(baseline_lineage_request &&\n      !RayConfig::instance\(\)\.enable_recovery_baseline_witness_batching\(\)\) \{\n.*?\n    return;\n  \}\n\n''',
        "",
        "always batch baseline lineage updates",
        flags=re.S)
    replace_once(
        files, rel,
        '''    if (RayConfig::instance().enable_recovery_baseline_batch_request_swap() &&
        RayConfig::instance().enable_recovery_witness_holder_baseline()) {
''',
        '''    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
''',
        "baseline batch Swap")

    # 9) Witness receiver: serialized input is valid whenever baseline sender chooses it.
    #    Use protobuf equality, move TaskSpec storage, retain manifest separately.
    rel = "src/ray/raylet/node_manager.cc"
    replace_once(
        files, rel,
        '''    if (!baseline_enabled ||
        !RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once() ||
        !decoded_task_spec.ParseFromString(request.serialized_task_spec())) {
''',
        '''    if (!baseline_enabled ||
        !decoded_task_spec.ParseFromString(request.serialized_task_spec())) {
''',
        "serialized baseline receiver")
    replace_once(
        files, rel,
        '''  const bool fast_manifest_validation =
      baseline_enabled &&
      RayConfig::instance().enable_recovery_baseline_fast_manifest_validation();
''',
        '''  const bool fast_manifest_validation = baseline_enabled;
''',
        "fast baseline manifest equality")
    replace_once(
        files, rel,
        '''        if (has_serialized_task_spec) {
          // decoded_task_spec is already owned by this handler.
          stored_task_spec.Swap(&decoded_task_spec);
        } else if (
            RayConfig::instance().enable_recovery_baseline_move_witness_task_spec()) {
          stored_task_spec.Swap(request.mutable_task_spec());
        } else {
          stored_task_spec.CopyFrom(*incoming_task_spec);
        }

        if (RayConfig::instance()
                .enable_recovery_baseline_separate_manifest_storage()) {
          stored_task_spec.clear_recovery_manifest();
        }
''',
        '''        if (has_serialized_task_spec) {
          stored_task_spec.Swap(&decoded_task_spec);
        } else {
          stored_task_spec.Swap(request.mutable_task_spec());
        }

        // The full replayable lineage remains here; the authoritative mutable
        // RecoveryManifest is retained exactly once in recovery_witness_manifests_.
        stored_task_spec.clear_recovery_manifest();
''',
        "move witness TaskSpec + separate manifest storage")
    replace_once(
        files, rel,
        '''        if (task_spec_it != recovery_witness_task_specs_.end() &&
            !RayConfig::instance()
                 .enable_recovery_baseline_separate_manifest_storage()) {
          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);
        }
''',
        '''        if (task_spec_it != recovery_witness_task_specs_.end() &&
            !baseline_enabled) {
          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);
        }
''',
        "keep retained baseline lineage manifest-free")

    # Claim path: replace any remaining baseline separate-manifest condition with
    # the already-correct storage-only behavior. We require at least one site.
    text = files[rel]
    pat = r'''(?s)if \(RayConfig::instance\(\)\s*\.enable_recovery_baseline_separate_manifest_storage\(\)\) \{\s*reply->mutable_task_spec\(\)\s*->mutable_recovery_manifest\(\)\s*->CopyFrom\(stored_manifest\);\s*\}'''
    text, n = re.subn(
        pat,
        '''reply->mutable_task_spec()\n              ->mutable_recovery_manifest()\n              ->CopyFrom(stored_manifest);''',
        text)
    if n < 1:
        raise PatchError("could not find recovery reply manifest reattachment site")
    files[rel] = text
    print(f"[stage] authoritative manifest reattachment on reply ({n} sites)")

    # Remove the old non-separate mutation branch if present.
    pat = r'''(?s)\n\s*// With separate-manifest storage the retained full lineage.*?\n\s*if \(!RayConfig::instance\(\)\s*\.enable_recovery_baseline_separate_manifest_storage\(\)\) \{\s*task_spec_it->second\s*\.mutable_recovery_manifest\(\)\s*->CopyFrom\(stored_manifest\);\s*\}\n'''
    files[rel], n = re.subn(pat, "\n", files[rel], count=1)
    if n != 1:
        raise PatchError(f"could not remove retained manifest mutation branch: {n}")
    print("[stage] immutable retained baseline lineage on recovery claim")

    # 10) Strong postconditions.
    production = "\n".join(files[x] for x in FILES)
    for token in REMOVED_FLAGS:
        if token in production:
            raise PatchError(f"removed flag still referenced: {token}")
    if "enable_recovery_baseline_serialize_task_spec_once" not in production:
        raise PatchError("serialize-once experimental switch disappeared")

    for needle in [
        "RAY_CHECK_EQ(manifest.witness_count(), target_holder_count)",
        "Installed full TaskSpec on all",
        "incoming_task_spec->has_recovery_manifest()",
        "stored_task_spec.clear_recovery_manifest()",
    ]:
        if needle not in production:
            raise PatchError(f"correctness marker disappeared: {needle}")

    changed = [x for x in FILES if files[x] != original[x]]
    print("\nStaged cleanup validated. Changed files:")
    for x in changed:
        print("  " + x)

    if args.check:
        print("\n--check: no files written")
        return

    for rel in changed:
        (root / rel).write_text(files[rel])
    print("\nCleanup applied. Rebuild Ray and run correctness before performance.")
    print("Recommended after review: git rm gossip_benchmarks/apply_baseline_optimization_suite.py")


if __name__ == "__main__":
    main()
