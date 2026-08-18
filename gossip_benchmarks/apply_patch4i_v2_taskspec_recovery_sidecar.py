#!/usr/bin/env python3
"""Apply Patch 4I: TaskSpec-level recovery argument sidecar.

Run from the root of the Ray repository after Patch 4H.

Goals:
  * Keep recovery metadata out of ObjectReference fields on the normal TaskSpec wire path.
  * Attach one recovery sidecar per unique dependency ObjectID at TaskSpec level.
  * Reuse Patch 4H compact metadata inside that sidecar.
  * Preserve the Patch 4F H1 TaskSpec piggyback and all admission/witness/replay semantics.
  * Retain legacy ObjectReference recovery metadata as a receiver/sender compatibility fallback.

Files modified:
  src/ray/protobuf/common.proto
  src/ray/core_worker/recovery_succession_manager.cc
  gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py  (label only)

The script backs up touched files under .patch4i_backups/<timestamp>/ and runs
`git diff --check` after writing.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import time
from pathlib import Path


PATCHER_VERSION = "4I-v2-brace-aware"

ROOT = Path.cwd()
COMMON = ROOT / "src/ray/protobuf/common.proto"
MANAGER_CC = ROOT / "src/ray/core_worker/recovery_succession_manager.cc"
BENCH16 = ROOT / "gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py"
FILES = [COMMON, MANAGER_CC, BENCH16]


def die(msg: str) -> "NoReturn":
    raise SystemExit(f"Patch 4I: {msg}")


def require(path: Path) -> str:
    if not path.is_file():
        die(f"missing expected file: {path}")
    return path.read_text()


def body_bounds(text: str, marker: str) -> tuple[int, int]:
    """Return indices of the opening and matching closing brace for a C++ function."""
    pos = text.find(marker)
    if pos < 0:
        die(f"could not find C++ marker: {marker!r}")
    open_brace = text.find("{", pos)
    if open_brace < 0:
        die(f"could not find opening brace after: {marker!r}")

    depth = 0
    in_string = False
    in_char = False
    escape = False
    line_comment = False
    block_comment = False
    i = open_brace
    while i < len(text):
        c = text[i]
        n = text[i + 1] if i + 1 < len(text) else ""

        if line_comment:
            if c == "\n":
                line_comment = False
            i += 1
            continue
        if block_comment:
            if c == "*" and n == "/":
                block_comment = False
                i += 2
            else:
                i += 1
            continue
        if in_string:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_string = False
            i += 1
            continue
        if in_char:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == "'":
                in_char = False
            i += 1
            continue

        if c == "/" and n == "/":
            line_comment = True
            i += 2
            continue
        if c == "/" and n == "*":
            block_comment = True
            i += 2
            continue
        if c == '"':
            in_string = True
            i += 1
            continue
        if c == "'":
            in_char = True
            i += 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return open_brace, i
        i += 1

    die(f"unmatched braces after marker: {marker!r}")


def replace_body(text: str, marker: str, body: str) -> str:
    a, b = body_bounds(text, marker)
    return text[: a + 1] + "\n" + body.rstrip() + "\n" + text[b:]


def insert_once(text: str, anchor: str, insertion: str, *, after: bool = True) -> str:
    if anchor not in text:
        die(f"missing insertion anchor: {anchor!r}")
    if text.count(anchor) != 1:
        die(f"anchor is not unique ({text.count(anchor)} matches): {anchor!r}")
    return text.replace(anchor, anchor + insertion if after else insertion + anchor, 1)


def validate_pre_state(common: str, manager: str) -> None:
    # Require the exact architectural state this patch was designed for.
    required_common = [
        "message RecoveryObjectTransportManifest",
        "RecoveryObjectTransportManifest compact_manifest = 5;",
        "optional RecoveryManifest recovery_manifest = 47;",
    ]
    for needle in required_common:
        if needle not in common:
            die(f"Patch 4H prerequisite missing from common.proto: {needle}")

    required_manager = [
        "Patch 4H: compact task-argument recovery metadata.",
        "WriteCompactTaskArgumentRecoveryMetadata(",
        "ExpandTaskArgumentRecoveryMetadata(",
        "void RecoverySuccessionManager::PopulateTaskArgumentMetadata(",
        "RecoverySuccessionManager::RegisterExecutorTask(",
    ]
    for needle in required_manager:
        if needle not in manager:
            die(f"Patch 4H prerequisite missing from manager: {needle}")


def patch_common(common: str) -> str:
    if "repeated RecoveryTaskArgumentMetadata recovery_argument_metadata = 48;" in common:
        return common

    sidecar_msg = r'''

// Patch 4I: transport recovery metadata once per unique dependency at the
// enclosing TaskSpec level instead of embedding it in ObjectReference. This
// keeps the ordinary ObjectReference hot path free of Recovery Succession
// fields while preserving all information required by the receiver.
message RecoveryTaskArgumentMetadata {
  bytes object_id = 1;
  Address owner_address = 2;
  RecoveryObjectMetadata recovery_metadata = 3;
}
'''

    # Insert after the complete RecoveryObjectMetadata message using brace
    # matching instead of depending on exact newline/blank-line formatting.
    _, metadata_close = body_bounds(common, "message RecoveryObjectMetadata {")
    insert_pos = metadata_close + 1
    # Preserve whatever newline convention/spacing follows the message.
    if insert_pos < len(common) and common[insert_pos] == "\r":
        insert_pos += 1
    if insert_pos < len(common) and common[insert_pos] == "\n":
        insert_pos += 1
    common = common[:insert_pos] + sidecar_msg + common[insert_pos:]

    task_field = r'''

  // Patch 4I transport-only dependency recovery sidecars. Entries are
  // deduplicated by object_id by the sender and consumed by CoreWorker before
  // task execution. Retained/replayed TaskSpecs keep compact dependency state,
  // but nested first-holder TaskSpec piggybacks are stripped.
  repeated RecoveryTaskArgumentMetadata recovery_argument_metadata = 48;
'''

    # Restrict insertion to TaskSpec itself and place the field immediately
    # after recovery_manifest=47 without requiring a particular trailing newline.
    task_open, task_close = body_bounds(common, "message TaskSpec {")
    task_body = common[task_open + 1 : task_close]
    field_needle = "optional RecoveryManifest recovery_manifest = 47;"
    field_rel = task_body.find(field_needle)
    if field_rel < 0:
        die("missing TaskSpec recovery_manifest=47 insertion point")
    field_end = task_open + 1 + field_rel + len(field_needle)
    common = common[:field_end] + task_field + common[field_end:]
    return common


def patch_manager(manager: str) -> str:
    if "// Patch 4I: TaskSpec-level recovery argument sidecar." not in manager:
        manager = manager.replace(
            "// Patch 4H: compact task-argument recovery metadata.\n",
            "// Patch 4H: compact task-argument recovery metadata.\n"
            "// Patch 4I: TaskSpec-level recovery argument sidecar.\n",
            1,
        )

    # Retained lineage must keep dependency recovery metadata for replay, but it
    # must never recursively retain full upstream TaskSpecs.
    clear_body = r'''  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      arg.mutable_object_ref()
          ->mutable_recovery_metadata()
          ->clear_first_holder_task_spec();
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        nested_ref.mutable_recovery_metadata()->clear_first_holder_task_spec();
      }
    }
  }

  // Patch 4I sidecars are part of the downstream TaskSpec's dependency
  // recovery description and therefore must survive replay. Only the nested
  // full-lineage piggyback is transport-only and must be stripped.
  for (rpc::RecoveryTaskArgumentMetadata &entry :
       *task_spec->mutable_recovery_argument_metadata()) {
    if (entry.has_recovery_metadata()) {
      entry.mutable_recovery_metadata()->clear_first_holder_task_spec();
    }
  }'''
    manager = replace_body(
        manager,
        "void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec)",
        clear_body,
    )

    # Add a sidecar decoder that deliberately reuses the already-tested 4H
    # expansion boundary. The synthetic ObjectReference is local-only and never
    # enters normal Ray transport.
    helper_marker = "bool ExpandTaskSidecarRecoveryMetadata("
    if helper_marker not in manager:
        anchor = "const std::string &RecoveryBenchmarkAblationMode() {"
        helper = r'''
// Patch 4I TaskSpec-level sidecar expansion. Reuse the Patch-4H expansion
// logic through a local synthetic ObjectReference so all downstream manager
// state continues to see the exact ordinary RecoveryObjectMetadata shape.
bool ExpandTaskSidecarRecoveryMetadata(
    const rpc::RecoveryTaskArgumentMetadata &entry,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || entry.object_id().empty() ||
      !entry.has_recovery_metadata()) {
    return false;
  }

  rpc::ObjectReference synthetic_ref;
  synthetic_ref.set_object_id(entry.object_id());
  if (entry.has_owner_address()) {
    synthetic_ref.mutable_owner_address()->CopyFrom(entry.owner_address());
  }
  synthetic_ref.mutable_recovery_metadata()->CopyFrom(entry.recovery_metadata());
  return ExpandTaskArgumentRecoveryMetadata(synthetic_ref, expanded);
}

'''
        if anchor not in manager:
            die("could not find helper insertion point before RecoveryBenchmarkAblationMode")
        manager = manager.replace(anchor, helper + anchor, 1)

    carries_body = r'''  if (task_spec.has_recovery_manifest() ||
      task_spec.recovery_argument_metadata_size() > 0) {
    return true;
  }

  // Backward compatibility for TaskSpecs created by pre-4I workers/tests.
  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      return true;
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        return true;
      }
    }
  }

  return false;'''
    manager = replace_body(
        manager,
        "bool RecoverySuccessionManager::CarriesRecoveryMetadata(",
        carries_body,
    )

    # Change only the collection prefix of RegisterExecutorTask. The remainder
    # of the function (provisional piggyback state, candidate reports, etc.) is
    # intentionally left untouched.
    a, b = body_bounds(manager, "RecoverySuccessionManager::RegisterExecutorTask(")
    body = manager[a + 1 : b]
    pivot = "  const bool should_store_task ="
    pivot_pos = body.find(pivot)
    if pivot_pos < 0:
        die("could not locate RegisterExecutorTask should_store_task pivot")

    register_prefix = r'''
  const auto patch4g_start = std::chrono::steady_clock::now();
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;
  absl::flat_hash_set<ObjectID> received_object_ids;

  auto append_metadata = [&received_metadata, &received_object_ids](
                             const ObjectID &object_id,
                             rpc::RecoveryObjectMetadata metadata) {
    if (!received_object_ids.insert(object_id).second) {
      return;
    }
    received_metadata.emplace_back(object_id, std::move(metadata));
  };

  // Patch 4I primary path: one TaskSpec-level sidecar per unique dependency.
  for (const rpc::RecoveryTaskArgumentMetadata &entry :
       task_spec.recovery_argument_metadata()) {
    if (entry.object_id().size() != ObjectID::Size()) {
      continue;
    }

    rpc::RecoveryObjectMetadata metadata;
    if (!ExpandTaskSidecarRecoveryMetadata(entry, &metadata)) {
      continue;
    }

    append_metadata(ObjectID::FromBinary(entry.object_id()), std::move(metadata));
  }

  // Backward-compatible path for pre-4I TaskSpecs. A TaskSpec-level entry wins
  // if both representations are present for the same ObjectID.
  auto collect_legacy_metadata =
      [&received_object_ids, &append_metadata](const rpc::ObjectReference &object_ref) {
        if (object_ref.object_id().empty() || !object_ref.has_recovery_metadata() ||
            object_ref.object_id().size() != ObjectID::Size()) {
          return;
        }

        const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
        if (received_object_ids.contains(object_id)) {
          return;
        }

        rpc::RecoveryObjectMetadata metadata;
        if (!ExpandTaskArgumentRecoveryMetadata(object_ref, &metadata)) {
          return;
        }

        append_metadata(object_id, std::move(metadata));
      };

  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref()) {
      collect_legacy_metadata(arg.object_ref());
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      collect_legacy_metadata(nested_ref);
    }
  }

'''
    new_body = register_prefix + body[pivot_pos:]
    manager = manager[: a + 1] + new_body + manager[b:]

    populate_body = r'''  if (task_spec == nullptr) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  // This field is transport-only. Rebuilding it from manager state also makes
  // this method idempotent if task construction revisits the same TaskSpec.
  task_spec->clear_recovery_argument_metadata();
  absl::flat_hash_set<ObjectID> attached_object_ids;

  auto populate_one = [this, task_spec, &attached_object_ids](
                          const ObjectID &object_id,
                          rpc::ObjectReference *object_ref) {
    if (object_ref == nullptr || object_id.IsNil()) {
      return;
    }

    // A legacy/pre-4I ObjectRef may already carry recovery metadata. Save it
    // as a compatibility fallback, then make the ObjectReference ordinary on
    // the TaskSpec wire path.
    rpc::RecoveryObjectMetadata legacy_transport;
    const bool had_legacy_transport = object_ref->has_recovery_metadata();
    if (had_legacy_transport) {
      legacy_transport.CopyFrom(object_ref->recovery_metadata());
    }
    object_ref->clear_recovery_metadata();

    // One sidecar per unique dependency even if the same ObjectRef appears in
    // multiple direct/nested argument positions.
    if (attached_object_ids.contains(object_id)) {
      return;
    }

    rpc::RecoveryObjectMetadata legacy_expanded;
    const rpc::RecoveryObjectMetadata *source = nullptr;

    const auto metadata_it = object_recovery_metadata_.find(object_id);
    if (metadata_it != object_recovery_metadata_.end()) {
      source = &metadata_it->second;
    } else if (had_legacy_transport) {
      rpc::ObjectReference synthetic_ref;
      synthetic_ref.set_object_id(object_id.Binary());
      if (object_ref->has_owner_address()) {
        synthetic_ref.mutable_owner_address()->CopyFrom(object_ref->owner_address());
      }
      synthetic_ref.mutable_recovery_metadata()->CopyFrom(legacy_transport);
      if (ExpandTaskArgumentRecoveryMetadata(synthetic_ref, &legacy_expanded)) {
        source = &legacy_expanded;
      }
    }

    if (source == nullptr || source->task_id().empty() || !source->has_manifest()) {
      return;
    }

    rpc::RecoveryTaskArgumentMetadata *entry =
        task_spec->add_recovery_argument_metadata();
    entry->set_object_id(object_id.Binary());
    if (object_ref->has_owner_address()) {
      entry->mutable_owner_address()->CopyFrom(object_ref->owner_address());
    }

    rpc::RecoveryObjectMetadata *out = entry->mutable_recovery_metadata();
    bool compact_transport = false;

    // Keep witness-as-holder baseline semantics and representation unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    } else if (entry->has_owner_address()) {
      compact_transport = WriteCompactTaskArgumentRecoveryMetadata(
          *source, source->manifest(), entry->owner_address(), out);
      if (!compact_transport) {
        out->CopyFrom(*source);
        out->clear_first_holder_task_spec();
        out->clear_compact_manifest();
      }
    } else {
      // Safety fallback: a full manifest does not need owner reconstruction.
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }

    attached_object_ids.insert(object_id);

    if (profiling_enabled_) {
      ++profile_.task_argument_metadata_refs_attached;
      profile_.task_argument_metadata_full_bytes_equivalent +=
          static_cast<uint64_t>(source->ByteSizeLong());
      profile_.task_argument_metadata_transport_bytes +=
          static_cast<uint64_t>(out->ByteSizeLong());
      if (compact_transport) {
        ++profile_.task_argument_metadata_compact_refs;
      } else if (!RayConfig::instance().enable_recovery_witness_holder_baseline()) {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
    }

    // Keep the witness-as-holder baseline unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      return;
    }

    // Patch 4G benchmark ablations that isolate compact metadata and/or the
    // candidate RPC must not put the full TaskSpec on PushTask. no_piggyback
    // recreates the pre-4F H1 transport while keeping full admission semantics.
    const std::string &patch4g_mode = RecoveryBenchmarkAblationMode();
    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
        patch4g_mode == "no_piggyback") {
      return;
    }

    const TaskID producer_task_id = TaskID::FromBinary(source->task_id());
    const auto task_it = task_states_.find(producer_task_id);
    if (task_it == task_states_.end()) {
      return;
    }

    TaskRecoveryState &state = task_it->second;

    // Claim exactly one full-lineage piggyback while the committed succession
    // is still [A]. Later holders use the ordinary Patch-4E install path.
    if (state.first_holder_piggyback_sent ||
        !state.manifest_committed ||
        !state.task_spec.has_value() ||
        state.manifest.tombstoned() ||
        state.manifest.frozen() ||
        state.manifest.succession_size() != 1 ||
        state.manifest.task_id() != source->task_id()) {
      return;
    }

    const rpc::RecoveryHolder *owner = FindHolderByRank(state.manifest, 0);
    if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
      return;
    }

    if (!IsEligibleTask(state.task_spec.value()) ||
        !state.task_spec->has_recovery_manifest() ||
        state.task_spec->task_id() != state.manifest.task_id()) {
      return;
    }

    // Pair the piggybacked lineage with the manager's exact current manifest.
    // If compact encoding cannot faithfully reproduce rank 0, retain the old
    // full-manifest fallback rather than weakening recovery semantics.
    if (!entry->has_owner_address() ||
        !WriteCompactTaskArgumentRecoveryMetadata(
            *source, state.manifest, entry->owner_address(), out)) {
      out->CopyFrom(*source);
      out->mutable_manifest()->CopyFrom(state.manifest);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }

    const auto serialize_start = std::chrono::steady_clock::now();
    std::string serialized_task_spec;
    const bool ok = state.task_spec->SerializeToString(&serialized_task_spec);
    const auto serialize_end = std::chrono::steady_clock::now();

    if (!ok || serialized_task_spec.empty()) {
      return;
    }

    out->set_first_holder_task_spec(serialized_task_spec);
    state.first_holder_piggyback_sent = true;

    if (profiling_enabled_) {
      ++profile_.first_holder_piggyback_copies_sent;
      profile_.first_holder_piggyback_bytes_sent +=
          static_cast<uint64_t>(serialized_task_spec.size());
      profile_.task_spec_bytes_sent +=
          static_cast<uint64_t>(serialized_task_spec.size());
      profile_.first_holder_piggyback_serialize_time_ns +=
          static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  serialize_end - serialize_start)
                  .count());
    }
  };

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && !arg.object_ref().object_id().empty() &&
        arg.object_ref().object_id().size() == ObjectID::Size()) {
      populate_one(ObjectID::FromBinary(arg.object_ref().object_id()),
                   arg.mutable_object_ref());
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.object_id().empty() ||
          nested_ref.object_id().size() != ObjectID::Size()) {
        continue;
      }
      populate_one(ObjectID::FromBinary(nested_ref.object_id()), &nested_ref);
    }
  }'''
    manager = replace_body(
        manager,
        "void RecoverySuccessionManager::PopulateTaskArgumentMetadata(",
        populate_body,
    )

    return manager


def patch_bench(text: str) -> str:
    # No behavioral benchmark change; only make the output label identify the
    # current transport implementation. Keep the case key `full` stable.
    text = text.replace(
        'Case("full", "Full4F", True, "full")',
        'Case("full", "Full4I", True, "full")',
    )
    text = text.replace(
        "  Full4F                    ordinary Patch-4F recovery",
        "  Full4I                    ordinary recovery with Patch-4I TaskSpec sidecar",
    )
    return text


def main() -> None:
    print(f"Patch 4I patcher version: {PATCHER_VERSION}")
    originals = {path: require(path) for path in FILES}
    validate_pre_state(originals[COMMON], originals[MANAGER_CC])

    already = (
        "repeated RecoveryTaskArgumentMetadata recovery_argument_metadata = 48;"
        in originals[COMMON]
        and "Patch 4I: TaskSpec-level recovery argument sidecar."
        in originals[MANAGER_CC]
    )
    if already:
        print("Patch 4I already appears to be applied; no changes made.")
        return

    patched = dict(originals)
    patched[COMMON] = patch_common(patched[COMMON])
    patched[MANAGER_CC] = patch_manager(patched[MANAGER_CC])
    patched[BENCH16] = patch_bench(patched[BENCH16])

    # Post-edit structural checks before touching the working tree.
    checks = [
        (COMMON, "message RecoveryTaskArgumentMetadata"),
        (COMMON, "repeated RecoveryTaskArgumentMetadata recovery_argument_metadata = 48;"),
        (MANAGER_CC, "ExpandTaskSidecarRecoveryMetadata("),
        (MANAGER_CC, "task_spec.recovery_argument_metadata_size() > 0"),
        (MANAGER_CC, "task_spec->clear_recovery_argument_metadata();"),
        (MANAGER_CC, "object_ref->clear_recovery_metadata();"),
        (MANAGER_CC, "task_spec->add_recovery_argument_metadata();"),
    ]
    for path, needle in checks:
        if needle not in patched[path]:
            die(f"post-edit validation failed for {path}: missing {needle!r}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup_root = ROOT / ".patch4i_backups" / stamp
    for path in FILES:
        rel = path.relative_to(ROOT)
        dst = backup_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dst)

    try:
        for path in FILES:
            path.write_text(patched[path])

        result = subprocess.run(
            ["git", "diff", "--check"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            raise RuntimeError("git diff --check failed:\n" + result.stdout)
    except Exception:
        # Restore atomically enough for an experimental patcher: every original
        # is already backed up before the first write.
        for path in FILES:
            path.write_text(originals[path])
        raise

    print("Applied Patch 4I successfully.")
    print(f"Backups: {backup_root}")
    print("Touched:")
    for path in FILES:
        if patched[path] != originals[path]:
            print(f"  - {path.relative_to(ROOT)}")
    print("\nNext:")
    print("  git diff --check")
    print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")


if __name__ == "__main__":
    main()
