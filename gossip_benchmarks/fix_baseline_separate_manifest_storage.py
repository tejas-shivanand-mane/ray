#!/usr/bin/env python3
"""
Fix the separate-manifest baseline optimization.

The original optimization incorrectly changed both:
  1) witness transport representation, and
  2) witness retained representation.

This fix makes it STORAGE-ONLY:
  * every baseline holder still receives/validates a complete TaskSpec with the
    exact authoritative RecoveryManifest embedded, exactly like the original baseline;
  * after successful validation, the witness may clear the duplicated embedded
    manifest from its retained TaskSpec;
  * the authoritative witness manifest remains in recovery_witness_manifests_;
  * on recovery claim, the manifest is reattached before the TaskSpec is returned.

Thus the optimization reduces retained duplicate state without weakening or
special-casing the installation contract.
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path


class PatchError(RuntimeError):
    pass


FILES = [
    "src/ray/core_worker/core_worker.cc",
    "src/ray/raylet/node_manager.cc",
]


def replace_once(files, rel, old, new, label):
    text = files[rel]
    if new in text:
        print(f"[already] {label}")
        return
    count = text.count(old)
    if count != 1:
        raise PatchError(
            f"{label}: expected exactly one match in {rel}, found {count}"
        )
    files[rel] = text.replace(old, new, 1)
    print(f"[stage] {label}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    files, originals = {}, {}
    for rel in FILES:
        p = root / rel
        if not p.exists():
            raise PatchError(f"missing expected file: {p}")
        text = p.read_text()
        files[rel] = text
        originals[rel] = text

    # 1) Serialize-once always serializes the same complete replayable TaskSpec
    #    that the original baseline sends. Separate-manifest changes storage only.
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """    if (serialize_task_spec_once) {
      if (separate_manifest_storage) {
        serialized_baseline_task_spec = task_proto.SerializeAsString();
      } else {
        baseline_task_spec.CopyFrom(task_proto);
        baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
        serialized_baseline_task_spec = baseline_task_spec.SerializeAsString();
      }
      publish_serialized_task_spec = &serialized_baseline_task_spec;
    } else if (separate_manifest_storage || elide_intermediate_copy) {
      // Publication will either keep the manifest separate or attach it
      // directly to each outgoing request.
      publish_task_spec = &task_proto;
""",
        """    if (serialize_task_spec_once) {
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
""",
        "make serialize-once preserve complete baseline wire TaskSpec",
    )

    # 2) Never strip the manifest before transport. If publishing the owner's
    #    original lineage TaskSpec, attach the manifest to each outgoing request.
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """      if (recovery_witness_holder_baseline_enabled_) {
        if (RayConfig::instance()
                .enable_recovery_baseline_separate_manifest_storage()) {
          request.mutable_task_spec()->clear_recovery_manifest();
        } else if (RayConfig::instance()
                       .enable_recovery_baseline_elide_task_spec_copy()) {
          request.mutable_task_spec()
              ->mutable_recovery_manifest()
              ->CopyFrom(manifest);
        }
      }
""",
        """      if (recovery_witness_holder_baseline_enabled_ &&
          (RayConfig::instance()
               .enable_recovery_baseline_separate_manifest_storage() ||
           RayConfig::instance()
               .enable_recovery_baseline_elide_task_spec_copy())) {
        // Even with separate retained storage, installation uses the original
        // full-lineage baseline wire contract. The manifest is removed only
        // after the witness has validated and accepted this request.
        request.mutable_task_spec()
            ->mutable_recovery_manifest()
            ->CopyFrom(manifest);
      }
""",
        "keep complete TaskSpec on baseline witness wire",
    )

    # 3) Remove the relaxed validation that allowed a baseline TaskSpec with no
    #    embedded manifest. All baseline installation paths now obey one contract.
    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """  if (incoming_task_spec != nullptr) {
    const bool separate_manifest_storage =
        RayConfig::instance().enable_recovery_baseline_separate_manifest_storage();

    bool valid_lineage =
        baseline_enabled &&
        incoming_task_spec->task_id() == incoming.task_id();

    if (valid_lineage && separate_manifest_storage) {
      valid_lineage =
          !incoming_task_spec->has_recovery_manifest() ||
          manifests_equal(incoming_task_spec->recovery_manifest(), incoming);
    } else if (valid_lineage) {
      valid_lineage =
          incoming_task_spec->has_recovery_manifest() &&
          manifests_equal(incoming_task_spec->recovery_manifest(), incoming);
    }

    if (!valid_lineage) {
""",
        """  if (incoming_task_spec != nullptr) {
    // Keep one strict installation contract for both the original and optimized
    // fixed-R baseline: every holder must receive a complete replayable TaskSpec
    // whose embedded manifest exactly matches the separately supplied manifest.
    const bool valid_lineage =
        baseline_enabled &&
        incoming_task_spec->task_id() == incoming.task_id() &&
        incoming_task_spec->has_recovery_manifest() &&
        manifests_equal(incoming_task_spec->recovery_manifest(), incoming);

    if (!valid_lineage) {
""",
        "restore strict full-lineage baseline validation",
    )

    # Safety postconditions.
    cc = files["src/ray/core_worker/core_worker.cc"]
    nm = files["src/ray/raylet/node_manager.cc"]
    if "serialized_baseline_task_spec = task_proto.SerializeAsString();" in cc:
        raise PatchError(
            "unsafe raw TaskSpec serialization still remains in baseline separate-manifest path"
        )
    if "clear_recovery_manifest();" not in nm:
        raise PatchError(
            "witness-side retained-manifest elision unexpectedly missing"
        )
    if "enable_recovery_baseline_separate_manifest_storage" not in nm:
        raise PatchError(
            "separate-manifest retained-storage optimization unexpectedly missing"
        )

    changed = [rel for rel in FILES if files[rel] != originals[rel]]
    print("\nPreflight passed.")
    for rel in changed:
        print(f"  - {rel}")

    if args.check:
        print("\n--check requested; no files were written.")
        return

    for rel in changed:
        (root / rel).write_text(files[rel])

    print("\nSeparate-manifest optimization fixed.")
    print("Rebuild Ray, then rerun bisect cases 06 and 07.")


if __name__ == "__main__":
    try:
        main()
    except PatchError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
