#!/usr/bin/env python3
"""Apply the nested task-argument Recovery Frontier deferral fix.

Benchmark 47 showed that a Benchmark-30-shaped nested ``[ObjectRef]`` argument
synchronously crossed the all-R Recovery Frontier durability barrier during
Cython argument preparation, while a direct ObjectRef correctly returned before
ACK completion.

The cause is ``prepare_args_internal`` calling ``CoreWorker::GetObjectRefs`` for
``serialized_arg.contained_object_refs``.  ``GetObjectRefs`` currently calls
``TryPopulateRecoveryMetadataForObject`` without a deferred-group sink, so K>1
publishes synchronously before ``BuildCommonTaskSpec`` gets a chance to apply its
existing deferred-dispatch gate.

This patch threads the existing ``task_argument_serialization`` signal into
``GetObjectRefs``.  During K>1 witness-holder Frontier task-argument
serialization, ``GetObjectRefs`` now copies already-committed metadata only and
does not activate new protection.  ``BuildCommonTaskSpec`` subsequently sees the
same ``nested_inlined_refs()``, activates the group through
``EnsureRecoverySuccessionForTaskArguments(..., deferred_groups)``, and gates
remote dispatch until the all-R ACK.  Generic/out-of-band serialization and
non-Frontier behavior remain unchanged.

The script intentionally uses exact one-occurrence replacements and aborts on
any mismatch rather than making a fuzzy source edit.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"Expected exactly one match in {path}, found {count}. "
            "Refusing to modify the checkout."
        )
    target.write_text(text.replace(old, new, 1))
    print(f"patched {path}")


def main() -> None:
    replace_once(
        "src/ray/core_worker/core_worker.h",
        """  std::vector<rpc::ObjectReference> GetObjectRefs(\n      const std::vector<ObjectID> &object_ids) const;\n""",
        """  std::vector<rpc::ObjectReference> GetObjectRefs(\n      const std::vector<ObjectID> &object_ids,\n      bool task_argument_serialization = false) const;\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(\n    const std::vector<ObjectID> &object_ids) const {\n  std::vector<rpc::ObjectReference> refs;\n  refs.reserve(object_ids.size());\n\n  for (const auto &object_id : object_ids) {\n    rpc::ObjectReference ref;\n    ref.set_object_id(object_id.Binary());\n\n    rpc::Address owner_address;\n    if (reference_counter_->GetOwner(object_id, &owner_address)) {\n      // NOTE(swang): Detached actors do not have an\n      // owner address set.\n      *ref.mutable_owner_address() = std::move(owner_address);\n    }\n\n    if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {\n      rpc::RecoveryObjectMetadata metadata;\n\n      if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {\n        ref.mutable_recovery_metadata()->CopyFrom(metadata);\n      }\n    }\n\n    refs.emplace_back(std::move(ref));\n  }\n\n  return refs;\n}\n""",
        """std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(\n    const std::vector<ObjectID> &object_ids,\n    bool task_argument_serialization) const {\n  std::vector<rpc::ObjectReference> refs;\n  refs.reserve(object_ids.size());\n\n  const bool defer_frontier_task_argument_activation =\n      task_argument_serialization &&\n      recovery_witness_holder_baseline_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      recovery_succession_manager_->RecoveryFrontierEnabled() &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n\n  for (const auto &object_id : object_ids) {\n    rpc::ObjectReference ref;\n    ref.set_object_id(object_id.Binary());\n\n    rpc::Address owner_address;\n    if (reference_counter_->GetOwner(object_id, &owner_address)) {\n      // NOTE(swang): Detached actors do not have an\n      // owner address set.\n      *ref.mutable_owner_address() = std::move(owner_address);\n    }\n\n    if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {\n      rpc::RecoveryObjectMetadata metadata;\n\n      if (defer_frontier_task_argument_activation) {\n        // Nested ObjectRefs in a by-value task argument are seen again by\n        // BuildCommonTaskSpec via nested_inlined_refs().  For K>1, preserve\n        // already-committed metadata here without starting a synchronous\n        // publication. BuildCommonTaskSpec will activate any uncommitted group\n        // through its deferred-group path and gate remote dispatch on the same\n        // all-R durability ACK.\n        if (recovery_succession_manager_->PopulateRecoveryMetadata(object_id,\n                                                                   &metadata)) {\n          ref.mutable_recovery_metadata()->CopyFrom(metadata);\n        }\n      } else if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {\n        ref.mutable_recovery_metadata()->CopyFrom(metadata);\n      }\n    }\n\n    refs.emplace_back(std::move(ref));\n  }\n\n  return refs;\n}\n""",
    )

    replace_once(
        "python/ray/includes/libcoreworker.pxd",
        """        c_vector[CObjectReference] GetObjectRefs(\n                const c_vector[CObjectID] &object_ids) const\n""",
        """        c_vector[CObjectReference] GetObjectRefs(\n                const c_vector[CObjectID] &object_ids,\n                c_bool task_argument_serialization) const\n""",
    )

    replace_once(
        "python/ray/_raylet.pyx",
        """                inlined_refs = (CCoreWorkerProcess.GetCoreWorker()\n                                .GetObjectRefs(inlined_ids))\n""",
        """                inlined_refs = (CCoreWorkerProcess.GetCoreWorker()\n                                .GetObjectRefs(\n                                    inlined_ids,\n                                    <c_bool>task_argument_serialization.get()))\n""",
    )

    print("\nNested Recovery Frontier task-argument deferral fix applied.")
    print("Review with: git diff --check && git diff -- src/ray/core_worker/core_worker.h src/ray/core_worker/core_worker.cc python/ray/includes/libcoreworker.pxd python/ray/_raylet.pyx")


if __name__ == "__main__":
    main()
