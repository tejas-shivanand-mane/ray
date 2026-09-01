#!/usr/bin/env python3
"""Apply the ordinary K=1 transient H1 TaskSpec piggyback optimization.

This source-to-source patch is intentionally exact and fail-closed. It changes only
RecoverySuccessionManager::RegisterOwnedTaskLazy and
PopulateTaskArgumentMetadataInternal. The optimization keeps a sanitized owner
TaskSpec copy only from lazy activation until the first downstream metadata build,
piggybacks it to exactly one H1 candidate, then drops the owner-side duplicate.

Fixed-R, Recovery Frontier, certificate admission, R/W, witness confirmation, and
replay semantics are unchanged.
"""
from __future__ import annotations

from pathlib import Path

PATH = Path("src/ray/core_worker/recovery_succession_manager.cc")

OLD_LAZY = '''  TaskRecoveryState task_state;\n  task_state.manifest.CopyFrom(manifest);\n  task_state.owned_num_returns =\n      static_cast<uint32_t>(task_spec.NumReturns());\n  task_state.manifest_committed = true;\n\n  task_states_[task_id] = std::move(task_state);\n\n  // Patch 4L deliberately retains one dormant owner TaskSpec copy, so the\n  // legacy Patch-4J "copy avoided" counter must remain zero.\n'''

NEW_LAZY = '''  TaskRecoveryState task_state;\n  task_state.manifest.CopyFrom(manifest);\n  task_state.owned_num_returns =\n      static_cast<uint32_t>(task_spec.NumReturns());\n  task_state.manifest_committed = true;\n\n  // Ordinary adaptive K=1 only: retain one short-lived producer recipe so the\n  // first downstream borrower can receive H1 lineage on its existing PushTask.\n  // TaskManager remains the authoritative owner recipe. Frontier keeps its\n  // typed membership sidecar and Fixed-R/certificate modes are unchanged.\n  if (recovery_succession_enabled_config_ &&\n      !recovery_witness_holder_baseline_enabled_config_ &&\n      !recovery_frontier_enabled_config_ &&\n      !recovery_succession_certificate_admission_enabled_config_ &&\n      !manifest.tombstoned() && !manifest.frozen() &&\n      manifest.succession_size() == 1) {\n    rpc::TaskSpec transient_task_spec;\n    transient_task_spec.CopyFrom(task_proto);\n    ClearFirstHolderTaskSpecPiggybacks(&transient_task_spec);\n    transient_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);\n    task_state.task_spec = std::move(transient_task_spec);\n  }\n\n  task_states_[task_id] = std::move(task_state);\n\n  // Production adaptive owner lineage continues to live in TaskManager after\n  // the one-shot H1 transport copy is released.\n'''

OLD_TRANSPORT = '''    } else {\n      // Original baseline representation or safety fallback when the owner\n      // address cannot reconstruct rank 0. Preserve only a Frontier membership\n      // marker; ordinary first-holder TaskSpec piggybacks remain transport-only.\n      out->CopyFrom(*source);\n      recovery_succession_internal::ClearFirstHolderPayloadUnlessFrontierMembership(\n          out);\n      out->clear_compact_manifest();\n    }\n\n    attached_object_ids.insert(object_id);\n'''

NEW_TRANSPORT = '''    } else {\n      // Original baseline representation or safety fallback when the owner\n      // address cannot reconstruct rank 0. Preserve only a Frontier membership\n      // marker; ordinary first-holder TaskSpec piggybacks remain transport-only.\n      out->CopyFrom(*source);\n      recovery_succession_internal::ClearFirstHolderPayloadUnlessFrontierMembership(\n          out);\n      out->clear_compact_manifest();\n    }\n\n    // Ordinary adaptive K=1 H1 fast path. Lazy activation staged one sanitized\n    // producer TaskSpec in the existing task state. Exactly one downstream\n    // metadata build claims it under mutex_, transports it in the already\n    // supported Patch-4F field, then releases the owner-side duplicate. The\n    // receiver remains provisional and must still verify witness durability.\n    if (!recovery_witness_holder_baseline_enabled_config_ &&\n        !recovery_frontier_enabled_config_ &&\n        !recovery_succession_certificate_admission_enabled_config_) {\n      const TaskID producer_task_id = object_id.TaskId();\n      const auto producer_it = task_states_.find(producer_task_id);\n      if (producer_it != task_states_.end()) {\n        TaskRecoveryState &state = producer_it->second;\n        if (!state.first_holder_piggyback_sent &&\n            state.manifest_committed &&\n            !state.manifest.tombstoned() &&\n            !state.manifest.frozen() &&\n            state.manifest.succession_size() == 1 &&\n            state.manifest.task_id() == source->task_id() &&\n            state.task_spec.has_value() &&\n            state.task_spec->task_id() == source->task_id()) {\n          const auto piggyback_start = std::chrono::steady_clock::now();\n\n          rpc::TaskSpec piggyback_task_spec;\n          piggyback_task_spec.CopyFrom(state.task_spec.value());\n          ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);\n          piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(state.manifest);\n\n          std::string serialized_task_spec;\n          if (piggyback_task_spec.SerializeToString(&serialized_task_spec) &&\n              !serialized_task_spec.empty()) {\n            out->set_first_holder_task_spec(serialized_task_spec);\n            state.first_holder_piggyback_sent = true;\n            state.task_spec.reset();\n\n            if (profiling_enabled_) {\n              const uint64_t piggyback_ns = static_cast<uint64_t>(\n                  std::chrono::duration_cast<std::chrono::nanoseconds>(\n                      std::chrono::steady_clock::now() - piggyback_start)\n                      .count());\n              const uint64_t piggyback_bytes =\n                  static_cast<uint64_t>(serialized_task_spec.size());\n              ++profile_.first_holder_piggyback_copies_sent;\n              profile_.first_holder_piggyback_bytes_sent += piggyback_bytes;\n              profile_.first_holder_piggyback_serialize_time_ns += piggyback_ns;\n              profile_.task_spec_bytes_sent += piggyback_bytes;\n            }\n          }\n        }\n      }\n    }\n\n    attached_object_ids.insert(object_id);\n'''


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def main() -> None:
    text = PATH.read_text()
    text = replace_once(text, OLD_LAZY, NEW_LAZY, "lazy activation block")
    text = replace_once(text, OLD_TRANSPORT, NEW_TRANSPORT, "metadata transport block")
    PATH.write_text(text)
    print(f"Patched {PATH}")


if __name__ == "__main__":
    main()
