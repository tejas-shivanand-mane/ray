#!/usr/bin/env python3
"""Apply the Recovery Frontier + Succession provisional-witness recovery fix.

Benchmark 54 exposes a deterministic owner-crash window:

    Frontier[T1,T2] -> provisional H1 -> witness ACK -> owner dies

The holder already has the Frontier snapshot containing T1/T2 replay recipes,
but normal provisional-holder confirmation is task-centric.  For non-leader T2
it queries witnesses using T2's TaskID and requires the returned witness
manifest to also be keyed by T2.  Recovery Frontier publishes the shared
Succession topology under the Frontier/group TaskID (the leader/group ID), so
that lookup can never succeed for T2 and recovery falls back to OwnerDiedError.

This patch keeps witness validation strong while adding the required group/member
translation:

  * HandleRecoverTaskOutput queries compact witnesses using the Frontier group
    ID when the requested task belongs to an adaptive Frontier.
  * The expected returned witness key is the group ID, not the member TaskID.
  * ConfirmProvisionalHolderFromWitness receives the requested replay TaskID and
    translates a witness-backed group manifest into the corresponding member
    manifest with BuildFrontierMemberManifest before any promotion decision.
  * Ordinary non-Frontier Succession retains the exact task_id == witness key
    requirement.

The script uses exact one-occurrence replacements and aborts on source drift.
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
        "src/ray/core_worker/recovery_succession_manager.h",
        """  /// Promotes a provisional holder only from a manifest obtained directly\n  /// from one of the task's compact witnesses.\n  ///\n  /// A newer witness-backed generation may also be adopted if this worker\n  /// remains in the succession list.\n  bool ConfirmProvisionalHolderFromWitness(\n      const rpc::RecoveryManifest &witness_manifest,\n      rpc::RecoveryManifest *confirmed_manifest);\n""",
        """  /// Promotes a provisional holder only from a manifest obtained directly\n  /// from one of the task's compact witnesses. For an adaptive Recovery\n  /// Frontier member, the witness record is group-keyed and is translated back\n  /// into the requested task's member manifest before promotion.\n  ///\n  /// A newer witness-backed generation may also be adopted if this worker\n  /// remains in the succession list.\n  bool ConfirmProvisionalHolderFromWitness(\n      const TaskID &task_id,\n      const rpc::RecoveryManifest &witness_manifest,\n      rpc::RecoveryManifest *confirmed_manifest);\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """    rpc::RecoveryManifest provisional_manifest;\n    provisional_manifest.CopyFrom(latest_manifest);\n\n    // TEST ONLY: deterministically make the holder's own witness\n""",
        """    rpc::RecoveryManifest provisional_manifest;\n    provisional_manifest.CopyFrom(latest_manifest);\n\n    // Adaptive Recovery Frontier witnesses store the shared topology under\n    // the group/leader TaskID, while replay requests remain task-centric.\n    // Query the shared witness key, but keep requested_task_id so the manager\n    // can translate the verified group manifest back to this member.\n    const TaskID requested_task_id = TaskID::FromBinary(request.task_id());\n    rpc::RecoveryManifest witness_lookup_manifest;\n    witness_lookup_manifest.CopyFrom(provisional_manifest);\n    std::string expected_witness_task_id = request.task_id();\n    const auto frontier_membership =\n        recovery_succession_manager_->GetRecoveryFrontierMembership(\n            requested_task_id);\n    if (frontier_membership.has_value()) {\n      expected_witness_task_id = frontier_membership->group_id.Binary();\n      witness_lookup_manifest.set_task_id(expected_witness_task_id);\n    }\n\n    // TEST ONLY: deterministically make the holder's own witness\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """    LookupRecoveryManifestFromWitnesses(\n        provisional_manifest,\n        [this,\n        request = std::move(request),\n        reply,\n        send_reply_callback = std::move(send_reply_callback)](\n            std::optional<rpc::RecoveryManifest> witness_manifest) mutable {\n          if (!witness_manifest.has_value() ||\n              witness_manifest->task_id() != request.task_id() ||\n              !witness_manifest->has_version()) {\n""",
        """    LookupRecoveryManifestFromWitnesses(\n        witness_lookup_manifest,\n        [this,\n        request = std::move(request),\n        requested_task_id,\n        expected_witness_task_id = std::move(expected_witness_task_id),\n        reply,\n        send_reply_callback = std::move(send_reply_callback)](\n            std::optional<rpc::RecoveryManifest> witness_manifest) mutable {\n          if (!witness_manifest.has_value() ||\n              witness_manifest->task_id() != expected_witness_task_id ||\n              !witness_manifest->has_version()) {\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """          if (!recovery_succession_manager_\n                  ->ConfirmProvisionalHolderFromWitness(\n                      witness_manifest.value(),\n                      &confirmed_manifest)) {\n""",
        """          if (!recovery_succession_manager_\n                  ->ConfirmProvisionalHolderFromWitness(\n                      requested_task_id,\n                      witness_manifest.value(),\n                      &confirmed_manifest)) {\n""",
    )

    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """bool RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(\n    const rpc::RecoveryManifest &witness_manifest,\n    rpc::RecoveryManifest *confirmed_manifest) {\n  if (confirmed_manifest == nullptr ||\n      witness_manifest.task_id().size() != TaskID::Size() ||\n      !witness_manifest.has_version() ||\n      witness_manifest.tombstoned()) {\n    return false;\n  }\n\n  const TaskID task_id =\n      TaskID::FromBinary(witness_manifest.task_id());\n\n  absl::MutexLock lock(&mutex_);\n\n  const auto task_it = task_states_.find(task_id);\n\n  if (task_it == task_states_.end() ||\n      !task_it->second.task_spec.has_value()) {\n    return false;\n  }\n\n  TaskRecoveryState &state = task_it->second;\n\n  if (state.manifest.task_id() != witness_manifest.task_id() ||\n      state.manifest.tombstoned() ||\n      !ContainsWorker(witness_manifest, self_address_)) {\n    return false;\n  }\n\n\n  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&\n      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {\n    // Patch 4M-CERT witness set promotion.  Presence in a directly queried\n    // witness's merged set is the durability proof; rank/prefix is irrelevant.\n    const bool installed_provisional =\n        !state.provisional_reservation_id.empty() &&\n        ContainsWorker(state.manifest, self_address_);\n    const bool piggyback_provisional = state.provisional_piggyback_task_spec;\n    if (!state.manifest_committed &&\n        !installed_provisional && !piggyback_provisional) {\n      return false;\n    }\n\n    rpc::RecoveryManifest merged;\n    merged.CopyFrom(state.manifest);\n    if (!MergeRecoveryHolderSets(witness_manifest, &merged) ||\n        !ContainsWorker(merged, self_address_)) {\n      return false;\n    }\n    UpdateManifestForTaskLocked(task_id, merged, true);\n    candidate_reports_sent_.insert(task_id);\n    confirmed_manifest->CopyFrom(task_states_[task_id].manifest);\n    return true;\n  }\n\n  const bool installed_provisional =\n      !state.provisional_reservation_id.empty() &&\n      ContainsWorker(state.manifest, self_address_);\n  const bool piggyback_provisional =\n      state.provisional_piggyback_task_spec;\n\n  // A normal committed holder must already appear in its local manifest.\n  // Patch 4F is intentionally different only while provisional: H1 initially\n  // has [A] locally, and may promote only if a directly fetched witness\n  // manifest contains this worker.\n  if (state.manifest_committed &&\n      !ContainsWorker(state.manifest, self_address_)) {\n    return false;\n  }\n\n  if (!state.manifest_committed &&\n      !installed_provisional &&\n      !piggyback_provisional) {\n    return false;\n  }\n\n  const int comparison =\n      CompareManifestVersions(witness_manifest, state.manifest);\n\n  if (comparison < 0) {\n    if (!state.manifest_committed) {\n      return false;\n    }\n\n    confirmed_manifest->CopyFrom(state.manifest);\n    return true;\n  }\n\n  if (comparison == 0 &&\n      witness_manifest.SerializeAsString() != state.manifest.SerializeAsString()) {\n    return false;\n  }\n\n  if (comparison > 0 || !state.manifest_committed) {\n    UpdateManifestForTaskLocked(task_id, witness_manifest, true);\n  }\n\n  candidate_reports_sent_.insert(task_id);\n  confirmed_manifest->CopyFrom(state.manifest);\n  return true;\n}\n""",
        """bool RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(\n    const TaskID &task_id,\n    const rpc::RecoveryManifest &witness_manifest,\n    rpc::RecoveryManifest *confirmed_manifest) {\n  if (task_id.IsNil() || confirmed_manifest == nullptr ||\n      witness_manifest.task_id().size() != TaskID::Size() ||\n      !witness_manifest.has_version() ||\n      witness_manifest.tombstoned()) {\n    return false;\n  }\n\n  absl::MutexLock lock(&mutex_);\n\n  // Ordinary Succession stores one witness record per task. Adaptive Frontier\n  // Succession stores the shared topology under the group/leader TaskID. The\n  // holder imported the Frontier snapshot before the owner published that\n  // witness generation, so a matching witness-backed group manifest can be\n  // translated safely into the requested member's task-local manifest.\n  rpc::RecoveryManifest task_witness_manifest;\n  task_witness_manifest.CopyFrom(witness_manifest);\n\n  if (recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n          recovery_frontier_planner_.get())) {\n    const auto membership = recovery_frontier_planner_->FindTask(task_id);\n    if (membership.has_value()) {\n      if (witness_manifest.task_id() != membership->group_id.Binary()) {\n        return false;\n      }\n\n      const RecoveryFrontierGroup *group =\n          recovery_frontier_planner_->GetGroup(membership->group_id);\n      if (group == nullptr ||\n          membership->member_index >= group->Members().size()) {\n        return false;\n      }\n\n      const RecoveryFrontierMember &member =\n          group->Members()[membership->member_index];\n      if (member.task_id != task_id) {\n        return false;\n      }\n\n      task_witness_manifest =\n          recovery_succession_internal::BuildFrontierMemberManifest(\n              witness_manifest, member);\n    } else if (witness_manifest.task_id() != task_id.Binary()) {\n      return false;\n    }\n  } else if (witness_manifest.task_id() != task_id.Binary()) {\n    return false;\n  }\n\n  const auto task_it = task_states_.find(task_id);\n\n  if (task_it == task_states_.end() ||\n      !task_it->second.task_spec.has_value()) {\n    return false;\n  }\n\n  TaskRecoveryState &state = task_it->second;\n\n  if (state.manifest.task_id() != task_id.Binary() ||\n      task_witness_manifest.task_id() != task_id.Binary() ||\n      state.manifest.tombstoned() ||\n      !ContainsWorker(task_witness_manifest, self_address_)) {\n    return false;\n  }\n\n\n  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&\n      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {\n    // Patch 4M-CERT witness set promotion. Presence in a directly queried\n    // witness's merged set is the durability proof; rank/prefix is irrelevant.\n    const bool installed_provisional =\n        !state.provisional_reservation_id.empty() &&\n        ContainsWorker(state.manifest, self_address_);\n    const bool piggyback_provisional = state.provisional_piggyback_task_spec;\n    if (!state.manifest_committed &&\n        !installed_provisional && !piggyback_provisional) {\n      return false;\n    }\n\n    rpc::RecoveryManifest merged;\n    merged.CopyFrom(state.manifest);\n    if (!MergeRecoveryHolderSets(task_witness_manifest, &merged) ||\n        !ContainsWorker(merged, self_address_)) {\n      return false;\n    }\n    UpdateManifestForTaskLocked(task_id, merged, true);\n    candidate_reports_sent_.insert(task_id);\n    confirmed_manifest->CopyFrom(task_states_[task_id].manifest);\n    return true;\n  }\n\n  const bool installed_provisional =\n      !state.provisional_reservation_id.empty() &&\n      ContainsWorker(state.manifest, self_address_);\n  const bool piggyback_provisional =\n      state.provisional_piggyback_task_spec;\n\n  // A normal committed holder must already appear in its local manifest.\n  // Patch 4F is intentionally different only while provisional: H1 initially\n  // has [A] locally, and may promote only if a directly fetched witness\n  // manifest contains this worker.\n  if (state.manifest_committed &&\n      !ContainsWorker(state.manifest, self_address_)) {\n    return false;\n  }\n\n  if (!state.manifest_committed &&\n      !installed_provisional &&\n      !piggyback_provisional) {\n    return false;\n  }\n\n  const int comparison =\n      CompareManifestVersions(task_witness_manifest, state.manifest);\n\n  if (comparison < 0) {\n    if (!state.manifest_committed) {\n      return false;\n    }\n\n    confirmed_manifest->CopyFrom(state.manifest);\n    return true;\n  }\n\n  if (comparison == 0 &&\n      task_witness_manifest.SerializeAsString() !=\n          state.manifest.SerializeAsString()) {\n    return false;\n  }\n\n  if (comparison > 0 || !state.manifest_committed) {\n    UpdateManifestForTaskLocked(task_id, task_witness_manifest, true);\n  }\n\n  candidate_reports_sent_.insert(task_id);\n  confirmed_manifest->CopyFrom(state.manifest);\n  return true;\n}\n""",
    )

    print("\nFrontier + Succession provisional witness recovery fix applied.")
    print(
        "Review with: git diff --check && git diff -- "
        "src/ray/core_worker/recovery_succession_manager.h "
        "src/ray/core_worker/recovery_succession_manager.cc "
        "src/ray/core_worker/core_worker.cc"
    )
    print("Then rebuild Ray and rerun Benchmark 54.")


if __name__ == "__main__":
    main()
