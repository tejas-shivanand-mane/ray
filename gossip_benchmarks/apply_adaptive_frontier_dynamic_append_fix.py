#!/usr/bin/env python3
"""Apply adaptive Recovery Frontier dynamic-suffix propagation.

Benchmark 55 exposes the pre-fix behavior:

    phase 1: Frontier[T1,T2] -> H1,H2
    phase 2: T3,T4           -> NEW H1,H2 topology

The cause is that the first adaptive holder admission permanently seals the
Frontier.  This patch keeps the group open up to K and gives the already
admitted Succession holders an append-only replay-recipe update path.

Design:
  * Stage the initial Frontier prefix at first adaptive holder admission, but do
    not SealGroup(). Every initial holder receives exactly that same prefix.
  * Commit the owner planner's initial prefix only after the shared Succession
    topology reaches its target holder count.
  * Once the topology is established, hide later members from ObjectRef
    metadata until their replay recipes are ACKed by every existing holder.
  * Reuse CommitRecoveryManifest as the control RPC, extended with an optional
    RecoveryFrontierAppend. This is recipe propagation, not holder admission.
  * Holder-side ApplyCommittedAppend enforces contiguous generation/member
    invariants and installs only the new TaskSpecs.
  * Owner commits the append prefix only after every H1..HR ACKs it.

The patch is deliberately correctness-first. Dynamic append publication is
serialized on the owner and synchronous with the export that first needs the
new member, so an unprotected appended member can never escape. Fixed-R
Recovery Frontier and ordinary Succession keep their existing paths.

Run this after the Benchmark-54 provisional-witness patch. The replacements
below do not overlap that patch's edited blocks.
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


def insert_before_once(path: str, marker: str, addition: str) -> None:
    target = ROOT / path
    text = target.read_text()
    count = text.count(marker)
    if count != 1:
        raise RuntimeError(
            f"Expected exactly one insertion marker in {path}, found {count}. "
            "Refusing to modify the checkout."
        )
    target.write_text(text.replace(marker, addition + marker, 1))
    print(f"patched {path}")


def main() -> None:
    # ------------------------------------------------------------------
    # Frontier append serialization helper.
    # ------------------------------------------------------------------
    insert_before_once(
        "src/ray/core_worker/recovery_succession_manager_internal.h",
        """inline bool BuildFrontierSuccessionSnapshot(const RecoveryFrontierGroup &group,\n""",
        """inline bool BuildFrontierSuccessionAppend(\n    const RecoveryFrontierAppendBatch &batch,\n    rpc::RecoveryFrontierAppend *append) {\n  if (append == nullptr || batch.group_id.IsNil() ||\n      batch.members.empty() ||\n      batch.end_member_index <= batch.begin_member_index ||\n      batch.members.size() != static_cast<size_t>(\n          batch.end_member_index - batch.begin_member_index)) {\n    return false;\n  }\n\n  append->Clear();\n  append->set_group_id(batch.group_id.Binary());\n  append->set_base_generation(batch.base_generation);\n  append->set_generation(batch.generation);\n  append->set_begin_member_index(batch.begin_member_index);\n  append->set_end_member_index(batch.end_member_index);\n\n  for (const RecoveryFrontierMember &member : batch.members) {\n    if (member.task_spec == nullptr ||\n        member.task_id.IsNil() ||\n        member.task_spec->task_id() != member.task_id.Binary()) {\n      append->Clear();\n      return false;\n    }\n\n    rpc::RecoveryFrontierMemberRecord *record = append->add_members();\n    record->set_task_id(member.task_id.Binary());\n    record->set_member_index(member.member_index);\n    record->set_first_group_return_index(member.first_group_return_index);\n    record->set_num_returns(member.num_returns);\n    record->mutable_task_spec()->CopyFrom(*member.task_spec);\n  }\n  return true;\n}\n\n""",
    )

    # ------------------------------------------------------------------
    # Manager public API + owner-side initial-prefix cache.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/recovery_succession_manager.h",
        """  bool CommitRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n  bool AbortRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n""",
        """  bool CommitRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n  bool AbortRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n\n  /// Serialize one staged append using the shared holder wire format.\n  static bool BuildRecoveryFrontierAppendProto(\n      const RecoveryFrontierAppendBatch &batch,\n      rpc::RecoveryFrontierAppend *append);\n\n  /// Commit an adaptive Frontier recipe suffix on the owner after every\n  /// already-admitted Succession holder ACKed that exact append.\n  bool CommitAdaptiveRecoveryFrontierAppend(\n      const RecoveryFrontierAppendBatch &batch,\n      const rpc::RecoveryManifest &group_manifest);\n\n  /// Holder-side import of a committed adaptive Frontier recipe suffix.\n  /// The shared Succession topology is unchanged; only the newly appended\n  /// member TaskSpecs become replayable.\n  bool ApplyAdaptiveRecoveryFrontierAppend(\n      const rpc::RecoveryFrontierAppend &append,\n      const rpc::RecoveryManifest &group_manifest);\n""",
    )

    replace_once(
        "src/ray/core_worker/recovery_succession_manager.h",
        """  absl::flat_hash_map<TaskID, rpc::RecoveryManifest>\n      recovery_frontier_protection_manifests_ ABSL_GUARDED_BY(mutex_);\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n""",
        """  absl::flat_hash_map<TaskID, rpc::RecoveryManifest>\n      recovery_frontier_protection_manifests_ ABSL_GUARDED_BY(mutex_);\n\n  /// Exact initial recipe prefix frozen for the duration of adaptive holder\n  /// admission. The Frontier itself remains open so later owner tasks can join\n  /// it; H1..HR nevertheless all receive the same initial replay snapshot.\n  absl::flat_hash_map<TaskID, RecoveryFrontierAppendBatch>\n      adaptive_frontier_initial_append_batches_ ABSL_GUARDED_BY(mutex_);\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n""",
    )

    # ------------------------------------------------------------------
    # Manager implementation: adaptive append owner/holder APIs.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """bool RecoverySuccessionManager::AbortRecoveryFrontierAppend(\n    const RecoveryFrontierAppendBatch &batch) {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return false;\n  }\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(batch.group_id);\n  return group != nullptr && group->AbortAppend(batch);\n}\n\nbool RecoverySuccessionManager::ExtractRecoveryFrontierTaskForReturn(\n""",
        """bool RecoverySuccessionManager::AbortRecoveryFrontierAppend(\n    const RecoveryFrontierAppendBatch &batch) {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return false;\n  }\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(batch.group_id);\n  return group != nullptr && group->AbortAppend(batch);\n}\n\nbool RecoverySuccessionManager::BuildRecoveryFrontierAppendProto(\n    const RecoveryFrontierAppendBatch &batch,\n    rpc::RecoveryFrontierAppend *append) {\n  return recovery_succession_internal::BuildFrontierSuccessionAppend(\n      batch, append);\n}\n\nbool RecoverySuccessionManager::CommitAdaptiveRecoveryFrontierAppend(\n    const RecoveryFrontierAppendBatch &batch,\n    const rpc::RecoveryManifest &group_manifest) {\n  if (batch.group_id.IsNil() ||\n      group_manifest.task_id() != batch.group_id.Binary() ||\n      !group_manifest.frozen() ||\n      !ContainsWorker(group_manifest, self_address_)) {\n    return false;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  if (!recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n          recovery_frontier_planner_.get())) {\n    return false;\n  }\n\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(batch.group_id);\n  if (group == nullptr || !group->CommitAppend(batch)) {\n    return false;\n  }\n\n  recovery_frontier_protection_manifests_[batch.group_id].CopyFrom(\n      group_manifest);\n\n  for (const RecoveryFrontierMember &member : batch.members) {\n    if (member.task_spec == nullptr ||\n        !group->IsTaskCommitted(member.task_id)) {\n      return false;\n    }\n    rpc::RecoveryManifest member_manifest =\n        recovery_succession_internal::BuildFrontierMemberManifest(\n            group_manifest, member);\n    UpdateManifestForTaskLocked(member.task_id, member_manifest, true);\n  }\n\n  return true;\n}\n\nbool RecoverySuccessionManager::ApplyAdaptiveRecoveryFrontierAppend(\n    const rpc::RecoveryFrontierAppend &append,\n    const rpc::RecoveryManifest &group_manifest) {\n  if (append.group_id().size() != TaskID::Size() ||\n      group_manifest.task_id() != append.group_id() ||\n      !group_manifest.frozen() ||\n      !ContainsWorker(group_manifest, self_address_) ||\n      append.members_size() <= 0) {\n    return false;\n  }\n\n  for (const rpc::RecoveryFrontierMemberRecord &record : append.members()) {\n    if (!record.has_task_spec() ||\n        record.task_spec().task_id() != record.task_id() ||\n        !IsEligibleTask(record.task_spec())) {\n      return false;\n    }\n  }\n\n  const TaskID group_id = TaskID::FromBinary(append.group_id());\n  absl::MutexLock lock(&mutex_);\n  if (!recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n          recovery_frontier_planner_.get()) ||\n      !recovery_frontier_planner_->ApplyCommittedAppend(append)) {\n    return false;\n  }\n\n  const RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetGroup(group_id);\n  if (group == nullptr ||\n      append.end_member_index() > group->MemberCount()) {\n    return false;\n  }\n\n  recovery_frontier_protection_manifests_[group_id].CopyFrom(group_manifest);\n\n  for (uint32_t index = append.begin_member_index();\n       index < append.end_member_index();\n       ++index) {\n    const RecoveryFrontierMember &member = group->Members()[index];\n    if (member.task_spec == nullptr || !IsEligibleTask(*member.task_spec)) {\n      return false;\n    }\n\n    rpc::RecoveryManifest member_manifest =\n        recovery_succession_internal::BuildFrontierMemberManifest(\n            group_manifest, member);\n\n    rpc::TaskSpec stored_task_spec;\n    stored_task_spec.CopyFrom(*member.task_spec);\n    ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);\n    stored_task_spec.mutable_recovery_manifest()->CopyFrom(member_manifest);\n\n    TaskRecoveryState &member_state = task_states_[member.task_id];\n    member_state.manifest.CopyFrom(member_manifest);\n    member_state.task_spec = std::move(stored_task_spec);\n    member_state.manifest_committed = true;\n    member_state.provisional_reservation_id.clear();\n    member_state.provisional_piggyback_task_spec = false;\n  }\n\n  candidate_reports_sent_.insert(group_id);\n  return true;\n}\n\nbool RecoverySuccessionManager::ExtractRecoveryFrontierTaskForReturn(\n""",
    )

    # ------------------------------------------------------------------
    # Initial adaptive admission: stage a fixed prefix, but do NOT seal group.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  const bool frontier_group_admission =\n      recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n          recovery_frontier_planner_.get()) &&\n      recovery_frontier_planner_->GetGroup(task_id) != nullptr;\n\n  if (frontier_group_admission) {\n    // First adaptive admission freezes the set of recipes in this initial\n    // composition slice. Later dynamic append propagation is implemented\n    // separately; a new owner task therefore opens a new Frontier after this.\n    RAY_CHECK(recovery_frontier_planner_->SealGroup(task_id));\n  }\n\n  const TaskRecoveryState &task_state = task_it->second;\n""",
        """  const bool frontier_group_admission =\n      recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n          recovery_frontier_planner_.get()) &&\n      recovery_frontier_planner_->GetGroup(task_id) != nullptr;\n\n  std::optional<RecoveryFrontierAppendBatch> frontier_install_batch;\n  if (frontier_group_admission) {\n    RecoveryFrontierGroup *group =\n        recovery_frontier_planner_->GetMutableGroup(task_id);\n    RAY_CHECK(group != nullptr);\n\n    // Freeze only the INITIAL RECIPE PREFIX, not the Frontier itself. This\n    // guarantees that every concurrently admitted H1..HR receives the exact\n    // same snapshot while later owner tasks remain free to join the group.\n    if (group->CommittedMemberCount() == 0) {\n      auto initial_it =\n          adaptive_frontier_initial_append_batches_.find(task_id);\n      if (initial_it ==\n          adaptive_frontier_initial_append_batches_.end()) {\n        auto staged = group->StageAppend();\n        if (!staged.has_value()) {\n          return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;\n        }\n        initial_it =\n            adaptive_frontier_initial_append_batches_\n                .emplace(task_id, std::move(staged.value()))\n                .first;\n      }\n      frontier_install_batch = initial_it->second;\n    }\n  }\n\n  const TaskRecoveryState &task_state = task_it->second;\n""",
    )

    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  if (frontier_group_admission) {\n    const RecoveryFrontierGroup *group =\n        recovery_frontier_planner_->GetGroup(task_id);\n    RAY_CHECK(group != nullptr);\n    rpc::RecoveryFrontierAppend snapshot;\n    if (!recovery_succession_internal::BuildFrontierSuccessionSnapshot(\n            *group, &snapshot)) {\n      EraseHolderReservationLocked(reservation_id);\n      return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;\n    }\n    recovery_succession_internal::PutFrontierSuccessionAppendCapsule(\n        snapshot, &plan->task_spec);\n  }\n""",
        """  if (frontier_group_admission) {\n    const RecoveryFrontierGroup *group =\n        recovery_frontier_planner_->GetGroup(task_id);\n    RAY_CHECK(group != nullptr);\n    rpc::RecoveryFrontierAppend snapshot;\n    const bool built =\n        frontier_install_batch.has_value()\n            ? recovery_succession_internal::BuildFrontierSuccessionAppend(\n                  frontier_install_batch.value(), &snapshot)\n            : recovery_succession_internal::BuildFrontierSuccessionSnapshot(\n                  *group, &snapshot);\n    if (!built) {\n      EraseHolderReservationLocked(reservation_id);\n      return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;\n    }\n    recovery_succession_internal::PutFrontierSuccessionAppendCapsule(\n        snapshot, &plan->task_spec);\n  }\n""",
    )

    # ------------------------------------------------------------------
    # Holder-topology commit updates only the recipe prefix that every current
    # holder is known to have. Commit the staged initial prefix when R is full.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  auto update_committed_topology =\n      [this, &task_id](const rpc::RecoveryManifest &group_manifest) {\n        if (!recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n                recovery_frontier_planner_.get())) {\n          UpdateManifestForTaskLocked(task_id, group_manifest, true);\n          return;\n        }\n\n        const RecoveryFrontierGroup *group =\n            recovery_frontier_planner_->GetGroup(task_id);\n        if (group == nullptr) {\n          UpdateManifestForTaskLocked(task_id, group_manifest, true);\n          return;\n        }\n\n        recovery_frontier_protection_manifests_[task_id].CopyFrom(group_manifest);\n        for (const RecoveryFrontierMember &member : group->Members()) {\n          rpc::RecoveryManifest member_manifest =\n              recovery_succession_internal::BuildFrontierMemberManifest(\n                  group_manifest, member);\n          UpdateManifestForTaskLocked(member.task_id, member_manifest, true);\n        }\n      };\n\n\n""",
        """  auto committed_frontier_member_limit =\n      [this, &task_id](const RecoveryFrontierGroup &group) {\n        uint32_t limit = group.CommittedMemberCount();\n        const auto initial_it =\n            adaptive_frontier_initial_append_batches_.find(task_id);\n        if (initial_it != adaptive_frontier_initial_append_batches_.end()) {\n          limit = std::max(\n              limit, initial_it->second.end_member_index);\n        }\n        return limit;\n      };\n\n  auto update_committed_topology =\n      [this, &task_id, &committed_frontier_member_limit](\n          const rpc::RecoveryManifest &group_manifest) {\n        if (!recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n                recovery_frontier_planner_.get())) {\n          UpdateManifestForTaskLocked(task_id, group_manifest, true);\n          return;\n        }\n\n        const RecoveryFrontierGroup *group =\n            recovery_frontier_planner_->GetGroup(task_id);\n        if (group == nullptr) {\n          UpdateManifestForTaskLocked(task_id, group_manifest, true);\n          return;\n        }\n\n        recovery_frontier_protection_manifests_[task_id].CopyFrom(group_manifest);\n        const uint32_t member_limit =\n            committed_frontier_member_limit(*group);\n        for (const RecoveryFrontierMember &member : group->Members()) {\n          if (member.member_index >= member_limit) {\n            break;\n          }\n          rpc::RecoveryManifest member_manifest =\n              recovery_succession_internal::BuildFrontierMemberManifest(\n                  group_manifest, member);\n          UpdateManifestForTaskLocked(member.task_id, member_manifest, true);\n        }\n      };\n\n  auto commit_initial_frontier_prefix_if_ready =\n      [this, &task_id](const rpc::RecoveryManifest &group_manifest) {\n        if (!recovery_succession_internal::AdaptiveFrontierSuccessionEnabled(\n                recovery_frontier_planner_.get()) ||\n            !group_manifest.frozen()) {\n          return true;\n        }\n\n        const auto initial_it =\n            adaptive_frontier_initial_append_batches_.find(task_id);\n        if (initial_it ==\n            adaptive_frontier_initial_append_batches_.end()) {\n          return true;\n        }\n\n        RecoveryFrontierGroup *group =\n            recovery_frontier_planner_->GetMutableGroup(task_id);\n        if (group == nullptr ||\n            !group->CommitAppend(initial_it->second)) {\n          return false;\n        }\n        adaptive_frontier_initial_append_batches_.erase(initial_it);\n        return true;\n      };\n\n\n""",
    )

    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """    update_committed_topology(merged);\n\n    if (profiling_enabled_) {\n""",
        """    if (!commit_initial_frontier_prefix_if_ready(merged)) {\n      return false;\n    }\n    update_committed_topology(merged);\n\n    if (profiling_enabled_) {\n""",
    )

    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  update_committed_topology(proposed);\n\n  if (profiling_enabled_) {\n""",
        """  if (!commit_initial_frontier_prefix_if_ready(proposed)) {\n    return false;\n  }\n  update_committed_topology(proposed);\n\n  if (profiling_enabled_) {\n""",
    )

    # ------------------------------------------------------------------
    # Once an adaptive topology is fully established, uncommitted later
    # members are not capabilities yet. They become visible only after append.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/recovery_succession_manager.cc",
        """        if (group != nullptr && leader_it != task_states_.end() &&\n            !leader_it->second.manifest.task_id().empty() &&\n            membership->member_index < group->Members().size()) {\n          const RecoveryFrontierMember &member =\n              group->Members()[membership->member_index];\n""",
        """        if (group != nullptr && leader_it != task_states_.end() &&\n            !leader_it->second.manifest.task_id().empty() &&\n            membership->member_index < group->Members().size()) {\n          const auto protection_it =\n              recovery_frontier_protection_manifests_.find(\n                  membership->group_id);\n          const bool adaptive_topology_established =\n              protection_it !=\n                  recovery_frontier_protection_manifests_.end() &&\n              protection_it->second.frozen();\n          if (require_frontier_commit &&\n              adaptive_topology_established &&\n              !group->IsTaskCommitted(task_id)) {\n            return false;\n          }\n\n          const RecoveryFrontierMember &member =\n              group->Members()[membership->member_index];\n""",
    )

    # ------------------------------------------------------------------
    # Proto: reuse CommitRecoveryManifest for recipe suffix ACKs.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/protobuf/core_worker.proto",
        """import \"src/ray/protobuf/common.proto\";\nimport \"src/ray/protobuf/pubsub.proto\";\n""",
        """import \"src/ray/protobuf/common.proto\";\nimport \"src/ray/protobuf/pubsub.proto\";\nimport \"src/ray/protobuf/frontier/recovery_frontier.proto\";\n""",
    )

    replace_once(
        "src/ray/protobuf/core_worker.proto",
        """message CommitRecoveryManifestRequest {\n  RecoveryManifest manifest = 1;\n}\n\nmessage CommitRecoveryManifestReply {}\n""",
        """message CommitRecoveryManifestRequest {\n  RecoveryManifest manifest = 1;\n\n  // Optional adaptive Recovery Frontier recipe suffix. When present, the\n  // manifest carries the already-committed shared Succession topology and the\n  // append carries only new replay recipes for that topology.\n  RecoveryFrontierAppend frontier_append = 2;\n}\n\nmessage CommitRecoveryManifestReply {\n  bool applied = 1;\n}\n""",
    )

    replace_once(
        "src/ray/protobuf/BUILD.bazel",
        """    deps = [\n        \":common_proto\",\n        \":gcs_service_proto\",\n        \":pubsub_proto\",\n    ],\n)\n""",
        """    deps = [\n        \":common_proto\",\n        \":gcs_service_proto\",\n        \":pubsub_proto\",\n        \"//src/ray/protobuf/frontier:recovery_frontier_proto\",\n    ],\n)\n""",
    )

    # ------------------------------------------------------------------
    # CoreWorker declaration + owner serialization lock.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/core_worker.h",
        """  void PublishRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n\n\n  // Patch 4M-CERT delta publication.\n""",
        """  void PublishRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n\n  /// Publish every currently uncommitted adaptive Frontier recipe suffix to\n  /// the already-admitted H1..HR topology. Returns only after all holders ACK\n  /// and the owner planner advances the same committed prefix.\n  bool PublishAdaptiveRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n\n\n  // Patch 4M-CERT delta publication.\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.h",
        """  mutable absl::flat_hash_map<\n      TaskID, std::shared_ptr<RecoveryFrontierPublicationState>>\n      recovery_frontier_publications_;\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n""",
        """  mutable absl::flat_hash_map<\n      TaskID, std::shared_ptr<RecoveryFrontierPublicationState>>\n      recovery_frontier_publications_;\n\n  // Correctness-first serialization for adaptive recipe suffix publication.\n  // Topology admission remains independently pipelined; this lock covers only\n  // appends to an already-frozen H1..HR topology.\n  mutable std::mutex recovery_adaptive_frontier_append_mutex_;\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n""",
    )

    # ------------------------------------------------------------------
    # CoreWorker lazy export: detect established adaptive group and push the
    # recipe suffix before metadata can escape.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """  const bool recovery_frontier_grouping_enabled =\n      recovery_frontier_enabled &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n\n  std::optional<RecoveryFrontierMembership> frontier_membership;\n""",
        """  const bool recovery_frontier_grouping_enabled =\n      recovery_frontier_enabled &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n  const bool adaptive_recovery_frontier_grouping_enabled =\n      !recovery_witness_holder_baseline_enabled_ &&\n      recovery_succession_manager_->RecoveryFrontierEnabled() &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n\n  std::optional<RecoveryFrontierMembership> frontier_membership;\n""",
    )

    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """  if (register_start_ns != 0 && initialized_now) {\n    recovery_succession_manager_->RecordRegisterOwnedTaskLatency(\n        RecoveryProfileNowNs() - register_start_ns);\n  }\n\n  if (recovery_witness_holder_baseline_enabled_) {\n""",
        """  if (register_start_ns != 0 && initialized_now) {\n    recovery_succession_manager_->RecordRegisterOwnedTaskLatency(\n        RecoveryProfileNowNs() - register_start_ns);\n  }\n\n  if (adaptive_recovery_frontier_grouping_enabled) {\n    const auto adaptive_membership =\n        recovery_succession_manager_->GetRecoveryFrontierMembership(\n            task_id);\n    if (adaptive_membership.has_value()) {\n      rpc::RecoveryManifest adaptive_protection_manifest;\n      const bool has_shared_topology =\n          recovery_succession_manager_\n              ->GetRecoveryFrontierProtectionManifest(\n                  adaptive_membership->group_id,\n                  &adaptive_protection_manifest) &&\n          adaptive_protection_manifest.frozen();\n\n      if (has_shared_topology &&\n          recovery_succession_manager_\n              ->RecoveryFrontierGroupHasUncommittedMembers(\n                  adaptive_membership->group_id)) {\n        RAY_CHECK(PublishAdaptiveRecoveryFrontierGroup(\n            adaptive_membership->group_id,\n            adaptive_protection_manifest))\n            << \"Failed to make adaptive Recovery Frontier suffix durable for \"\n            << adaptive_membership->group_id;\n      }\n    }\n  }\n\n  if (recovery_witness_holder_baseline_enabled_) {\n""",
    )

    # ------------------------------------------------------------------
    # CoreWorker adaptive append publisher. Reuse existing control client/RPC.
    # ------------------------------------------------------------------
    insert_before_once(
        "src/ray/core_worker/core_worker.cc",
        """void CoreWorker::SendRecoveryHolderRollback(\n""",
        """bool CoreWorker::PublishAdaptiveRecoveryFrontierGroup(\n    const TaskID &group_id,\n    const rpc::RecoveryManifest &protection_manifest) const {\n  if (!recovery_succession_enabled_ ||\n      recovery_witness_holder_baseline_enabled_ ||\n      recovery_succession_manager_ == nullptr ||\n      group_id.IsNil() ||\n      protection_manifest.task_id() != group_id.Binary() ||\n      !protection_manifest.frozen()) {\n    return false;\n  }\n\n  std::lock_guard<std::mutex> publish_lock(\n      recovery_adaptive_frontier_append_mutex_);\n\n  while (recovery_succession_manager_\n             ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {\n    auto staged =\n        recovery_succession_manager_->StageRecoveryFrontierAppend(\n            group_id);\n    if (!staged.has_value()) {\n      return false;\n    }\n\n    rpc::RecoveryFrontierAppend append;\n    if (!RecoverySuccessionManager::BuildRecoveryFrontierAppendProto(\n            staged.value(), &append)) {\n      recovery_succession_manager_->AbortRecoveryFrontierAppend(\n          staged.value());\n      return false;\n    }\n\n    std::vector<rpc::Address> holders;\n    for (const rpc::RecoveryHolder &holder :\n         protection_manifest.succession()) {\n      if (holder.rank() == 0) {\n        continue;\n      }\n      if (holder.address().worker_id().empty()) {\n        recovery_succession_manager_->AbortRecoveryFrontierAppend(\n            staged.value());\n        return false;\n      }\n      holders.push_back(holder.address());\n    }\n\n    if (holders.size() !=\n        static_cast<size_t>(\n            protection_manifest.target_holder_count())) {\n      recovery_succession_manager_->AbortRecoveryFrontierAppend(\n          staged.value());\n      return false;\n    }\n\n    auto remaining =\n        std::make_shared<std::atomic<size_t>>(holders.size());\n    auto all_applied =\n        std::make_shared<std::atomic<bool>>(true);\n    auto done = std::make_shared<std::promise<void>>();\n    auto future = done->get_future();\n\n    for (const rpc::Address &holder_address : holders) {\n      rpc::CommitRecoveryManifestRequest request;\n      request.mutable_manifest()->CopyFrom(protection_manifest);\n      request.mutable_frontier_append()->CopyFrom(append);\n\n      auto client =\n          core_worker_client_pool_->GetOrConnect(holder_address);\n      client->CommitRecoveryManifest(\n          std::move(request),\n          [remaining, all_applied, done](\n              const Status &status,\n              rpc::CommitRecoveryManifestReply &&reply) {\n            if (!status.ok() || !reply.applied()) {\n              all_applied->store(false, std::memory_order_release);\n            }\n            if (remaining->fetch_sub(\n                    1, std::memory_order_acq_rel) == 1) {\n              done->set_value();\n            }\n          });\n    }\n\n    future.wait();\n\n    if (!all_applied->load(std::memory_order_acquire)) {\n      // Keep the previously acknowledged prefix authoritative. This first\n      // dynamic implementation surfaces the failed export rather than\n      // advertising a member that is missing at any holder.\n      recovery_succession_manager_->AbortRecoveryFrontierAppend(\n          staged.value());\n      return false;\n    }\n\n    if (!recovery_succession_manager_\n             ->CommitAdaptiveRecoveryFrontierAppend(\n                 staged.value(), protection_manifest)) {\n      return false;\n    }\n\n    RAY_LOG(INFO).WithField(group_id)\n        << \"Committed adaptive Recovery Frontier recipe append generation \"\n        << staged->generation << \" members [\"\n        << staged->begin_member_index << \",\"\n        << staged->end_member_index << \")\";\n  }\n\n  return true;\n}\n\n\n""",
    )

    # ------------------------------------------------------------------
    # Holder RPC handler: distinguish ordinary manifest commit/rollback from a
    # Frontier recipe append and ACK exact application.
    # ------------------------------------------------------------------
    replace_once(
        "src/ray/core_worker/core_worker.cc",
        """void CoreWorker::HandleCommitRecoveryManifest(\n    rpc::CommitRecoveryManifestRequest request,\n    rpc::CommitRecoveryManifestReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  static_cast<void>(reply);\n\n  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr &&\n      request.has_manifest()) {\n    const bool applied =\n        recovery_succession_manager_->ApplyCommittedManifest(request.manifest());\n\n    if (applied) {\n      RAY_LOG(INFO).WithField(TaskID::FromBinary(request.manifest().task_id()))\n          << \"Applied committed recovery \"\n             \"succession manifest\";\n    }\n  }\n\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
        """void CoreWorker::HandleCommitRecoveryManifest(\n    rpc::CommitRecoveryManifestRequest request,\n    rpc::CommitRecoveryManifestReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  bool applied = false;\n\n  if (recovery_succession_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      request.has_manifest()) {\n    if (request.has_frontier_append()) {\n      applied =\n          recovery_succession_manager_\n              ->ApplyAdaptiveRecoveryFrontierAppend(\n                  request.frontier_append(),\n                  request.manifest());\n      if (applied) {\n        RAY_LOG(INFO)\n            .WithField(\n                TaskID::FromBinary(\n                    request.frontier_append().group_id()))\n            << \"Applied adaptive Recovery Frontier recipe append generation \"\n            << request.frontier_append().generation();\n      }\n    } else {\n      applied =\n          recovery_succession_manager_->ApplyCommittedManifest(\n              request.manifest());\n\n      if (applied) {\n        RAY_LOG(INFO)\n            .WithField(\n                TaskID::FromBinary(\n                    request.manifest().task_id()))\n            << \"Applied committed recovery succession manifest\";\n      }\n    }\n  }\n\n  reply->set_applied(applied);\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
    )

    print("\nAdaptive Recovery Frontier dynamic append fix applied.")
    print(
        "Review with: git diff --check && git diff -- "
        "src/ray/protobuf/core_worker.proto src/ray/protobuf/BUILD.bazel "
        "src/ray/core_worker/recovery_succession_manager_internal.h "
        "src/ray/core_worker/recovery_succession_manager.h "
        "src/ray/core_worker/recovery_succession_manager.cc "
        "src/ray/core_worker/core_worker.h src/ray/core_worker/core_worker.cc"
    )
    print("Then rebuild Ray and rerun Benchmark 55.")


if __name__ == "__main__":
    main()
