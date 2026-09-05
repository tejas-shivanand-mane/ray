// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/core_worker/recovery_succession_manager.h"

// recovery_succession_manager.cc still contains the pre-Frontier compact
// metadata encoder while the adaptive Frontier path uses the extended encoder
// below, which preserves the group-membership marker. Keep -Werror enabled and
// mark only that transitional TU-local definition as intentionally unused.
// This declaration denotes the same unnamed-namespace function when this header
// is included by recovery_succession_manager.cc.
namespace ray::core {
namespace {
[[maybe_unused]] bool WriteCompactTaskArgumentRecoveryMetadata(
    const rpc::RecoveryObjectMetadata &source,
    const rpc::RecoveryManifest &manifest,
    const rpc::Address &object_owner,
    rpc::RecoveryObjectMetadata *out);
}  // namespace
}  // namespace ray::core

namespace ray::core::recovery_succession_internal {

inline const rpc::RecoveryHolder *FindHolderByRank(
    const rpc::RecoveryManifest &manifest, uint32_t rank) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == rank) {
      return &holder;
    }
  }
  return nullptr;
}

inline bool SameWorker(const rpc::Address &left, const rpc::Address &right) {
  return !left.worker_id().empty() && left.worker_id() == right.worker_id();
}

inline bool ContainsWorker(const rpc::RecoveryManifest &manifest,
                           const rpc::Address &address) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (SameWorker(holder.address(), address)) {
      return true;
    }
  }
  return false;
}

inline int CompareManifestVersions(const rpc::RecoveryManifest &left,
                                   const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }
  if (left.version().generation() > right.version().generation()) {
    return 1;
  }
  return 0;
}

inline bool MergeRecoveryHolderSets(const rpc::RecoveryManifest &incoming,
                                    rpc::RecoveryManifest *state) {
  if (state == nullptr || incoming.task_id().empty() || !incoming.has_version()) {
    return false;
  }
  if (state->task_id().empty()) {
    state->CopyFrom(incoming);
    return true;
  }
  if (state->task_id() != incoming.task_id() || !state->has_version()) {
    return false;
  }

  if (incoming.tombstoned()) {
    if (incoming.version().generation() >= state->version().generation()) {
      state->CopyFrom(incoming);
      return true;
    }
    return false;
  }
  if (state->tombstoned()) {
    return true;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(*state, 0);
  if (owner == nullptr) {
    owner = FindHolderByRank(incoming, 0);
  }
  if (owner == nullptr) {
    return false;
  }

  std::vector<rpc::RecoveryHolder> holders;
  auto add_unique = [&holders, owner](const rpc::RecoveryManifest &manifest) {
    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0 || SameWorker(holder.address(), owner->address())) {
        continue;
      }
      bool duplicate = false;
      for (const rpc::RecoveryHolder &existing : holders) {
        if (SameWorker(existing.address(), holder.address())) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        holders.push_back(holder);
      }
    }
  };

  add_unique(*state);
  add_unique(incoming);
  if (holders.size() > static_cast<size_t>(state->target_holder_count())) {
    return false;
  }

  std::sort(holders.begin(), holders.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  rpc::RecoveryHolder owner_copy;
  owner_copy.CopyFrom(*owner);
  state->clear_succession();
  rpc::RecoveryHolder *out_owner = state->add_succession();
  out_owner->CopyFrom(owner_copy);
  out_owner->set_rank(0);
  for (size_t i = 0; i < holders.size(); ++i) {
    rpc::RecoveryHolder *out = state->add_succession();
    out->CopyFrom(holders[i]);
    out->set_rank(static_cast<uint32_t>(i + 1));
  }

  state->mutable_version()->set_generation(
      std::max(state->version().generation(), incoming.version().generation()));
  state->set_recovery_attempt(
      std::max(state->recovery_attempt(), incoming.recovery_attempt()));
  state->set_frozen(static_cast<uint32_t>(holders.size()) >=
                    state->target_holder_count());
  return true;
}

inline bool MergeConfirmedHolder(const rpc::RecoveryHolder &candidate,
                                 uint64_t certificate_generation,
                                 rpc::RecoveryManifest *manifest) {
  if (manifest == nullptr || manifest->task_id().empty() || manifest->tombstoned() ||
      candidate.address().worker_id().empty()) {
    return false;
  }

  rpc::RecoveryManifest delta;
  delta.CopyFrom(*manifest);
  delta.clear_succession();

  const rpc::RecoveryHolder *owner = FindHolderByRank(*manifest, 0);
  if (owner == nullptr) {
    return false;
  }
  delta.add_succession()->CopyFrom(*owner);
  rpc::RecoveryHolder *holder = delta.add_succession();
  holder->CopyFrom(candidate);
  holder->set_rank(1);
  delta.mutable_version()->set_generation(certificate_generation);
  return MergeRecoveryHolderSets(delta, manifest);
}

inline bool AdaptiveFrontierSuccessionEnabled(
    const RecoveryFrontierPlanner *planner) {
  return RayConfig::instance().enable_recovery_succession() &&
         !RayConfig::instance().enable_recovery_witness_holder_baseline() &&
         planner != nullptr && planner->GroupSize() > 1;
}

inline constexpr char kFrontierSuccessionMemberMarker[] =
    "RAY_FRONTIER_SUCCESSION_MEMBER_V1:";
inline constexpr char kFrontierSuccessionAppendMarker[] =
    "RAY_FRONTIER_SUCCESSION_APPEND_V1:";

inline std::string EncodeFrontierSuccessionMemberMarker(const TaskID &group_id) {
  return std::string(kFrontierSuccessionMemberMarker) + group_id.Binary();
}

inline bool ParseFrontierSuccessionMemberMarker(const std::string &payload,
                                                TaskID *group_id) {
  constexpr size_t prefix_size = sizeof(kFrontierSuccessionMemberMarker) - 1;
  if (group_id == nullptr ||
      payload.size() != prefix_size + TaskID::Size() ||
      payload.compare(0, prefix_size, kFrontierSuccessionMemberMarker) != 0) {
    return false;
  }
  *group_id = TaskID::FromBinary(payload.substr(prefix_size));
  return !group_id->IsNil();
}

inline std::string EncodeFrontierSuccessionAppend(
    const rpc::RecoveryFrontierAppend &append) {
  return std::string(kFrontierSuccessionAppendMarker) + append.SerializeAsString();
}

inline bool ParseFrontierSuccessionAppend(const std::string &payload,
                                          rpc::RecoveryFrontierAppend *append) {
  constexpr size_t prefix_size = sizeof(kFrontierSuccessionAppendMarker) - 1;
  if (append == nullptr || payload.size() <= prefix_size ||
      payload.compare(0, prefix_size, kFrontierSuccessionAppendMarker) != 0) {
    return false;
  }
  append->Clear();
  return append->ParseFromArray(payload.data() + prefix_size,
                                static_cast<int>(payload.size() - prefix_size));
}

inline void ClearFirstHolderPayloadUnlessFrontierMembership(
    rpc::RecoveryObjectMetadata *metadata) {
  if (metadata == nullptr || metadata->first_holder_task_spec().empty()) {
    return;
  }
  TaskID group_id;
  if (!ParseFrontierSuccessionMemberMarker(metadata->first_holder_task_spec(),
                                           &group_id)) {
    metadata->clear_first_holder_task_spec();
  }
}

inline void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec) {
  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      ClearFirstHolderPayloadUnlessFrontierMembership(
          arg.mutable_object_ref()->mutable_recovery_metadata());
    }
    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        ClearFirstHolderPayloadUnlessFrontierMembership(
            nested_ref.mutable_recovery_metadata());
      }
    }
  }

  for (rpc::RecoveryTaskArgumentMetadata &entry :
       *task_spec->mutable_recovery_argument_metadata()) {
    if (entry.has_recovery_metadata()) {
      ClearFirstHolderPayloadUnlessFrontierMembership(
          entry.mutable_recovery_metadata());
    }
  }
}

inline bool WriteCompactTaskArgumentRecoveryMetadata(
    const rpc::RecoveryObjectMetadata &source,
    const rpc::RecoveryManifest &manifest,
    const rpc::Address &object_owner,
    rpc::RecoveryObjectMetadata *out) {
  if (out == nullptr || source.task_id().empty() || manifest.task_id().empty() ||
      source.task_id() != manifest.task_id() || !manifest.has_version()) {
    return false;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(manifest, 0);
  if (owner == nullptr || object_owner.worker_id().empty() ||
      !SameWorker(owner->address(), object_owner)) {
    return false;
  }

  out->Clear();
  out->set_return_index(source.return_index());

  TaskID frontier_group_id;
  if (ParseFrontierSuccessionMemberMarker(source.first_holder_task_spec(),
                                          &frontier_group_id)) {
    out->set_first_holder_task_spec(source.first_holder_task_spec());
  }

  rpc::RecoveryObjectTransportManifest *compact = out->mutable_compact_manifest();
  compact->set_target_holder_count(manifest.target_holder_count());
  compact->set_witness_count(manifest.witness_count());
  compact->set_generation(manifest.version().generation());
  compact->set_frozen(manifest.frozen());
  compact->set_tombstoned(manifest.tombstoned());
  compact->set_recovery_attempt(manifest.recovery_attempt());
  compact->set_max_recovery_attempts(manifest.max_recovery_attempts());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    compact->add_witness_raylets()->CopyFrom(witness);
  }
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == 0) {
      continue;
    }
    if (holder.rank() !=
        static_cast<uint32_t>(compact->non_owner_holders_size() + 1)) {
      out->Clear();
      return false;
    }
    compact->add_non_owner_holders()->CopyFrom(holder.address());
  }
  return true;
}

inline bool ExpandTaskArgumentRecoveryMetadata(
    const rpc::ObjectReference &object_ref,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || object_ref.object_id().empty() ||
      !object_ref.has_recovery_metadata()) {
    return false;
  }

  const rpc::RecoveryObjectMetadata &transport = object_ref.recovery_metadata();
  if (!transport.task_id().empty() && transport.has_manifest()) {
    expanded->CopyFrom(transport);
    expanded->clear_compact_manifest();
    return true;
  }

  if (!transport.has_compact_manifest() || !object_ref.has_owner_address() ||
      object_ref.owner_address().worker_id().empty() ||
      object_ref.object_id().size() != ObjectID::Size()) {
    return false;
  }

  const rpc::RecoveryObjectTransportManifest &compact = transport.compact_manifest();
  if (compact.generation() == 0) {
    return false;
  }

  const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
  const TaskID task_id = object_id.TaskId();

  expanded->Clear();
  expanded->set_task_id(task_id.Binary());
  expanded->set_return_index(transport.return_index());
  if (!transport.first_holder_task_spec().empty()) {
    expanded->set_first_holder_task_spec(transport.first_holder_task_spec());
  }

  rpc::RecoveryManifest *manifest = expanded->mutable_manifest();
  manifest->set_task_id(task_id.Binary());
  manifest->set_job_id(task_id.JobId().Binary());
  manifest->set_target_holder_count(compact.target_holder_count());
  manifest->set_witness_count(compact.witness_count());
  manifest->mutable_version()->set_generation(compact.generation());
  manifest->set_frozen(compact.frozen());
  manifest->set_tombstoned(compact.tombstoned());
  manifest->set_recovery_attempt(compact.recovery_attempt());
  manifest->set_max_recovery_attempts(compact.max_recovery_attempts());

  for (const rpc::Address &witness : compact.witness_raylets()) {
    manifest->add_witness_raylets()->CopyFrom(witness);
  }

  rpc::RecoveryHolder *owner = manifest->add_succession();
  owner->mutable_address()->CopyFrom(object_ref.owner_address());
  owner->set_rank(0);
  owner->set_failure_domain_id(object_ref.owner_address().node_id());

  for (int i = 0; i < compact.non_owner_holders_size(); ++i) {
    const rpc::Address &address = compact.non_owner_holders(i);
    if (address.worker_id().empty()) {
      expanded->Clear();
      return false;
    }
    rpc::RecoveryHolder *holder = manifest->add_succession();
    holder->mutable_address()->CopyFrom(address);
    holder->set_rank(static_cast<uint32_t>(i + 1));
    holder->set_failure_domain_id(address.node_id());
  }
  return true;
}

inline bool ExpandTaskSidecarRecoveryMetadata(
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

inline rpc::RecoveryManifest BuildFrontierMemberManifest(
    const rpc::RecoveryManifest &group_manifest,
    const RecoveryFrontierMember &member) {
  rpc::RecoveryManifest manifest;
  manifest.CopyFrom(group_manifest);
  manifest.set_task_id(member.task_id.Binary());
  if (member.task_spec != nullptr) {
    manifest.set_job_id(member.task_spec->job_id());
    manifest.set_max_recovery_attempts(member.task_spec->max_retries());
  } else {
    manifest.set_job_id(member.task_id.JobId().Binary());
  }
  return manifest;
}

inline bool BuildFrontierSuccessionAppend(
    const RecoveryFrontierAppendBatch &batch,
    rpc::RecoveryFrontierAppend *append) {
  if (append == nullptr || batch.group_id.IsNil() ||
      batch.members.empty() ||
      batch.end_member_index <= batch.begin_member_index ||
      batch.members.size() != static_cast<size_t>(
          batch.end_member_index - batch.begin_member_index)) {
    return false;
  }

  append->Clear();
  append->set_group_id(batch.group_id.Binary());
  append->set_base_generation(batch.base_generation);
  append->set_generation(batch.generation);
  append->set_begin_member_index(batch.begin_member_index);
  append->set_end_member_index(batch.end_member_index);

  for (const RecoveryFrontierMember &member : batch.members) {
    if (member.task_spec == nullptr ||
        member.task_id.IsNil() ||
        member.task_spec->task_id() != member.task_id.Binary()) {
      append->Clear();
      return false;
    }

    rpc::RecoveryFrontierMemberRecord *record = append->add_members();
    record->set_task_id(member.task_id.Binary());
    record->set_member_index(member.member_index);
    record->set_first_group_return_index(member.first_group_return_index);
    record->set_num_returns(member.num_returns);
    record->mutable_task_spec()->CopyFrom(*member.task_spec);
  }
  return true;
}

inline bool BuildFrontierSuccessionSnapshot(const RecoveryFrontierGroup &group,
                                            rpc::RecoveryFrontierAppend *append) {
  if (append == nullptr || group.MemberCount() == 0) {
    return false;
  }

  append->Clear();
  append->set_group_id(group.GroupId().Binary());
  append->set_base_generation(0);
  append->set_generation(1);
  append->set_begin_member_index(0);
  append->set_end_member_index(group.MemberCount());

  for (const RecoveryFrontierMember &member : group.Members()) {
    if (member.task_spec == nullptr) {
      append->Clear();
      return false;
    }
    rpc::RecoveryFrontierMemberRecord *record = append->add_members();
    record->set_task_id(member.task_id.Binary());
    record->set_member_index(member.member_index);
    record->set_first_group_return_index(member.first_group_return_index);
    record->set_num_returns(member.num_returns);
    record->mutable_task_spec()->CopyFrom(*member.task_spec);
  }
  return true;
}

inline void PutFrontierSuccessionAppendCapsule(
    const rpc::RecoveryFrontierAppend &append, rpc::TaskSpec *task_spec) {
  RAY_CHECK(task_spec != nullptr);
  task_spec->clear_recovery_argument_metadata();
  rpc::RecoveryTaskArgumentMetadata *entry =
      task_spec->add_recovery_argument_metadata();
  entry->mutable_recovery_metadata()->set_first_holder_task_spec(
      EncodeFrontierSuccessionAppend(append));
}

inline bool ExtractFrontierSuccessionAppendCapsule(
    const rpc::TaskSpec &task_spec, rpc::RecoveryFrontierAppend *append) {
  for (const rpc::RecoveryTaskArgumentMetadata &entry :
       task_spec.recovery_argument_metadata()) {
    if (!entry.has_recovery_metadata()) {
      continue;
    }
    if (ParseFrontierSuccessionAppend(
            entry.recovery_metadata().first_holder_task_spec(), append)) {
      return true;
    }
  }
  return false;
}

}  // namespace ray::core::recovery_succession_internal
