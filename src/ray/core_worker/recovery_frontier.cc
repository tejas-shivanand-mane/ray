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

#include "ray/core_worker/recovery_frontier.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "ray/util/logging.h"

namespace ray::core {
namespace {

bool CanShareRecoveryFrontierReplayRecipe(const rpc::TaskSpec &task_spec) {
  // The owner submission path normally reaches Frontier before any recovery
  // state is attached. In that common case the TaskManager-owned protobuf is
  // already exactly the replay recipe we want and can be shared directly.
  if (task_spec.has_recovery_manifest()) {
    return false;
  }

  // Patch-4F full-lineage piggybacks are transport-only and must never become
  // part of retained replay lineage. Fall back to a sanitized private copy if
  // one is present instead of mutating the shared source TaskSpec.
  for (const rpc::RecoveryTaskArgumentMetadata &entry :
       task_spec.recovery_argument_metadata()) {
    if (entry.has_recovery_metadata() &&
        !entry.recovery_metadata().first_holder_task_spec().empty()) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<const rpc::TaskSpec> MakeSanitizedRecoveryFrontierReplayRecipe(
    const rpc::TaskSpec &task_spec) {
  auto stored_task_spec = std::make_shared<rpc::TaskSpec>();
  stored_task_spec->CopyFrom(task_spec);
  stored_task_spec->clear_recovery_manifest();
  for (rpc::RecoveryTaskArgumentMetadata &entry :
       *stored_task_spec->mutable_recovery_argument_metadata()) {
    if (entry.has_recovery_metadata()) {
      entry.mutable_recovery_metadata()->clear_first_holder_task_spec();
    }
  }
  return stored_task_spec;
}

}  // namespace

RecoveryFrontierGroup::RecoveryFrontierGroup(TaskID group_id, uint32_t max_members)
    : group_id_(std::move(group_id)), max_members_(max_members) {
  RAY_CHECK(!group_id_.IsNil());
  RAY_CHECK_GT(max_members_, 0U);
}

std::optional<RecoveryFrontierMembership> RecoveryFrontierGroup::AddTask(
    std::shared_ptr<const rpc::TaskSpec> task_spec) {
  if (task_spec == nullptr || task_spec->task_id().size() != TaskID::Size() ||
      task_spec->num_returns() == 0) {
    return std::nullopt;
  }

  const TaskID task_id = TaskID::FromBinary(task_spec->task_id());
  const auto existing = task_to_member_index_.find(task_id);
  if (existing != task_to_member_index_.end()) {
    const RecoveryFrontierMember &member = members_[existing->second];
    return RecoveryFrontierMembership{
        group_id_,
        member.member_index,
        member.first_group_return_index,
        member.num_returns,
        member.member_index == 0,
        Full()};
  }

  if (Full()) {
    return std::nullopt;
  }

  RecoveryFrontierMember member;
  member.task_id = task_id;
  member.member_index = static_cast<uint32_t>(members_.size());
  member.first_group_return_index = next_group_return_index_;
  member.num_returns = static_cast<uint32_t>(task_spec->num_returns());

  // Benchmark 50 showed that the canonical protobuf CopyFrom is a material
  // per-task cost. The normal owner path already has immutable shared ownership
  // through TaskSpecification/TaskManager, so reuse it. If recovery-only fields
  // need stripping, preserve the old semantics with a sanitized private copy.
  if (CanShareRecoveryFrontierReplayRecipe(*task_spec)) {
    member.task_spec = std::move(task_spec);
  } else {
    member.task_spec = MakeSanitizedRecoveryFrontierReplayRecipe(*task_spec);
  }

  const uint32_t member_index = member.member_index;
  const uint32_t first_return = member.first_group_return_index;
  const uint32_t num_returns = member.num_returns;
  next_group_return_index_ += num_returns;

  task_to_member_index_.emplace(task_id, member_index);
  members_.push_back(std::move(member));

  return RecoveryFrontierMembership{
      group_id_,
      member_index,
      first_return,
      num_returns,
      member_index == 0,
      Full()};
}

std::optional<RecoveryFrontierAppendBatch> RecoveryFrontierGroup::StageAppend(
    uint32_t max_batch_members) {
  if (append_in_flight_ || !HasUncommittedMembers()) {
    return std::nullopt;
  }

  const uint32_t pending = MemberCount() - committed_member_count_;
  const uint32_t batch_size =
      max_batch_members == 0 ? pending : std::min(max_batch_members, pending);
  RAY_CHECK_GT(batch_size, 0U);

  RecoveryFrontierAppendBatch batch;
  batch.group_id = group_id_;
  batch.base_generation = generation_;
  batch.generation = generation_ + 1;
  batch.begin_member_index = committed_member_count_;
  batch.end_member_index = committed_member_count_ + batch_size;
  batch.members.reserve(batch_size);

  // RecoveryFrontierMember is intentionally cheap to copy: the exact replay
  // TaskSpec is immutable and shared. This preserves the staged batch lifetime
  // needed by asynchronous publication without duplicating TaskSpec payloads.
  for (uint32_t i = batch.begin_member_index; i < batch.end_member_index; ++i) {
    batch.members.push_back(members_[i]);
  }

  append_in_flight_ = true;
  in_flight_generation_ = batch.generation;
  in_flight_begin_ = batch.begin_member_index;
  in_flight_end_ = batch.end_member_index;
  return batch;
}

bool RecoveryFrontierGroup::MatchesInFlight(
    const RecoveryFrontierAppendBatch &batch) const {
  return append_in_flight_ && batch.group_id == group_id_ &&
         batch.base_generation == generation_ &&
         batch.generation == in_flight_generation_ &&
         batch.begin_member_index == in_flight_begin_ &&
         batch.end_member_index == in_flight_end_ &&
         batch.end_member_index <= MemberCount() &&
         batch.members.size() ==
             static_cast<size_t>(batch.end_member_index - batch.begin_member_index);
}

bool RecoveryFrontierGroup::CommitAppend(const RecoveryFrontierAppendBatch &batch) {
  if (!MatchesInFlight(batch) || batch.begin_member_index != committed_member_count_) {
    return false;
  }

  generation_ = batch.generation;
  committed_member_count_ = batch.end_member_index;
  append_in_flight_ = false;
  in_flight_generation_ = 0;
  in_flight_begin_ = 0;
  in_flight_end_ = 0;
  return true;
}

bool RecoveryFrontierGroup::AbortAppend(const RecoveryFrontierAppendBatch &batch) {
  if (!MatchesInFlight(batch)) {
    return false;
  }

  append_in_flight_ = false;
  in_flight_generation_ = 0;
  in_flight_begin_ = 0;
  in_flight_end_ = 0;
  return true;
}

bool RecoveryFrontierGroup::IsTaskCommitted(const TaskID &task_id) const {
  const auto it = task_to_member_index_.find(task_id);
  return it != task_to_member_index_.end() && it->second < committed_member_count_;
}

std::optional<RecoveryFrontierMembership> RecoveryFrontierGroup::FindTask(
    const TaskID &task_id) const {
  const auto it = task_to_member_index_.find(task_id);
  if (it == task_to_member_index_.end()) {
    return std::nullopt;
  }
  const RecoveryFrontierMember &member = members_[it->second];
  return RecoveryFrontierMembership{
      group_id_,
      member.member_index,
      member.first_group_return_index,
      member.num_returns,
      member.member_index == 0,
      Full()};
}

bool RecoveryFrontierGroup::ExtractTaskForReturn(uint32_t group_return_index,
                                                 rpc::TaskSpec *task_spec,
                                                 uint32_t *task_return_index) const {
  if (task_spec == nullptr || task_return_index == nullptr) {
    return false;
  }

  for (const RecoveryFrontierMember &member : members_) {
    if (member.member_index >= committed_member_count_) {
      break;
    }

    const uint32_t begin = member.first_group_return_index;
    const uint32_t end = begin + member.num_returns;
    if (group_return_index < begin || group_return_index >= end) {
      continue;
    }
    RAY_CHECK(member.task_spec != nullptr);
    task_spec->CopyFrom(*member.task_spec);
    *task_return_index = group_return_index - begin;
    return true;
  }
  return false;
}

RecoveryFrontierPlanner::RecoveryFrontierPlanner(uint32_t group_size)
    : group_size_(group_size) {
  RAY_CHECK_GT(group_size_, 0U);
}

RecoveryFrontierMembership RecoveryFrontierPlanner::RegisterTask(
    const rpc::TaskSpec &task_spec) {
  return RegisterTask(MakeSanitizedRecoveryFrontierReplayRecipe(task_spec));
}

RecoveryFrontierMembership RecoveryFrontierPlanner::RegisterTask(
    std::shared_ptr<const rpc::TaskSpec> task_spec) {
  RAY_CHECK(task_spec != nullptr);
  RAY_CHECK_EQ(task_spec->task_id().size(), TaskID::Size());
  const TaskID task_id = TaskID::FromBinary(task_spec->task_id());

  const auto existing = membership_by_task_.find(task_id);
  if (existing != membership_by_task_.end()) {
    return existing->second;
  }

  if (open_group_id_.IsNil()) {
    open_group_id_ = task_id;
    groups_.try_emplace(open_group_id_, open_group_id_, group_size_);
  }

  RecoveryFrontierGroup *group = GetMutableGroup(open_group_id_);
  RAY_CHECK(group != nullptr);
  auto membership = group->AddTask(task_spec);

  if (!membership.has_value()) {
    open_group_id_ = task_id;
    groups_.try_emplace(open_group_id_, open_group_id_, group_size_);
    group = GetMutableGroup(open_group_id_);
    RAY_CHECK(group != nullptr);
    membership = group->AddTask(std::move(task_spec));
  }

  RAY_CHECK(membership.has_value());
  membership_by_task_.emplace(task_id, membership.value());

  if (group->Full()) {
    open_group_id_ = TaskID::Nil();
  }

  return membership.value();
}

std::optional<RecoveryFrontierMembership> RecoveryFrontierPlanner::FindTask(
    const TaskID &task_id) const {
  const auto it = membership_by_task_.find(task_id);
  if (it == membership_by_task_.end()) {
    return std::nullopt;
  }
  return it->second;
}

const RecoveryFrontierGroup *RecoveryFrontierPlanner::GetGroup(
    const TaskID &group_id) const {
  const auto it = groups_.find(group_id);
  return it == groups_.end() ? nullptr : &it->second;
}

RecoveryFrontierGroup *RecoveryFrontierPlanner::GetMutableGroup(
    const TaskID &group_id) {
  const auto it = groups_.find(group_id);
  return it == groups_.end() ? nullptr : &it->second;
}

bool RecoveryFrontierPlanner::SealGroup(const TaskID &group_id) {
  if (group_id.IsNil() || groups_.find(group_id) == groups_.end()) {
    return false;
  }
  if (open_group_id_ == group_id) {
    open_group_id_ = TaskID::Nil();
  }
  return true;
}

bool RecoveryFrontierPlanner::EraseGroup(const TaskID &group_id) {
  const auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return false;
  }
  if (open_group_id_ == group_id) {
    open_group_id_ = TaskID::Nil();
  }
  for (const RecoveryFrontierMember &member : group_it->second.Members()) {
    membership_by_task_.erase(member.task_id);
  }
  groups_.erase(group_it);
  return true;
}

}  // namespace ray::core
