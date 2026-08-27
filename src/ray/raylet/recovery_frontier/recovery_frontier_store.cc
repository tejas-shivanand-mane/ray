// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/raylet/recovery_frontier/recovery_frontier_store.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace ray::raylet {
namespace {

// Keep this short because it is paid once per logical frontier append, but make
// it distinctive enough that ordinary protobuf TaskSpec bytes cannot be
// mistaken for a frontier payload in practice. The trailing NUL is part of the
// envelope and makes accidental text-prefix collisions even less likely.
constexpr char kRecoveryFrontierAppendMagic[] = {'R', 'A', 'Y', 'F', 'R', 'N', '1', '\0'};
constexpr size_t kRecoveryFrontierAppendMagicSize =
    sizeof(kRecoveryFrontierAppendMagic);

}  // namespace

bool IsRecoveryFrontierAppendEnvelope(std::string_view payload) {
  return payload.size() >= kRecoveryFrontierAppendMagicSize &&
         payload.compare(0,
                         kRecoveryFrontierAppendMagicSize,
                         kRecoveryFrontierAppendMagic,
                         kRecoveryFrontierAppendMagicSize) == 0;
}

std::string SerializeRecoveryFrontierAppendEnvelope(
    const rpc::RecoveryFrontierAppend &append) {
  std::string payload(kRecoveryFrontierAppendMagic,
                      kRecoveryFrontierAppendMagicSize);
  append.AppendToString(&payload);
  return payload;
}

bool ParseRecoveryFrontierAppendEnvelope(std::string_view payload,
                                         rpc::RecoveryFrontierAppend *append) {
  // A magic-only envelope is not a valid frontier append. Protobuf accepts an
  // empty byte sequence as a valid default message, so explicitly require a
  // non-empty message body before parsing.
  if (append == nullptr || !IsRecoveryFrontierAppendEnvelope(payload) ||
      payload.size() == kRecoveryFrontierAppendMagicSize) {
    return false;
  }
  append->Clear();
  return append->ParseFromArray(
      payload.data() + kRecoveryFrontierAppendMagicSize,
      static_cast<int>(payload.size() - kRecoveryFrontierAppendMagicSize));
}

bool RecoveryFrontierStore::ValidAppendShape(
    const rpc::RecoveryFrontierAppend &append) {
  if (append.group_id().size() != TaskID::Size() || append.generation() == 0 ||
      append.generation() != append.base_generation() + 1 ||
      append.begin_member_index() >= append.end_member_index()) {
    return false;
  }

  const uint32_t expected_members =
      append.end_member_index() - append.begin_member_index();
  if (append.members_size() != static_cast<int>(expected_members)) {
    return false;
  }

  uint64_t expected_group_return = 0;
  bool have_expected_group_return = false;
  for (int i = 0; i < append.members_size(); ++i) {
    const rpc::RecoveryFrontierMemberRecord &member = append.members(i);
    const uint32_t expected_member_index =
        append.begin_member_index() + static_cast<uint32_t>(i);

    if (member.task_id().size() != TaskID::Size() ||
        member.task_spec().task_id() != member.task_id() ||
        member.member_index() != expected_member_index || member.num_returns() == 0 ||
        member.task_spec().num_returns() != member.num_returns()) {
      return false;
    }

    if (i == 0 && append.begin_member_index() == 0 &&
        member.task_id() != append.group_id()) {
      // The group ID is the first member's TaskID. This keeps the leader
      // immediately addressable and makes K=1 identical to per-task grouping.
      return false;
    }

    if (have_expected_group_return &&
        member.first_group_return_index() != expected_group_return) {
      return false;
    }

    expected_group_return =
        static_cast<uint64_t>(member.first_group_return_index()) +
        static_cast<uint64_t>(member.num_returns());
    if (expected_group_return > std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    have_expected_group_return = true;
  }

  return true;
}

bool RecoveryFrontierStore::SameMember(
    const rpc::RecoveryFrontierMemberRecord &left,
    const rpc::RecoveryFrontierMemberRecord &right) {
  return left.task_id() == right.task_id() &&
         left.member_index() == right.member_index() &&
         left.first_group_return_index() == right.first_group_return_index() &&
         left.num_returns() == right.num_returns() &&
         left.task_spec().SerializeAsString() == right.task_spec().SerializeAsString();
}

bool RecoveryFrontierStore::AppendMatchesCommittedSuffix(
    const rpc::RecoveryFrontierAppend &append,
    const GroupState &state) {
  if (append.end_member_index() > state.committed_member_count ||
      append.members_size() !=
          static_cast<int>(append.end_member_index() - append.begin_member_index())) {
    return false;
  }

  for (int i = 0; i < append.members_size(); ++i) {
    const uint32_t member_index =
        append.begin_member_index() + static_cast<uint32_t>(i);
    if (member_index >= state.members.size() ||
        !SameMember(append.members(i), state.members[member_index])) {
      return false;
    }
  }
  return true;
}

RecoveryFrontierStore::ApplyResult RecoveryFrontierStore::ApplyAppend(
    const rpc::RecoveryFrontierAppend &append) {
  if (!ValidAppendShape(append)) {
    return ApplyResult::INVALID;
  }

  const TaskID group_id = TaskID::FromBinary(append.group_id());
  auto existing = groups_.find(group_id);
  if (existing == groups_.end()) {
    if (append.base_generation() != 0 || append.generation() != 1 ||
        append.begin_member_index() != 0 ||
        append.members(0).first_group_return_index() != 0) {
      return ApplyResult::STALE;
    }

    GroupState state;
    state.generation = append.generation();
    state.committed_member_count = append.end_member_index();
    state.members.reserve(static_cast<size_t>(append.members_size()));
    for (const rpc::RecoveryFrontierMemberRecord &member : append.members()) {
      state.members.push_back(member);
    }
    const auto &last = state.members.back();
    state.next_group_return_index =
        last.first_group_return_index() + last.num_returns();
    groups_.emplace(group_id, std::move(state));
    return ApplyResult::APPLIED;
  }

  GroupState &state = existing->second;

  if (append.generation() < state.generation) {
    return ApplyResult::STALE;
  }
  if (append.generation() == state.generation) {
    return AppendMatchesCommittedSuffix(append, state) ? ApplyResult::IDEMPOTENT
                                                        : ApplyResult::STALE;
  }

  if (append.base_generation() != state.generation) {
    return ApplyResult::STALE;
  }
  if (append.begin_member_index() != state.committed_member_count ||
      append.members(0).first_group_return_index() !=
          state.next_group_return_index) {
    return ApplyResult::INVALID;
  }

  for (const rpc::RecoveryFrontierMemberRecord &member : append.members()) {
    state.members.push_back(member);
  }
  state.generation = append.generation();
  state.committed_member_count = append.end_member_index();
  const auto &last = state.members.back();
  state.next_group_return_index =
      last.first_group_return_index() + last.num_returns();
  return ApplyResult::APPLIED;
}

bool RecoveryFrontierStore::ExtractTaskForReturn(
    const TaskID &group_id,
    uint32_t group_return_index,
    rpc::TaskSpec *task_spec,
    uint32_t *task_return_index) const {
  if (task_spec == nullptr || task_return_index == nullptr) {
    return false;
  }

  const auto group_it = groups_.find(group_id);
  if (group_it == groups_.end()) {
    return false;
  }

  const GroupState &state = group_it->second;
  for (const rpc::RecoveryFrontierMemberRecord &member : state.members) {
    const uint64_t begin = member.first_group_return_index();
    const uint64_t end = begin + member.num_returns();
    if (group_return_index >= begin && group_return_index < end) {
      task_spec->CopyFrom(member.task_spec());
      *task_return_index =
          group_return_index - member.first_group_return_index();
      return true;
    }
  }
  return false;
}

std::optional<uint64_t> RecoveryFrontierStore::Generation(
    const TaskID &group_id) const {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return std::nullopt;
  }
  return it->second.generation;
}

std::optional<uint32_t> RecoveryFrontierStore::CommittedMemberCount(
    const TaskID &group_id) const {
  const auto it = groups_.find(group_id);
  if (it == groups_.end()) {
    return std::nullopt;
  }
  return it->second.committed_member_count;
}

void RecoveryFrontierStore::EraseGroup(const TaskID &group_id) {
  groups_.erase(group_id);
}

}  // namespace ray::raylet