// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier_wire/recovery_frontier_wire.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "ray/util/logging.h"

namespace ray::core {
namespace {

// Keep this short because it is paid once per logical frontier append, but make
// it distinctive enough that ordinary protobuf TaskSpec bytes cannot be
// mistaken for a frontier payload in practice. The trailing NUL is part of the
// envelope and makes accidental text-prefix collisions even less likely.
constexpr char kRecoveryFrontierAppendMagic[] = {'R', 'A', 'Y', 'F', 'R', 'N', '1', '\0'};
constexpr size_t kRecoveryFrontierAppendMagicSize =
    sizeof(kRecoveryFrontierAppendMagic);

}  // namespace

rpc::RecoveryFrontierAppend BuildRecoveryFrontierAppend(
    const RecoveryFrontierAppendBatch &batch) {
  RAY_CHECK(!batch.group_id.IsNil());
  RAY_CHECK_GT(batch.generation, 0U);
  RAY_CHECK_EQ(batch.generation, batch.base_generation + 1);
  RAY_CHECK_LT(batch.begin_member_index, batch.end_member_index);
  RAY_CHECK_EQ(batch.members.size(),
               static_cast<size_t>(batch.end_member_index -
                                   batch.begin_member_index));

  rpc::RecoveryFrontierAppend append;
  append.set_group_id(batch.group_id.Binary());
  append.set_base_generation(batch.base_generation);
  append.set_generation(batch.generation);
  append.set_begin_member_index(batch.begin_member_index);
  append.set_end_member_index(batch.end_member_index);
  append.mutable_members()->Reserve(static_cast<int>(batch.members.size()));

  for (size_t i = 0; i < batch.members.size(); ++i) {
    const RecoveryFrontierMember &member = batch.members[i];
    const uint32_t expected_member_index =
        batch.begin_member_index + static_cast<uint32_t>(i);

    RAY_CHECK_EQ(member.member_index, expected_member_index);
    RAY_CHECK(!member.task_id.IsNil());
    RAY_CHECK_EQ(member.task_spec.task_id(), member.task_id.Binary());
    RAY_CHECK_GT(member.num_returns, 0U);
    RAY_CHECK_EQ(static_cast<uint32_t>(member.task_spec.num_returns()),
                 member.num_returns);

    rpc::RecoveryFrontierMemberRecord *record = append.add_members();
    record->set_task_id(member.task_id.Binary());
    record->set_member_index(member.member_index);
    record->set_first_group_return_index(member.first_group_return_index);
    record->set_num_returns(member.num_returns);
    record->mutable_task_spec()->CopyFrom(member.task_spec);
  }

  return append;
}

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

}  // namespace ray::core
