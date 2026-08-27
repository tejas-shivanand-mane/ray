// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier_wire/recovery_frontier_wire.h"

#include <cstdint>

#include "ray/util/logging.h"

namespace ray::core {

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

}  // namespace ray::core
