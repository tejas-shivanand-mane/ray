// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier_wire/recovery_frontier_wire.h"

#include <string>

#include <gtest/gtest.h>

#include "ray/raylet/recovery_frontier/recovery_frontier_store.h"

namespace ray::core {
namespace {

rpc::TaskSpec MakeTask(char byte, int64_t num_returns) {
  rpc::TaskSpec spec;
  spec.set_task_id(std::string(TaskID::Size(), byte));
  spec.set_num_returns(num_returns);
  spec.set_max_retries(2);
  return spec;
}

TEST(RecoveryFrontierWireTest, OwnerAppendIsAcceptedByHolderStore) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', 2);
  const rpc::TaskSpec second = MakeTask('b', 3);

  const auto first_membership = planner.RegisterTask(first);
  const auto second_membership = planner.RegisterTask(second);
  ASSERT_EQ(first_membership.group_id, second_membership.group_id);
  ASSERT_EQ(second_membership.first_group_return_index, 2U);

  RecoveryFrontierGroup *group = planner.GetMutableGroup(first_membership.group_id);
  ASSERT_NE(group, nullptr);

  auto batch = group->StageAppend();
  ASSERT_TRUE(batch.has_value());

  const rpc::RecoveryFrontierAppend wire = BuildRecoveryFrontierAppend(*batch);
  EXPECT_EQ(wire.group_id(), first_membership.group_id.Binary());
  EXPECT_EQ(wire.base_generation(), 0U);
  EXPECT_EQ(wire.generation(), 1U);
  EXPECT_EQ(wire.begin_member_index(), 0U);
  EXPECT_EQ(wire.end_member_index(), 2U);
  ASSERT_EQ(wire.members_size(), 2);
  EXPECT_EQ(wire.members(1).task_id(), second.task_id());
  EXPECT_EQ(wire.members(1).first_group_return_index(), 2U);

  raylet::RecoveryFrontierStore holder_store;
  EXPECT_EQ(holder_store.ApplyAppend(wire),
            raylet::RecoveryFrontierStore::ApplyResult::APPLIED);

  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(holder_store.ExtractTaskForReturn(first_membership.group_id,
                                                /*group_return_index=*/4,
                                                &replay,
                                                &local_return));
  EXPECT_EQ(replay.task_id(), second.task_id());
  EXPECT_EQ(local_return, 2U);

  ASSERT_TRUE(group->CommitAppend(*batch));
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(second.task_id())));
}

TEST(RecoveryFrontierWireTest, IncrementalSuffixPreservesGenerationAndOffsets) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', 1);
  const rpc::TaskSpec second = MakeTask('b', 2);

  const auto first_membership = planner.RegisterTask(first);
  RecoveryFrontierGroup *group = planner.GetMutableGroup(first_membership.group_id);
  ASSERT_NE(group, nullptr);

  auto leader_batch = group->StageAppend(/*max_batch_members=*/1);
  ASSERT_TRUE(leader_batch.has_value());
  const rpc::RecoveryFrontierAppend leader_wire =
      BuildRecoveryFrontierAppend(*leader_batch);

  raylet::RecoveryFrontierStore holder_store;
  ASSERT_EQ(holder_store.ApplyAppend(leader_wire),
            raylet::RecoveryFrontierStore::ApplyResult::APPLIED);
  ASSERT_TRUE(group->CommitAppend(*leader_batch));

  const auto second_membership = planner.RegisterTask(second);
  ASSERT_EQ(second_membership.group_id, first_membership.group_id);
  ASSERT_EQ(second_membership.first_group_return_index, 1U);

  auto suffix_batch = group->StageAppend();
  ASSERT_TRUE(suffix_batch.has_value());
  const rpc::RecoveryFrontierAppend suffix_wire =
      BuildRecoveryFrontierAppend(*suffix_batch);

  EXPECT_EQ(suffix_wire.base_generation(), 1U);
  EXPECT_EQ(suffix_wire.generation(), 2U);
  EXPECT_EQ(suffix_wire.begin_member_index(), 1U);
  EXPECT_EQ(suffix_wire.end_member_index(), 2U);
  ASSERT_EQ(suffix_wire.members_size(), 1);
  EXPECT_EQ(suffix_wire.members(0).first_group_return_index(), 1U);

  ASSERT_EQ(holder_store.ApplyAppend(suffix_wire),
            raylet::RecoveryFrontierStore::ApplyResult::APPLIED);
  ASSERT_TRUE(group->CommitAppend(*suffix_batch));

  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(holder_store.ExtractTaskForReturn(first_membership.group_id,
                                                /*group_return_index=*/2,
                                                &replay,
                                                &local_return));
  EXPECT_EQ(replay.task_id(), second.task_id());
  EXPECT_EQ(local_return, 1U);
}

TEST(RecoveryFrontierWireTest, EnvelopeRoundTripsAndRejectsMagicOnlyPayload) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const auto membership = planner.RegisterTask(MakeTask('a', 1));
  RecoveryFrontierGroup *group = planner.GetMutableGroup(membership.group_id);
  ASSERT_NE(group, nullptr);

  auto batch = group->StageAppend();
  ASSERT_TRUE(batch.has_value());
  const rpc::RecoveryFrontierAppend wire = BuildRecoveryFrontierAppend(*batch);
  const std::string payload = SerializeRecoveryFrontierAppendEnvelope(wire);

  EXPECT_TRUE(IsRecoveryFrontierAppendEnvelope(payload));

  rpc::RecoveryFrontierAppend decoded;
  ASSERT_TRUE(ParseRecoveryFrontierAppendEnvelope(payload, &decoded));
  EXPECT_EQ(decoded.SerializeAsString(), wire.SerializeAsString());

  const std::string ordinary_task_spec =
      wire.members(0).task_spec().SerializeAsString();
  EXPECT_FALSE(IsRecoveryFrontierAppendEnvelope(ordinary_task_spec));
  EXPECT_FALSE(ParseRecoveryFrontierAppendEnvelope(ordinary_task_spec, &decoded));

  const std::string magic_only = payload.substr(0, 8);
  EXPECT_TRUE(IsRecoveryFrontierAppendEnvelope(magic_only));
  EXPECT_FALSE(ParseRecoveryFrontierAppendEnvelope(magic_only, &decoded));
}

}  // namespace
}  // namespace ray::core
