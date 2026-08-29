// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier.h"

#include <string>

#include <gtest/gtest.h>

namespace ray::core {
namespace {

rpc::TaskSpec MakeRetryTask(char byte) {
  rpc::TaskSpec spec;
  spec.set_task_id(std::string(TaskID::Size(), byte));
  spec.set_num_returns(1);
  spec.set_max_retries(2);
  return spec;
}

TEST(RecoveryFrontierRetryTest, AbortedGenerationRetriesExactRecipeSlice) {
  RecoveryFrontierPlanner planner(/*group_size=*/5);

  const rpc::TaskSpec t1 = MakeRetryTask('a');
  const rpc::TaskSpec t2 = MakeRetryTask('b');
  const rpc::TaskSpec t3 = MakeRetryTask('c');
  const rpc::TaskSpec t4 = MakeRetryTask('d');
  const rpc::TaskSpec t5 = MakeRetryTask('e');

  const auto leader = planner.RegisterTask(t1);
  planner.RegisterTask(t2);

  RecoveryFrontierGroup *group = planner.GetMutableGroup(leader.group_id);
  ASSERT_NE(group, nullptr);

  auto generation1 = group->StageAppend();
  ASSERT_TRUE(generation1.has_value());
  ASSERT_EQ(generation1->base_generation, 0U);
  ASSERT_EQ(generation1->generation, 1U);
  ASSERT_EQ(generation1->begin_member_index, 0U);
  ASSERT_EQ(generation1->end_member_index, 2U);
  ASSERT_TRUE(group->CommitAppend(*generation1));

  planner.RegisterTask(t3);
  planner.RegisterTask(t4);

  auto failed_generation2 = group->StageAppend();
  ASSERT_TRUE(failed_generation2.has_value());
  ASSERT_EQ(failed_generation2->base_generation, 1U);
  ASSERT_EQ(failed_generation2->generation, 2U);
  ASSERT_EQ(failed_generation2->begin_member_index, 2U);
  ASSERT_EQ(failed_generation2->end_member_index, 4U);
  ASSERT_EQ(failed_generation2->members.size(), 2U);
  ASSERT_EQ(failed_generation2->members[0].task_id,
            TaskID::FromBinary(t3.task_id()));
  ASSERT_EQ(failed_generation2->members[1].task_id,
            TaskID::FromBinary(t4.task_id()));

  // A later task is allowed to join the still-open Frontier while generation
  // 2 is being published. It must remain behind the exact retry boundary.
  const auto t5_membership = planner.RegisterTask(t5);
  ASSERT_EQ(t5_membership.group_id, leader.group_id);
  ASSERT_EQ(t5_membership.member_index, 4U);
  ASSERT_EQ(group->MemberCount(), 5U);

  ASSERT_TRUE(group->AbortAppend(*failed_generation2));
  ASSERT_FALSE(group->AppendInFlight());
  ASSERT_EQ(group->Generation(), 1U);
  ASSERT_EQ(group->CommittedMemberCount(), 2U);

  // Even a caller asking for a smaller batch may not reshape an aborted
  // generation. The retry is exactly generation 2 [T3,T4].
  auto retry_generation2 = group->StageAppend(/*max_batch_members=*/1);
  ASSERT_TRUE(retry_generation2.has_value());
  EXPECT_EQ(retry_generation2->base_generation,
            failed_generation2->base_generation);
  EXPECT_EQ(retry_generation2->generation,
            failed_generation2->generation);
  EXPECT_EQ(retry_generation2->begin_member_index,
            failed_generation2->begin_member_index);
  EXPECT_EQ(retry_generation2->end_member_index,
            failed_generation2->end_member_index);
  ASSERT_EQ(retry_generation2->members.size(),
            failed_generation2->members.size());

  for (size_t i = 0; i < failed_generation2->members.size(); ++i) {
    ASSERT_NE(retry_generation2->members[i].task_spec, nullptr);
    ASSERT_NE(failed_generation2->members[i].task_spec, nullptr);
    EXPECT_EQ(retry_generation2->members[i].task_id,
              failed_generation2->members[i].task_id);
    EXPECT_EQ(retry_generation2->members[i].member_index,
              failed_generation2->members[i].member_index);
    EXPECT_EQ(retry_generation2->members[i].first_group_return_index,
              failed_generation2->members[i].first_group_return_index);
    EXPECT_EQ(retry_generation2->members[i].num_returns,
              failed_generation2->members[i].num_returns);
    EXPECT_EQ(retry_generation2->members[i].task_spec->SerializeAsString(),
              failed_generation2->members[i].task_spec->SerializeAsString());
  }

  ASSERT_TRUE(group->CommitAppend(*retry_generation2));
  EXPECT_EQ(group->Generation(), 2U);
  EXPECT_EQ(group->CommittedMemberCount(), 4U);
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(t3.task_id())));
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(t4.task_id())));
  EXPECT_FALSE(group->IsTaskCommitted(TaskID::FromBinary(t5.task_id())));

  // Only after exact generation 2 commits may T5 advance as generation 3.
  auto generation3 = group->StageAppend();
  ASSERT_TRUE(generation3.has_value());
  EXPECT_EQ(generation3->base_generation, 2U);
  EXPECT_EQ(generation3->generation, 3U);
  EXPECT_EQ(generation3->begin_member_index, 4U);
  EXPECT_EQ(generation3->end_member_index, 5U);
  ASSERT_EQ(generation3->members.size(), 1U);
  EXPECT_EQ(generation3->members[0].task_id,
            TaskID::FromBinary(t5.task_id()));

  ASSERT_TRUE(group->CommitAppend(*generation3));
  EXPECT_EQ(group->Generation(), 3U);
  EXPECT_EQ(group->CommittedMemberCount(), 5U);
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(t5.task_id())));
}

}  // namespace
}  // namespace ray::core
