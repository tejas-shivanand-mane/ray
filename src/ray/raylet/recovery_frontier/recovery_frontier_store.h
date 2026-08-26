// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/frontier/recovery_frontier.pb.h"

namespace ray::raylet {

// Holder-local materialization of append-only Recovery Frontier capsules.
//
// This class deliberately has no networking or recovery-claim policy. It only
// validates and atomically advances the durable prefix represented by
// RecoveryFrontierAppend. NodeManager can therefore reuse exactly the same
// storage semantics for Fixed-R and Succession frontier protection.
class RecoveryFrontierStore {
 public:
  enum class ApplyResult {
    APPLIED,
    IDEMPOTENT,
    STALE,
    INVALID,
  };

  ApplyResult ApplyAppend(const rpc::RecoveryFrontierAppend &append);

  // Resolve a committed group-global return index to the original task recipe
  // and its task-local return index.
  bool ExtractTaskForReturn(const TaskID &group_id,
                            uint32_t group_return_index,
                            rpc::TaskSpec *task_spec,
                            uint32_t *task_return_index) const;

  std::optional<uint64_t> Generation(const TaskID &group_id) const;
  std::optional<uint32_t> CommittedMemberCount(const TaskID &group_id) const;

 private:
  struct GroupState {
    uint64_t generation = 0;
    uint32_t committed_member_count = 0;
    uint32_t next_group_return_index = 0;
    std::vector<rpc::RecoveryFrontierMemberRecord> members;
  };

  static bool ValidAppendShape(const rpc::RecoveryFrontierAppend &append);
  static bool SameMember(const rpc::RecoveryFrontierMemberRecord &left,
                         const rpc::RecoveryFrontierMemberRecord &right);
  static bool AppendMatchesCommittedSuffix(const rpc::RecoveryFrontierAppend &append,
                                           const GroupState &state);

  absl::flat_hash_map<TaskID, GroupState> groups_;
};

}  // namespace ray::raylet
