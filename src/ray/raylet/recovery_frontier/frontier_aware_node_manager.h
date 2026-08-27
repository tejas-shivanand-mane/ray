// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/recovery_frontier/recovery_frontier_store.h"

namespace ray::raylet {

// Recovery Frontiers are an orthogonal protection layer, not a replacement for
// NodeManager's existing Fixed-R or Succession semantics. This subclass
// intercepts only frontier-specific witness traffic and delegates every
// ordinary witness request to NodeManager unchanged.
class FrontierAwareNodeManager final : public NodeManager {
 public:
  using NodeManager::NodeManager;

  void HandleUpdateRecoveryWitness(
      rpc::UpdateRecoveryWitnessRequest request,
      rpc::UpdateRecoveryWitnessReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleUpdateRecoveryWitnessBatch(
      rpc::UpdateRecoveryWitnessBatchRequest request,
      rpc::UpdateRecoveryWitnessBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetRecoveryWitness(
      rpc::GetRecoveryWitnessRequest request,
      rpc::GetRecoveryWitnessReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

 private:
  struct FrontierMemberClaimState {
    rpc::Address acting_owner;
    uint32_t recovery_attempt = 0;
  };

  absl::Mutex recovery_frontier_mutex_;
  RecoveryFrontierStore recovery_frontier_store_
      ABSL_GUARDED_BY(recovery_frontier_mutex_);
  absl::flat_hash_map<TaskID, FrontierMemberClaimState> recovery_frontier_claims_
      ABSL_GUARDED_BY(recovery_frontier_mutex_);
};

}  // namespace ray::raylet
