// Copyright 2026 The Ray Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <optional>
#include <string>
#include <unordered_set>

#include "ray/common/status.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

Status ValidateRecoveryStreamDescriptor(const rpc::RecoveryStreamDescriptor &descriptor);
Status ValidateRecoveryStreamRecipe(const rpc::TaskSpec &recipe);
Status ValidateRecoveryStreamInputs(const rpc::TaskSpec &recipe,
                                   const rpc::Address &consumer);
bool SameRecoveryStreamDescriptor(const rpc::RecoveryStreamDescriptor &left,
                                  const rpc::RecoveryStreamDescriptor &right);
bool RecoveryStreamClaimantMatches(const rpc::TaskSpec &recipe,
                                   const rpc::Address &claimant);
// Mutable manifest storage is separate; all other recipe fields are immutable.
bool SameRecoveryStreamRecipe(const rpc::TaskSpec &left, const rpc::TaskSpec &right);

// Validate a response from a selected witness before any local ownership
// mutation. Version 1 permits one owner-node failure and one recovery claim.
// Caller must have completed enrollment, observed owner loss, and obtained this
// reply through the Fixed-R RPC; arbitrary caller-supplied protobufs are not
// evidence of authority. Output is unchanged on rejection.
Status PrepareRecoveryStreamReplay(const rpc::RecoveryStreamDescriptor &descriptor,
                                   const rpc::Address &consumer,
                                   const rpc::GetRecoveryWitnessReply &reply,
                                   rpc::TaskSpec *replay);

// Owner-local enrollment gate. Callbacks for one installation must be serialized
// by its owner. A witness callback is associated with the exact recipe/descriptor
// sent to that witness, never inferred from metadata presence or RPC submission.
// The consumer receipt acknowledges retaining the offered descriptor and cursor
// state. Only after both requirements may the owner announce protection ready.
// Cancellation/failure is absorbing; create a new gate for a new enrollment.
class RecoveryStreamInstallation {
 public:
  Status Initialize(const rpc::RecoveryStreamDescriptor &descriptor);
  Status RecordWitnessReply(const rpc::Address &witness,
                            const Status &status,
                            const rpc::UpdateRecoveryWitnessReply &reply);
  Status RecordConsumerReceipt(const rpc::Address &consumer,
                               const rpc::RecoveryStreamDescriptor &descriptor);
  void Cancel();
  bool IsReady() const;
  std::optional<rpc::RecoveryStreamDescriptor> ReadyDescriptor() const;

 private:
  std::optional<rpc::RecoveryStreamDescriptor> descriptor_;
  std::unordered_set<std::string> acknowledged_nodes_;
  bool consumer_received_ = false;
  bool terminal_ = false;
};

}  // namespace ray
