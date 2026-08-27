// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <string>
#include <string_view>

#include "ray/core_worker/recovery_frontier.h"
#include "src/ray/protobuf/frontier/recovery_frontier.pb.h"

namespace ray::core {

/// Convert one owner-side staged frontier append into the canonical wire
/// representation consumed by RecoveryFrontierStore on a holder raylet.
///
/// This function is intentionally pure: staging/commit/abort remains owned by
/// RecoveryFrontierGroup. The network layer may therefore retry the same wire
/// record without mutating owner state until every required backend holder ACKs.
rpc::RecoveryFrontierAppend BuildRecoveryFrontierAppend(
    const RecoveryFrontierAppendBatch &batch);

/// Versioned transport envelope used while Recovery Frontiers share the
/// existing UpdateRecoveryWitness RPC. The prefix makes frontier appends
/// unambiguous with the fixed-R baseline's serialized TaskSpec payload.
bool IsRecoveryFrontierAppendEnvelope(std::string_view payload);
std::string SerializeRecoveryFrontierAppendEnvelope(
    const rpc::RecoveryFrontierAppend &append);
bool ParseRecoveryFrontierAppendEnvelope(std::string_view payload,
                                         rpc::RecoveryFrontierAppend *append);

}  // namespace ray::core
