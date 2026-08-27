// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/core_worker.h"

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/core_worker/recovery_frontier_wire/recovery_frontier_wire.h"
#include "ray/core_worker/recovery_succession_manager.h"
#include "ray/util/logging.h"

namespace ray::core {

void CoreWorker::PublishRecoveryFrontierGroup(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest) const {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !recovery_succession_manager_->RecoveryFrontierEnabled() ||
      group_id.IsNil() ||
      protection_manifest.task_id() != group_id.Binary()) {
    return;
  }

  const uint32_t target_holder_count =
      RayConfig::instance().recovery_succession_target_holder_count();
  RAY_CHECK_EQ(
      static_cast<uint32_t>(protection_manifest.witness_raylets_size()),
      target_holder_count)
      << "Recovery Frontier fixed-R publication requires exactly "
      << target_holder_count << " holder raylets for group " << group_id;
  RAY_CHECK_EQ(protection_manifest.witness_count(), target_holder_count);

  // This method is the owner-side visibility barrier. The triggering export
  // must not return recovery metadata until every replay recipe it depends on
  // is inside the frontier's acknowledged prefix on all fixed-R holders.
  //
  // Usually this loop executes once. A second exporter can race with an append
  // already in flight; in that case StageRecoveryFrontierAppend() returns no
  // batch while HasUncommittedMembers remains true. The exporter waits for the
  // first publication to advance the prefix instead of escaping with an
  // ordinary, non-recoverable ObjectRef.
  while (recovery_succession_manager_
             ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
    auto staged =
        recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
    if (!staged.has_value()) {
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }

    auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
        std::move(staged.value()));
    const std::string serialized_append =
        BuildRecoveryFrontierAppendEnvelope(*batch);

    auto completion = std::make_shared<std::promise<bool>>();
    std::future<bool> completion_future = completion->get_future();

    // Passing a serialized payload makes PublishRecoveryManifestToWitnesses
    // use the existing fixed-R all-witness durability rule. The frontier-aware
    // raylet handler recognizes the versioned envelope, applies the group
    // manifest through the legacy witness path, then atomically advances its
    // local capsule prefix.
    PublishRecoveryManifestToWitnesses(
        protection_manifest,
        [manager = recovery_succession_manager_,
         group_id,
         batch,
         completion](bool stored,
                     std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
          if (!stored) {
            const bool aborted =
                manager->AbortRecoveryFrontierAppend(*batch);
            RAY_CHECK(aborted)
                << "Failed to abort Recovery Frontier append generation "
                << batch->generation << " for group " << group_id;

            // Resolve the local waiter before the fatal log so tests or crash
            // handlers never leave a blocked owner thread behind.
            completion->set_value(false);

            // Keep fixed-R semantics strict. A partial append must never
            // advance the owner committed prefix.
            RAY_LOG(FATAL)
                .WithField(group_id)
                << "Recovery Frontier failed to install append generation "
                << batch->generation << " on every fixed-R holder."
                << (newer_manifest.has_value()
                        ? " A newer holder manifest was observed."
                        : "");
            return;
          }

          const bool committed =
              manager->CommitRecoveryFrontierAppend(*batch);
          RAY_CHECK(committed)
              << "Stale or mismatched Recovery Frontier ACK for generation "
              << batch->generation << " group " << group_id;

          RAY_LOG(INFO)
              .WithField(group_id)
              << "Committed Recovery Frontier append generation "
              << batch->generation << " members=["
              << batch->begin_member_index << ","
              << batch->end_member_index << ") on all fixed-R holders";

          completion->set_value(true);
        },
        /*task_spec=*/nullptr,
        &serialized_append);

    // CoreWorker's ordinary object-export APIs are synchronous. Waiting here
    // therefore preserves their contract while making durability and metadata
    // visibility atomic from the caller's perspective.
    const bool committed = completion_future.get();
    RAY_CHECK(committed)
        << "Recovery Frontier publication returned without a durable prefix for group "
        << group_id;
  }
}

}  // namespace ray::core
