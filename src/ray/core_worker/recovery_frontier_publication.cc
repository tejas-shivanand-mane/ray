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

void CoreWorker::PublishRecoveryFrontierGroupAsync(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest,
    RecoveryFrontierPublicationCallback callback) const {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !recovery_succession_manager_->RecoveryFrontierEnabled() ||
      group_id.IsNil() ||
      protection_manifest.task_id() != group_id.Binary()) {
    callback();
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

  if (!recovery_succession_manager_
           ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
    callback();
    return;
  }

  auto staged = recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
  if (!staged.has_value()) {
    io_service_.post(
        [this,
         group_id,
         protection_manifest,
         callback = std::move(callback)]() mutable {
          PublishRecoveryFrontierGroupAsync(
              group_id, protection_manifest, std::move(callback));
        },
        "CoreWorker.RetryRecoveryFrontierPublication",
        /*delay_us=*/50);
    return;
  }

  auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
      std::move(staged.value()));
  const std::string serialized_append = BuildRecoveryFrontierAppendEnvelope(*batch);
  const uint64_t publish_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  PublishRecoveryManifestToWitnesses(
      protection_manifest,
      [this,
       manager = recovery_succession_manager_,
       group_id,
       protection_manifest,
       batch,
       publish_start_ns,
       callback = std::move(callback)](
          bool stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - publish_start_ns);
        }

        if (!stored) {
          const bool aborted = manager->AbortRecoveryFrontierAppend(*batch);
          RAY_CHECK(aborted)
              << "Failed to abort Recovery Frontier append generation "
              << batch->generation << " for group " << group_id;
          RAY_LOG(FATAL)
              .WithField(group_id)
              << "Recovery Frontier failed to install append generation "
              << batch->generation << " on every fixed-R holder."
              << (newer_manifest.has_value()
                      ? " A newer holder manifest was observed."
                      : "");
          return;
        }

        const bool committed = manager->CommitRecoveryFrontierAppend(*batch);
        RAY_CHECK(committed)
            << "Stale or mismatched Recovery Frontier ACK for generation "
            << batch->generation << " group " << group_id;

        RAY_LOG(INFO)
            .WithField(group_id)
            << "Committed Recovery Frontier append generation "
            << batch->generation << " members=["
            << batch->begin_member_index << ","
            << batch->end_member_index << ") on all fixed-R holders";

        PublishRecoveryFrontierGroupAsync(
            group_id, protection_manifest, std::move(callback));
      },
      /*task_spec=*/nullptr,
      &serialized_append);
}

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
      target_holder_count);
  RAY_CHECK_EQ(protection_manifest.witness_count(), target_holder_count);

  while (recovery_succession_manager_
             ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
    auto staged = recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
    if (!staged.has_value()) {
      // An asynchronous normal-task dispatch may own the append. Explicit
      // serialization keeps the old blocking visibility contract without
      // depending on io_service_ progress.
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }

    auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
        std::move(staged.value()));
    const std::string serialized_append = BuildRecoveryFrontierAppendEnvelope(*batch);
    auto completion = std::make_shared<std::promise<bool>>();
    std::future<bool> completion_future = completion->get_future();
    const uint64_t publish_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

    PublishRecoveryManifestToWitnesses(
        protection_manifest,
        [this,
         manager = recovery_succession_manager_,
         group_id,
         batch,
         publish_start_ns,
         completion](bool stored,
                     std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
          if (publish_start_ns != 0) {
            manager->RecordWitnessPublishLatency(
                RecoveryProfileNowNs() - publish_start_ns);
          }

          if (!stored) {
            const bool aborted = manager->AbortRecoveryFrontierAppend(*batch);
            RAY_CHECK(aborted)
                << "Failed to abort Recovery Frontier append generation "
                << batch->generation << " for group " << group_id;
            completion->set_value(false);
            RAY_LOG(FATAL)
                .WithField(group_id)
                << "Recovery Frontier failed to install append generation "
                << batch->generation << " on every fixed-R holder."
                << (newer_manifest.has_value()
                        ? " A newer holder manifest was observed."
                        : "");
            return;
          }

          const bool committed = manager->CommitRecoveryFrontierAppend(*batch);
          RAY_CHECK(committed)
              << "Stale or mismatched Recovery Frontier ACK for generation "
              << batch->generation << " group " << group_id;
          completion->set_value(true);
        },
        /*task_spec=*/nullptr,
        &serialized_append);

    RAY_CHECK(completion_future.get())
        << "Recovery Frontier synchronous publication failed for group " << group_id;
  }
}


}  // namespace ray::core
