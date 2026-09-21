// Copyright 2026 The Ray Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/core_worker.h"

#include "ray/common/ray_config.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/task_submission/normal_task_submitter.h"

namespace ray::core {

Status CoreWorker::PrepareRecoveryStreamSubmission(TaskSpecification *spec,
                                                  const TaskOptions &options) {
  if (!recovery_succession_enabled_ || !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !RayConfig::instance().enable_recovery_streaming_fixed_r() ||
      RayConfig::instance().recovery_succession_target_holder_count() == 0 ||
      RayConfig::instance().recovery_frontier_group_size() != 1 ||
      RayConfig::instance().recovery_baseline_perf_protect_every_n() != 1) {
    return Status::Invalid("Streaming enrollment requires explicit Fixed-R streaming K=1");
  }
  rpc::RecoveryStreamDescriptor descriptor;
  descriptor.set_version(1);
  descriptor.set_task_id(spec->TaskId().Binary());
  descriptor.set_generator_id(ObjectID::FromIndex(spec->TaskId(), 1).Binary());
  descriptor.set_expected_returns(options.recovery_stream_expected_returns);
  descriptor.mutable_consumer_address()->CopyFrom(options.recovery_stream_consumer);
  auto manifest = recovery_succession_manager_->BuildInitialManifest(
      spec->TaskId(), spec->JobId(), spec->MaxRetries());
  PopulateRecoveryWitnesses(&manifest);
  descriptor.mutable_manifest()->CopyFrom(manifest);

  // Validate a temporary recipe before acquiring any pending-task refs. Keep
  // the ordinary recovery_manifest absent: the static manager must not enroll
  // this stream through its lazy-export or static-return lifetime paths.
  rpc::TaskSpec recipe(spec->GetMessage());
  recipe.mutable_recovery_stream_descriptor()->CopyFrom(descriptor);
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamRecipe(recipe));
  spec->GetMutableMessage().mutable_recovery_stream_descriptor()->CopyFrom(descriptor);
  return Status::OK();
}

void CoreWorker::PublishRecoveryStreamSubmission(const TaskSpecification &spec) {
  const TaskID task_id = spec.TaskId();
  auto state = std::make_shared<RecoveryStreamSubmission>();
  state->recipe.CopyFrom(spec.GetMessage());
  const auto &descriptor = state->recipe.recovery_stream_descriptor();
  RAY_CHECK_OK(state->installation.Initialize(descriptor));
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    RAY_CHECK(recovery_stream_submissions_.emplace(task_id, state).second);
  }

  // Associate each callback with the exact descriptor/full recipe sent to this
  // selected holder. A compact-manifest ACK must never count as a recipe ACK.
  for (const auto &witness : descriptor.manifest().witness_raylets()) {
    rpc::UpdateRecoveryWitnessRequest request;
    request.mutable_manifest()->CopyFrom(descriptor.manifest());
    request.mutable_task_spec()->CopyFrom(state->recipe);
    request.mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(
        descriptor.manifest());
    raylet_client_pool_->GetOrConnectByAddress(witness)->UpdateRecoveryWitness(
        std::move(request),
        [this, task_id, state, witness](
            const Status &status, rpc::UpdateRecoveryWitnessReply &&reply) {
          bool failed = false;
          {
            std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
            if (state->cancelled) {
              return;
            }
            const Status recorded =
                state->installation.RecordWitnessReply(witness, status, reply);
            if (!recorded.ok()) {
              state->status = recorded;
              failed = true;
            }
          }
          if (failed) {
            CancelRecoveryStreamSubmission(ObjectID::FromIndex(task_id, 1));
          } else {
            MaybeDispatchRecoveryStream(task_id, state);
          }
        });
  }
}

Status CoreWorker::GetStreamingRecoverySubmission(const ObjectID &generator_id,
                                                 std::string *descriptor,
                                                 bool *ready) const {
  if (descriptor == nullptr || ready == nullptr || generator_id.ObjectIndex() != 1) {
    return Status::Invalid("Expected an original streaming completion ID");
  }
  std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
  const auto it = recovery_stream_submissions_.find(generator_id.TaskId());
  if (it == recovery_stream_submissions_.end()) {
    return Status::Invalid("No local streaming enrollment for this generator");
  }
  RAY_RETURN_NOT_OK(it->second->status);
  *descriptor = it->second->recipe.recovery_stream_descriptor().SerializeAsString();
  *ready = it->second->dispatch_scheduled && !it->second->cancelled;
  return Status::OK();
}

Status CoreWorker::ConfirmStreamingRecoveryReceipt(
    const ObjectID &generator_id,
    const std::string &serialized_descriptor,
    const std::string &serialized_consumer_address) {
  rpc::RecoveryStreamDescriptor descriptor;
  rpc::Address consumer;
  if (generator_id.ObjectIndex() != 1 ||
      !descriptor.ParseFromString(serialized_descriptor) ||
      !consumer.ParseFromString(serialized_consumer_address)) {
    return Status::Invalid("Malformed streaming consumer receipt");
  }
  std::shared_ptr<RecoveryStreamSubmission> state;
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    const auto it = recovery_stream_submissions_.find(generator_id.TaskId());
    if (it == recovery_stream_submissions_.end()) {
      return Status::Invalid("No local streaming enrollment for this receipt");
    }
    state = it->second;
    RAY_RETURN_NOT_OK(state->status);
    RAY_RETURN_NOT_OK(state->installation.RecordConsumerReceipt(consumer, descriptor));
  }
  MaybeDispatchRecoveryStream(generator_id.TaskId(), state);
  return Status::OK();
}

void CoreWorker::MaybeDispatchRecoveryStream(
    const TaskID &task_id, const std::shared_ptr<RecoveryStreamSubmission> &state) {
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    if (state->cancelled || state->dispatch_scheduled || !state->status.ok() ||
        !state->installation.IsReady()) {
      return;
    }
    state->dispatch_scheduled = true;
  }
  io_service_.post(
      [this, task_id, state] {
        {
          std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
          if (state->cancelled) {
            return;
          }
          state->submitted = true;
        }
        RAY_LOG(INFO).WithField(task_id)
            << "Dispatching enrolled stream after all holder ACKs and consumer receipt";
        // Keep a separate immutable owner recipe through EOF. TaskManager may
        // update its own TaskSpec's observed count during ordinary completion.
        normal_task_submitter_->SubmitTask(TaskSpecification(state->recipe));
      },
      "CoreWorker.DispatchEnrolledStream");
}

bool CoreWorker::CancelRecoveryStreamSubmission(const ObjectID &object_id,
                                               bool force_kill,
                                               bool recursive,
                                               bool only_if_unready) {
  std::shared_ptr<RecoveryStreamSubmission> state;
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    const auto it = recovery_stream_submissions_.find(object_id.TaskId());
    if (it == recovery_stream_submissions_.end()) {
      return false;
    }
    state = it->second;
    if (only_if_unready && state->dispatch_scheduled) {
      return false;
    }
    // Preserve ray.cancel's ability to escalate an earlier graceful request.
    if (state->cancelled && !force_kill) {
      return true;
    }
    state->cancelled = true;
    state->installation.Cancel();
    if (state->status.ok()) {
      state->status = Status::Invalid("Streaming enrollment was cancelled");
    }
  }
  // Execute cancellation on the same IO loop as dispatch. Cancellation of a
  // held task must settle TaskManager directly because no submitter owns it yet.
  io_service_.post(
      [this, state, force_kill, recursive] {
        const TaskID task_id = TaskID::FromBinary(state->recipe.task_id());
        bool submitted;
        Status failure;
        {
          std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
          submitted = state->submitted;
          failure = state->status;
        }
        if (!submitted) {
          task_manager_->FailPendingTask(task_id, rpc::ErrorType::TASK_CANCELLED, &failure);
        } else {
          auto task = task_manager_->GetTaskSpec(task_id);
          if (task.has_value()) {
            normal_task_submitter_->CancelTask(*task, force_kill, recursive);
          }
        }
        rpc::RecoveryManifest tombstone(
            state->recipe.recovery_stream_descriptor().manifest());
        tombstone.set_tombstoned(true);
        tombstone.mutable_version()->set_generation(tombstone.version().generation() + 1);
        PublishRecoveryManifestToWitnesses(
            tombstone,
            [task_id](bool stored, std::optional<rpc::RecoveryManifest>) {
              if (!stored) {
                RAY_LOG(WARNING).WithField(task_id)
                    << "Streaming cancellation tombstone did not receive a witness ACK";
              }
            });
      },
      "CoreWorker.CancelEnrolledStream");
  return true;
}

void CoreWorker::RetireRecoveryStreamSubmission(const ObjectID &generator_id) {
  if (CancelRecoveryStreamSubmission(generator_id)) {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    recovery_stream_submissions_.erase(generator_id.TaskId());
  }
}

void CoreWorker::CancelUnreadyRecoveryStreamSubmissions() {
  std::vector<ObjectID> held;
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    for (const auto &[task_id, state] : recovery_stream_submissions_) {
      if (!state->dispatch_scheduled) {
        held.push_back(ObjectID::FromIndex(task_id, 1));
      }
    }
  }
  for (const auto &generator_id : held) {
    // Recheck under the cancellation lock: receipt/ACK could have won since
    // the snapshot. Tasks already released to the submitter drain normally.
    CancelRecoveryStreamSubmission(generator_id,
                                   /*force_kill=*/false,
                                   /*recursive=*/true,
                                   /*only_if_unready=*/true);
  }
}

}  // namespace ray::core
