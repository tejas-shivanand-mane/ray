from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))

# Add per-group single-flight publication state.
replace_once(
    "src/ray/core_worker/core_worker.h",
    '''  // Locally returned task refs can precede upstream Frontier durability, but\n  // chained activation must never cross that boundary.\n  mutable std::mutex recovery_frontier_deferred_task_mutex_;\n  mutable absl::flat_hash_map<TaskID, std::shared_ptr<DeferredRecoveryTaskState>>\n      recovery_frontier_deferred_tasks_;\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n''',
    '''  // Locally returned task refs can precede upstream Frontier durability, but\n  // chained activation must never cross that boundary.\n  mutable std::mutex recovery_frontier_deferred_task_mutex_;\n  mutable absl::flat_hash_map<TaskID, std::shared_ptr<DeferredRecoveryTaskState>>\n      recovery_frontier_deferred_tasks_;\n\n  struct RecoveryFrontierPublicationState {\n    bool driving = false;\n    rpc::RecoveryManifest protection_manifest;\n    std::vector<RecoveryFrontierPublicationCallback> waiters;\n  };\n\n  // Single-flight publication per frontier group. The first downstream task\n  // drives the holder append; later tasks only register completion callbacks.\n  mutable std::mutex recovery_frontier_publication_mutex_;\n  mutable absl::flat_hash_map<\n      TaskID, std::shared_ptr<RecoveryFrontierPublicationState>>\n      recovery_frontier_publications_;\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n''')

# Replace the async publication driver with single-flight coalescing.
p = Path("src/ray/core_worker/recovery_frontier_publication.cc")
text = p.read_text()
start = text.index("void CoreWorker::PublishRecoveryFrontierGroupAsync(\n")
end = text.index("\n\nvoid CoreWorker::PublishRecoveryFrontierGroup(\n", start)
new_async = r'''void CoreWorker::PublishRecoveryFrontierGroupAsync(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest,
    RecoveryFrontierPublicationCallback callback) const {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !recovery_succession_manager_->RecoveryFrontierEnabled() ||
      group_id.IsNil() ||
      protection_manifest.task_id() != group_id.Binary()) {
    if (callback) {
      callback();
    }
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

  std::shared_ptr<RecoveryFrontierPublicationState> state;
  {
    std::lock_guard<std::mutex> lock(recovery_frontier_publication_mutex_);
    auto it = recovery_frontier_publications_.find(group_id);
    if (it == recovery_frontier_publications_.end()) {
      state = std::make_shared<RecoveryFrontierPublicationState>();
      state->protection_manifest.CopyFrom(protection_manifest);
      recovery_frontier_publications_.emplace(group_id, state);
    } else {
      state = it->second;
      RAY_CHECK_EQ(state->protection_manifest.task_id(),
                   protection_manifest.task_id())
          << "Recovery Frontier single-flight topology changed for group "
          << group_id;
    }

    if (callback) {
      state->waiters.push_back(std::move(callback));
    }

    if (state->driving) {
      return;
    }
    state->driving = true;
  }

  // Finalization is checked while holding the publication mutex. If a new
  // member registers after this check, its later Async() call creates a fresh
  // single-flight generation. Existing waiters depend only on the prefix that
  // is already durable and may safely dispatch.
  if (!recovery_succession_manager_
           ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
    std::vector<RecoveryFrontierPublicationCallback> waiters;
    bool continue_driving = false;
    {
      std::lock_guard<std::mutex> lock(recovery_frontier_publication_mutex_);
      auto it = recovery_frontier_publications_.find(group_id);
      if (it == recovery_frontier_publications_.end() || it->second != state) {
        return;
      }

      if (recovery_succession_manager_
              ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
        state->driving = false;
        continue_driving = true;
      } else {
        waiters.swap(state->waiters);
        recovery_frontier_publications_.erase(it);
      }
    }

    if (continue_driving) {
      PublishRecoveryFrontierGroupAsync(
          group_id, state->protection_manifest, {});
      return;
    }

    for (auto &waiter : waiters) {
      if (waiter) {
        waiter();
      }
    }
    return;
  }

  auto staged = recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
  if (!staged.has_value()) {
    // A synchronous explicit-export path may currently own the append. Keep a
    // single delayed retry for the whole group instead of one retry loop per
    // downstream task.
    io_service_.post(
        [this, group_id, state]() mutable {
          {
            std::lock_guard<std::mutex> lock(
                recovery_frontier_publication_mutex_);
            auto it = recovery_frontier_publications_.find(group_id);
            if (it == recovery_frontier_publications_.end() ||
                it->second != state) {
              return;
            }
            state->driving = false;
          }
          PublishRecoveryFrontierGroupAsync(
              group_id, state->protection_manifest, {});
        },
        "CoreWorker.RetryRecoveryFrontierPublication",
        /*delay_us=*/50);
    return;
  }

  auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
      std::move(staged.value()));
  const std::string serialized_append = BuildRecoveryFrontierAppendEnvelope(*batch);
  const uint64_t publish_start_ns =
      recovery_succession_profiling_enabled_
          ? RecoveryFrontierProfileNowNs()
          : 0;

  PublishRecoveryManifestToWitnesses(
      protection_manifest,
      [this,
       manager = recovery_succession_manager_,
       group_id,
       state,
       batch,
       publish_start_ns](
          bool stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryFrontierProfileNowNs() - publish_start_ns);
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

        {
          std::lock_guard<std::mutex> lock(
              recovery_frontier_publication_mutex_);
          auto it = recovery_frontier_publications_.find(group_id);
          if (it == recovery_frontier_publications_.end() ||
              it->second != state) {
            return;
          }
          state->driving = false;
        }

        PublishRecoveryFrontierGroupAsync(
            group_id, state->protection_manifest, {});
      },
      /*task_spec=*/nullptr,
      &serialized_append);
}
'''
text = text[:start] + new_async + text[end:]
p.write_text(text)

print("Recovery Frontier single-flight publication patch applied")
