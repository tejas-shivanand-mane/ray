from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))


# Capture the commit-visibility mode used by the per-argument lambda.
replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    "auto populate_one = [this, task_spec, &attached_object_ids](\n",
    "auto populate_one = [this, task_spec, &attached_object_ids, require_frontier_commit](\n",
)

# Keep explicit ObjectRef/status export on the old blocking loop. The async
# publisher may schedule a retry on io_service_; a synchronous caller could
# itself be on that event loop, so wrapping Async()+future.get() risks deadlock.
path = "src/ray/core_worker/recovery_frontier_publication.cc"
p = Path(path)
text = p.read_text()
if "#include <thread>\n" not in text:
    text = text.replace("#include <string>\n", "#include <string>\n#include <thread>\n", 1)
start = text.index("void CoreWorker::PublishRecoveryFrontierGroup(\n")
end = text.index("\n\n}  // namespace ray::core", start)
replacement = r'''void CoreWorker::PublishRecoveryFrontierGroup(
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
'''
text = text[:start] + replacement + text[end:]
p.write_text(text)

print("Deferred Recovery Frontier follow-up fixes applied")
