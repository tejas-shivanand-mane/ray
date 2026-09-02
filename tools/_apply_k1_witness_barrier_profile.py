#!/usr/bin/env python3
from __future__ import annotations

import subprocess
from pathlib import Path

CHECKPOINT = "1ce790c5b20620b961ad62ff75cbad964d7ac287"


def restore(path: str) -> None:
    data = subprocess.check_output(["git", "show", f"{CHECKPOINT}:{path}"])
    Path(path).write_bytes(data)


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected 1 occurrence, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


def replace_n(path: str, old: str, new: str, expected: int) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != expected:
        raise RuntimeError(
            f"{path}: expected {expected} occurrences, found {count}: {old[:120]!r}"
        )
    p.write_text(text.replace(old, new))


# Remove the rejected witness-coordinator experiment by restoring every file it
# touched to the validated checkpoint. The rest of this script adds profiling
# only; healthy non-profiling behavior must remain checkpoint-equivalent.
for path in [
    "src/ray/core_worker/core_worker.cc",
    "src/ray/raylet/node_manager.cc",
    "src/ray/protobuf/node_manager.proto",
]:
    restore(path)

# ---------------------------------------------------------------------------
# Protobuf: return profiling metadata with a logical witness reply. These
# fields are diagnostic only and remain zero/unset when profiling is disabled.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/protobuf/node_manager.proto",
    """message UpdateRecoveryWitnessReply {\n  bool stored = 1;\n  optional RecoveryManifest latest_manifest = 2;\n  // Returned on a conflicting Fixed-R claim so the coordinator can fail\n  // closed or retry while authoritative failure information converges.\n  optional RecoveryWitnessClaim latest_recovery_claim = 3;\n}\n""",
    """message UpdateRecoveryWitnessReply {\n  bool stored = 1;\n  optional RecoveryManifest latest_manifest = 2;\n  // Returned on a conflicting Fixed-R claim so the coordinator can fail\n  // closed or retry while authoritative failure information converges.\n  optional RecoveryWitnessClaim latest_recovery_claim = 3;\n\n  // Profiling-only timing metadata for Benchmark 70. These fields do not\n  // participate in recovery correctness and remain zero when profiling is off.\n  uint64 client_queue_time_ns = 4;\n  uint64 witness_handler_time_ns = 5;\n  uint64 witness_mutex_wait_time_ns = 6;\n  uint64 witness_mutex_hold_time_ns = 7;\n  uint32 client_batch_size = 8;\n  bool client_batch_leader = 9;\n  uint64 witness_batch_queue_time_ns = 10;\n}\n""",
)

# ---------------------------------------------------------------------------
# RayletClient: measure how long each logical update waits behind the one
# in-flight witness batch and expose the physical batch shape to the owner.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.h",
    """  struct PendingRecoveryWitnessUpdate {\n    rpc::UpdateRecoveryWitnessRequest request;\n    rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> callback;\n  };\n""",
    """  struct PendingRecoveryWitnessUpdate {\n    rpc::UpdateRecoveryWitnessRequest request;\n    rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> callback;\n    bool profiling = false;\n    uint64_t enqueue_time_ns = 0;\n    uint64_t client_queue_time_ns = 0;\n  };\n""",
)

replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    """#include <algorithm>\n#include <limits>\n""",
    """#include <algorithm>\n#include <chrono>\n#include <limits>\n""",
)

replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    """namespace ray {\nnamespace rpc {\n\nRayletClient::RayletClient""",
    """namespace ray {\nnamespace rpc {\n\nnamespace {\nuint64_t RecoveryWitnessClientProfileNowNs() {\n  return static_cast<uint64_t>(\n      std::chrono::duration_cast<std::chrono::nanoseconds>(\n          std::chrono::steady_clock::now().time_since_epoch())\n          .count());\n}\n}  // namespace\n\nRayletClient::RayletClient""",
)

replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    """  PendingRecoveryWitnessUpdate item{std::move(request), callback};\n  auto state = recovery_witness_batch_state_;\n""",
    """  PendingRecoveryWitnessUpdate item{std::move(request), callback};\n  item.profiling =\n      ::RayConfig::instance().enable_recovery_succession_profiling();\n  if (item.profiling) {\n    item.enqueue_time_ns = RecoveryWitnessClientProfileNowNs();\n  }\n  auto state = recovery_witness_batch_state_;\n""",
)

replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    """  rpc::UpdateRecoveryWitnessBatchRequest request;\n  for (auto &item : *batch) {\n    rpc::UpdateRecoveryWitnessRequest *update = request.add_updates();\n""",
    """  rpc::UpdateRecoveryWitnessBatchRequest request;\n  bool profile_batch = false;\n  for (const auto &item : *batch) {\n    profile_batch = profile_batch || item.profiling;\n  }\n  const uint64_t dispatch_ns =\n      profile_batch ? RecoveryWitnessClientProfileNowNs() : 0;\n\n  for (auto &item : *batch) {\n    if (item.profiling && item.enqueue_time_ns != 0) {\n      item.client_queue_time_ns = dispatch_ns - item.enqueue_time_ns;\n    }\n    rpc::UpdateRecoveryWitnessRequest *update = request.add_updates();\n""",
)

replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    """          // Transport failures retain their non-OK status. A malformed\n          // successful batch reply yields the default stored=false item reply,\n          // which safely fails that logical witness update.\n          (*batch)[i].callback(status, std::move(item_reply));\n""",
    """          // Transport failures retain their non-OK status. A malformed\n          // successful batch reply yields the default stored=false item reply,\n          // which safely fails that logical witness update.\n          if ((*batch)[i].profiling) {\n            item_reply.set_client_queue_time_ns((*batch)[i].client_queue_time_ns);\n            item_reply.set_client_batch_size(\n                static_cast<uint32_t>(batch->size()));\n            item_reply.set_client_batch_leader(i == 0);\n          }\n          (*batch)[i].callback(status, std::move(item_reply));\n""",
)

# ---------------------------------------------------------------------------
# Witness raylet: profile total handler time and, for the ordinary full-manifest
# path used by B70, split recovery_witness_mutex_ wait/hold. Also account for
# serial position inside one physical batch.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/raylet/node_manager.cc",
    """#include <algorithm>\n#include <boost/bind/bind.hpp>\n#include <cctype>\n""",
    """#include <algorithm>\n#include <boost/bind/bind.hpp>\n#include <chrono>\n#include <cctype>\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """#include <google/protobuf/util/message_differencer.h>\n\n#include \"absl/strings/str_format.h\"\n""",
    """#include <google/protobuf/util/message_differencer.h>\n\n#include \"absl/cleanup/cleanup.h\"\n#include \"absl/strings/str_format.h\"\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """int CompareRecoveryManifestVersions(const rpc::RecoveryManifest &left,\n                                    const rpc::RecoveryManifest &right) {\n  if (left.version().generation() < right.version().generation()) {\n    return -1;\n  }\n\n  if (left.version().generation() > right.version().generation()) {\n    return 1;\n  }\n\n  return 0;\n}\n\n""",
    """int CompareRecoveryManifestVersions(const rpc::RecoveryManifest &left,\n                                    const rpc::RecoveryManifest &right) {\n  if (left.version().generation() < right.version().generation()) {\n    return -1;\n  }\n\n  if (left.version().generation() > right.version().generation()) {\n    return 1;\n  }\n\n  return 0;\n}\n\nuint64_t RecoveryWitnessProfileNowNs() {\n  return static_cast<uint64_t>(\n      std::chrono::duration_cast<std::chrono::nanoseconds>(\n          std::chrono::steady_clock::now().time_since_epoch())\n          .count());\n}\n\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """void NodeManager::HandleUpdateRecoveryWitness(\n    rpc::UpdateRecoveryWitnessRequest request,\n    rpc::UpdateRecoveryWitnessReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  if (!RayConfig::instance().enable_recovery_succession()) {\n""",
    """void NodeManager::HandleUpdateRecoveryWitness(\n    rpc::UpdateRecoveryWitnessRequest request,\n    rpc::UpdateRecoveryWitnessReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  const bool profile_witness =\n      RayConfig::instance().enable_recovery_succession_profiling();\n  const uint64_t handler_start_ns =\n      profile_witness ? RecoveryWitnessProfileNowNs() : 0;\n  absl::Cleanup record_handler_time = [reply, handler_start_ns]() {\n    if (handler_start_ns != 0) {\n      reply->set_witness_handler_time_ns(\n          RecoveryWitnessProfileNowNs() - handler_start_ns);\n    }\n  };\n\n  if (!RayConfig::instance().enable_recovery_succession()) {\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """  {\n    absl::MutexLock lock(&recovery_witness_mutex_);\n\n    auto existing_it = recovery_witness_manifests_.find(task_id);\n""",
    """  const uint64_t witness_mutex_wait_start_ns =\n      profile_witness ? RecoveryWitnessProfileNowNs() : 0;\n  uint64_t witness_mutex_acquired_ns = 0;\n  {\n    absl::MutexLock lock(&recovery_witness_mutex_);\n    if (witness_mutex_wait_start_ns != 0) {\n      witness_mutex_acquired_ns = RecoveryWitnessProfileNowNs();\n      reply->set_witness_mutex_wait_time_ns(\n          witness_mutex_acquired_ns - witness_mutex_wait_start_ns);\n    }\n\n    auto existing_it = recovery_witness_manifests_.find(task_id);\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """      } else {\n        auto task_spec_it = recovery_witness_task_specs_.find(task_id);\n        if (task_spec_it != recovery_witness_task_specs_.end() &&\n            !baseline_enabled) {\n          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);\n        }\n      }\n    }\n  }\n\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
    """      } else {\n        auto task_spec_it = recovery_witness_task_specs_.find(task_id);\n        if (task_spec_it != recovery_witness_task_specs_.end() &&\n            !baseline_enabled) {\n          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);\n        }\n      }\n    }\n\n    if (witness_mutex_acquired_ns != 0) {\n      reply->set_witness_mutex_hold_time_ns(\n          RecoveryWitnessProfileNowNs() - witness_mutex_acquired_ns);\n    }\n  }\n\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
)
replace_one(
    "src/ray/raylet/node_manager.cc",
    """void NodeManager::HandleUpdateRecoveryWitnessBatch(\n    rpc::UpdateRecoveryWitnessBatchRequest request,\n    rpc::UpdateRecoveryWitnessBatchReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  // Reuse the single-update implementation so batching cannot diverge from\n  // the existing validation/versioning/baseline semantics. The single-item\n  // handler is synchronous; its send callback only marks that logical item\n  // complete, so a no-op callback is sufficient inside this outer RPC.\n  for (int i = 0; i < request.updates_size(); ++i) {\n    rpc::UpdateRecoveryWitnessRequest item_request;\n    item_request.Swap(request.mutable_updates(i));\n\n    auto *item_reply = reply->add_replies();\n    HandleUpdateRecoveryWitness(\n        std::move(item_request),\n        item_reply,\n        [](Status, std::function<void()>, std::function<void()>) {});\n  }\n\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
    """void NodeManager::HandleUpdateRecoveryWitnessBatch(\n    rpc::UpdateRecoveryWitnessBatchRequest request,\n    rpc::UpdateRecoveryWitnessBatchReply *reply,\n    rpc::SendReplyCallback send_reply_callback) {\n  // Reuse the single-update implementation so batching cannot diverge from\n  // the existing validation/versioning/baseline semantics. The single-item\n  // handler is synchronous; its send callback only marks that logical item\n  // complete, so a no-op callback is sufficient inside this outer RPC.\n  const bool profile_witness =\n      RayConfig::instance().enable_recovery_succession_profiling();\n  const uint64_t batch_start_ns =\n      profile_witness ? RecoveryWitnessProfileNowNs() : 0;\n\n  for (int i = 0; i < request.updates_size(); ++i) {\n    rpc::UpdateRecoveryWitnessRequest item_request;\n    item_request.Swap(request.mutable_updates(i));\n\n    const uint64_t item_start_ns =\n        profile_witness ? RecoveryWitnessProfileNowNs() : 0;\n    auto *item_reply = reply->add_replies();\n    HandleUpdateRecoveryWitness(\n        std::move(item_request),\n        item_reply,\n        [](Status, std::function<void()>, std::function<void()>) {});\n    if (item_start_ns != 0) {\n      item_reply->set_witness_batch_queue_time_ns(\n          item_start_ns - batch_start_ns);\n    }\n  }\n\n  send_reply_callback(Status::OK(), nullptr, nullptr);\n}\n""",
)

# ---------------------------------------------------------------------------
# RecoverySuccessionManager profile accounting.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/core_worker/recovery_succession_manager.h",
    """    uint64_t witness_update_rpcs_sent = 0;\n    uint64_t witness_update_rpcs_completed = 0;\n\n    // Wall-clock latency of the whole witness-publication stage.\n""",
    """    uint64_t witness_update_rpcs_sent = 0;\n    uint64_t witness_update_rpcs_completed = 0;\n\n    // Benchmark-70 witness barrier decomposition. All timing sums are per\n    // logical witness update; physical batch counters are derived from the\n    // client-side demultiplexing metadata.\n    uint64_t witness_update_client_queue_time_ns = 0;\n    uint64_t witness_update_server_batch_queue_time_ns = 0;\n    uint64_t witness_update_handler_time_ns = 0;\n    uint64_t witness_update_mutex_wait_time_ns = 0;\n    uint64_t witness_update_mutex_hold_time_ns = 0;\n    uint64_t witness_update_physical_batches_completed = 0;\n    uint64_t witness_update_physical_batch_items = 0;\n\n    // Opportunistic H2 readiness sampled at the instant ordinary K=1 H1\n    // begins witness publication. This does not delay H1.\n    uint64_t h1_publish_readiness_samples = 0;\n    uint64_t h2_reserved_at_h1_publish = 0;\n    uint64_t h2_installed_at_h1_publish = 0;\n\n    // Wall-clock latency of the whole witness-publication stage.\n""",
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.h",
    """  void RecordWitnessUpdateRpcLatency(uint64_t latency_ns);\n\n  void RecordWitnessPublishLatency(uint64_t latency_ns);\n""",
    """  void RecordWitnessUpdateRpcLatency(uint64_t latency_ns);\n\n  void RecordWitnessUpdateRpcBreakdown(uint64_t client_queue_ns,\n                                       uint64_t server_batch_queue_ns,\n                                       uint64_t handler_ns,\n                                       uint64_t mutex_wait_ns,\n                                       uint64_t mutex_hold_ns,\n                                       bool batch_leader,\n                                       uint32_t batch_size);\n\n  void RecordH2ReadinessAtH1Publish(bool h2_reserved, bool h2_installed);\n\n  void RecordWitnessPublishLatency(uint64_t latency_ns);\n""",
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.cc",
    """void RecoverySuccessionManager::RecordWitnessUpdateRpcLatency(\n    uint64_t latency_ns) {\n  if (!profiling_enabled_) {\n    return;\n  }\n\n  absl::MutexLock lock(&mutex_);\n\n  ++profile_.witness_update_rpcs_completed;\n  profile_.witness_update_rpc_time_ns += latency_ns;\n}\n\n\nvoid RecoverySuccessionManager::RecordWitnessPublishLatency(\n""",
    """void RecoverySuccessionManager::RecordWitnessUpdateRpcLatency(\n    uint64_t latency_ns) {\n  if (!profiling_enabled_) {\n    return;\n  }\n\n  absl::MutexLock lock(&mutex_);\n\n  ++profile_.witness_update_rpcs_completed;\n  profile_.witness_update_rpc_time_ns += latency_ns;\n}\n\nvoid RecoverySuccessionManager::RecordWitnessUpdateRpcBreakdown(\n    uint64_t client_queue_ns,\n    uint64_t server_batch_queue_ns,\n    uint64_t handler_ns,\n    uint64_t mutex_wait_ns,\n    uint64_t mutex_hold_ns,\n    bool batch_leader,\n    uint32_t batch_size) {\n  if (!profiling_enabled_) {\n    return;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  profile_.witness_update_client_queue_time_ns += client_queue_ns;\n  profile_.witness_update_server_batch_queue_time_ns += server_batch_queue_ns;\n  profile_.witness_update_handler_time_ns += handler_ns;\n  profile_.witness_update_mutex_wait_time_ns += mutex_wait_ns;\n  profile_.witness_update_mutex_hold_time_ns += mutex_hold_ns;\n  if (batch_leader) {\n    ++profile_.witness_update_physical_batches_completed;\n    profile_.witness_update_physical_batch_items += batch_size;\n  }\n}\n\nvoid RecoverySuccessionManager::RecordH2ReadinessAtH1Publish(\n    bool h2_reserved, bool h2_installed) {\n  if (!profiling_enabled_) {\n    return;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  ++profile_.h1_publish_readiness_samples;\n  if (h2_reserved) {\n    ++profile_.h2_reserved_at_h1_publish;\n  }\n  if (h2_installed) {\n    ++profile_.h2_installed_at_h1_publish;\n  }\n}\n\n\nvoid RecoverySuccessionManager::RecordWitnessPublishLatency(\n""",
)

# ---------------------------------------------------------------------------
# CoreWorker: export the new profile fields, sample H2 readiness without any
# wait, and consume reply timing metadata in existing witness callbacks.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/core_worker/core_worker.cc",
    """  result[\"witness_update_rpcs_completed\"] =\n      profile.witness_update_rpcs_completed;\n\n  result[\"task_spec_bytes_sent\"] =\n""",
    """  result[\"witness_update_rpcs_completed\"] =\n      profile.witness_update_rpcs_completed;\n  result[\"witness_update_client_queue_time_ns\"] =\n      profile.witness_update_client_queue_time_ns;\n  result[\"witness_update_server_batch_queue_time_ns\"] =\n      profile.witness_update_server_batch_queue_time_ns;\n  result[\"witness_update_handler_time_ns\"] =\n      profile.witness_update_handler_time_ns;\n  result[\"witness_update_mutex_wait_time_ns\"] =\n      profile.witness_update_mutex_wait_time_ns;\n  result[\"witness_update_mutex_hold_time_ns\"] =\n      profile.witness_update_mutex_hold_time_ns;\n  result[\"witness_update_physical_batches_completed\"] =\n      profile.witness_update_physical_batches_completed;\n  result[\"witness_update_physical_batch_items\"] =\n      profile.witness_update_physical_batch_items;\n  result[\"h1_publish_readiness_samples\"] =\n      profile.h1_publish_readiness_samples;\n  result[\"h2_reserved_at_h1_publish\"] =\n      profile.h2_reserved_at_h1_publish;\n  result[\"h2_installed_at_h1_publish\"] =\n      profile.h2_installed_at_h1_publish;\n\n  result[\"task_spec_bytes_sent\"] =\n""",
)
replace_one(
    "src/ray/core_worker/core_worker.cc",
    """  if (recovery_succession_profiling_enabled_) {\n    witness_publish_start_ns = RecoveryProfileNowNs();\n  }\n\n  PublishRecoveryManifestToWitnesses(\n""",
    """  if (recovery_succession_profiling_enabled_) {\n    witness_publish_start_ns = RecoveryProfileNowNs();\n\n    // Benchmark 70: sample whether H2 is already prepared exactly when H1\n    // starts publication. This is observation only: H1 is never delayed.\n    if (!recovery_witness_holder_baseline_enabled_ &&\n        !manager->RecoveryFrontierEnabled() &&\n        !RayConfig::instance().enable_recovery_succession_certificate_admission() &&\n        state->rank == 1 && state->proposed_manifest.target_holder_count() == 2) {\n      bool h2_reserved = false;\n      bool h2_installed = false;\n      {\n        absl::MutexLock lock(&recovery_holder_admission_mutex_);\n        const auto task_it = recovery_holder_admission_states_.find(state->task_id);\n        if (task_it != recovery_holder_admission_states_.end()) {\n          const auto h2_it = task_it->second.pending_by_rank.find(2);\n          if (h2_it != task_it->second.pending_by_rank.end() &&\n              !h2_it->second->aborted) {\n            h2_reserved = true;\n            h2_installed = h2_it->second->installed;\n          }\n        }\n      }\n      manager->RecordH2ReadinessAtH1Publish(h2_reserved, h2_installed);\n    }\n  }\n\n  PublishRecoveryManifestToWitnesses(\n""",
)
replace_one(
    "src/ray/core_worker/core_worker.cc",
    """      if (witness_start_ns != 0) {\n        manager->RecordWitnessUpdateRpcLatency(\n            RecoveryProfileNowNs() - witness_start_ns);\n      }\n\n      bool report_success = false;\n""",
    """      if (witness_start_ns != 0) {\n        manager->RecordWitnessUpdateRpcLatency(\n            RecoveryProfileNowNs() - witness_start_ns);\n        manager->RecordWitnessUpdateRpcBreakdown(\n            reply.client_queue_time_ns(),\n            reply.witness_batch_queue_time_ns(),\n            reply.witness_handler_time_ns(),\n            reply.witness_mutex_wait_time_ns(),\n            reply.witness_mutex_hold_time_ns(),\n            reply.client_batch_leader(),\n            reply.client_batch_size());\n      }\n\n      bool report_success = false;\n""",
)
replace_one(
    "src/ray/core_worker/core_worker.cc",
    """          if (witness_start_ns != 0) {\n            manager->RecordWitnessUpdateRpcLatency(\n                RecoveryProfileNowNs() - witness_start_ns);\n          }\n\n          bool success = false;\n""",
    """          if (witness_start_ns != 0) {\n            manager->RecordWitnessUpdateRpcLatency(\n                RecoveryProfileNowNs() - witness_start_ns);\n            manager->RecordWitnessUpdateRpcBreakdown(\n                reply.client_queue_time_ns(),\n                reply.witness_batch_queue_time_ns(),\n                reply.witness_handler_time_ns(),\n                reply.witness_mutex_wait_time_ns(),\n                reply.witness_mutex_hold_time_ns(),\n                reply.client_batch_leader(),\n                reply.client_batch_size());\n          }\n\n          bool success = false;\n""",
)

# ---------------------------------------------------------------------------
# Benchmark 70: print the decomposition and the zero-wait H2 readiness signal.
# ---------------------------------------------------------------------------
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    """        print(\n            \"  whole holder admission                = \"\n            f\"{_avg_us(owner, 'holder_admission_time_ns', 'holder_admissions_committed'):.1f} us\"\n        )\n        print()\n\n        print(\"Synchronous CPU/copy work:\")\n""",
    """        print(\n            \"  whole holder admission                = \"\n            f\"{_avg_us(owner, 'holder_admission_time_ns', 'holder_admissions_committed'):.1f} us\"\n        )\n        print()\n\n        witness_completed = owner.get(\"witness_update_rpcs_completed\", 0)\n        client_queue_ns = owner.get(\"witness_update_client_queue_time_ns\", 0)\n        server_batch_queue_ns = owner.get(\"witness_update_server_batch_queue_time_ns\", 0)\n        handler_ns = owner.get(\"witness_update_handler_time_ns\", 0)\n        mutex_wait_ns = owner.get(\"witness_update_mutex_wait_time_ns\", 0)\n        mutex_hold_ns = owner.get(\"witness_update_mutex_hold_time_ns\", 0)\n        rtt_ns = owner.get(\"witness_update_rpc_time_ns\", 0)\n        handler_outside_mutex_ns = max(0, handler_ns - mutex_wait_ns - mutex_hold_ns)\n        residual_ns = max(\n            0,\n            rtt_ns - client_queue_ns - server_batch_queue_ns - handler_ns,\n        )\n        physical_batches = owner.get(\"witness_update_physical_batches_completed\", 0)\n        physical_batch_items = owner.get(\"witness_update_physical_batch_items\", 0)\n        h1_samples = owner.get(\"h1_publish_readiness_samples\", 0)\n        h2_reserved = owner.get(\"h2_reserved_at_h1_publish\", 0)\n        h2_installed = owner.get(\"h2_installed_at_h1_publish\", 0)\n\n        def per_completed_us(total_ns: int) -> float:\n            return total_ns / witness_completed / 1e3 if witness_completed else 0.0\n\n        print(\"Witness publication barrier decomposition:\")\n        print(\n            \"  client witness-batch queue            = \"\n            f\"{per_completed_us(client_queue_ns):.1f} us / logical update\"\n        )\n        print(\n            \"  witness batch serial-position queue   = \"\n            f\"{per_completed_us(server_batch_queue_ns):.1f} us / logical update\"\n        )\n        print(\n            \"  witness handler total                 = \"\n            f\"{per_completed_us(handler_ns):.1f} us / logical update\"\n        )\n        print(\n            \"    recovery_witness_mutex wait         = \"\n            f\"{per_completed_us(mutex_wait_ns):.1f} us\"\n        )\n        print(\n            \"    recovery_witness_mutex hold         = \"\n            f\"{per_completed_us(mutex_hold_ns):.1f} us\"\n        )\n        print(\n            \"    handler outside mutex               = \"\n            f\"{per_completed_us(handler_outside_mutex_ns):.1f} us\"\n        )\n        print(\n            \"  transport + callback residual         = \"\n            f\"{per_completed_us(residual_ns):.1f} us / logical update\"\n        )\n        print(\n            \"  physical witness batches              = \"\n            f\"{physical_batches} \"\n            f\"({_per_task(owner, 'witness_update_physical_batches_completed', args.tasks):.3f} / task)\"\n        )\n        print(\n            \"  logical updates / physical batch      = \"\n            f\"{physical_batch_items / physical_batches if physical_batches else 0.0:.2f}\"\n        )\n        print(\n            \"  H2 reserved when H1 publish starts    = \"\n            f\"{h2_reserved}/{h1_samples} \"\n            f\"({100.0 * h2_reserved / h1_samples if h1_samples else 0.0:.1f}%)\"\n        )\n        print(\n            \"  H2 installed when H1 publish starts   = \"\n            f\"{h2_installed}/{h1_samples} \"\n            f\"({100.0 * h2_installed / h1_samples if h1_samples else 0.0:.1f}%)\"\n        )\n        print()\n\n        print(\"Synchronous CPU/copy work:\")\n""",
)
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    """        print(\"Decision signal:\")\n        if publish_us >= install_us:\n            print(\"  witness publication >= holder-install RTT -> prioritize collapsing H1/H2 durable publications\")\n        else:\n            print(\"  holder-install RTT > witness publication -> prioritize reducing second-holder install/lineage transfer\")\n        print(\n            \"  admission vs (install + publish)       = \"\n            f\"{admission_us:.1f} us vs {install_us + publish_us:.1f} us\"\n        )\n        print(\"  R=2 and W=2 remain unchanged; this is diagnosis only.\")\n""",
    """        print(\"Decision signal:\")\n        print(\n            \"  admission vs (install + publish)       = \"\n            f\"{admission_us:.1f} us vs {install_us + publish_us:.1f} us\"\n        )\n        print(\n            \"  use the barrier decomposition + H2 readiness above to choose the next optimization\"\n        )\n        print(\"  R=2 and W=2 remain unchanged; this is diagnosis only.\")\n""",
)

# Sanity markers: rejected coordinator/delta code must not survive.
for path, forbidden in [
    ("src/ray/core_worker/core_worker.cc", "Patch 4N-WCHAIN"),
    ("src/ray/core_worker/core_worker.cc", "Patch 4N-DELTA"),
    ("src/ray/raylet/node_manager.cc", "Patch 4N-WCHAIN"),
    ("src/ray/raylet/node_manager.cc", "ApplyOrderedK1RecoveryHolderDelta"),
    ("src/ray/protobuf/node_manager.proto", "witness_forwarded"),
]:
    if forbidden in Path(path).read_text():
        raise RuntimeError(f"rejected experiment marker survived: {path}: {forbidden}")

print("Applied K1 witness barrier profiling patch.")
