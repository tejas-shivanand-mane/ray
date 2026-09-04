#!/usr/bin/env python3
from pathlib import Path


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected 1 occurrence, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))

# ---------------------------------------------------------------------------
# Proto: carry client-side physical RPC phase timestamps on the outer batch
# reply, then per-logical-update durations after RayletClient demultiplexing.
# These are profiling-only and never participate in recovery semantics.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/protobuf/node_manager.proto",
    '''  bool client_batch_leader = 9;\n  uint64 witness_batch_queue_time_ns = 10;\n}\n''',
    '''  bool client_batch_leader = 9;\n  uint64 witness_batch_queue_time_ns = 10;\n  uint64 client_submit_to_cq_time_ns = 11;\n  uint64 client_cq_to_main_loop_time_ns = 12;\n  uint64 client_main_loop_to_batch_callback_time_ns = 13;\n}\n''',
)
replace_one(
    "src/ray/protobuf/node_manager.proto",
    '''message UpdateRecoveryWitnessBatchReply {\n  repeated UpdateRecoveryWitnessReply replies = 1;\n\n  // Profiling-only physical-batch server wall time for Benchmark 70.\n  // Zero when recovery succession profiling is disabled.\n  uint64 witness_batch_handler_time_ns = 2;\n}\n''',
    '''message UpdateRecoveryWitnessBatchReply {\n  repeated UpdateRecoveryWitnessReply replies = 1;\n\n  // Profiling-only physical-batch server wall time for Benchmark 70.\n  // Zero when recovery succession profiling is disabled.\n  uint64 witness_batch_handler_time_ns = 2;\n\n  // Client-side physical RPC phase timestamps, all from steady_clock in the\n  // submitting CoreWorker process. They are populated by ClientCallManager only\n  // while recovery succession profiling is enabled.\n  uint64 client_submit_time_ns = 3;\n  uint64 client_cq_receive_time_ns = 4;\n  uint64 client_main_loop_start_time_ns = 5;\n}\n''',
)

# ---------------------------------------------------------------------------
# Generic gRPC client: for replies that expose the three profiling setters,
# stamp T0 at call construction, T1 when the gRPC completion queue wakes, and
# T2 when the posted callback begins on the main event loop. The compile-time
# trait keeps every other Reply type untouched; RayConfig gates timestamps to
# profiling runs only.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/rpc/client_call.h",
    '''#include <chrono>\n#include <memory>\n#include <string>\n#include <utility>\n#include <vector>\n''',
    '''#include <chrono>\n#include <cstdint>\n#include <memory>\n#include <string>\n#include <type_traits>\n#include <utility>\n#include <vector>\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''#include "ray/common/id.h"\n#include "ray/common/status.h"\n''',
    '''#include "ray/common/id.h"\n#include "ray/common/ray_config.h"\n#include "ray/common/status.h"\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''namespace ray {\nnamespace rpc {\n\n/// Represents an outgoing gRPC request.\n''',
    '''namespace ray {\nnamespace rpc {\n\ninline uint64_t RecoveryWitnessClientCallNowNs() {\n  return static_cast<uint64_t>(\n      std::chrono::duration_cast<std::chrono::nanoseconds>(\n          std::chrono::steady_clock::now().time_since_epoch())\n          .count());\n}\n\ntemplate <typename Reply, typename = void>\nstruct HasRecoveryWitnessClientTimingFields : std::false_type {};\n\ntemplate <typename Reply>\nstruct HasRecoveryWitnessClientTimingFields<\n    Reply,\n    std::void_t<\n        decltype(std::declval<Reply &>().set_client_submit_time_ns(uint64_t{})),\n        decltype(std::declval<Reply &>().set_client_cq_receive_time_ns(uint64_t{})),\n        decltype(std::declval<Reply &>().set_client_main_loop_start_time_ns(\n            uint64_t{}))>> : std::true_type {};\n\n/// Represents an outgoing gRPC request.\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''  /// Get stats handle for this RPC (for recording end).\n  virtual std::shared_ptr<StatsHandle> GetStatsHandle() = 0;\n\n  virtual ~ClientCall() = default;\n};\n''',
    '''  /// Get stats handle for this RPC (for recording end).\n  virtual std::shared_ptr<StatsHandle> GetStatsHandle() = 0;\n\n  bool RecoveryWitnessTimingProfiled() const {\n    return recovery_witness_timing_profiled_;\n  }\n\n  void EnableRecoveryWitnessTimingProfile() {\n    recovery_witness_timing_profiled_ = true;\n    recovery_witness_submit_time_ns_ = RecoveryWitnessClientCallNowNs();\n  }\n\n  void MarkRecoveryWitnessCompletionQueueReceived() {\n    if (recovery_witness_timing_profiled_) {\n      recovery_witness_cq_receive_time_ns_ = RecoveryWitnessClientCallNowNs();\n    }\n  }\n\n  void MarkRecoveryWitnessMainLoopCallbackStarted() {\n    if (recovery_witness_timing_profiled_) {\n      recovery_witness_main_loop_start_time_ns_ =\n          RecoveryWitnessClientCallNowNs();\n    }\n  }\n\n  uint64_t RecoveryWitnessSubmitTimeNs() const {\n    return recovery_witness_submit_time_ns_;\n  }\n  uint64_t RecoveryWitnessCqReceiveTimeNs() const {\n    return recovery_witness_cq_receive_time_ns_;\n  }\n  uint64_t RecoveryWitnessMainLoopStartTimeNs() const {\n    return recovery_witness_main_loop_start_time_ns_;\n  }\n\n  virtual ~ClientCall() = default;\n\n private:\n  bool recovery_witness_timing_profiled_ = false;\n  uint64_t recovery_witness_submit_time_ns_ = 0;\n  uint64_t recovery_witness_cq_receive_time_ns_ = 0;\n  uint64_t recovery_witness_main_loop_start_time_ns_ = 0;\n};\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n  }\n''',
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n      if (RayConfig::instance().enable_recovery_succession_profiling()) {\n        EnableRecoveryWitnessTimingProfile();\n      }\n    }\n  }\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''    if (callback_ != nullptr) {\n      // This should be only called once.\n      callback_(status, std::move(reply_));\n    }\n''',
    '''    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n      if (RecoveryWitnessTimingProfiled()) {\n        reply_.set_client_submit_time_ns(RecoveryWitnessSubmitTimeNs());\n        reply_.set_client_cq_receive_time_ns(RecoveryWitnessCqReceiveTimeNs());\n        reply_.set_client_main_loop_start_time_ns(\n            RecoveryWitnessMainLoopStartTimeNs());\n      }\n    }\n    if (callback_ != nullptr) {\n      // This should only be called once.\n      callback_(status, std::move(reply_));\n    }\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''        auto tag = static_cast<ClientCallTag *>(got_tag);\n        // Refresh the tag.\n        got_tag = nullptr;\n        tag->GetCall()->SetReturnStatus();\n''',
    '''        auto tag = static_cast<ClientCallTag *>(got_tag);\n        // Refresh the tag.\n        got_tag = nullptr;\n        tag->GetCall()->MarkRecoveryWitnessCompletionQueueReceived();\n        tag->GetCall()->SetReturnStatus();\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''          main_service_.post(\n              [tag]() {\n                tag->GetCall()->OnReplyReceived();\n''',
    '''          main_service_.post(\n              [tag]() {\n                tag->GetCall()->MarkRecoveryWitnessMainLoopCallbackStarted();\n                tag->GetCall()->OnReplyReceived();\n''',
)

# ---------------------------------------------------------------------------
# RayletClient: T3 is the entry to the physical batch callback. Convert the
# outer T0/T1/T2 timestamps into durations and copy them into every logical
# item reply so the existing owner profile can aggregate them with correct
# logical-item weighting.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''        const bool reply_shape_ok =\n            !status.ok() ||\n            static_cast<size_t>(reply.replies_size()) == batch->size();\n        const uint64_t amortized_witness_handler_ns =\n''',
    '''        const bool reply_shape_ok =\n            !status.ok() ||\n            static_cast<size_t>(reply.replies_size()) == batch->size();\n        const uint64_t batch_callback_entry_ns =\n            reply.client_main_loop_start_time_ns() != 0\n                ? RecoveryWitnessClientProfileNowNs()\n                : 0;\n        const uint64_t client_submit_to_cq_ns =\n            reply.client_submit_time_ns() != 0 &&\n                    reply.client_cq_receive_time_ns() >=\n                        reply.client_submit_time_ns()\n                ? reply.client_cq_receive_time_ns() -\n                      reply.client_submit_time_ns()\n                : 0;\n        const uint64_t client_cq_to_main_loop_ns =\n            reply.client_cq_receive_time_ns() != 0 &&\n                    reply.client_main_loop_start_time_ns() >=\n                        reply.client_cq_receive_time_ns()\n                ? reply.client_main_loop_start_time_ns() -\n                      reply.client_cq_receive_time_ns()\n                : 0;\n        const uint64_t client_main_loop_to_batch_callback_ns =\n            batch_callback_entry_ns != 0 &&\n                    batch_callback_entry_ns >=\n                        reply.client_main_loop_start_time_ns()\n                ? batch_callback_entry_ns -\n                      reply.client_main_loop_start_time_ns()\n                : 0;\n        const uint64_t amortized_witness_handler_ns =\n''',
)
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''            item_reply.set_client_batch_size(\n                static_cast<uint32_t>(batch->size()));\n            item_reply.set_client_batch_leader(i == 0);\n''',
    '''            item_reply.set_client_batch_size(\n                static_cast<uint32_t>(batch->size()));\n            item_reply.set_client_batch_leader(i == 0);\n            item_reply.set_client_submit_to_cq_time_ns(\n                client_submit_to_cq_ns);\n            item_reply.set_client_cq_to_main_loop_time_ns(\n                client_cq_to_main_loop_ns);\n            item_reply.set_client_main_loop_to_batch_callback_time_ns(\n                client_main_loop_to_batch_callback_ns);\n''',
)

# ---------------------------------------------------------------------------
# RecoverySuccessionManager profile: add the three client phases, coverage
# counters, and H2 readiness sampled at H1 ACK.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''    uint64_t witness_update_client_queue_time_ns = 0;\n    uint64_t witness_update_server_batch_queue_time_ns = 0;\n    uint64_t witness_update_handler_time_ns = 0;\n''',
    '''    uint64_t witness_update_client_queue_time_ns = 0;\n    uint64_t witness_update_client_submit_to_cq_time_ns = 0;\n    uint64_t witness_update_client_cq_to_main_loop_time_ns = 0;\n    uint64_t witness_update_client_main_loop_to_batch_callback_time_ns = 0;\n    uint64_t witness_update_client_phase_samples = 0;\n    uint64_t witness_update_server_batch_queue_time_ns = 0;\n    uint64_t witness_update_handler_time_ns = 0;\n    uint64_t witness_update_handler_samples = 0;\n''',
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''    uint64_t h1_publish_readiness_samples = 0;\n    uint64_t h2_reserved_at_h1_publish = 0;\n    uint64_t h2_installed_at_h1_publish = 0;\n''',
    '''    uint64_t h1_publish_readiness_samples = 0;\n    uint64_t h2_reserved_at_h1_publish = 0;\n    uint64_t h2_installed_at_h1_publish = 0;\n    uint64_t h1_ack_readiness_samples = 0;\n    uint64_t h2_reserved_at_h1_ack = 0;\n    uint64_t h2_installed_at_h1_ack = 0;\n''',
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''  void RecordWitnessUpdateRpcBreakdown(uint64_t client_queue_ns,\n                                       uint64_t server_batch_queue_ns,\n                                       uint64_t handler_ns,\n                                       uint64_t mutex_wait_ns,\n                                       uint64_t mutex_hold_ns,\n                                       bool batch_leader,\n                                       uint32_t batch_size);\n\n  void RecordH2ReadinessAtH1Publish(bool h2_reserved, bool h2_installed);\n''',
    '''  void RecordWitnessUpdateRpcBreakdown(\n      uint64_t client_queue_ns,\n      uint64_t client_submit_to_cq_ns,\n      uint64_t client_cq_to_main_loop_ns,\n      uint64_t client_main_loop_to_batch_callback_ns,\n      uint64_t server_batch_queue_ns,\n      uint64_t handler_ns,\n      uint64_t mutex_wait_ns,\n      uint64_t mutex_hold_ns,\n      bool batch_leader,\n      uint32_t batch_size);\n\n  void RecordH2ReadinessAtH1Publish(bool h2_reserved, bool h2_installed);\n  void RecordH2ReadinessAtH1Ack(bool h2_reserved, bool h2_installed);\n''',
)

replace_one(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''void RecoverySuccessionManager::RecordWitnessUpdateRpcBreakdown(\n    uint64_t client_queue_ns,\n    uint64_t server_batch_queue_ns,\n    uint64_t handler_ns,\n    uint64_t mutex_wait_ns,\n    uint64_t mutex_hold_ns,\n    bool batch_leader,\n    uint32_t batch_size) {\n''',
    '''void RecoverySuccessionManager::RecordWitnessUpdateRpcBreakdown(\n    uint64_t client_queue_ns,\n    uint64_t client_submit_to_cq_ns,\n    uint64_t client_cq_to_main_loop_ns,\n    uint64_t client_main_loop_to_batch_callback_ns,\n    uint64_t server_batch_queue_ns,\n    uint64_t handler_ns,\n    uint64_t mutex_wait_ns,\n    uint64_t mutex_hold_ns,\n    bool batch_leader,\n    uint32_t batch_size) {\n''',
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''  profile_.witness_update_client_queue_time_ns += client_queue_ns;\n  profile_.witness_update_server_batch_queue_time_ns += server_batch_queue_ns;\n  profile_.witness_update_handler_time_ns += handler_ns;\n''',
    '''  profile_.witness_update_client_queue_time_ns += client_queue_ns;\n  profile_.witness_update_client_submit_to_cq_time_ns +=\n      client_submit_to_cq_ns;\n  profile_.witness_update_client_cq_to_main_loop_time_ns +=\n      client_cq_to_main_loop_ns;\n  profile_.witness_update_client_main_loop_to_batch_callback_time_ns +=\n      client_main_loop_to_batch_callback_ns;\n  if (client_submit_to_cq_ns != 0 || client_cq_to_main_loop_ns != 0 ||\n      client_main_loop_to_batch_callback_ns != 0) {\n    ++profile_.witness_update_client_phase_samples;\n  }\n  profile_.witness_update_server_batch_queue_time_ns += server_batch_queue_ns;\n  profile_.witness_update_handler_time_ns += handler_ns;\n  if (handler_ns != 0) {\n    ++profile_.witness_update_handler_samples;\n  }\n''',
)
replace_one(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''  if (h2_installed) {\n    ++profile_.h2_installed_at_h1_publish;\n  }\n}\n\n\nvoid RecoverySuccessionManager::RecordWitnessPublishLatency(\n''',
    '''  if (h2_installed) {\n    ++profile_.h2_installed_at_h1_publish;\n  }\n}\n\nvoid RecoverySuccessionManager::RecordH2ReadinessAtH1Ack(\n    bool h2_reserved, bool h2_installed) {\n  if (!profiling_enabled_) {\n    return;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  ++profile_.h1_ack_readiness_samples;\n  if (h2_reserved) {\n    ++profile_.h2_reserved_at_h1_ack;\n  }\n  if (h2_installed) {\n    ++profile_.h2_installed_at_h1_ack;\n  }\n}\n\n\nvoid RecoverySuccessionManager::RecordWitnessPublishLatency(\n''',
)

# ---------------------------------------------------------------------------
# CoreWorker: export the phase counters; pass the new reply timings through the
# two witness callback sites; sample H2 readiness immediately on successful H1
# publication callback before local H1 commit/bookkeeping proceeds.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/core_worker/core_worker.cc",
    '''  result["witness_update_client_queue_time_ns"] =\n      profile.witness_update_client_queue_time_ns;\n  result["witness_update_server_batch_queue_time_ns"] =\n''',
    '''  result["witness_update_client_queue_time_ns"] =\n      profile.witness_update_client_queue_time_ns;\n  result["witness_update_client_submit_to_cq_time_ns"] =\n      profile.witness_update_client_submit_to_cq_time_ns;\n  result["witness_update_client_cq_to_main_loop_time_ns"] =\n      profile.witness_update_client_cq_to_main_loop_time_ns;\n  result["witness_update_client_main_loop_to_batch_callback_time_ns"] =\n      profile.witness_update_client_main_loop_to_batch_callback_time_ns;\n  result["witness_update_client_phase_samples"] =\n      profile.witness_update_client_phase_samples;\n  result["witness_update_server_batch_queue_time_ns"] =\n''',
)
replace_one(
    "src/ray/core_worker/core_worker.cc",
    '''  result["witness_update_handler_time_ns"] =\n      profile.witness_update_handler_time_ns;\n  result["witness_update_mutex_wait_time_ns"] =\n''',
    '''  result["witness_update_handler_time_ns"] =\n      profile.witness_update_handler_time_ns;\n  result["witness_update_handler_samples"] =\n      profile.witness_update_handler_samples;\n  result["witness_update_mutex_wait_time_ns"] =\n''',
)
replace_one(
    "src/ray/core_worker/core_worker.cc",
    '''  result["h2_installed_at_h1_publish"] =\n      profile.h2_installed_at_h1_publish;\n\n  result["task_spec_bytes_sent"] =\n''',
    '''  result["h2_installed_at_h1_publish"] =\n      profile.h2_installed_at_h1_publish;\n  result["h1_ack_readiness_samples"] =\n      profile.h1_ack_readiness_samples;\n  result["h2_reserved_at_h1_ack"] =\n      profile.h2_reserved_at_h1_ack;\n  result["h2_installed_at_h1_ack"] =\n      profile.h2_installed_at_h1_ack;\n\n  result["task_spec_bytes_sent"] =\n''',
)

# Both witness callback sites use the same breakdown argument sequence.
core = Path("src/ray/core_worker/core_worker.cc")
text = core.read_text()
old = '''manager->RecordWitnessUpdateRpcBreakdown(\n            reply.client_queue_time_ns(),\n            reply.witness_batch_queue_time_ns(),\n            reply.witness_handler_time_ns(),\n            reply.witness_mutex_wait_time_ns(),\n            reply.witness_mutex_hold_time_ns(),\n            reply.client_batch_leader(),\n            reply.client_batch_size());'''
new = '''manager->RecordWitnessUpdateRpcBreakdown(\n            reply.client_queue_time_ns(),\n            reply.client_submit_to_cq_time_ns(),\n            reply.client_cq_to_main_loop_time_ns(),\n            reply.client_main_loop_to_batch_callback_time_ns(),\n            reply.witness_batch_queue_time_ns(),\n            reply.witness_handler_time_ns(),\n            reply.witness_mutex_wait_time_ns(),\n            reply.witness_mutex_hold_time_ns(),\n            reply.client_batch_leader(),\n            reply.client_batch_size());'''
count = text.count(old)
if count != 2:
    raise RuntimeError(f"core_worker.cc: expected 2 witness breakdown sites, found {count}")
text = text.replace(old, new)
core.write_text(text)

# Insert H2-at-ACK sampling directly after publication-latency accounting and
# before the failure/commit branches. This exact block occurs once in the holder
# admission publication callback.
replace_one(
    "src/ray/core_worker/core_worker.cc",
    '''        if (witness_publish_start_ns != 0) {\n          manager->RecordWitnessPublishLatency(\n              RecoveryProfileNowNs() - witness_publish_start_ns);\n        }\n\n        if (!witness_stored) {\n''',
    '''        if (witness_publish_start_ns != 0) {\n          manager->RecordWitnessPublishLatency(\n              RecoveryProfileNowNs() - witness_publish_start_ns);\n\n          // Benchmark 70: observe whether H2 became prepared while H1 was\n          // waiting on witness durability. This is sampled at the successful\n          // H1 publication callback, before local H1 commit/bookkeeping.\n          if (witness_stored &&\n              !recovery_witness_holder_baseline_enabled_ &&\n              !manager->RecoveryFrontierEnabled() &&\n              !RayConfig::instance()\n                   .enable_recovery_succession_certificate_admission() &&\n              state->rank == 1 &&\n              state->proposed_manifest.target_holder_count() == 2) {\n            bool h2_reserved = false;\n            bool h2_installed = false;\n            {\n              absl::MutexLock lock(&recovery_holder_admission_mutex_);\n              const auto task_it =\n                  recovery_holder_admission_states_.find(state->task_id);\n              if (task_it != recovery_holder_admission_states_.end()) {\n                const auto h2_it = task_it->second.pending_by_rank.find(2);\n                if (h2_it != task_it->second.pending_by_rank.end() &&\n                    !h2_it->second->aborted) {\n                  h2_reserved = true;\n                  h2_installed = h2_it->second->installed;\n                }\n              }\n            }\n            manager->RecordH2ReadinessAtH1Ack(\n                h2_reserved, h2_installed);\n          }\n        }\n\n        if (!witness_stored) {\n''',
)

# ---------------------------------------------------------------------------
# Benchmark 70: print exact client phases and H2 readiness at both H1 start and
# H1 ACK. The residual no longer subtracts the server handler because that is a
# subset of submit->CQ, not an independent phase.
# ---------------------------------------------------------------------------
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    '''        client_queue_ns = owner.get("witness_update_client_queue_time_ns", 0)\n        server_batch_queue_ns = owner.get("witness_update_server_batch_queue_time_ns", 0)\n        handler_ns = owner.get("witness_update_handler_time_ns", 0)\n        mutex_wait_ns = owner.get("witness_update_mutex_wait_time_ns", 0)\n        mutex_hold_ns = owner.get("witness_update_mutex_hold_time_ns", 0)\n        rtt_ns = owner.get("witness_update_rpc_time_ns", 0)\n        handler_outside_mutex_ns = max(0, handler_ns - mutex_wait_ns - mutex_hold_ns)\n        residual_ns = max(\n            0,\n            rtt_ns - client_queue_ns - server_batch_queue_ns - handler_ns,\n        )\n        physical_batches = owner.get("witness_update_physical_batches_completed", 0)\n        physical_batch_items = owner.get("witness_update_physical_batch_items", 0)\n        h1_samples = owner.get("h1_publish_readiness_samples", 0)\n        h2_reserved = owner.get("h2_reserved_at_h1_publish", 0)\n        h2_installed = owner.get("h2_installed_at_h1_publish", 0)\n''',
    '''        client_queue_ns = owner.get("witness_update_client_queue_time_ns", 0)\n        submit_to_cq_ns = owner.get("witness_update_client_submit_to_cq_time_ns", 0)\n        cq_to_main_ns = owner.get("witness_update_client_cq_to_main_loop_time_ns", 0)\n        main_to_batch_ns = owner.get(\n            "witness_update_client_main_loop_to_batch_callback_time_ns", 0\n        )\n        client_phase_samples = owner.get("witness_update_client_phase_samples", 0)\n        server_batch_queue_ns = owner.get("witness_update_server_batch_queue_time_ns", 0)\n        handler_ns = owner.get("witness_update_handler_time_ns", 0)\n        handler_samples = owner.get("witness_update_handler_samples", 0)\n        mutex_wait_ns = owner.get("witness_update_mutex_wait_time_ns", 0)\n        mutex_hold_ns = owner.get("witness_update_mutex_hold_time_ns", 0)\n        rtt_ns = owner.get("witness_update_rpc_time_ns", 0)\n        handler_outside_mutex_ns = max(0, handler_ns - mutex_wait_ns - mutex_hold_ns)\n        residual_ns = max(\n            0,\n            rtt_ns\n            - client_queue_ns\n            - submit_to_cq_ns\n            - cq_to_main_ns\n            - main_to_batch_ns,\n        )\n        physical_batches = owner.get("witness_update_physical_batches_completed", 0)\n        physical_batch_items = owner.get("witness_update_physical_batch_items", 0)\n        h1_samples = owner.get("h1_publish_readiness_samples", 0)\n        h2_reserved = owner.get("h2_reserved_at_h1_publish", 0)\n        h2_installed = owner.get("h2_installed_at_h1_publish", 0)\n        h1_ack_samples = owner.get("h1_ack_readiness_samples", 0)\n        h2_reserved_at_ack = owner.get("h2_reserved_at_h1_ack", 0)\n        h2_installed_at_ack = owner.get("h2_installed_at_h1_ack", 0)\n''',
)
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    '''        print(\n            "  witness batch serial-position queue   = "\n            f"{per_completed_us(server_batch_queue_ns):.1f} us / logical update"\n        )\n        print(\n            "  witness handler total (amortized)     = "\n            f"{per_completed_us(handler_ns):.1f} us / logical update"\n        )\n''',
    '''        print(\n            "  client submit -> gRPC CQ              = "\n            f"{per_completed_us(submit_to_cq_ns):.1f} us / logical update"\n        )\n        print(\n            "  gRPC CQ -> main event loop            = "\n            f"{per_completed_us(cq_to_main_ns):.1f} us / logical update"\n        )\n        print(\n            "  main loop -> Raylet batch callback    = "\n            f"{per_completed_us(main_to_batch_ns):.1f} us / logical update"\n        )\n        print(\n            "  client phase timing coverage          = "\n            f"{client_phase_samples}/{witness_completed} logical updates"\n        )\n        print(\n            "  witness batch serial-position queue   = "\n            f"{per_completed_us(server_batch_queue_ns):.1f} us / logical update"\n        )\n        print(\n            "  witness handler total (amortized)     = "\n            f"{per_completed_us(handler_ns):.1f} us / logical update "\n            f"({handler_samples}/{witness_completed} nonzero samples)"\n        )\n''',
)
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    '''        print(\n            "  transport + callback residual         = "\n            f"{per_completed_us(residual_ns):.1f} us / logical update"\n        )\n''',
    '''        print(\n            "  unaccounted logical callback tail     = "\n            f"{per_completed_us(residual_ns):.1f} us / logical update"\n        )\n''',
)
replace_one(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    '''        print(\n            "  H2 installed when H1 publish starts   = "\n            f"{h2_installed}/{h1_samples} "\n            f"({100.0 * h2_installed / h1_samples if h1_samples else 0.0:.1f}%)"\n        )\n        print()\n''',
    '''        print(\n            "  H2 installed when H1 publish starts   = "\n            f"{h2_installed}/{h1_samples} "\n            f"({100.0 * h2_installed / h1_samples if h1_samples else 0.0:.1f}%)"\n        )\n        print(\n            "  H2 reserved when H1 witness ACKs      = "\n            f"{h2_reserved_at_ack}/{h1_ack_samples} "\n            f"({100.0 * h2_reserved_at_ack / h1_ack_samples if h1_ack_samples else 0.0:.1f}%)"\n        )\n        print(\n            "  H2 installed when H1 witness ACKs     = "\n            f"{h2_installed_at_ack}/{h1_ack_samples} "\n            f"({100.0 * h2_installed_at_ack / h1_ack_samples if h1_ack_samples else 0.0:.1f}%)"\n        )\n        print()\n''',
)

print("Applied K1 witness RPC phase + H2-at-ACK profiling instrumentation.")
