#!/usr/bin/env python3
"""Apply Patch 4G: Recovery Succession hot-path profiling + B1 ablations.

Run from the Ray repository root after Patch 4F:
    python gossip_benchmarks/apply_patch4g_hotpath_profiling_ablation.py

Patch 4G is diagnostic. Default mode remains `full` and preserves Patch-4F
semantics. Non-full modes are BENCHMARK ONLY and intentionally weaken/disable
parts of protection to isolate steady-state overhead.
"""
from __future__ import annotations

import datetime as _dt
import shutil
import subprocess
from pathlib import Path

ROOT = Path.cwd()

FILES = {
    "config": ROOT / "src/ray/common/ray_config_def.h",
    "mgr_h": ROOT / "src/ray/core_worker/recovery_succession_manager.h",
    "mgr_cc": ROOT / "src/ray/core_worker/recovery_succession_manager.cc",
    "core_cc": ROOT / "src/ray/core_worker/core_worker.cc",
    "common_py": ROOT / "gossip_benchmarks/_benchmark_common.py",
    "bench09": ROOT / "gossip_benchmarks/09_patch4a_holder_formation.py",
    "bench16": ROOT / "gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py",
}

MARKER = "Patch 4G: hot-path profiling and B1 ablations."


def die(msg: str) -> None:
    raise SystemExit(f"Patch 4G: {msg}")


def read(path: Path) -> str:
    if not path.exists():
        die(f"missing expected file: {path}")
    return path.read_text()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    n = text.count(old)
    if n != 1:
        die(f"anchor {label!r} matched {n} times (expected exactly 1)")
    return text.replace(old, new, 1)


def insert_after_once(text: str, anchor: str, addition: str, label: str) -> str:
    return replace_once(text, anchor, anchor + addition, label)


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


# ---------------------------------------------------------------------------
# Verify current tree is the exact Patch-4F family we expect.
# ---------------------------------------------------------------------------
required = {
    "config": ["enable_recovery_succession_profiling"],
    "mgr_h": [
        "Patch 4F: first-holder TaskSpec piggyback.",
        "first_holder_piggyback_copies_sent",
        "provisional_piggyback_task_spec",
        "RecordTaskArgumentMetadataLatency",
    ],
    "mgr_cc": [
        "ClearFirstHolderTaskSpecPiggybacks",
        "first_holder_task_spec",
        "candidate_already_stores_task_spec",
        "RecordRegisterOwnedTaskLatency",
    ],
    "core_cc": [
        "Patch 4F: first-holder TaskSpec piggyback.",
        "QueueRecoveryCandidateReport",
        "recovery_succession_test_fail_after_witness_ack",
        "RecordTaskArgumentMetadataLatency",
    ],
    "common_py": ["enable_recovery_succession_profiling"],
    "bench09": [
        "first_holder_piggyback_copies_sent",
        "profile_logical_task_spec_copies_sent",
    ],
}

texts: dict[str, str] = {}
for key, needles in required.items():
    texts[key] = read(FILES[key])
    for needle in needles:
        if needle not in texts[key]:
            die(f"{FILES[key]} is not the expected post-4F version; missing {needle!r}")

if MARKER in texts["mgr_h"] and FILES["bench16"].exists():
    print("Patch 4G already appears to be applied; nothing to do.")
    raise SystemExit(0)

# Refuse a partially-applied 4G tree.
partial_needles = [
    "recovery_succession_benchmark_ablation_mode",
    "candidate_rpc_logical_reports_sent",
    "ensure_task_arguments_time_ns",
]
if any(n in "\n".join(texts.values()) for n in partial_needles) or FILES["bench16"].exists():
    die("tree appears partially patched with 4G; restore/revert before retrying")

edited = dict(texts)

# ---------------------------------------------------------------------------
# 1) RayConfig: one benchmark-only mode string. Default full = no semantic change.
# ---------------------------------------------------------------------------
old = '''/// Enables lightweight profiling of recovery-succession holder formation.
/// Intended only for experiments/debugging. When false, no timing or
/// protobuf-size measurements are performed.
RAY_CONFIG(bool, enable_recovery_succession_profiling, false)
'''
new = '''/// Enables lightweight profiling of recovery-succession holder formation.
/// Intended only for experiments/debugging. When false, no timing or
/// protobuf-size measurements are performed.
RAY_CONFIG(bool, enable_recovery_succession_profiling, false)

/// Patch 4G BENCHMARK ONLY. Selects a diagnostic B1 ablation. Production and
/// correctness runs must use the default "full" mode.
/// Supported values:
///   full                    - ordinary Patch-4F behavior
///   no_piggyback            - full admission, but H1 uses InstallRecoveryHolder
///   metadata_only           - compact metadata propagation; no TaskSpec piggyback/report
///   piggyback_no_candidate  - metadata + H1 TaskSpec sidecar; no candidate report
///   candidate_rpc_no_admit  - metadata + candidate RPC; owner replies NO_SLOT
RAY_CONFIG(std::string, recovery_succession_benchmark_ablation_mode, "full")
'''
edited["config"] = replace_once(edited["config"], old, new, "ray config profiling block")

# ---------------------------------------------------------------------------
# 2) RecoverySuccessionManager profile fields + record methods.
# ---------------------------------------------------------------------------
old = '''    uint64_t register_owned_task_count = 0;
    uint64_t register_owned_task_time_ns = 0;
'''
addition = '''

    // Patch 4G: synchronous hot-path costs. These are CPU/wall-clock durations
    // spent inside the calling thread, not asynchronous control-RPC latency.
    uint64_t recovery_metadata_lookup_calls = 0;
    uint64_t recovery_metadata_lookup_hits = 0;
    uint64_t recovery_metadata_lookup_time_ns = 0;

    uint64_t ensure_task_arguments_calls = 0;
    uint64_t ensure_task_arguments_time_ns = 0;

    uint64_t register_executor_task_calls = 0;
    uint64_t register_executor_task_time_ns = 0;
    uint64_t register_executor_metadata_refs_seen = 0;
    uint64_t register_executor_candidate_reports_built = 0;

    uint64_t candidate_report_build_calls = 0;
    uint64_t candidate_reports_built = 0;
    uint64_t candidate_report_build_time_ns = 0;

    uint64_t candidate_queue_calls = 0;
    uint64_t candidate_queue_time_ns = 0;

    // Candidate-report transport. logical_reports counts individual tasks;
    // physical_rpcs counts actual single/batched gRPCs.
    uint64_t candidate_rpc_logical_reports_sent = 0;
    uint64_t candidate_rpc_logical_reports_completed = 0;
    uint64_t candidate_rpc_physical_rpcs_sent = 0;
    uint64_t candidate_rpc_physical_rpcs_completed = 0;
    uint64_t candidate_rpc_request_bytes_sent = 0;
    uint64_t candidate_rpc_time_ns = 0;
'''
edited["mgr_h"] = insert_after_once(
    edited["mgr_h"], old, addition, "manager profile tail")

old = '''  void RecordRegisterOwnedTaskLatency(uint64_t latency_ns);
'''
addition = '''

  // Patch 4G hot-path profiling.
  void RecordEnsureTaskArgumentsLatency(uint64_t latency_ns);
  void RecordCandidateQueueLatency(uint64_t latency_ns);
  void RecordCandidateRpcSent(uint64_t logical_reports, uint64_t request_bytes);
  void RecordCandidateRpcLatency(uint64_t logical_reports, uint64_t latency_ns);
'''
edited["mgr_h"] = insert_after_once(
    edited["mgr_h"], old, addition, "manager record-method tail")
edited["mgr_h"] = edited["mgr_h"].replace(
    "/// Patch 4F: first-holder TaskSpec piggyback.\n",
    "/// Patch 4F: first-holder TaskSpec piggyback.\n/// Patch 4G: hot-path profiling and B1 ablations.\n",
    1,
)
edited["mgr_h"] = replace_once(
    edited["mgr_h"],
    "  RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n",
    "  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n",
    "mutable profiling snapshot",
)

# ---------------------------------------------------------------------------
# 3) Manager implementation: mode helper, profile timings, ablation gates.
# ---------------------------------------------------------------------------
old = '#include "ray/common/ray_config.h"\n#include <cstddef>\n'
new = '#include "ray/common/ray_config.h"\n#include "absl/cleanup/cleanup.h"\n#include <cstddef>\n'
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "manager includes")

old = '''void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec) {
  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      arg.mutable_object_ref()
          ->mutable_recovery_metadata()
          ->clear_first_holder_task_spec();
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        nested_ref.mutable_recovery_metadata()->clear_first_holder_task_spec();
      }
    }
  }
}

}  // namespace
'''
new = '''void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec) {
  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      arg.mutable_object_ref()
          ->mutable_recovery_metadata()
          ->clear_first_holder_task_spec();
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        nested_ref.mutable_recovery_metadata()->clear_first_holder_task_spec();
      }
    }
  }
}

const std::string &RecoveryBenchmarkAblationMode() {
  static const std::string mode =
      RayConfig::instance().recovery_succession_benchmark_ablation_mode();
  RAY_CHECK(mode == "full" || mode == "no_piggyback" ||
            mode == "metadata_only" || mode == "piggyback_no_candidate" ||
            mode == "candidate_rpc_no_admit")
      << "Unknown recovery_succession_benchmark_ablation_mode=" << mode;
  return mode;
}

}  // namespace
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "manager ablation helper")

# RegisterExecutorTask: start timer and record at function exit.
old = '''RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;
'''
new = '''RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
  const auto patch4g_start = std::chrono::steady_clock::now();
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "RegisterExecutorTask start")

old = '''  return reports;
}

void RecoverySuccessionManager::RegisterBorrowedObject(
'''
new = '''  if (profiling_enabled_) {
    ++profile_.register_executor_task_calls;
    profile_.register_executor_task_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    profile_.register_executor_metadata_refs_seen +=
        static_cast<uint64_t>(received_metadata.size());
    profile_.register_executor_candidate_reports_built +=
        static_cast<uint64_t>(reports.size());
  }

  return reports;
}

void RecoverySuccessionManager::RegisterBorrowedObject(
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "RegisterExecutorTask end")

# PopulateRecoveryMetadata: measure the map lookup+protobuf copy fast path.
old = '''bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto metadata_it = object_recovery_metadata_.find(object_id);

  if (metadata_it == object_recovery_metadata_.end()) {
    return false;
  }

  metadata->CopyFrom(metadata_it->second);

  return true;
}
'''
new = '''bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  const auto patch4g_start = std::chrono::steady_clock::now();
  absl::MutexLock lock(&mutex_);

  const auto metadata_it = object_recovery_metadata_.find(object_id);
  const bool hit = metadata_it != object_recovery_metadata_.end();

  if (hit) {
    metadata->CopyFrom(metadata_it->second);
  }

  if (profiling_enabled_) {
    ++profile_.recovery_metadata_lookup_calls;
    if (hit) {
      ++profile_.recovery_metadata_lookup_hits;
    }
    profile_.recovery_metadata_lookup_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
  }

  return hit;
}
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "PopulateRecoveryMetadata")

# Piggyback ablation gate.
old = '''    // Keep the witness-as-holder baseline unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline() ||
        out->task_id().empty() || !out->has_manifest()) {
      return;
    }

    const TaskID producer_task_id = TaskID::FromBinary(out->task_id());
'''
new = '''    // Keep the witness-as-holder baseline unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline() ||
        out->task_id().empty() || !out->has_manifest()) {
      return;
    }

    // Patch 4G benchmark ablations that isolate compact metadata and/or the
    // candidate RPC must not put the full TaskSpec on PushTask. no_piggyback
    // recreates the pre-4F H1 transport while keeping full admission semantics.
    const std::string &patch4g_mode = RecoveryBenchmarkAblationMode();
    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
        patch4g_mode == "no_piggyback") {
      return;
    }

    const TaskID producer_task_id = TaskID::FromBinary(out->task_id());
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "piggyback ablation gate")

# Candidate-build profiling + suppression modes.
old = '''void RecoverySuccessionManager::MaybeAddCandidateReportLocked(
    const rpc::RecoveryManifest &manifest,
    bool already_stores_task_spec,
    std::vector<CandidateReport> *reports) {

  if (RayConfig::instance().enable_recovery_succession() &&
'''
new = '''void RecoverySuccessionManager::MaybeAddCandidateReportLocked(
    const rpc::RecoveryManifest &manifest,
    bool already_stores_task_spec,
    std::vector<CandidateReport> *reports) {
  const auto patch4g_start = std::chrono::steady_clock::now();
  const size_t patch4g_reports_before = reports == nullptr ? 0 : reports->size();
  absl::Cleanup patch4g_profile = [this, patch4g_start, patch4g_reports_before, reports] {
    if (!profiling_enabled_) {
      return;
    }
    ++profile_.candidate_report_build_calls;
    profile_.candidate_report_build_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    if (reports != nullptr && reports->size() > patch4g_reports_before) {
      profile_.candidate_reports_built +=
          static_cast<uint64_t>(reports->size() - patch4g_reports_before);
    }
  };

  const std::string &patch4g_mode = RecoveryBenchmarkAblationMode();
  if (patch4g_mode == "metadata_only" ||
      patch4g_mode == "piggyback_no_candidate") {
    return;
  }

  if (RayConfig::instance().enable_recovery_succession() &&
'''
edited["mgr_cc"] = replace_once(edited["mgr_cc"], old, new, "candidate build profiling/gate")

# Add record method implementations immediately after the existing function.
old = '''void RecoverySuccessionManager::RecordRegisterOwnedTaskLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.register_owned_task_count;
  profile_.register_owned_task_time_ns += latency_ns;
}
'''
addition = '''

void RecoverySuccessionManager::RecordEnsureTaskArgumentsLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.ensure_task_arguments_calls;
  profile_.ensure_task_arguments_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateQueueLatency(uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.candidate_queue_calls;
  profile_.candidate_queue_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateRpcSent(
    uint64_t logical_reports, uint64_t request_bytes) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_sent += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_sent;
  profile_.candidate_rpc_request_bytes_sent += request_bytes;
}

void RecoverySuccessionManager::RecordCandidateRpcLatency(
    uint64_t logical_reports, uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_completed += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_completed;
  profile_.candidate_rpc_time_ns += latency_ns;
}
'''
edited["mgr_cc"] = insert_after_once(
    edited["mgr_cc"], old, addition, "manager profiling method tail")
edited["mgr_cc"] = edited["mgr_cc"].replace(
    "// Patch 4F: first-holder TaskSpec piggyback.\n",
    "// Patch 4F: first-holder TaskSpec piggyback.\n// Patch 4G: hot-path profiling and B1 ablations.\n",
    1,
)

# ---------------------------------------------------------------------------
# 4) CoreWorker: mode helper, owner/borrower timers, RPC profiling, no-admit mode.
# ---------------------------------------------------------------------------
old = '''uint64_t RecoveryProfileNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

int CompareRecoveryManifestVersions'''
new = '''uint64_t RecoveryProfileNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

const std::string &RecoveryBenchmarkAblationMode() {
  static const std::string mode =
      RayConfig::instance().recovery_succession_benchmark_ablation_mode();
  RAY_CHECK(mode == "full" || mode == "no_piggyback" ||
            mode == "metadata_only" || mode == "piggyback_no_candidate" ||
            mode == "candidate_rpc_no_admit")
      << "Unknown recovery_succession_benchmark_ablation_mode=" << mode;
  return mode;
}

int CompareRecoveryManifestVersions'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "core ablation helper")

# JSON export of new fields.
old = '''  result["register_owned_task_count"] =
      profile.register_owned_task_count;
  result["register_owned_task_time_ns"] =
      profile.register_owned_task_time_ns;
'''
edited["core_cc"] = insert_after_once(
    edited["core_cc"], old, '''

  result["recovery_metadata_lookup_calls"] = profile.recovery_metadata_lookup_calls;
  result["recovery_metadata_lookup_hits"] = profile.recovery_metadata_lookup_hits;
  result["recovery_metadata_lookup_time_ns"] = profile.recovery_metadata_lookup_time_ns;
  result["ensure_task_arguments_calls"] = profile.ensure_task_arguments_calls;
  result["ensure_task_arguments_time_ns"] = profile.ensure_task_arguments_time_ns;
  result["register_executor_task_calls"] = profile.register_executor_task_calls;
  result["register_executor_task_time_ns"] = profile.register_executor_task_time_ns;
  result["register_executor_metadata_refs_seen"] =
      profile.register_executor_metadata_refs_seen;
  result["register_executor_candidate_reports_built"] =
      profile.register_executor_candidate_reports_built;
  result["candidate_report_build_calls"] = profile.candidate_report_build_calls;
  result["candidate_reports_built"] = profile.candidate_reports_built;
  result["candidate_report_build_time_ns"] = profile.candidate_report_build_time_ns;
  result["candidate_queue_calls"] = profile.candidate_queue_calls;
  result["candidate_queue_time_ns"] = profile.candidate_queue_time_ns;
  result["candidate_rpc_logical_reports_sent"] =
      profile.candidate_rpc_logical_reports_sent;
  result["candidate_rpc_logical_reports_completed"] =
      profile.candidate_rpc_logical_reports_completed;
  result["candidate_rpc_physical_rpcs_sent"] =
      profile.candidate_rpc_physical_rpcs_sent;
  result["candidate_rpc_physical_rpcs_completed"] =
      profile.candidate_rpc_physical_rpcs_completed;
  result["candidate_rpc_request_bytes_sent"] =
      profile.candidate_rpc_request_bytes_sent;
  result["candidate_rpc_time_ns"] = profile.candidate_rpc_time_ns;''', "core profile JSON tail")

# EnsureRecoverySuccessionForTaskArguments total time.
old = '''void CoreWorker::EnsureRecoverySuccessionForTaskArguments(
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr || !recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return;
  }

  rpc::RecoveryObjectMetadata ignored_metadata;
'''
new = '''void CoreWorker::EnsureRecoverySuccessionForTaskArguments(
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr || !recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return;
  }

  const uint64_t patch4g_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  rpc::RecoveryObjectMetadata ignored_metadata;
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "ensure args start")

old = '''      TryPopulateRecoveryMetadataForObject(nested_id, &ignored_metadata);
    }
  }
}

std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
'''
new = '''      TryPopulateRecoveryMetadataForObject(nested_id, &ignored_metadata);
    }
  }

  if (patch4g_start_ns != 0) {
    recovery_succession_manager_->RecordEnsureTaskArgumentsLatency(
        RecoveryProfileNowNs() - patch4g_start_ns);
  }
}

std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "ensure args end")

# QueueRecoveryCandidateReport: scope CPU timer.
old = '''  const TaskID task_id = TaskID::FromBinary(request.task_id());

  // Preserve deterministic failure-injection semantics.'''
new = '''  const TaskID task_id = TaskID::FromBinary(request.task_id());
  const uint64_t patch4g_queue_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  auto patch4g_manager = recovery_succession_manager_;
  absl::Cleanup patch4g_queue_profile =
      [patch4g_manager, patch4g_queue_start_ns] {
        if (patch4g_manager != nullptr && patch4g_queue_start_ns != 0) {
          patch4g_manager->RecordCandidateQueueLatency(
              RecoveryProfileNowNs() - patch4g_queue_start_ns);
        }
      };

  // Preserve deterministic failure-injection semantics.'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "candidate queue timer")

# Fast-path candidate RPC send/callback profile.
old = '''    auto manager = recovery_succession_manager_;
    auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);
    client->ReportRecoveryCandidate(
        std::move(request),
        [manager, task_id](const Status &status,
                           rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (!status.ok()) {
'''
new = '''    auto manager = recovery_succession_manager_;
    auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);
    const uint64_t patch4g_rpc_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (patch4g_rpc_start_ns != 0) {
      manager->RecordCandidateRpcSent(
          1, static_cast<uint64_t>(request.ByteSizeLong()));
    }
    client->ReportRecoveryCandidate(
        std::move(request),
        [manager, task_id, patch4g_rpc_start_ns](
            const Status &status,
            rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (patch4g_rpc_start_ns != 0) {
            manager->RecordCandidateRpcLatency(
                1, RecoveryProfileNowNs() - patch4g_rpc_start_ns);
          }
          if (!status.ok()) {
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "fast candidate RPC profile")

# Flush single candidate RPC profile (distinct occurrence).
old = '''    client->ReportRecoveryCandidate(
        std::move(single_request),
        [manager, task_id](const Status &status,
                           rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (!status.ok()) {
'''
new = '''    const uint64_t patch4g_rpc_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (patch4g_rpc_start_ns != 0) {
      manager->RecordCandidateRpcSent(
          1, static_cast<uint64_t>(single_request.ByteSizeLong()));
    }
    client->ReportRecoveryCandidate(
        std::move(single_request),
        [manager, task_id, patch4g_rpc_start_ns](
            const Status &status,
            rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (patch4g_rpc_start_ns != 0) {
            manager->RecordCandidateRpcLatency(
                1, RecoveryProfileNowNs() - patch4g_rpc_start_ns);
          }
          if (!status.ok()) {
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "flush single candidate RPC profile")

# Batch candidate RPC profile.
old = '''  client->ReportRecoveryCandidateBatch(
      std::move(batch_request),
      [manager, task_ids = std::move(task_ids)](
          const Status &status,
          rpc::ReportRecoveryCandidateBatchReply &&batch_reply) mutable {
        if (!status.ok()) {
'''
new = '''  const uint64_t patch4g_rpc_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
  if (patch4g_rpc_start_ns != 0) {
    manager->RecordCandidateRpcSent(
        static_cast<uint64_t>(task_ids.size()),
        static_cast<uint64_t>(batch_request.ByteSizeLong()));
  }
  client->ReportRecoveryCandidateBatch(
      std::move(batch_request),
      [manager,
       task_ids = std::move(task_ids),
       patch4g_rpc_start_ns](
          const Status &status,
          rpc::ReportRecoveryCandidateBatchReply &&batch_reply) mutable {
        if (patch4g_rpc_start_ns != 0) {
          manager->RecordCandidateRpcLatency(
              static_cast<uint64_t>(task_ids.size()),
              RecoveryProfileNowNs() - patch4g_rpc_start_ns);
        }
        if (!status.ok()) {
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "batch candidate RPC profile")

# candidate_rpc_no_admit: owner replies immediately, no reservation/witness/install.
old = '''  const uint64_t admission_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  auto manager = recovery_succession_manager_;
  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
'''
new = '''  auto manager = recovery_succession_manager_;

  // Patch 4G BENCHMARK ONLY: preserve candidate-report construction and the
  // physical RPC, but stop before holder reservation, install, or witness work.
  // This mode intentionally does not provide recovery durability.
  if (RecoveryBenchmarkAblationMode() == "candidate_rpc_no_admit") {
    if (recovery_succession_profiling_enabled_) {
      manager->RecordCandidateReport(false);
    }
    reply->set_result(rpc::ReportRecoveryCandidateReply::NO_SLOT);
    if (request.has_cached_manifest()) {
      reply->mutable_latest_manifest()->CopyFrom(request.cached_manifest());
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return std::nullopt;
  }

  const uint64_t admission_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
'''
edited["core_cc"] = replace_once(edited["core_cc"], old, new, "candidate no-admit ablation")

edited["core_cc"] = edited["core_cc"].replace(
    "// Patch 4F: first-holder TaskSpec piggyback.\n",
    "// Patch 4F: first-holder TaskSpec piggyback.\n// Patch 4G: hot-path profiling and B1 ablations.\n",
    1,
)

# ---------------------------------------------------------------------------
# 5) Benchmark common config accepts the mode, defaulting to full.
# ---------------------------------------------------------------------------
old = '''def system_config(
    method: Method,
    *,
    witness_count: int = 2,
    object_timeout_ms: int | None = None,
    profiling_enabled: bool = False,
) -> dict[str, Any]:
'''
new = '''def system_config(
    method: Method,
    *,
    witness_count: int = 2,
    object_timeout_ms: int | None = None,
    profiling_enabled: bool = False,
    ablation_mode: str = "full",
) -> dict[str, Any]:
'''
edited["common_py"] = replace_once(edited["common_py"], old, new, "system_config signature")

old = '''        "enable_recovery_succession_profiling": bool(profiling_enabled),
    }
    if method.recovery_enabled:
        config["recovery_succession_target_holder_count"] = int(method.holders)
'''
new = '''        "enable_recovery_succession_profiling": bool(profiling_enabled),
        "recovery_succession_benchmark_ablation_mode": str(ablation_mode),
    }
    if method.recovery_enabled:
        config["recovery_succession_target_holder_count"] = int(method.holders)
'''
edited["common_py"] = replace_once(edited["common_py"], old, new, "system_config mode")

# ---------------------------------------------------------------------------
# 6) Benchmark 09 must retain all 4G fields in raw CSVs.
# ---------------------------------------------------------------------------
old = '''    "max_non_owner_holders",
    "frozen_commits",
]
'''
new = '''    "max_non_owner_holders",
    "frozen_commits",
    "task_argument_metadata_calls",
    "task_argument_metadata_time_ns",
    "initial_manifest_build_count",
    "initial_manifest_build_time_ns",
    "initial_manifest_bytes",
    "witness_selection_count",
    "witness_selection_time_ns",
    "witness_gcs_query_count",
    "witness_gcs_query_time_ns",
    "task_spec_manifest_attach_count",
    "task_spec_manifest_attach_time_ns",
    "register_owned_task_count",
    "register_owned_task_time_ns",
    "recovery_metadata_lookup_calls",
    "recovery_metadata_lookup_hits",
    "recovery_metadata_lookup_time_ns",
    "ensure_task_arguments_calls",
    "ensure_task_arguments_time_ns",
    "register_executor_task_calls",
    "register_executor_task_time_ns",
    "register_executor_metadata_refs_seen",
    "register_executor_candidate_reports_built",
    "candidate_report_build_calls",
    "candidate_reports_built",
    "candidate_report_build_time_ns",
    "candidate_queue_calls",
    "candidate_queue_time_ns",
    "candidate_rpc_logical_reports_sent",
    "candidate_rpc_logical_reports_completed",
    "candidate_rpc_physical_rpcs_sent",
    "candidate_rpc_physical_rpcs_completed",
    "candidate_rpc_request_bytes_sent",
    "candidate_rpc_time_ns",
]
'''
edited["bench09"] = replace_once(edited["bench09"], old, new, "benchmark09 profile keys")

# ---------------------------------------------------------------------------
# 7) New focused B1 benchmark. Two repetitions by default.
# ---------------------------------------------------------------------------
bench16 = r'''#!/usr/bin/env python3
"""Patch 4G: focused B1 hot-path profiling and ablation benchmark.

This benchmark intentionally runs only one real downstream borrower (B1), since
B1 is the unresolved ~20% steady-state overhead case.

Cases:
  Disabled
  MetadataOnly              compact recovery metadata, no TaskSpec/candidate
  PiggybackNoCandidate      metadata + 4F TaskSpec sidecar, no candidate
  CandidateRpcNoAdmit       metadata + candidate RPC, owner immediately NO_SLOT
  NoPiggyback               full recovery; H1 uses InstallRecoveryHolder
  Full4F                    ordinary Patch-4F recovery

The three middle ablations intentionally weaken durability and are BENCHMARK ONLY.
Default repetitions = 2 to keep iteration time reasonable.
"""
from __future__ import annotations

import os
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import argparse
import math
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    disabled,
    mean_ci95,
    percentile,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

TARGET_HOLDERS = 4
BORROWERS = 1

PROFILE_KEYS = [
    "profiling_enabled",
    "candidate_reports_received",
    "candidate_reports_accepted",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "owner_task_spec_copy_count",
    "owner_task_spec_copy_time_ns",
    "first_holder_piggyback_copies_sent",
    "first_holder_piggyback_bytes_sent",
    "first_holder_piggyback_serialize_time_ns",
    "holder_install_rpc_time_ns",
    "holder_commit_rpc_time_ns",
    "witness_update_rpc_time_ns",
    "witness_publish_count",
    "witness_publish_time_ns",
    "witness_publish_max_time_ns",
    "holder_admissions_committed",
    "holder_admission_time_ns",
    "holder_admission_max_time_ns",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
    "task_argument_metadata_calls",
    "task_argument_metadata_time_ns",
    "initial_manifest_build_count",
    "initial_manifest_build_time_ns",
    "initial_manifest_bytes",
    "witness_selection_count",
    "witness_selection_time_ns",
    "witness_gcs_query_count",
    "witness_gcs_query_time_ns",
    "task_spec_manifest_attach_count",
    "task_spec_manifest_attach_time_ns",
    "register_owned_task_count",
    "register_owned_task_time_ns",
    "recovery_metadata_lookup_calls",
    "recovery_metadata_lookup_hits",
    "recovery_metadata_lookup_time_ns",
    "ensure_task_arguments_calls",
    "ensure_task_arguments_time_ns",
    "register_executor_task_calls",
    "register_executor_task_time_ns",
    "register_executor_metadata_refs_seen",
    "register_executor_candidate_reports_built",
    "candidate_report_build_calls",
    "candidate_reports_built",
    "candidate_report_build_time_ns",
    "candidate_queue_calls",
    "candidate_queue_time_ns",
    "candidate_rpc_logical_reports_sent",
    "candidate_rpc_logical_reports_completed",
    "candidate_rpc_physical_rpcs_sent",
    "candidate_rpc_physical_rpcs_completed",
    "candidate_rpc_request_bytes_sent",
    "candidate_rpc_time_ns",
]

SUM_KEYS = set(PROFILE_KEYS) - {"profiling_enabled", "max_generation", "max_non_owner_holders"}
MAX_KEYS = {"max_generation", "max_non_owner_holders"}
ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
]


@dataclass(frozen=True)
class Case:
    key: str
    label: str
    recovery: bool
    mode: str


def cases() -> list[Case]:
    return [
        Case("disabled", "Disabled", False, "full"),
        Case("metadata_only", "MetadataOnly", True, "metadata_only"),
        Case("piggyback_no_candidate", "PiggybackNoCandidate", True, "piggyback_no_candidate"),
        Case("candidate_rpc_no_admit", "CandidateRpcNoAdmit", True, "candidate_rpc_no_admit"),
        Case("no_piggyback", "NoPiggyback", True, "no_piggyback"),
        Case("full", "Full4F", True, "full"),
    ]


def method_for(case: Case) -> Method:
    return succession(TARGET_HOLDERS) if case.recovery else disabled()


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {k: (False if k == "profiling_enabled" else 0) for k in PROFILE_KEYS}
    if raw:
        for k in PROFILE_KEYS:
            if k in raw:
                out[k] = raw[k]
    return out


def aggregate_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [profile_defaults(p) for p in profiles]
    out = profile_defaults()
    out["profiling_enabled"] = any(bool(p["profiling_enabled"]) for p in vals)
    for k in SUM_KEYS:
        out[k] = sum(int(p[k]) for p in vals)
    for k in MAX_KEYS:
        out[k] = max((int(p[k]) for p in vals), default=0)
    return out


def outstanding(profile: dict[str, Any]) -> int:
    return sum(max(0, int(profile[a]) - int(profile[b])) for a, b in ASYNC_PAIRS)


def avg_us(total_ns: Any, count: Any) -> float:
    c = int(count)
    return math.nan if c <= 0 else float(total_ns) / c / 1e3


def start_cluster(case: Case, args: argparse.Namespace) -> tuple[Cluster, list[str]]:
    method = method_for(case)
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=args.witness_count,
            profiling_enabled=case.recovery,
            ablation_mode=case.mode,
        ),
        include_dashboard=False,
    )
    workers = [cluster.add_node(num_cpus=args.cpus_per_node, resources={"producer_node": 1})]
    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(num_cpus=args.cpus_per_node, resources={f"consumer_{i}": 1})
        )
    return cluster, [n.node_id for n in workers]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int) -> bytes:
        prefix = request_id.to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch_and_export(self, wrapped_ref):
            ref = wrapped_ref[0]
            value = ray.get(ref)
            if not value:
                raise RuntimeError("empty payload")
            return [ref]

        def ping(self) -> int:
            import os
            return os.getpid()

        def reset_recovery_profile(self) -> None:
            from ray._private.worker import global_worker as gw
            gw.core_worker.reset_recovery_succession_profile()

        def recovery_profile(self) -> dict[str, Any]:
            from ray._private.worker import global_worker as gw
            return gw.core_worker.get_recovery_succession_profile()

    return produce, Consumer


def run_workload(*, produce, consumer, producer_strategy, args) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, tuple[int, bool]] = {}
    request_id = 0
    tagged_pending = 0
    completed = 0
    tagged_submitted = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    warmup_end = start_ns + int(args.warmup_seconds * 1e9)
    measure_end = warmup_end + int(args.duration_seconds * 1e9)

    def submit_one() -> None:
        nonlocal request_id, tagged_pending, tagged_submitted
        now = time.perf_counter_ns()
        tagged = warmup_end <= now < measure_end
        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, args.payload_bytes)
        stage = consumer.touch_and_export.remote([payload_ref])
        pending[stage] = (now, tagged)
        request_id += 1
        if tagged:
            tagged_pending += 1
            tagged_submitted += 1

    def process_one(resubmit: bool) -> bool:
        nonlocal tagged_pending, completed
        if not pending:
            return False
        ready, _ = ray.wait(list(pending), num_returns=1, timeout=args.wait_timeout_seconds)
        if not ready:
            return False
        ref = ready[0]
        ray.get(ref)
        submitted_ns, tagged = pending.pop(ref)
        done = time.perf_counter_ns()
        if warmup_end <= done < measure_end:
            completed += 1
        if tagged:
            latencies_ms.append((done - submitted_ns) / 1e6)
            tagged_pending -= 1
        if resubmit and (time.perf_counter_ns() < measure_end or tagged_pending > 0):
            submit_one()
        return True

    for _ in range(args.inflight):
        submit_one()
    while time.perf_counter_ns() < measure_end or tagged_pending > 0:
        process_one(True)
    deadline = time.monotonic() + args.drain_timeout_seconds
    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(f"drain timeout with {len(pending)} pending")
        process_one(False)

    return {
        "completed_in_window": completed,
        "total_pipeline_submitted": request_id,
        "latency_sample_count": len(latencies_ms),
        "latency_tagged_submitted": tagged_submitted,
        "throughput_rps": completed / args.duration_seconds,
        "latency_mean_ms": statistics.fmean(latencies_ms) if latencies_ms else math.nan,
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def profile_snapshot(consumers) -> tuple[dict[str, Any], dict[str, Any]]:
    owner = profile_defaults(global_worker.core_worker.get_recovery_succession_profile())
    borrower_raw = ray.get([c.recovery_profile.remote() for c in consumers])
    borrower = aggregate_profiles(borrower_raw)
    return owner, borrower


def wait_for_profile_quiescence(consumers, args) -> tuple[dict[str, Any], dict[str, Any], bool]:
    deadline = time.monotonic() + args.profile_quiescence_timeout_seconds
    last_sig = None
    stable_since = None
    owner, borrower = profile_snapshot(consumers)
    while time.monotonic() < deadline:
        owner, borrower = profile_snapshot(consumers)
        sig = tuple(owner[k] for k in PROFILE_KEYS) + tuple(borrower[k] for k in PROFILE_KEYS)
        now = time.monotonic()
        if outstanding(owner) == 0 and outstanding(borrower) == 0:
            if sig == last_sig:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= args.profile_stable_seconds:
                    return owner, borrower, True
            else:
                stable_since = now
        else:
            stable_since = None
        last_sig = sig
        time.sleep(0.08)
    return owner, borrower, False


def add_scope(row: dict[str, Any], prefix: str, p: dict[str, Any]) -> None:
    for k in PROFILE_KEYS:
        row[f"{prefix}_{k}"] = p[k]


def add_derived(row: dict[str, Any], owner: dict[str, Any], borrower: dict[str, Any]) -> None:
    tasks = max(1, int(row["total_pipeline_submitted"]))
    # Owner-side submission/export hot path.
    row["owner_metadata_lookup_avg_us"] = avg_us(owner["recovery_metadata_lookup_time_ns"], owner["recovery_metadata_lookup_calls"])
    row["owner_ensure_args_avg_us"] = avg_us(owner["ensure_task_arguments_time_ns"], owner["ensure_task_arguments_calls"])
    row["owner_populate_arg_metadata_avg_us"] = avg_us(owner["task_argument_metadata_time_ns"], owner["task_argument_metadata_calls"])
    row["owner_initial_manifest_avg_us"] = avg_us(owner["initial_manifest_build_time_ns"], owner["initial_manifest_build_count"])
    row["owner_witness_selection_avg_us"] = avg_us(owner["witness_selection_time_ns"], owner["witness_selection_count"])
    row["owner_register_owned_avg_us"] = avg_us(owner["register_owned_task_time_ns"], owner["register_owned_task_count"])
    row["owner_piggyback_serialize_avg_us"] = avg_us(owner["first_holder_piggyback_serialize_time_ns"], owner["first_holder_piggyback_copies_sent"])
    row["owner_holder_admission_avg_us"] = avg_us(owner["holder_admission_time_ns"], owner["holder_admissions_committed"])
    row["owner_witness_publish_avg_us"] = avg_us(owner["witness_publish_time_ns"], owner["witness_publish_count"])

    # H1 receive/report hot path.
    row["borrower_register_executor_avg_us"] = avg_us(borrower["register_executor_task_time_ns"], borrower["register_executor_task_calls"])
    row["borrower_candidate_build_avg_us"] = avg_us(borrower["candidate_report_build_time_ns"], borrower["candidate_report_build_calls"])
    row["borrower_candidate_queue_avg_us"] = avg_us(borrower["candidate_queue_time_ns"], borrower["candidate_queue_calls"])
    row["borrower_candidate_rpc_avg_us"] = avg_us(borrower["candidate_rpc_time_ns"], borrower["candidate_rpc_physical_rpcs_completed"])

    # Per-pipeline CPU totals make tiny repeated costs visible.
    row["owner_metadata_lookup_cpu_us_per_pipeline"] = float(owner["recovery_metadata_lookup_time_ns"]) / tasks / 1e3
    row["owner_ensure_args_cpu_us_per_pipeline"] = float(owner["ensure_task_arguments_time_ns"]) / tasks / 1e3
    row["owner_populate_arg_metadata_cpu_us_per_pipeline"] = float(owner["task_argument_metadata_time_ns"]) / tasks / 1e3
    row["borrower_register_executor_cpu_us_per_pipeline"] = float(borrower["register_executor_task_time_ns"]) / tasks / 1e3
    row["borrower_candidate_build_cpu_us_per_pipeline"] = float(borrower["candidate_report_build_time_ns"]) / tasks / 1e3
    row["borrower_candidate_queue_cpu_us_per_pipeline"] = float(borrower["candidate_queue_time_ns"]) / tasks / 1e3
    row["borrower_candidate_reports_per_pipeline"] = float(borrower["candidate_reports_built"]) / tasks
    row["borrower_candidate_rpc_reports_per_pipeline"] = float(borrower["candidate_rpc_logical_reports_sent"]) / tasks
    row["owner_piggyback_copies_per_pipeline"] = float(owner["first_holder_piggyback_copies_sent"]) / tasks
    row["owner_install_rpcs_per_pipeline"] = float(owner["holder_install_rpcs_sent"]) / tasks
    row["owner_control_bytes_per_pipeline"] = (
        float(owner["task_spec_bytes_sent"] + owner["manifest_bytes_sent"] + borrower["candidate_rpc_request_bytes_sent"]) / tasks
    )


def run_one(case: Case, repetition: int, args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    try:
        cluster, node_ids = start_cluster(case, args)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, TARGET_HOLDERS + 2, args.cluster_timeout_seconds)
        produce, Consumer = make_remote_types()
        consumers = [
            Consumer.options(resources={f"consumer_{i}": 0.01}, num_cpus=0).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([c.ping.remote() for c in consumers])

        if case.recovery:
            global_worker.core_worker.reset_recovery_succession_profile()
            ray.get([c.reset_recovery_profile.remote() for c in consumers])

        result = run_workload(
            produce=produce,
            consumer=consumers[0],
            producer_strategy=NodeAffinitySchedulingStrategy(node_id=node_ids[0], soft=False),
            args=args,
        )

        if case.recovery:
            owner, borrower, quiescent = wait_for_profile_quiescence(consumers, args)
        else:
            owner, borrower = profile_defaults(), profile_defaults()
            quiescent = True

        row: dict[str, Any] = {
            "repetition": repetition,
            "case": case.key,
            "label": case.label,
            "recovery_enabled": int(case.recovery),
            "ablation_mode": case.mode,
            "borrower_count": BORROWERS,
            "target_holders": TARGET_HOLDERS,
            "payload_bytes": args.payload_bytes,
            "profile_quiescent": int(quiescent),
            **result,
        }
        add_scope(row, "owner", owner)
        add_scope(row, "borrower", borrower)
        add_derived(row, owner, borrower)
        return row
    finally:
        safe_shutdown(ray, cluster)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metric_names = [
        "owner_metadata_lookup_avg_us",
        "owner_ensure_args_avg_us",
        "owner_populate_arg_metadata_avg_us",
        "owner_initial_manifest_avg_us",
        "owner_witness_selection_avg_us",
        "owner_register_owned_avg_us",
        "owner_piggyback_serialize_avg_us",
        "owner_holder_admission_avg_us",
        "owner_witness_publish_avg_us",
        "borrower_register_executor_avg_us",
        "borrower_candidate_build_avg_us",
        "borrower_candidate_queue_avg_us",
        "borrower_candidate_rpc_avg_us",
        "owner_metadata_lookup_cpu_us_per_pipeline",
        "owner_ensure_args_cpu_us_per_pipeline",
        "owner_populate_arg_metadata_cpu_us_per_pipeline",
        "borrower_register_executor_cpu_us_per_pipeline",
        "borrower_candidate_build_cpu_us_per_pipeline",
        "borrower_candidate_queue_cpu_us_per_pipeline",
        "borrower_candidate_reports_per_pipeline",
        "borrower_candidate_rpc_reports_per_pipeline",
        "owner_piggyback_copies_per_pipeline",
        "owner_install_rpcs_per_pipeline",
        "owner_control_bytes_per_pipeline",
    ]

    for case in cases():
        g = [r for r in rows if r["case"] == case.key]
        if not g:
            continue
        t_mean, t_ci = mean_ci95(float(r["throughput_rps"]) for r in g)
        p50_mean, p50_ci = mean_ci95(float(r["latency_p50_ms"]) for r in g)
        p95_mean, p95_ci = mean_ci95(float(r["latency_p95_ms"]) for r in g)
        row: dict[str, Any] = {
            "case": case.key,
            "label": case.label,
            "ablation_mode": case.mode,
            "repetitions": len(g),
            "throughput_mean_rps": t_mean,
            "throughput_ci95_rps": t_ci,
            "p50_latency_mean_ms": p50_mean,
            "p50_latency_ci95_ms": p50_ci,
            "p95_latency_mean_ms": p95_mean,
            "p95_latency_ci95_ms": p95_ci,
            "profile_quiescent_all": min(int(r["profile_quiescent"]) for r in g),
            "owner_max_non_owner_holders_max": max(int(r["owner_max_non_owner_holders"]) for r in g),
        }
        for name in metric_names:
            vals = [float(r[name]) for r in g if not math.isnan(float(r[name]))]
            row[f"{name}_mean"] = statistics.fmean(vals) if vals else math.nan
        out.append(row)

    disabled_rows = [r for r in out if r["case"] == "disabled"]
    base = float(disabled_rows[0]["throughput_mean_rps"]) if disabled_rows else math.nan
    for r in out:
        r["throughput_loss_vs_disabled_pct"] = (
            100.0 * (base - float(r["throughput_mean_rps"])) / base
            if r["case"] != "disabled" and base > 0 else 0.0
        )
    return out


def run_benchmark(args: argparse.Namespace) -> None:
    order_base = cases()
    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []
    total = args.repetitions * len(order_base)
    idx = 0
    for rep in range(1, args.repetitions + 1):
        order = order_base[:]
        if not args.fixed_order:
            rng.shuffle(order)
        for case in order:
            idx += 1
            print(f"[{idx}/{total}] rep={rep} case={case.label} mode={case.mode}")
            rows.append(run_one(case, rep, args))
    root = Path(args.output_dir)
    write_csv(root / "patch4g_b1_runs.csv", rows)
    summary = summarize(rows)
    write_csv(root / "patch4g_b1_summary.csv", summary)
    print(f"Wrote {root / 'patch4g_b1_summary.csv'}")
    print("\nB1 throughput loss vs Disabled:")
    for r in summary:
        print(f"  {r['label']:24s} {float(r['throughput_mean_rps']):9.1f} rps  loss={float(r['throughput_loss_vs_disabled_pct']):6.2f}%")


def plot(args: argparse.Namespace) -> None:
    import csv
    import matplotlib.pyplot as plt
    root = Path(args.output_dir)
    with (root / "patch4g_b1_summary.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))
    labels = [r["label"] for r in rows]
    losses = [float(r["throughput_loss_vs_disabled_pct"]) for r in rows]
    plt.figure(figsize=(9, 4.8))
    plt.bar(labels, losses)
    plt.axhline(10.0, linestyle="--")
    plt.ylabel("Throughput loss vs Disabled (%)")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    (root / "plots").mkdir(parents=True, exist_ok=True)
    plt.savefig(root / "plots" / "b1_ablation_throughput_loss.png", dpi=160)
    plt.close()

    hot = [
        ("owner_ensure_args_cpu_us_per_pipeline_mean", "Owner ensure args"),
        ("owner_populate_arg_metadata_cpu_us_per_pipeline_mean", "Owner arg metadata"),
        ("borrower_register_executor_cpu_us_per_pipeline_mean", "H1 register executor"),
        ("borrower_candidate_build_cpu_us_per_pipeline_mean", "H1 candidate build"),
        ("borrower_candidate_queue_cpu_us_per_pipeline_mean", "H1 candidate queue"),
    ]
    x = range(len(labels))
    plt.figure(figsize=(9, 5.2))
    for key, name in hot:
        plt.plot(x, [float(r[key]) if r[key] else math.nan for r in rows], marker="o", label=name)
    plt.xticks(list(x), labels, rotation=25, ha="right")
    plt.ylabel("Measured CPU/wall time per pipeline (us)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(root / "plots" / "b1_hotpath_cpu_per_pipeline.png", dpi=160)
    plt.close()
    print(f"Wrote plots to {root / 'plots'}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(s):
        s.add_argument("--repetitions", type=int, default=2)
        s.add_argument("--warmup-seconds", type=float, default=3.0)
        s.add_argument("--duration-seconds", type=float, default=15.0)
        s.add_argument("--inflight", type=int, default=64)
        s.add_argument("--payload-bytes", type=int, default=1024)
        s.add_argument("--cpus-per-node", type=int, default=4)
        s.add_argument("--witness-count", type=int, default=2)
        s.add_argument("--wait-timeout-seconds", type=float, default=1.0)
        s.add_argument("--drain-timeout-seconds", type=float, default=30.0)
        s.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
        s.add_argument("--profile-quiescence-timeout-seconds", type=float, default=8.0)
        s.add_argument("--profile-stable-seconds", type=float, default=0.25)
        s.add_argument("--seed", type=int, default=42)
        s.add_argument("--fixed-order", action="store_true")
        s.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")

    r = sub.add_parser("run")
    add_common(r)
    rp = sub.add_parser("run-and-plot")
    add_common(rp)
    pl = sub.add_parser("plot")
    pl.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.command in {"run", "run-and-plot"}:
        run_benchmark(args)
    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
'''

# Mark manager header for idempotency.
edited["mgr_h"] = edited["mgr_h"].replace(
    "/// Patch 4G: hot-path profiling and B1 ablations.\n",
    f"/// {MARKER}\n",
    1,
)

# ---------------------------------------------------------------------------
# Back up, write, lint.
# ---------------------------------------------------------------------------
stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
backup_root = ROOT / ".patch4g_backups" / stamp
for key in ["config", "mgr_h", "mgr_cc", "core_cc", "common_py", "bench09"]:
    src = FILES[key]
    dst = backup_root / src.relative_to(ROOT)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

# Do not overwrite an existing benchmark file.
if FILES["bench16"].exists():
    die(f"refusing to overwrite existing {FILES['bench16']}")

for key in ["config", "mgr_h", "mgr_cc", "core_cc", "common_py", "bench09"]:
    FILES[key].write_text(edited[key])
FILES["bench16"].write_text(bench16)
FILES["bench16"].chmod(0o755)

try:
    run(["python", "-m", "py_compile", str(FILES["common_py"]), str(FILES["bench09"]), str(FILES["bench16"])])
    run(["git", "diff", "--check"])
except Exception:
    print(f"Patch 4G wrote files but validation failed. Backups are in {backup_root}")
    raise

print("Patch 4G applied successfully.")
print(f"Backups: {backup_root}")
print("Next: rebuild Ray, then run gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py")
