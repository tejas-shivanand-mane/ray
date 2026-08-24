#!/usr/bin/env python3
"""
Apply the optimized fixed-R witness-holder baseline suite.

All optimizations are correctness-preserving and default OFF.  Benchmark env:

  RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS=1

Individual switches (explicit 0/1 overrides ALL):
  RAY_RECOVERY_BASELINE_COMPACT_METADATA
  RAY_RECOVERY_BASELINE_WITNESS_BATCHING
  RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY
  RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE
  RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST
  RAY_RECOVERY_BASELINE_FAST_RECEIVER
  RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION
  RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC
  RAY_RECOVERY_BASELINE_BATCH_SWAP
  RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION

When ALL=1, the existing RAY_RECOVERY_TASKMANAGER_PIN also defaults ON for the
baseline unless explicitly overridden with RAY_RECOVERY_TASKMANAGER_PIN=0.

Algorithmic baseline semantics are unchanged:
  * lazy first-borrow activation;
  * exactly R node-distinct fixed witness holders;
  * R complete replayable TaskSpec lineages;
  * all R acknowledgements required for protection readiness;
  * no borrower-driven holder admission.

Usage:
  python apply_baseline_optimization_suite.py /path/to/ray --check
  python apply_baseline_optimization_suite.py /path/to/ray
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


class PatchError(RuntimeError):
    pass


FILES = [
    "src/ray/common/ray_config_def.h",
    "gossip_benchmarks/_benchmark_common.py",
    "src/ray/core_worker/recovery_succession_manager.cc",
    "src/ray/core_worker/core_worker.cc",
    "src/ray/core_worker/core_worker.h",
    "src/ray/protobuf/node_manager.proto",
    "src/ray/raylet_rpc_client/raylet_client.cc",
    "src/ray/raylet/node_manager.cc",
]


def replace_once(files, rel, old, new, label):
    text = files[rel]
    if new in text:
        print(f"[already] {label}")
        return
    count = text.count(old)
    if count != 1:
        raise PatchError(
            f"{label}: expected exactly one match in {rel}, found {count}"
        )
    files[rel] = text.replace(old, new, 1)
    print(f"[stage] {label}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo).resolve()
    files = {}
    originals = {}
    for rel in FILES:
        path = root / rel
        if not path.exists():
            raise PatchError(f"missing expected file: {path}")
        text = path.read_text()
        files[rel] = text
        originals[rel] = text

    # ------------------------------------------------------------------
    # 1. Independent default-off config flags.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/common/ray_config_def.h",
        """RAY_CONFIG(bool, enable_recovery_witness_holder_baseline, false)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
""",
        """RAY_CONFIG(bool, enable_recovery_witness_holder_baseline, false)

/// Strong fixed-R baseline optimizations. All are default-off so the original
/// baseline remains a reproducible control.
///
/// Use Patch-4H compact dependency metadata on ordinary downstream TaskSpecs.
/// Internal recovery state and witness state remain full RecoveryManifest.
RAY_CONFIG(bool, enable_recovery_baseline_compact_argument_metadata, false)

/// Allow full-lineage baseline witness updates to use the existing per-raylet
/// UpdateRecoveryWitnessBatch transport.
RAY_CONFIG(bool, enable_recovery_baseline_witness_batching, false)

/// Avoid the owner's extra intermediate baseline_task_spec deep copy.
RAY_CONFIG(bool, enable_recovery_baseline_elide_task_spec_copy, false)

/// Serialize complete baseline lineage once at activation and transport those
/// bytes to all R holders instead of traversing the protobuf independently R times.
RAY_CONFIG(bool, enable_recovery_baseline_serialize_task_spec_once, false)

/// Store the authoritative RecoveryManifest separately from the witness's full
/// lineage TaskSpec; reattach it only when granting replay.
RAY_CONFIG(bool, enable_recovery_baseline_separate_manifest_storage, false)

/// For ordinary downstream baseline tasks, register only borrowed-object recovery
/// state and skip Succession-only piggyback/candidate-report processing.
RAY_CONFIG(bool, enable_recovery_baseline_fast_receiver, false)

/// Compare baseline manifests as protobuf messages instead of serializing both
/// messages to temporary strings for equality checks.
RAY_CONFIG(bool, enable_recovery_baseline_fast_manifest_validation, false)

/// Move the already-parsed baseline TaskSpec request into witness storage rather
/// than deep-copying it again.
RAY_CONFIG(bool, enable_recovery_baseline_move_witness_task_spec, false)

/// Move queued baseline witness updates into the physical batch request instead
/// of deep-copying each logical update.
RAY_CONFIG(bool, enable_recovery_baseline_batch_request_swap, false)

/// Preserve identical deterministic top-R witness selection while avoiding
/// repeated TaskID serialization/string concatenation and a full candidate sort.
RAY_CONFIG(bool, enable_recovery_baseline_topk_witness_selection, false)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
""",
        "add baseline optimization config flags",
    )

    # ------------------------------------------------------------------
    # 2. Benchmark env plumbing. ALL=1 enables every baseline optimization;
    #    any individual env variable explicitly set to 0 or 1 overrides ALL.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        """    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )
    task_manager_pin = (
        os.environ.get("RAY_RECOVERY_TASKMANAGER_PIN", "0") == "1"
        and method.recovery_enabled
    )
    config: dict[str, Any] = {
""",
        """    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )

    baseline_all = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS", "0") == "1"
    )

    def baseline_opt(env_name: str) -> bool:
        if not method.baseline_enabled:
            return False
        raw = os.environ.get(env_name)
        return baseline_all if raw is None else raw == "1"

    # Patch 4N is already correctness-preserving. Include it in the maximally
    # optimized baseline unless explicitly disabled.
    task_manager_pin_raw = os.environ.get("RAY_RECOVERY_TASKMANAGER_PIN")
    task_manager_pin = method.recovery_enabled and (
        (task_manager_pin_raw == "1")
        if task_manager_pin_raw is not None
        else baseline_all
    )

    baseline_compact_metadata = baseline_opt(
        "RAY_RECOVERY_BASELINE_COMPACT_METADATA"
    )
    baseline_witness_batching = baseline_opt(
        "RAY_RECOVERY_BASELINE_WITNESS_BATCHING"
    )
    baseline_elide_taskspec_copy = baseline_opt(
        "RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY"
    )
    baseline_serialize_taskspec_once = baseline_opt(
        "RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"
    )
    baseline_separate_manifest = baseline_opt(
        "RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST"
    )
    baseline_fast_receiver = baseline_opt(
        "RAY_RECOVERY_BASELINE_FAST_RECEIVER"
    )
    baseline_fast_manifest_validation = baseline_opt(
        "RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION"
    )
    baseline_move_witness_taskspec = baseline_opt(
        "RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC"
    )
    baseline_batch_swap = baseline_opt(
        "RAY_RECOVERY_BASELINE_BATCH_SWAP"
    )
    baseline_topk_witness_selection = baseline_opt(
        "RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION"
    )

    config: dict[str, Any] = {
""",
        "add baseline optimization env parsing",
    )

    replace_once(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        """        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "recovery_succession_witness_count": max(1, int(witness_count)),
""",
        """        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "enable_recovery_baseline_compact_argument_metadata": baseline_compact_metadata,
        "enable_recovery_baseline_witness_batching": baseline_witness_batching,
        "enable_recovery_baseline_elide_task_spec_copy": baseline_elide_taskspec_copy,
        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "enable_recovery_baseline_separate_manifest_storage": baseline_separate_manifest,
        "enable_recovery_baseline_fast_receiver": baseline_fast_receiver,
        "enable_recovery_baseline_fast_manifest_validation": baseline_fast_manifest_validation,
        "enable_recovery_baseline_move_witness_task_spec": baseline_move_witness_taskspec,
        "enable_recovery_baseline_batch_request_swap": baseline_batch_swap,
        "enable_recovery_baseline_topk_witness_selection": baseline_topk_witness_selection,
        "recovery_succession_witness_count": max(1, int(witness_count)),
""",
        "pass baseline optimization system config",
    )

    # ------------------------------------------------------------------
    # 2b. Patch-4N accounting cleanup informed by the earlier diagnostics:
    #     when profiling is disabled, avoid ByteSizeLong solely for accounting.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  if (task_manager_pin) {
    // TaskManager already owns the TaskSpec. Keep only lifetime bookkeeping
    // here. ByteSizeLong remains for apples-to-apples benchmark accounting.
    retained.task_spec_bytes =
        static_cast<uint64_t>(task_proto.ByteSizeLong());
  } else {
""",
        """  if (task_manager_pin) {
    // TaskManager already owns the TaskSpec. Keep only lifetime bookkeeping.
    // ByteSizeLong is profiling/accounting only; do not traverse a potentially
    // large TaskSpec on the production fast path when profiling is disabled.
    retained.task_spec_bytes =
        profiling_enabled_
            ? static_cast<uint64_t>(task_proto.ByteSizeLong())
            : 0;
  } else {
""",
        "skip 4N TaskSpec ByteSizeLong when profiling is off",
    )

    # ------------------------------------------------------------------
    # 3. Compact normal-path metadata for baseline.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        """    // Keep witness-as-holder baseline semantics and representation unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    } else if (entry->has_owner_address()) {
      compact_transport = WriteCompactTaskArgumentRecoveryMetadata(
          *source, source->manifest(), entry->owner_address(), out);
      if (!compact_transport) {
        out->CopyFrom(*source);
        out->clear_first_holder_task_spec();
        out->clear_compact_manifest();
      }
    } else {
      // Safety fallback: a full manifest does not need owner reconstruction.
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }
""",
        """    const bool baseline_enabled =
        RayConfig::instance().enable_recovery_witness_holder_baseline();
    const bool compact_allowed =
        !baseline_enabled ||
        RayConfig::instance().enable_recovery_baseline_compact_argument_metadata();

    if (compact_allowed && entry->has_owner_address()) {
      compact_transport = WriteCompactTaskArgumentRecoveryMetadata(
          *source, source->manifest(), entry->owner_address(), out);
      if (!compact_transport) {
        out->CopyFrom(*source);
        out->clear_first_holder_task_spec();
        out->clear_compact_manifest();
      }
    } else {
      // Original baseline representation or safety fallback when the owner
      // address cannot reconstruct rank 0.
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }
""",
        "enable compact dependency metadata for baseline",
    )

    replace_once(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        """      if (compact_transport) {
        ++profile_.task_argument_metadata_compact_refs;
      } else if (!RayConfig::instance().enable_recovery_witness_holder_baseline()) {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
""",
        """      if (compact_transport) {
        ++profile_.task_argument_metadata_compact_refs;
      } else if (
          !RayConfig::instance().enable_recovery_witness_holder_baseline() ||
          RayConfig::instance().enable_recovery_baseline_compact_argument_metadata()) {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
""",
        "profile baseline compact fallbacks",
    )

    # ------------------------------------------------------------------
    # 4. Baseline executor fast path.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        """  const bool should_store_task = IsEligibleTask(task_spec) &&
                                 task_spec.has_recovery_manifest() &&
                                 !task_spec.task_id().empty();

  std::vector<CandidateReport> reports;
""",
        """  const bool should_store_task = IsEligibleTask(task_spec) &&
                                 task_spec.has_recovery_manifest() &&
                                 !task_spec.task_id().empty();

  if (RayConfig::instance().enable_recovery_witness_holder_baseline() &&
      RayConfig::instance().enable_recovery_baseline_fast_receiver() &&
      !should_store_task) {
    std::vector<CandidateReport> reports;
    absl::MutexLock lock(&mutex_);

    // Fixed-R executors never become lineage holders. Only borrowed-object
    // recovery state and the cached fixed witness manifest are needed.
    for (const auto &[object_id, metadata] : received_metadata) {
      if (metadata.task_id().size() != TaskID::Size() ||
          !metadata.has_manifest()) {
        continue;
      }

      const TaskID metadata_task_id = TaskID::FromBinary(metadata.task_id());
      const auto tombstone_it = task_states_.find(metadata_task_id);
      if (tombstone_it != task_states_.end() &&
          tombstone_it->second.manifest.tombstoned() &&
          CompareManifestVersions(tombstone_it->second.manifest,
                                  metadata.manifest()) >= 0) {
        continue;
      }

      BorrowedObjectRecoveryState borrowed_state;
      borrowed_state.task_id = metadata_task_id;
      borrowed_state.return_index = metadata.return_index();
      borrowed_objects_[object_id] = std::move(borrowed_state);

      TaskRecoveryState &dependency_state = task_states_[metadata_task_id];
      if (dependency_state.manifest.task_id().empty() ||
          CompareManifestVersions(metadata.manifest(),
                                  dependency_state.manifest) > 0) {
        dependency_state.manifest.CopyFrom(metadata.manifest());
      }
    }

    if (profiling_enabled_) {
      ++profile_.register_executor_task_calls;
      profile_.register_executor_task_time_ns += static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - patch4g_start)
              .count());
      profile_.register_executor_metadata_refs_seen +=
          static_cast<uint64_t>(received_metadata.size());
    }

    return reports;
  }

  std::vector<CandidateReport> reports;
""",
        "add baseline executor fast path",
    )

    # ------------------------------------------------------------------
    # 5. Same deterministic top-R placement with cheaper scoring + top-k.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """uint64_t StableWitnessScore(const TaskID &task_id, const NodeID &node_id) {
  // FNV-1a over TaskID || NodeID.
  constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;

  const std::string input = task_id.Binary() + node_id.Binary();

  for (const unsigned char byte : input) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }

  return hash;
}
""",
        """uint64_t StableWitnessScore(const TaskID &task_id, const NodeID &node_id) {
  // FNV-1a over TaskID || NodeID.
  constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;

  const std::string input = task_id.Binary() + node_id.Binary();

  for (const unsigned char byte : input) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }

  return hash;
}

uint64_t StableWitnessScoreOptimized(const std::string &task_id_binary,
                                     const NodeID &node_id) {
  // Bit-for-bit identical FNV-1a input to StableWitnessScore, but the TaskID
  // binary is computed once per selection and no concatenated string is built.
  constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;
  for (const unsigned char byte : task_id_binary) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }

  const std::string node_id_binary = node_id.Binary();
  for (const unsigned char byte : node_id_binary) {
    hash ^= static_cast<uint64_t>(byte);
    hash *= kPrime;
  }
  return hash;
}
""",
        "add allocation-reduced witness scorer",
    )

    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """  // Preserve the existing deterministic per-task witness selection.
  for (auto &[node_id, address] : alive_witnesses) {
    candidates.push_back(
        WitnessCandidate{
            StableWitnessScore(task_id, node_id),
            std::move(address)});
  }
""",
        """  // Preserve the exact deterministic per-task witness scores.
  const bool optimized_baseline_selection =
      recovery_witness_holder_baseline_enabled_ &&
      RayConfig::instance().enable_recovery_baseline_topk_witness_selection();
  const std::string task_id_binary =
      optimized_baseline_selection ? task_id.Binary() : std::string();

  for (auto &[node_id, address] : alive_witnesses) {
    candidates.push_back(
        WitnessCandidate{
            optimized_baseline_selection
                ? StableWitnessScoreOptimized(task_id_binary, node_id)
                : StableWitnessScore(task_id, node_id),
            std::move(address)});
  }
""",
        "reuse TaskID binary during baseline witness scoring",
    )
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """  std::sort(
      candidates.begin(),
      candidates.end(),
      [](const WitnessCandidate &left,
         const WitnessCandidate &right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }

        return left.address.node_id() <
               right.address.node_id();
      });

  const size_t selected_count =
      std::min<size_t>(
          requested_count,
          candidates.size());
""",
        """  const size_t selected_count =
      std::min<size_t>(
          requested_count,
          candidates.size());

  const auto better_witness =
      [](const WitnessCandidate &left,
         const WitnessCandidate &right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.address.node_id() < right.address.node_id();
      };

  if (recovery_witness_holder_baseline_enabled_ &&
      RayConfig::instance().enable_recovery_baseline_topk_witness_selection() &&
      selected_count < candidates.size()) {
    // O(N) partition plus O(R log R) ordering instead of O(N log N).
    std::nth_element(
        candidates.begin(),
        candidates.begin() + selected_count,
        candidates.end(),
        better_witness);
    std::sort(
        candidates.begin(),
        candidates.begin() + selected_count,
        better_witness);
  } else {
    std::sort(candidates.begin(), candidates.end(), better_witness);
  }
""",
        "use partial top-R witness selection",
    )

    # ------------------------------------------------------------------
    # 6. Serialized full-lineage transport.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/protobuf/node_manager.proto",
        """  // Patch 4M-CERT: delta update for normal Succession.  The witness unions
  // independently confirmed holder certificates into its materialized
  // RecoveryManifest instead of replacing the whole manifest per holder.
  optional RecoveryHolderCertificate holder_certificate = 3;
}
""",
        """  // Patch 4M-CERT: delta update for normal Succession.  The witness unions
  // independently confirmed holder certificates into its materialized
  // RecoveryManifest instead of replacing the whole manifest per holder.
  optional RecoveryHolderCertificate holder_certificate = 3;

  // Optimized fixed-R transport. This contains the exact same complete
  // replayable TaskSpec lineage as task_spec, serialized once by the owner.
  // task_spec and serialized_task_spec are mutually exclusive.
  bytes serialized_task_spec = 4;
}
""",
        "add serialized TaskSpec witness transport",
    )

    replace_once(
        files,
        "src/ray/core_worker/core_worker.h",
        """  void PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec = nullptr) const;
""",
        """  void PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec = nullptr,
    const std::string *serialized_task_spec = nullptr) const;
""",
        "extend witness publication API",
    )

    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """void CoreWorker::PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec) const {

  const bool require_all_witnesses =
    task_spec != nullptr;
""",
        """void CoreWorker::PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec,
    const std::string *serialized_task_spec) const {

  RAY_CHECK(task_spec == nullptr || serialized_task_spec == nullptr);
  const bool require_all_witnesses =
      task_spec != nullptr || serialized_task_spec != nullptr;
""",
        "extend witness publication implementation",
    )

    # ------------------------------------------------------------------
    # 7. Owner baseline construction / copy-elision / serialize-once.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """    // The original task was submitted without recovery metadata. Attach the
    // lazily-created manifest only to the private copy sent to baseline holders.
    rpc::TaskSpec baseline_task_spec;
    baseline_task_spec.CopyFrom(task_proto);
    baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);

    const uint64_t publish_start_ns =
        recovery_succession_profiling_enabled_
            ? RecoveryProfileNowNs()
            : 0;

    PublishRecoveryManifestToWitnesses(
        manifest,
        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns](
            bool stored,
            std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
""",
        """    const bool separate_manifest_storage =
        RayConfig::instance().enable_recovery_baseline_separate_manifest_storage();
    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();
    const bool elide_intermediate_copy =
        RayConfig::instance().enable_recovery_baseline_elide_task_spec_copy();

    rpc::TaskSpec baseline_task_spec;
    std::string serialized_baseline_task_spec;
    const rpc::TaskSpec *publish_task_spec = nullptr;
    const std::string *publish_serialized_task_spec = nullptr;

    if (serialize_task_spec_once) {
      if (separate_manifest_storage) {
        serialized_baseline_task_spec = task_proto.SerializeAsString();
      } else {
        baseline_task_spec.CopyFrom(task_proto);
        baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
        serialized_baseline_task_spec = baseline_task_spec.SerializeAsString();
      }
      publish_serialized_task_spec = &serialized_baseline_task_spec;
    } else if (separate_manifest_storage || elide_intermediate_copy) {
      // Publication will either keep the manifest separate or attach it
      // directly to each outgoing request.
      publish_task_spec = &task_proto;
    } else {
      // Original fixed-R baseline control.
      baseline_task_spec.CopyFrom(task_proto);
      baseline_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);
      publish_task_spec = &baseline_task_spec;
    }

    const uint64_t publish_start_ns =
        recovery_succession_profiling_enabled_
            ? RecoveryProfileNowNs()
            : 0;

    PublishRecoveryManifestToWitnesses(
        manifest,
        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns](
            bool stored,
            std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
""",
        "optimize owner baseline TaskSpec preparation",
    )

    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """        },
        &baseline_task_spec);
  }

  // If another thread won the initialization race, its metadata is visible
""",
        """        },
        publish_task_spec,
        publish_serialized_task_spec);
  }

  // If another thread won the initialization race, its metadata is visible
""",
        "publish selected baseline lineage representation",
    )

    # Build each logical witness update.
    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """    if (task_spec != nullptr) {
      request.mutable_task_spec()->CopyFrom(*task_spec);
    }

    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);
""",
        """    if (serialized_task_spec != nullptr) {
      request.set_serialized_task_spec(*serialized_task_spec);
    } else if (task_spec != nullptr) {
      request.mutable_task_spec()->CopyFrom(*task_spec);

      if (recovery_witness_holder_baseline_enabled_) {
        if (RayConfig::instance()
                .enable_recovery_baseline_separate_manifest_storage()) {
          request.mutable_task_spec()->clear_recovery_manifest();
        } else if (RayConfig::instance()
                       .enable_recovery_baseline_elide_task_spec_copy()) {
          request.mutable_task_spec()
              ->mutable_recovery_manifest()
              ->CopyFrom(manifest);
        }
      }
    }

    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);
""",
        "construct optimized baseline witness request",
    )

    replace_once(
        files,
        "src/ray/core_worker/core_worker.cc",
        """      const uint64_t task_spec_bytes =
          task_spec != nullptr
              ? static_cast<uint64_t>(
                    task_spec->ByteSizeLong())
              : 0;
""",
        """      const uint64_t task_spec_bytes =
          !request.serialized_task_spec().empty()
              ? static_cast<uint64_t>(request.serialized_task_spec().size())
              : (request.has_task_spec()
                     ? static_cast<uint64_t>(request.task_spec().ByteSizeLong())
                     : 0);
""",
        "profile actual transmitted lineage bytes",
    )

    # ------------------------------------------------------------------
    # 8. Baseline enters existing witness batch transport; queued requests move
    #    into physical batch when requested.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/raylet_rpc_client/raylet_client.cc",
        """  // Keep the witness-as-holder baseline on the original one-request RPC path.
  // Those requests can contain a full TaskSpec and are not the compact normal
  // Recovery Succession traffic targeted by Patch 4B-3.
  if (request.has_task_spec()) {
""",
        """  const bool baseline_lineage_request =
      request.has_task_spec() || !request.serialized_task_spec().empty();

  // Original baseline control: one physical RPC per logical witness update.
  // The optimized baseline feeds full-lineage updates through the existing
  // per-raylet batcher without changing logical callbacks or all-R durability.
  if (baseline_lineage_request &&
      !RayConfig::instance().enable_recovery_baseline_witness_batching()) {
""",
        "enable optional baseline witness batching",
    )

    replace_once(
        files,
        "src/ray/raylet_rpc_client/raylet_client.cc",
        """  rpc::UpdateRecoveryWitnessBatchRequest request;
  for (const auto &item : *batch) {
    request.add_updates()->CopyFrom(item.request);
  }
""",
        """  rpc::UpdateRecoveryWitnessBatchRequest request;
  for (auto &item : *batch) {
    rpc::UpdateRecoveryWitnessRequest *update = request.add_updates();
    if (RayConfig::instance().enable_recovery_baseline_batch_request_swap() &&
        RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      update->Swap(&item.request);
    } else {
      update->CopyFrom(item.request);
    }
  }
""",
        "move baseline logical updates into physical batches",
    )

    # ------------------------------------------------------------------
    # 9. Raylet baseline validation/storage optimizations.
    # ------------------------------------------------------------------
    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """#include <vector>

#include "absl/strings/str_format.h"
""",
        """#include <vector>

#include <google/protobuf/util/message_differencer.h>

#include "absl/strings/str_format.h"
""",
        "include protobuf MessageDifferencer",
    )

    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """    if (!certificate_mode || request.has_task_spec() ||
        !ValidRecoveryHolderCertificate(request.holder_certificate()) ||
""",
        """    if (!certificate_mode || request.has_task_spec() ||
        !request.serialized_task_spec().empty() ||
        !ValidRecoveryHolderCertificate(request.holder_certificate()) ||
""",
        "reject serialized full lineage on certificate path",
    )

    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """  const rpc::RecoveryManifest &incoming = request.manifest();
  const TaskID task_id = TaskID::FromBinary(incoming.task_id());

  if (request.has_task_spec()) {
    if (!baseline_enabled ||
        request.task_spec().task_id() != incoming.task_id() ||
        !request.task_spec().has_recovery_manifest() ||
        request.task_spec().recovery_manifest().SerializeAsString() !=
            incoming.SerializeAsString()) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
  }

  {
""",
        """  const rpc::RecoveryManifest &incoming = request.manifest();
  const TaskID task_id = TaskID::FromBinary(incoming.task_id());

  const bool has_serialized_task_spec =
      !request.serialized_task_spec().empty();

  if (request.has_task_spec() && has_serialized_task_spec) {
    reply->set_stored(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  rpc::TaskSpec decoded_task_spec;
  rpc::TaskSpec *incoming_task_spec = nullptr;

  if (request.has_task_spec()) {
    incoming_task_spec = request.mutable_task_spec();
  } else if (has_serialized_task_spec) {
    if (!baseline_enabled ||
        !RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once() ||
        !decoded_task_spec.ParseFromString(request.serialized_task_spec())) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
    incoming_task_spec = &decoded_task_spec;
  }

  const bool fast_manifest_validation =
      baseline_enabled &&
      RayConfig::instance().enable_recovery_baseline_fast_manifest_validation();

  const auto manifests_equal =
      [fast_manifest_validation](const rpc::RecoveryManifest &left,
                                 const rpc::RecoveryManifest &right) {
        if (fast_manifest_validation) {
          return google::protobuf::util::MessageDifferencer::Equals(left, right);
        }
        return left.SerializeAsString() == right.SerializeAsString();
      };

  if (incoming_task_spec != nullptr) {
    const bool separate_manifest_storage =
        RayConfig::instance().enable_recovery_baseline_separate_manifest_storage();

    bool valid_lineage =
        baseline_enabled &&
        incoming_task_spec->task_id() == incoming.task_id();

    if (valid_lineage && separate_manifest_storage) {
      valid_lineage =
          !incoming_task_spec->has_recovery_manifest() ||
          manifests_equal(incoming_task_spec->recovery_manifest(), incoming);
    } else if (valid_lineage) {
      valid_lineage =
          incoming_task_spec->has_recovery_manifest() &&
          manifests_equal(incoming_task_spec->recovery_manifest(), incoming);
    }

    if (!valid_lineage) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
  }

  {
""",
        "validate parsed/serialized baseline lineage",
    )

    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """      } else if (comparison == 0 &&
                 incoming.SerializeAsString() == existing.SerializeAsString()) {
        reply->set_stored(true);
""",
        """      } else if (comparison == 0 &&
                 manifests_equal(incoming, existing)) {
        reply->set_stored(true);
""",
        "avoid manifest string serialization on equal generation",
    )

    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """      } else if (baseline_enabled && request.has_task_spec()) {
        recovery_witness_task_specs_[task_id].CopyFrom(request.task_spec());
      } else {
        auto task_spec_it = recovery_witness_task_specs_.find(task_id);
        if (task_spec_it != recovery_witness_task_specs_.end()) {
          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);
        }
      }
""",
        """      } else if (baseline_enabled && incoming_task_spec != nullptr) {
        rpc::TaskSpec &stored_task_spec =
            recovery_witness_task_specs_[task_id];

        if (has_serialized_task_spec) {
          // decoded_task_spec is already owned by this handler.
          stored_task_spec.Swap(&decoded_task_spec);
        } else if (
            RayConfig::instance().enable_recovery_baseline_move_witness_task_spec()) {
          stored_task_spec.Swap(request.mutable_task_spec());
        } else {
          stored_task_spec.CopyFrom(*incoming_task_spec);
        }

        if (RayConfig::instance()
                .enable_recovery_baseline_separate_manifest_storage()) {
          stored_task_spec.clear_recovery_manifest();
        }
      } else {
        auto task_spec_it = recovery_witness_task_specs_.find(task_id);
        if (task_spec_it != recovery_witness_task_specs_.end() &&
            !RayConfig::instance()
                 .enable_recovery_baseline_separate_manifest_storage()) {
          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);
        }
      }
""",
        "move/store witness TaskSpec and separate manifest",
    )

    # There are two claim-reply TaskSpec copies: idempotent and newly granted.
    old_reply_copy = """              reply->mutable_task_spec()->CopyFrom(
                  task_spec_it->second);
"""
    new_reply_copy = """              reply->mutable_task_spec()->CopyFrom(
                  task_spec_it->second);
              if (RayConfig::instance()
                      .enable_recovery_baseline_separate_manifest_storage()) {
                reply->mutable_task_spec()
                    ->mutable_recovery_manifest()
                    ->CopyFrom(stored_manifest);
              }
"""
    count = files["src/ray/raylet/node_manager.cc"].count(old_reply_copy)
    if count != 1:
        raise PatchError(
            "reattach manifest for idempotent claim: expected exactly one "
            f"match, found {count}"
        )
    files["src/ray/raylet/node_manager.cc"] = files[
        "src/ray/raylet/node_manager.cc"
    ].replace(old_reply_copy, new_reply_copy, 1)
    print("[stage] reattach manifest for idempotent recovery claim")

    replace_once(
        files,
        "src/ray/raylet/node_manager.cc",
        """              // Keep the retained full TaskSpec synchronized with the
              // authoritative manifest stored at this witness.
              task_spec_it->second
                  .mutable_recovery_manifest()
                  ->CopyFrom(stored_manifest);

              RecoveryWitnessClaimState
""",
        """              // With separate-manifest storage the retained full lineage
              // remains immutable; the authoritative manifest is attached only
              // to the recovery reply.
              if (!RayConfig::instance()
                       .enable_recovery_baseline_separate_manifest_storage()) {
                task_spec_it->second
                    .mutable_recovery_manifest()
                    ->CopyFrom(stored_manifest);
              }

              RecoveryWitnessClaimState
""",
        "avoid duplicate witness manifest maintenance",
    )

    old_new_claim = """              reply->mutable_task_spec()
                  ->CopyFrom(
                      task_spec_it->second);
"""
    new_new_claim = """              reply->mutable_task_spec()
                  ->CopyFrom(
                      task_spec_it->second);
              if (RayConfig::instance()
                      .enable_recovery_baseline_separate_manifest_storage()) {
                reply->mutable_task_spec()
                    ->mutable_recovery_manifest()
                    ->CopyFrom(stored_manifest);
              }
"""
    count = files["src/ray/raylet/node_manager.cc"].count(old_new_claim)
    if count != 1:
        raise PatchError(
            "reattach manifest for new claim: expected exactly one match, "
            f"found {count}"
        )
    files["src/ray/raylet/node_manager.cc"] = files[
        "src/ray/raylet/node_manager.cc"
    ].replace(old_new_claim, new_new_claim, 1)
    print("[stage] reattach manifest for new recovery claim")

    # ------------------------------------------------------------------
    # 10. Validate intended feature preservation and patch completeness.
    # ------------------------------------------------------------------
    required = {
        "src/ray/common/ray_config_def.h": [
            "enable_recovery_baseline_compact_argument_metadata",
            "enable_recovery_baseline_witness_batching",
            "enable_recovery_baseline_elide_task_spec_copy",
            "enable_recovery_baseline_serialize_task_spec_once",
            "enable_recovery_baseline_separate_manifest_storage",
            "enable_recovery_baseline_fast_receiver",
            "enable_recovery_baseline_fast_manifest_validation",
            "enable_recovery_baseline_move_witness_task_spec",
            "enable_recovery_baseline_batch_request_swap",
            "enable_recovery_baseline_topk_witness_selection",
            "enable_recovery_succession_profiling",
            "enable_recovery_succession_task_manager_pin",
        ],
        "gossip_benchmarks/_benchmark_common.py": [
            "RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS",
            "RAY_RECOVERY_BASELINE_COMPACT_METADATA",
            "RAY_RECOVERY_BASELINE_WITNESS_BATCHING",
            "RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE",
        ],
        "src/ray/protobuf/node_manager.proto": [
            "serialized_task_spec = 4",
            "UpdateRecoveryWitnessBatchRequest",
        ],
        "src/ray/raylet_rpc_client/raylet_client.cc": [
            "enable_recovery_baseline_witness_batching",
            "enable_recovery_baseline_batch_request_swap",
        ],
        "src/ray/raylet/node_manager.cc": [
            "enable_recovery_baseline_fast_manifest_validation",
            "enable_recovery_baseline_move_witness_task_spec",
            "enable_recovery_baseline_separate_manifest_storage",
        ],
        "src/ray/core_worker/recovery_succession_manager.cc": [
            "MaybeAddCandidateReportLocked",
            "enable_recovery_baseline_fast_receiver",
        ],
    }
    for rel, tokens in required.items():
        for token in tokens:
            if token not in files[rel]:
                raise PatchError(
                    f"postcondition failed: {token!r} missing from {rel}"
                )

    changed = [rel for rel in FILES if files[rel] != originals[rel]]
    print("\nPreflight passed.")
    print("Files to change:")
    for rel in changed:
        print(f"  - {rel}")

    if args.check:
        print("\n--check requested; no files were written.")
        return

    for rel in changed:
        (root / rel).write_text(files[rel])

    print("\nOptimized fixed-R baseline suite applied.")
    print("Rebuild Ray before benchmarking.")
    print("Enable all: RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS=1")
    print("Explicit individual 0/1 settings override the ALL switch.")


if __name__ == "__main__":
    try:
        main()
    except PatchError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
