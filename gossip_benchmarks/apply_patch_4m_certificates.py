#!/usr/bin/env python3
"""
Patch 4M-CERT: CRDT-inspired independently witness-confirmed holder certificates.

Applies to tejas-shivanand-mane/ray main around Patch 4L/4K.

What it changes (behind a new config flag, default OFF):
  * owner still remains the single admission authority and enforces <= R
  * each installed candidate gets an independent holder certificate
  * certificate witness publications may run in parallel
  * witnesses merge certificates as an idempotent grow-only holder set
  * recovery order is derived deterministically by worker_id and written back as ranks
  * provisional possession still cannot replay without witness confirmation
  * tombstones remain absorbing

What it intentionally does NOT change yet:
  * candidate-report batching
  * holder-install batching
  * 4L owner TaskSpec retention
  * witness-update batching across tasks (do this as a follow-up once this ablation works)
  * retry-attempt ownership/claim semantics

Usage:
    cd /path/to/ray
    python /path/to/apply_patch_4m_certificates.py

Then rebuild Ray as you normally do and enable with:
    enable_recovery_succession_certificate_admission = true

The script creates *.pre4m_cert.bak backups and is designed to fail loudly if
expected Patch-4L/4K anchors are not found.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def die(msg: str) -> None:
    raise RuntimeError(msg)


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        die(f"{label}: expected exactly 1 match, found {count}")
    return text.replace(old, new, 1)


def insert_after(text: str, marker: str, addition: str, label: str) -> str:
    idx = text.find(marker)
    if idx < 0:
        die(f"{label}: marker not found")
    idx += len(marker)
    return text[:idx] + addition + text[idx:]


def find_function_span(text: str, signature_prefix: str) -> tuple[int, int]:
    start = text.find(signature_prefix)
    if start < 0:
        die(f"function not found: {signature_prefix}")
    brace = text.find("{", start)
    if brace < 0:
        die(f"opening brace not found: {signature_prefix}")
    depth = 0
    in_str = False
    in_char = False
    escape = False
    i = brace
    while i < len(text):
        c = text[i]
        if escape:
            escape = False
        elif c == "\\" and (in_str or in_char):
            escape = True
        elif c == '"' and not in_char:
            in_str = not in_str
        elif c == "'" and not in_str:
            in_char = not in_char
        elif not in_str and not in_char:
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return start, i + 1
        i += 1
    die(f"unterminated function: {signature_prefix}")


def replace_function(text: str, signature_prefix: str, replacement: str) -> str:
    start, end = find_function_span(text, signature_prefix)
    return text[:start] + replacement.rstrip() + "\n" + text[end:]


def edit_function(text: str, signature_prefix: str, editor) -> str:
    start, end = find_function_span(text, signature_prefix)
    old = text[start:end]
    new = editor(old)
    return text[:start] + new + text[end:]


def replace_between_in_function(
    text: str,
    signature_prefix: str,
    start_marker: str,
    end_marker: str,
    replacement: str,
) -> str:
    def editor(fn: str) -> str:
        s = fn.find(start_marker)
        if s < 0:
            die(f"{signature_prefix}: start marker not found")
        e = fn.find(end_marker, s)
        if e < 0:
            die(f"{signature_prefix}: end marker not found")
        return fn[:s] + replacement + fn[e:]
    return edit_function(text, signature_prefix, editor)


def insert_before_in_function(
    text: str, signature_prefix: str, marker: str, addition: str
) -> str:
    def editor(fn: str) -> str:
        idx = fn.find(marker)
        if idx < 0:
            die(f"{signature_prefix}: marker not found: {marker[:80]}")
        return fn[:idx] + addition + fn[idx:]
    return edit_function(text, signature_prefix, editor)


def load(path: Path) -> str:
    if not path.exists():
        die(f"missing file: {path}")
    return path.read_text()


def save(path: Path, text: str) -> None:
    backup = path.with_suffix(path.suffix + ".pre4m_cert.bak")
    if not backup.exists():
        shutil.copy2(path, backup)
    path.write_text(text)
    print(f"patched {path}")


def patch_common_proto(root: Path) -> None:
    path = root / "src/ray/protobuf/common.proto"
    text = load(path)
    if "message RecoveryHolderCertificate" in text:
        print(f"already patched {path}")
        return
    holder = """message RecoveryHolder {
  Address address = 1;
  uint32 rank = 2;

  // Initially use NodeID as the failure domain.
  bytes failure_domain_id = 3;
}
"""
    cert = holder + """

// Patch 4M-CERT: one independently mergeable holder-confirmation fact.
// `generation` is the owner-issued admission generation.  It is NOT used as
// last-writer-wins for certificates: same-task certificates are unioned by
// witnesses, and the materialized RecoveryManifest stores the max generation.
message RecoveryHolderCertificate {
  bytes task_id = 1;
  uint64 generation = 2;
  uint32 slot = 3;
  RecoveryHolder holder = 4;
}
"""
    text = replace_once(text, holder, cert, "common.proto RecoveryHolder")
    save(path, text)


def patch_node_manager_proto(root: Path) -> None:
    path = root / "src/ray/protobuf/node_manager.proto"
    text = load(path)
    if "holder_certificate = 3" in text:
        print(f"already patched {path}")
        return
    old = """  // Populated only by the witness-as-holder baseline.
  // Baseline witnesses retain the complete replayable lineage.
  optional TaskSpec task_spec = 2;
}"""
    new = """  // Populated only by the witness-as-holder baseline.
  // Baseline witnesses retain the complete replayable lineage.
  optional TaskSpec task_spec = 2;

  // Patch 4M-CERT: delta update for normal Succession.  The witness unions
  // independently confirmed holder certificates into its materialized
  // RecoveryManifest instead of replacing the whole manifest per holder.
  optional RecoveryHolderCertificate holder_certificate = 3;
}"""
    text = replace_once(text, old, new, "node_manager.proto witness request")
    save(path, text)


def patch_config(root: Path) -> None:
    path = root / "src/ray/common/ray_config_def.h"
    text = load(path)
    if "enable_recovery_succession_certificate_admission" in text:
        print(f"already patched {path}")
        return
    marker = "RAY_CONFIG(uint32_t, recovery_succession_witness_count, 2)\n"
    addition = """

/// Patch 4M-CERT experimental mode.  When true, normal Recovery Succession
/// keeps the owner as the single admission authority but witness-confirms each
/// installed holder with an independently mergeable certificate.  Certificate
/// publications may overlap; the default false preserves Patch 4L/4K exactly.
RAY_CONFIG(bool, enable_recovery_succession_certificate_admission, false)
"""
    text = insert_after(text, marker, addition, "ray_config_def certificate flag")
    save(path, text)


NODE_HELPERS = r'''

bool SameRecoveryWorker(const rpc::Address &left, const rpc::Address &right) {
  return !left.worker_id().empty() && left.worker_id() == right.worker_id();
}

bool ValidRecoveryHolderCertificate(const rpc::RecoveryHolderCertificate &certificate) {
  return certificate.task_id().size() == TaskID::Size() &&
         certificate.generation() > 0 && certificate.slot() > 0 &&
         certificate.has_holder() &&
         certificate.holder().address().worker_id().size() == WorkerID::Size() &&
         certificate.holder().address().node_id().size() == NodeID::Size();
}

// Merge one owner-issued certificate into the witness's materialized manifest.
// The logical state is a grow-only set keyed by holder worker_id.  The repeated
// `succession` field remains a compatibility/materialization format: after each
// merge we deterministically sort non-owner holders by worker_id and derive
// contiguous ranks 1..N.  Rank is therefore no longer the admission dependency.
bool MergeRecoveryHolderCertificate(
    const rpc::RecoveryHolderCertificate &certificate,
    rpc::RecoveryManifest *manifest) {
  if (manifest == nullptr || !ValidRecoveryManifest(*manifest) ||
      !ValidRecoveryHolderCertificate(certificate) ||
      manifest->task_id() != certificate.task_id() || manifest->tombstoned()) {
    return false;
  }

  const rpc::RecoveryHolder *owner = nullptr;
  std::vector<rpc::RecoveryHolder> non_owner;
  non_owner.reserve(static_cast<size_t>(manifest->succession_size()) + 1);

  for (const rpc::RecoveryHolder &holder : manifest->succession()) {
    if (holder.rank() == 0) {
      owner = &holder;
      continue;
    }
    if (SameRecoveryWorker(holder.address(), certificate.holder().address())) {
      // Idempotent retry of an already-merged certificate.
      manifest->mutable_version()->set_generation(
          std::max(manifest->version().generation(), certificate.generation()));
      return true;
    }
    non_owner.push_back(holder);
  }

  if (owner == nullptr ||
      static_cast<uint32_t>(non_owner.size()) >= manifest->target_holder_count()) {
    return false;
  }

  // Preserve the current failure-domain invariant.  The owner remains the
  // single writer and already performs this check, but the witness validates it
  // as a defensive invariant before accepting a delta.
  const std::string &new_domain = certificate.holder().failure_domain_id();
  if (new_domain.empty()) {
    return false;
  }
  if (owner->failure_domain_id() == new_domain) {
    return false;
  }
  for (const rpc::RecoveryHolder &holder : non_owner) {
    if (holder.failure_domain_id() == new_domain) {
      return false;
    }
  }

  non_owner.push_back(certificate.holder());
  std::sort(non_owner.begin(), non_owner.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  rpc::RecoveryHolder owner_copy;
  owner_copy.CopyFrom(*owner);
  manifest->clear_succession();
  rpc::RecoveryHolder *out_owner = manifest->add_succession();
  out_owner->CopyFrom(owner_copy);
  out_owner->set_rank(0);

  for (size_t i = 0; i < non_owner.size(); ++i) {
    rpc::RecoveryHolder *out = manifest->add_succession();
    out->CopyFrom(non_owner[i]);
    out->set_rank(static_cast<uint32_t>(i + 1));
  }

  manifest->mutable_version()->set_generation(
      std::max(manifest->version().generation(), certificate.generation()));
  manifest->set_frozen(static_cast<uint32_t>(non_owner.size()) >=
                       manifest->target_holder_count());
  return true;
}
'''


NODE_HANDLER = r'''void NodeManager::HandleUpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest request,
    rpc::UpdateRecoveryWitnessReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!RayConfig::instance().enable_recovery_succession()) {
    reply->set_stored(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const bool baseline_enabled =
      RayConfig::instance().enable_recovery_witness_holder_baseline();
  const bool certificate_mode =
      RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !baseline_enabled;

  // Patch 4M-CERT delta path.  A certificate is accepted only when this
  // witness already has the task's base manifest.  This preserves lazy
  // activation and keeps tombstones as absorbing state.
  if (request.has_holder_certificate()) {
    if (!certificate_mode || request.has_task_spec() ||
        !ValidRecoveryHolderCertificate(request.holder_certificate()) ||
        (request.has_manifest() &&
         (!ValidRecoveryManifest(request.manifest()) ||
          request.manifest().task_id() != request.holder_certificate().task_id() ||
          request.manifest().tombstoned()))) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }

    const rpc::RecoveryHolderCertificate &certificate =
        request.holder_certificate();
    const TaskID task_id = TaskID::FromBinary(certificate.task_id());

    {
      absl::MutexLock lock(&recovery_witness_mutex_);
      auto it = recovery_witness_manifests_.find(task_id);
      if (it == recovery_witness_manifests_.end() && request.has_manifest()) {
        recovery_witness_manifests_[task_id].CopyFrom(request.manifest());
        it = recovery_witness_manifests_.find(task_id);
      }
      if (it == recovery_witness_manifests_.end()) {
        reply->set_stored(false);
      } else if (it->second.tombstoned()) {
        reply->set_stored(false);
        reply->mutable_latest_manifest()->CopyFrom(it->second);
      } else {
        rpc::RecoveryManifest merged;
        merged.CopyFrom(it->second);
        if (MergeRecoveryHolderCertificate(certificate, &merged)) {
          it->second.CopyFrom(merged);
          reply->set_stored(true);
          // Useful for diagnostics and for callers that want the witness's
          // materialized set.  Success does not require consuming this field.
          reply->mutable_latest_manifest()->CopyFrom(it->second);
        } else {
          reply->set_stored(false);
          reply->mutable_latest_manifest()->CopyFrom(it->second);
        }
      }
    }

    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  // Existing full-manifest path: initial activation, tombstones, and the
  // witness-as-holder baseline retain Patch 4L/4K behavior.
  if (!request.has_manifest() || !ValidRecoveryManifest(request.manifest())) {
    reply->set_stored(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const rpc::RecoveryManifest &incoming = request.manifest();
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
    absl::MutexLock lock(&recovery_witness_mutex_);

    auto existing_it = recovery_witness_manifests_.find(task_id);
    if (existing_it == recovery_witness_manifests_.end()) {
      recovery_witness_manifests_[task_id].CopyFrom(incoming);
      reply->set_stored(true);
    } else {
      rpc::RecoveryManifest &existing = existing_it->second;
      const int comparison = CompareRecoveryManifestVersions(incoming, existing);

      // Patch 4M-CERT: a terminal tombstone wins an equal-generation race
      // against an in-flight holder certificate.  Older tombstones still lose.
      if (certificate_mode && incoming.tombstoned() && comparison >= 0) {
        existing.CopyFrom(incoming);
        reply->set_stored(true);
      } else if (comparison > 0) {
        existing.CopyFrom(incoming);
        reply->set_stored(true);
      } else if (comparison == 0 &&
                 incoming.SerializeAsString() == existing.SerializeAsString()) {
        reply->set_stored(true);
      } else {
        reply->set_stored(false);
        reply->mutable_latest_manifest()->CopyFrom(existing);
      }
    }

    if (reply->stored()) {
      rpc::RecoveryManifest &stored = recovery_witness_manifests_[task_id];
      if (stored.tombstoned()) {
        recovery_witness_task_specs_.erase(task_id);
        recovery_witness_claims_.erase(task_id);
      } else if (baseline_enabled && request.has_task_spec()) {
        recovery_witness_task_specs_[task_id].CopyFrom(request.task_spec());
      } else {
        auto task_spec_it = recovery_witness_task_specs_.find(task_id);
        if (task_spec_it != recovery_witness_task_specs_.end()) {
          task_spec_it->second.mutable_recovery_manifest()->CopyFrom(stored);
        }
      }
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}'''


def patch_node_manager_cc(root: Path) -> None:
    path = root / "src/ray/raylet/node_manager.cc"
    text = load(path)
    if "Patch 4M-CERT delta path" not in text:
        valid_fn = """bool ValidRecoveryManifest(const rpc::RecoveryManifest &manifest) {
  return manifest.task_id().size() == TaskID::Size() && manifest.has_version() &&
         manifest.version().generation() > 0;
}
"""
        text = insert_after(text, valid_fn, NODE_HELPERS, "node_manager.cc helpers")
        text = replace_function(
            text,
            "void NodeManager::HandleUpdateRecoveryWitness(",
            NODE_HANDLER,
        )
    else:
        print(f"already patched {path}")
        return
    save(path, text)


MANAGER_HELPERS = r'''

bool MergeRecoveryHolderSets(const rpc::RecoveryManifest &incoming,
                             rpc::RecoveryManifest *state) {
  if (state == nullptr || incoming.task_id().empty() || !incoming.has_version()) {
    return false;
  }
  if (state->task_id().empty()) {
    state->CopyFrom(incoming);
    return true;
  }
  if (state->task_id() != incoming.task_id() || !state->has_version()) {
    return false;
  }

  // Tombstones are terminal.  An equal-generation tombstone may race with an
  // independently published certificate; terminal state wins that tie.
  if (incoming.tombstoned()) {
    if (incoming.version().generation() >= state->version().generation()) {
      state->CopyFrom(incoming);
      return true;
    }
    return false;
  }
  if (state->tombstoned()) {
    return true;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(*state, 0);
  if (owner == nullptr) {
    owner = FindHolderByRank(incoming, 0);
  }
  if (owner == nullptr) {
    return false;
  }

  std::vector<rpc::RecoveryHolder> holders;
  auto add_unique = [&holders, owner](const rpc::RecoveryManifest &manifest) {
    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0 || SameWorker(holder.address(), owner->address())) {
        continue;
      }
      bool duplicate = false;
      for (const rpc::RecoveryHolder &existing : holders) {
        if (SameWorker(existing.address(), holder.address())) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        holders.push_back(holder);
      }
    }
  };

  add_unique(*state);
  add_unique(incoming);
  if (holders.size() > static_cast<size_t>(state->target_holder_count())) {
    return false;
  }

  std::sort(holders.begin(), holders.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  rpc::RecoveryHolder owner_copy;
  owner_copy.CopyFrom(*owner);
  state->clear_succession();
  rpc::RecoveryHolder *out_owner = state->add_succession();
  out_owner->CopyFrom(owner_copy);
  out_owner->set_rank(0);
  for (size_t i = 0; i < holders.size(); ++i) {
    rpc::RecoveryHolder *out = state->add_succession();
    out->CopyFrom(holders[i]);
    out->set_rank(static_cast<uint32_t>(i + 1));
  }

  state->mutable_version()->set_generation(
      std::max(state->version().generation(), incoming.version().generation()));
  state->set_recovery_attempt(
      std::max(state->recovery_attempt(), incoming.recovery_attempt()));
  state->set_frozen(static_cast<uint32_t>(holders.size()) >=
                    state->target_holder_count());
  return true;
}

bool MergeConfirmedHolder(const rpc::RecoveryHolder &candidate,
                          uint64_t certificate_generation,
                          rpc::RecoveryManifest *manifest) {
  if (manifest == nullptr || manifest->task_id().empty() || manifest->tombstoned() ||
      candidate.address().worker_id().empty()) {
    return false;
  }

  rpc::RecoveryManifest delta;
  delta.CopyFrom(*manifest);
  delta.clear_succession();

  const rpc::RecoveryHolder *owner = FindHolderByRank(*manifest, 0);
  if (owner == nullptr) {
    return false;
  }
  delta.add_succession()->CopyFrom(*owner);
  rpc::RecoveryHolder *holder = delta.add_succession();
  holder->CopyFrom(candidate);
  holder->set_rank(1);
  delta.mutable_version()->set_generation(certificate_generation);
  return MergeRecoveryHolderSets(delta, manifest);
}
'''


def patch_manager_h(root: Path) -> None:
    path = root / "src/ray/core_worker/recovery_succession_manager.h"
    text = load(path)
    if "Patch 4M-CERT: in certificate mode" in text:
        print(f"already patched {path}")
        return
    old = "uint32_t proposed_rank = 0;  // Patch 4D: speculative contiguous rank."
    new = """// Patch 4M-CERT: in certificate mode this is an owner-issued admission
    // slot/token.  The committed recovery rank is derived later from the merged set.
    uint32_t proposed_rank = 0;"""
    text = replace_once(text, old, new, "manager.h proposed_rank comment")
    old2 = """  /// Patch 4D: removes a failed provisional reservation and every
  /// speculative reservation at a higher rank for the same task.
  void AbortHolderAdmission(const std::string &reservation_id);"""
    new2 = """  /// Removes a failed provisional reservation.  Patch 4D removes the
  /// speculative suffix; Patch 4M-CERT removes only the failed independent
  /// certificate reservation.
  void AbortHolderAdmission(const std::string &reservation_id);"""
    text = replace_once(text, old2, new2, "manager.h abort comment")
    save(path, text)


def patch_manager_cc(root: Path) -> None:
    path = root / "src/ray/core_worker/recovery_succession_manager.cc"
    text = load(path)
    if "#include <algorithm>" not in text:
        text = replace_once(text, "#include <cstddef>\n", "#include <algorithm>\n#include <cstddef>\n", "manager.cc algorithm include")
    if "bool MergeRecoveryHolderSets(" not in text:
        marker = """int CompareManifestVersions(const rpc::RecoveryManifest &left,
                            const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }

  if (left.version().generation() > right.version().generation()) {
    return 1;
  }

  return 0;
}
"""
        text = insert_after(text, marker, MANAGER_HELPERS, "manager.cc merge helpers")

    # Independent owner-side commit branch; old ordered commit remains fallback.
    def add_cert_commit_branch(fn: str) -> str:
        if "Patch 4M-CERT independent commit" in fn:
            return fn
        marker = """  const rpc::RecoveryManifest &current = task_it->second.manifest;
  const rpc::RecoveryManifest &proposed = reservation.proposed_manifest;
"""
        branch = marker + r'''

  // Patch 4M-CERT independent commit.  Witness ACK authorizes exactly this
  // reservation's candidate; it does not require lower admission slots to have
  // committed first.  Materialized ranks are derived deterministically.
  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    const rpc::RecoveryHolder *candidate =
        FindHolderByRank(proposed, reservation.proposed_rank);
    if (candidate == nullptr ||
        !SameWorker(candidate->address(), reservation.candidate_address)) {
      return false;
    }

    rpc::RecoveryManifest merged;
    merged.CopyFrom(current);
    if (!MergeConfirmedHolder(*candidate,
                              proposed.version().generation(),
                              &merged)) {
      return false;
    }

    UpdateManifestForTaskLocked(task_id, merged, true);

    if (profiling_enabled_) {
      ++profile_.holder_admissions_committed;
      ++profile_.manifest_generations_committed;
      profile_.max_generation =
          std::max(profile_.max_generation, merged.version().generation());
      const uint64_t non_owner_holders =
          merged.succession_size() > 0
              ? static_cast<uint64_t>(merged.succession_size() - 1)
              : 0;
      profile_.max_non_owner_holders =
          std::max(profile_.max_non_owner_holders, non_owner_holders);
      if (merged.frozen()) {
        ++profile_.frozen_commits;
      }
    }

    committed_manifest->CopyFrom(merged);
    EraseHolderReservationLocked(reservation_id);
    return true;
  }
'''
        if fn.count(marker) != 1:
            die("CommitHolderAdmission marker mismatch")
        return fn.replace(marker, branch, 1)

    text = edit_function(
        text,
        "bool RecoverySuccessionManager::CommitHolderAdmission(",
        add_cert_commit_branch,
    )

    # Independent abort branch; retain old suffix rollback as fallback.
    def add_cert_abort_branch(fn: str) -> str:
        if "Patch 4M-CERT independent abort" in fn:
            return fn
        marker = """  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t failed_rank = reservation_it->second.proposed_rank;
"""
        branch = marker + r'''

  // Patch 4M-CERT independent abort: another certificate does not depend on
  // this reservation's prefix, so do not invalidate higher slots.
  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    EraseHolderReservationLocked(reservation_id);
    return;
  }
'''
        if fn.count(marker) != 1:
            die("AbortHolderAdmission marker mismatch")
        return fn.replace(marker, branch, 1)

    text = edit_function(
        text,
        "void RecoverySuccessionManager::AbortHolderAdmission(",
        add_cert_abort_branch,
    )

    # Apply committed/cached manifests by set union in certificate mode.
    old_apply_sig = "bool RecoverySuccessionManager::ApplyCommittedManifest("
    old_start, old_end = find_function_span(text, old_apply_sig)
    old_apply = text[old_start:old_end]
    if "Patch 4M-CERT set merge" not in old_apply:
        cert_prefix = r'''bool RecoverySuccessionManager::ApplyCommittedManifest(
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(manifest.task_id());
  absl::MutexLock lock(&mutex_);

  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    // Patch 4M-CERT set merge: equal-generation different subsets are valid
    // partial views and must converge by union, not fail byte-equality checks.
    auto it = task_states_.find(task_id);
    rpc::RecoveryManifest merged;
    if (it == task_states_.end() || it->second.manifest.task_id().empty()) {
      merged.CopyFrom(manifest);
    } else {
      merged.CopyFrom(it->second.manifest);
      if (!MergeRecoveryHolderSets(manifest, &merged)) {
        return false;
      }
    }
    UpdateManifestForTaskLocked(task_id, merged, true);
    if (ContainsWorker(merged, self_address_)) {
      candidate_reports_sent_.insert(task_id);
    }
    return true;
  }

'''
        # Reuse the original fallback body after its lock line.
        fallback_marker = "  const auto task_it = task_states_.find(task_id);"
        pos = old_apply.find(fallback_marker)
        if pos < 0:
            die("ApplyCommittedManifest fallback marker missing")
        # Original has already acquired the lock immediately before marker.
        fallback = old_apply[pos:]
        # Drop original final function brace and splice under our already-held lock.
        if not fallback.rstrip().endswith("}"):
            die("ApplyCommittedManifest malformed")
        fallback = fallback.rstrip()[:-1]
        replacement = cert_prefix + fallback + "}\n"
        text = text[:old_start] + replacement + text[old_end:]

    # UpdateBorrowedObjectManifest: add a certificate-mode union fast path.
    def add_update_borrowed_branch(fn: str) -> str:
        if "Patch 4M-CERT borrowed-view merge" in fn:
            return fn
        marker = """  if (borrowed_it == borrowed_objects_.end() ||
      borrowed_it->second.task_id.Binary() != manifest.task_id()) {
    return;
  }
"""
        branch = marker + r'''

  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    // Patch 4M-CERT borrowed-view merge.
    const TaskID task_id = borrowed_it->second.task_id;
    auto task_it = task_states_.find(task_id);
    rpc::RecoveryManifest merged;
    if (task_it == task_states_.end() || task_it->second.manifest.task_id().empty()) {
      merged.CopyFrom(manifest);
    } else {
      merged.CopyFrom(task_it->second.manifest);
      if (!MergeRecoveryHolderSets(manifest, &merged)) {
        return;
      }
    }
    UpdateManifestForTaskLocked(task_id, merged, true);
    return;
  }
'''
        if fn.count(marker) != 1:
            die("UpdateBorrowedObjectManifest marker mismatch")
        return fn.replace(marker, branch, 1)

    text = edit_function(
        text,
        "void RecoverySuccessionManager::UpdateBorrowedObjectManifest(",
        add_update_borrowed_branch,
    )

    # Provisional holder can promote from any witness set that contains itself.
    def add_confirm_branch(fn: str) -> str:
        if "Patch 4M-CERT witness set promotion" in fn:
            return fn
        marker = """  if (state.manifest.task_id() != witness_manifest.task_id() ||
      state.manifest.tombstoned() ||
      !ContainsWorker(witness_manifest, self_address_)) {
    return false;
  }
"""
        branch = marker + r'''

  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    // Patch 4M-CERT witness set promotion.  Presence in a directly queried
    // witness's merged set is the durability proof; rank/prefix is irrelevant.
    const bool installed_provisional =
        !state.provisional_reservation_id.empty() &&
        ContainsWorker(state.manifest, self_address_);
    const bool piggyback_provisional = state.provisional_piggyback_task_spec;
    if (!state.manifest_committed &&
        !installed_provisional && !piggyback_provisional) {
      return false;
    }

    rpc::RecoveryManifest merged;
    merged.CopyFrom(state.manifest);
    if (!MergeRecoveryHolderSets(witness_manifest, &merged) ||
        !ContainsWorker(merged, self_address_)) {
      return false;
    }
    UpdateManifestForTaskLocked(task_id, merged, true);
    candidate_reports_sent_.insert(task_id);
    confirmed_manifest->CopyFrom(task_states_[task_id].manifest);
    return true;
  }
'''
        if fn.count(marker) != 1:
            die("ConfirmProvisionalHolderFromWitness marker mismatch")
        return fn.replace(marker, branch, 1)

    text = edit_function(
        text,
        "bool RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(",
        add_confirm_branch,
    )

    # Replay freshness: union partial certificate views instead of requiring
    # equal-generation byte identity.
    cert_compare = r'''  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    rpc::RecoveryManifest merged;
    merged.CopyFrom(state.manifest);
    if (!MergeRecoveryHolderSets(request.requester_manifest(), &merged)) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }
    if (merged.tombstoned()) {
      latest_manifest->CopyFrom(merged);
      return ReplayPreparationResult::TOMBSTONED;
    }
    state.manifest.CopyFrom(merged);
    state.manifest_committed = true;
    latest_manifest->CopyFrom(state.manifest);
  } else {
    const int requester_comparison =
      CompareManifestVersions(
          request.requester_manifest(),
          state.manifest);

    if (requester_comparison < 0) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }

    if (requester_comparison == 0 &&
        request.requester_manifest().SerializeAsString() !=
            state.manifest.SerializeAsString()) {
      return ReplayPreparationResult::MANIFEST_STALE;
    }

    if (requester_comparison > 0) {
      if (request.requester_manifest().tombstoned()) {
        latest_manifest->CopyFrom(
            request.requester_manifest());
        return ReplayPreparationResult::TOMBSTONED;
      }

      if (!ContainsWorker(
              request.requester_manifest(),
              self_address_)) {
        return ReplayPreparationResult::WRONG_HOLDER;
      }

      state.manifest.CopyFrom(
          request.requester_manifest());
      state.manifest_committed = true;
      latest_manifest->CopyFrom(
          state.manifest);
    }
  }

'''
    if "enable_recovery_succession_certificate_admission()" not in text[text.find("RecoverySuccessionManager::PrepareTaskReplay("):text.find("RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(")]:
        text = replace_between_in_function(
            text,
            "RecoverySuccessionManager::PrepareTaskReplay(",
            "    const int requester_comparison =",
            "  const int32_t max_recovery_attempts = state.manifest.max_recovery_attempts();",
            cert_compare,
        )

    save(path, text)


CORE_MERGE_HELPER = r'''

bool MergeRecoveryWitnessViews(const rpc::RecoveryManifest &incoming,
                               rpc::RecoveryManifest *state) {
  if (state == nullptr || incoming.task_id().empty() || !incoming.has_version()) {
    return false;
  }
  if (state->task_id().empty()) {
    state->CopyFrom(incoming);
    return true;
  }
  if (state->task_id() != incoming.task_id() || !state->has_version()) {
    return false;
  }

  if (incoming.tombstoned()) {
    if (incoming.version().generation() >= state->version().generation()) {
      state->CopyFrom(incoming);
    }
    return true;
  }
  if (state->tombstoned()) {
    return true;
  }

  rpc::RecoveryHolder owner;
  bool have_owner = false;
  std::vector<rpc::RecoveryHolder> holders;

  auto absorb = [&](const rpc::RecoveryManifest &manifest) {
    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0) {
        if (!have_owner) {
          owner.CopyFrom(holder);
          have_owner = true;
        }
        continue;
      }
      bool duplicate = false;
      for (const rpc::RecoveryHolder &existing : holders) {
        if (!existing.address().worker_id().empty() &&
            existing.address().worker_id() == holder.address().worker_id()) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        holders.push_back(holder);
      }
    }
  };

  absorb(*state);
  absorb(incoming);
  if (!have_owner ||
      holders.size() > static_cast<size_t>(state->target_holder_count())) {
    return false;
  }

  std::sort(holders.begin(), holders.end(),
            [](const rpc::RecoveryHolder &a, const rpc::RecoveryHolder &b) {
              return a.address().worker_id() < b.address().worker_id();
            });

  state->clear_succession();
  rpc::RecoveryHolder *out_owner = state->add_succession();
  out_owner->CopyFrom(owner);
  out_owner->set_rank(0);
  for (size_t i = 0; i < holders.size(); ++i) {
    rpc::RecoveryHolder *out = state->add_succession();
    out->CopyFrom(holders[i]);
    out->set_rank(static_cast<uint32_t>(i + 1));
  }

  state->mutable_version()->set_generation(
      std::max(state->version().generation(), incoming.version().generation()));
  state->set_recovery_attempt(
      std::max(state->recovery_attempt(), incoming.recovery_attempt()));
  state->set_frozen(static_cast<uint32_t>(holders.size()) >=
                    state->target_holder_count());
  return true;
}
'''

CERT_PUBLISHER = r'''void CoreWorker::PublishRecoveryHolderCertificateToWitnesses(
    const rpc::RecoveryManifest &manifest,
    const rpc::RecoveryHolderCertificate &certificate,
    RecoveryWitnessPublishCallback callback) const {
  if (!recovery_succession_enabled_ || manifest.task_id().empty() ||
      manifest.witness_raylets_size() == 0 || certificate.task_id() != manifest.task_id()) {
    callback(false, std::nullopt);
    return;
  }

  struct PublishState {
    absl::Mutex mutex;
    size_t completed ABSL_GUARDED_BY(mutex) = 0;
    bool callback_sent ABSL_GUARDED_BY(mutex) = false;
    std::optional<rpc::RecoveryManifest> newest ABSL_GUARDED_BY(mutex);
  };

  auto state = std::make_shared<PublishState>();
  const size_t witness_count = static_cast<size_t>(manifest.witness_raylets_size());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    rpc::UpdateRecoveryWitnessRequest request;
    // Carry the last committed/base view only as a bootstrap in case this
    // witness missed lazy activation.  An existing witness never replaces its
    // merged set with this base; it only unions the certificate.
    request.mutable_manifest()->CopyFrom(manifest);
    request.mutable_holder_certificate()->CopyFrom(certificate);

    const uint64_t witness_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (witness_start_ns != 0) {
      recovery_succession_manager_->RecordWitnessUpdateRpcSent(
          0, static_cast<uint64_t>(certificate.ByteSizeLong()));
    }

    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);
    witness_client->UpdateRecoveryWitness(
        std::move(request),
        [state,
         witness_count,
         callback,
         manager = recovery_succession_manager_,
         witness_start_ns](const Status &status,
                           rpc::UpdateRecoveryWitnessReply &&reply) mutable {
          if (witness_start_ns != 0) {
            manager->RecordWitnessUpdateRpcLatency(
                RecoveryProfileNowNs() - witness_start_ns);
          }

          bool success = false;
          bool failure = false;
          std::optional<rpc::RecoveryManifest> newest;
          {
            absl::MutexLock lock(&state->mutex);
            ++state->completed;

            if (reply.has_latest_manifest()) {
              if (!state->newest.has_value() ||
                  CompareRecoveryManifestVersions(reply.latest_manifest(),
                                                  state->newest.value()) > 0) {
                state->newest = reply.latest_manifest();
              }
            }

            if (!state->callback_sent) {
              if (status.ok() && reply.stored()) {
                // Preserve current Succession durability semantics: one compact
                // witness acknowledgement is sufficient.
                state->callback_sent = true;
                success = true;
              } else if (state->completed == witness_count) {
                state->callback_sent = true;
                newest = state->newest;
                failure = true;
              }
            }
          }

          if (success) {
            callback(true, std::nullopt);
          } else if (failure) {
            callback(false, std::move(newest));
          }
        });
  }
}'''

CERT_FINISH = r'''void CoreWorker::FinishRecoveryHolderAdmissionCertificate(
    std::shared_ptr<PendingRecoveryHolderAdmission> state) {
  if (state == nullptr) {
    return;
  }

  const rpc::RecoveryHolder *holder = nullptr;
  for (const rpc::RecoveryHolder &candidate : state->proposed_manifest.succession()) {
    if (candidate.rank() == state->rank &&
        candidate.address().worker_id() == state->candidate_address.worker_id()) {
      holder = &candidate;
      break;
    }
  }
  if (holder == nullptr) {
    AbortRecoveryHolderAdmissionSuffix(
        state, rpc::ReportRecoveryCandidateReply::STALE_MANIFEST, state->latest_manifest);
    return;
  }

  rpc::RecoveryHolderCertificate certificate;
  certificate.set_task_id(state->proposed_manifest.task_id());
  certificate.set_generation(state->proposed_manifest.version().generation());
  certificate.set_slot(state->rank);
  certificate.mutable_holder()->CopyFrom(*holder);

  auto manager = recovery_succession_manager_;
  const uint64_t publish_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  PublishRecoveryHolderCertificateToWitnesses(
      state->latest_manifest,
      certificate,
      [this, manager, state, publish_start_ns](
          bool witness_stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - publish_start_ns);
        }

        if (!witness_stored) {
          rpc::RecoveryManifest rollback;
          if (newer_manifest.has_value()) {
            rollback.CopyFrom(newer_manifest.value());
            manager->ApplyCommittedManifest(newer_manifest.value());
          } else {
            rollback.CopyFrom(state->latest_manifest);
          }
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              rollback);
          return;
        }

        if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack()) {
          RAY_LOG(WARNING).WithField(state->task_id)
              << "TEST ONLY: Patch 4M-CERT failure after certificate witness ACK";
          state->send_reply_callback(
              Status::IOError(
                  "Injected Patch 4M-CERT failure after witness ACK before owner commit"),
              nullptr,
              nullptr);
          return;
        }

        rpc::RecoveryManifest committed_manifest;
        if (!manager->CommitHolderAdmission(state->reservation_id,
                                            &committed_manifest)) {
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              state->latest_manifest);
          return;
        }

        if (state->admission_start_ns != 0) {
          manager->RecordHolderAdmissionLatency(
              RecoveryProfileNowNs() - state->admission_start_ns);
        }

        state->reply->set_result(rpc::ReportRecoveryCandidateReply::ACCEPTED);
        state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
        state->send_reply_callback(Status::OK(), nullptr, nullptr);

        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          auto task_it = recovery_holder_admission_states_.find(state->task_id);
          if (task_it != recovery_holder_admission_states_.end()) {
            auto rank_it = task_it->second.pending_by_rank.find(state->rank);
            if (rank_it != task_it->second.pending_by_rank.end() &&
                rank_it->second->reservation_id == state->reservation_id) {
              task_it->second.pending_by_rank.erase(rank_it);
            }
            if (task_it->second.pending_by_rank.empty()) {
              recovery_holder_admission_states_.erase(task_it);
            }
          }
        }

        RAY_LOG(INFO).WithField(state->task_id)
            << "Patch 4M-CERT witness-confirmed independent holder slot "
            << state->rank;
        RAY_LOG(INFO).WithField(state->task_id)
            << "Committed recovery succession manifest after witness publication with "
            << committed_manifest.succession_size() << " total members";
        TryAdvanceRecoveryHolderAdmissions(state->task_id);
      });
}'''

TRY_ADVANCE = r'''void CoreWorker::TryAdvanceRecoveryHolderAdmissions(const TaskID &task_id) {
  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_) {
    std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> ready;
    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      const auto task_it = recovery_holder_admission_states_.find(task_id);
      if (task_it == recovery_holder_admission_states_.end()) {
        return;
      }
      for (auto &[slot, state] : task_it->second.pending_by_rank) {
        static_cast<void>(slot);
        if (state->installed && !state->aborted && !state->witness_publish_started) {
          state->witness_publish_started = true;
          ready.push_back(state);
        }
      }
    }

    // No rank gate: every installed independent certificate can enter witness
    // confirmation concurrently.
    for (auto &state : ready) {
      FinishRecoveryHolderAdmissionCertificate(std::move(state));
    }
    return;
  }

  // Patch 4D/4K fallback: installs may overlap but witness publication and
  // durable commit remain strictly rank ordered.
  std::shared_ptr<PendingRecoveryHolderAdmission> next;
  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    const auto task_it = recovery_holder_admission_states_.find(task_id);
    if (task_it == recovery_holder_admission_states_.end()) {
      return;
    }

    auto &task_state = task_it->second;
    if (task_state.witness_publish_rank != 0 || task_state.pending_by_rank.empty()) {
      return;
    }

    const auto first = task_state.pending_by_rank.begin();
    if (!first->second->installed || first->second->aborted) {
      return;
    }

    task_state.witness_publish_rank = first->first;
    next = first->second;
  }

  FinishRecoveryHolderAdmission(std::move(next));
}'''

LOOKUP = r'''void CoreWorker::LookupRecoveryManifestFromWitnesses(
    const rpc::RecoveryManifest &cached_manifest,
    RecoveryWitnessLookupCallback callback) {
  if (!recovery_succession_enabled_ || cached_manifest.task_id().empty() ||
      cached_manifest.witness_raylets_size() == 0) {
    callback(std::nullopt);
    return;
  }

  struct LookupState {
    absl::Mutex mutex;
    size_t completed ABSL_GUARDED_BY(mutex) = 0;
    std::optional<rpc::RecoveryManifest> merged ABSL_GUARDED_BY(mutex);
  };

  auto state = std::make_shared<LookupState>();
  const size_t witness_count =
      static_cast<size_t>(cached_manifest.witness_raylets_size());
  const bool certificate_mode =
      RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_;

  for (const rpc::Address &witness : cached_manifest.witness_raylets()) {
    rpc::GetRecoveryWitnessRequest request;
    request.set_task_id(cached_manifest.task_id());
    auto witness_client = raylet_client_pool_->GetOrConnectByAddress(witness);

    witness_client->GetRecoveryWitness(
        std::move(request),
        [state, witness_count, certificate_mode, callback](
            const Status &status,
            rpc::GetRecoveryWitnessReply &&reply) mutable {
          bool finished = false;
          std::optional<rpc::RecoveryManifest> result;
          {
            absl::MutexLock lock(&state->mutex);
            ++state->completed;

            if (status.ok() && reply.found() && reply.has_manifest()) {
              if (!state->merged.has_value()) {
                state->merged = reply.manifest();
              } else if (certificate_mode) {
                rpc::RecoveryManifest merged;
                merged.CopyFrom(state->merged.value());
                if (MergeRecoveryWitnessViews(reply.manifest(), &merged)) {
                  state->merged = std::move(merged);
                }
              } else if (CompareRecoveryManifestVersions(
                             reply.manifest(), state->merged.value()) > 0) {
                state->merged = reply.manifest();
              }
            }

            if (state->completed == witness_count) {
              result = state->merged;
              finished = true;
            }
          }

          if (finished) {
            callback(std::move(result));
          }
        });
  }
}'''


def patch_core_worker_h(root: Path) -> None:
    path = root / "src/ray/core_worker/core_worker.h"
    text = load(path)
    if "witness_publish_started" not in text:
        old = """    bool installed = false;
    bool aborted = false;
    rpc::RecoveryManifest abort_manifest;"""
        new = """    bool installed = false;
    bool aborted = false;
    // Patch 4M-CERT: prevents duplicate concurrent publication of this
    // independent certificate while retaining the old per-task rank gate.
    bool witness_publish_started = false;
    rpc::RecoveryManifest abort_manifest;"""
        text = replace_once(text, old, new, "core_worker.h state field")

    if "PublishRecoveryHolderCertificateToWitnesses" not in text:
        marker = """  void PublishRecoveryManifestToWitnesses(
    const rpc::RecoveryManifest &manifest,
    RecoveryWitnessPublishCallback callback,
    const rpc::TaskSpec *task_spec = nullptr) const;
"""
        addition = marker + """

  // Patch 4M-CERT delta publication.  Same witness durability rule as normal
  // Succession, but only one owner-issued holder certificate is transmitted.
  void PublishRecoveryHolderCertificateToWitnesses(
      const rpc::RecoveryManifest &manifest,
      const rpc::RecoveryHolderCertificate &certificate,
      RecoveryWitnessPublishCallback callback) const;
"""
        text = replace_once(text, marker, addition, "core_worker.h certificate publisher")

    if "FinishRecoveryHolderAdmissionCertificate" not in text:
        marker = """  void FinishRecoveryHolderAdmission(
      std::shared_ptr<PendingRecoveryHolderAdmission> state);
"""
        addition = marker + """

  void FinishRecoveryHolderAdmissionCertificate(
      std::shared_ptr<PendingRecoveryHolderAdmission> state);
"""
        text = replace_once(text, marker, addition, "core_worker.h certificate finisher")

    save(path, text)


def patch_core_worker_cc(root: Path) -> None:
    path = root / "src/ray/core_worker/core_worker.cc"
    text = load(path)

    if "bool MergeRecoveryWitnessViews(" not in text:
        marker = """int CompareRecoveryManifestVersions(const rpc::RecoveryManifest &left,
                                    const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }

  if (left.version().generation() > right.version().generation()) {
    return 1;
  }

  return 0;
}
"""
        text = insert_after(text, marker, CORE_MERGE_HELPER, "core_worker.cc merge helper")

    if "void CoreWorker::PublishRecoveryHolderCertificateToWitnesses(" not in text:
        # Place directly after the existing full-manifest publisher.
        _, end = find_function_span(text, "void CoreWorker::PublishRecoveryManifestToWitnesses(")
        text = text[:end] + "\n\n" + CERT_PUBLISHER + "\n" + text[end:]

    if "void CoreWorker::FinishRecoveryHolderAdmissionCertificate(" not in text:
        start, _ = find_function_span(text, "void CoreWorker::FinishRecoveryHolderAdmission(")
        text = text[:start] + CERT_FINISH + "\n\n" + text[start:]

    text = replace_function(
        text,
        "void CoreWorker::TryAdvanceRecoveryHolderAdmissions(",
        TRY_ADVANCE,
    )

    # In certificate mode a failed independent admission must not abort the
    # speculative suffix.  Insert an early single-state cleanup branch.
    def add_single_abort(fn: str) -> str:
        if "Patch 4M-CERT independent failure cleanup" in fn:
            return fn
        marker = """  if (failed_state == nullptr) {
    return;
  }
"""
        branch = marker + r'''

  if (RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !recovery_witness_holder_baseline_enabled_) {
    // Patch 4M-CERT independent failure cleanup: only this certificate fails.
    recovery_succession_manager_->AbortHolderAdmission(
        failed_state->reservation_id);

    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      auto task_it = recovery_holder_admission_states_.find(failed_state->task_id);
      if (task_it != recovery_holder_admission_states_.end()) {
        auto rank_it = task_it->second.pending_by_rank.find(failed_state->rank);
        if (rank_it != task_it->second.pending_by_rank.end() &&
            rank_it->second->reservation_id == failed_state->reservation_id) {
          rank_it->second->aborted = true;
          rank_it->second->abort_manifest.CopyFrom(committed_manifest);
          task_it->second.pending_by_rank.erase(rank_it);
        }
        if (task_it->second.pending_by_rank.empty()) {
          recovery_holder_admission_states_.erase(task_it);
        }
      }
    }

    if (failed_state->reply != nullptr) {
      failed_state->reply->set_result(failed_result);
      if (!committed_manifest.task_id().empty()) {
        failed_state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
      }
    }
    SendRecoveryHolderRollback(failed_state, committed_manifest);
    failed_state->send_reply_callback(Status::OK(), nullptr, nullptr);
    TryAdvanceRecoveryHolderAdmissions(failed_state->task_id);
    return;
  }
'''
        if fn.count(marker) != 1:
            die("AbortRecoveryHolderAdmissionSuffix marker mismatch")
        return fn.replace(marker, branch, 1)

    text = edit_function(
        text,
        "void CoreWorker::AbortRecoveryHolderAdmissionSuffix(",
        add_single_abort,
    )

    text = replace_function(
        text,
        "void CoreWorker::LookupRecoveryManifestFromWitnesses(",
        LOOKUP,
    )

    save(path, text)


def patch_benchmark_common(root: Path) -> None:
    path = root / "gossip_benchmarks/_benchmark_common.py"
    if not path.exists():
        print("gossip_benchmarks/_benchmark_common.py not found; skipping benchmark toggle")
        return
    text = load(path)
    if "RAY_RECOVERY_CERTIFICATE_ADMISSION" in text:
        print(f"already patched {path}")
        return

    marker = '''    config: dict[str, Any] = {
        "enable_recovery_succession": method.recovery_enabled,
        "enable_recovery_witness_holder_baseline": method.baseline_enabled,
        "recovery_succession_witness_count": max(1, int(witness_count)),
        "enable_recovery_succession_profiling": bool(profiling_enabled),
        "recovery_succession_benchmark_ablation_mode": str(ablation_mode),
    }
'''
    replacement = '''    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )
    config: dict[str, Any] = {
        "enable_recovery_succession": method.recovery_enabled,
        "enable_recovery_witness_holder_baseline": method.baseline_enabled,
        "enable_recovery_succession_certificate_admission": certificate_admission,
        "recovery_succession_witness_count": max(1, int(witness_count)),
        "enable_recovery_succession_profiling": bool(profiling_enabled),
        "recovery_succession_benchmark_ablation_mode": str(ablation_mode),
    }
'''
    text = replace_once(text, marker, replacement, "benchmark common certificate toggle")
    save(path, text)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".", help="Ray repository root")
    args = ap.parse_args()
    root = Path(args.repo).resolve()

    expected = root / "src/ray/core_worker/recovery_succession_manager.cc"
    if not expected.exists():
        die(f"{root} does not look like the Ray repo (missing {expected.relative_to(root)})")

    patch_common_proto(root)
    patch_node_manager_proto(root)
    patch_config(root)
    patch_node_manager_cc(root)
    patch_manager_h(root)
    patch_manager_cc(root)
    patch_core_worker_h(root)
    patch_core_worker_cc(root)
    patch_benchmark_common(root)

    print("\nPatch 4M-CERT applied.")
    print("New config flag (default false):")
    print("  enable_recovery_succession_certificate_admission")
    print("Benchmark helper toggle:")
    print("  export RAY_RECOVERY_CERTIFICATE_ADMISSION=1")
    print("\nRecommended first experiment:")
    print("  run existing small-TaskSpec B=1..4 benchmark twice with the flag false/true")
    print("  and compare throughput, p95, protection-ready latency, witness RPCs/task.")
    print("\nBackups: each modified file has a .pre4m_cert.bak copy.")


if __name__ == "__main__":
    main()
