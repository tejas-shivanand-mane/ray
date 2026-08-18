#!/usr/bin/env python3
"""Apply Patch 4D: pipelined Recovery Succession holder admission.

Run from the root of the custom Ray repository:

    python gossip_benchmarks/apply_patch4d_pipelined_holder_admission.py

What this patch changes
-----------------------
1. Allows multiple provisional holder reservations for the same task.
2. Assigns those reservations contiguous speculative ranks H1..HR.
3. Sends InstallRecoveryHolder RPCs concurrently.
4. Keeps witness publication + coordinator commit strictly ordered by rank.
5. Aborts a failed rank and its speculative suffix conservatively.
6. Reuses the existing CommitRecoveryManifest RPC as a failure-only rollback/
   cleanup notification for provisional candidates. Normal-path Patch 4B-2
   remains intact: there is still no explicit commit RPC on successful admission.
7. Allows a provisional candidate to roll back to an older committed manifest
   only when that committed manifest does not contain the candidate. This is
   used solely to clean up aborted speculative installations.

The patch intentionally does NOT change:
- lazy activation (4C),
- adaptive witness batching (4B-3),
- compact witness durability semantics,
- the requirement that witness publication gates each committed rank,
- target holder count R,
- recovery replay semantics.

The script is deliberately fail-fast: if the expected current code shape is not
found, it refuses to make a partial edit.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import sys


ROOT = Path.cwd()
MANAGER_H = ROOT / "src/ray/core_worker/recovery_succession_manager.h"
MANAGER_CC = ROOT / "src/ray/core_worker/recovery_succession_manager.cc"
CORE_H = ROOT / "src/ray/core_worker/core_worker.h"
CORE_CC = ROOT / "src/ray/core_worker/core_worker.cc"

PATCH_MARKER = "Patch 4D: pipelined holder admission"


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def read_required(path: Path) -> str:
    if not path.exists():
        fail(f"Missing {path}. Run this script from the Ray repository root.")
    return path.read_text()


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        fail(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def find_matching_cpp_brace(text: str, open_brace: int) -> int:
    """Find a matching C++ brace while ignoring comments and literals."""
    if open_brace < 0 or open_brace >= len(text) or text[open_brace] != "{":
        raise ValueError("open_brace must point at '{'")

    depth = 0
    i = open_brace
    state = "code"
    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                i += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                i += 2
                continue
            if ch == '"':
                state = "string"
                i += 1
                continue
            if ch == "'":
                state = "char"
                i += 1
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return i
            i += 1
            continue

        if state == "line_comment":
            if ch == "\n":
                state = "code"
            i += 1
            continue

        if state == "block_comment":
            if ch == "*" and nxt == "/":
                state = "code"
                i += 2
            else:
                i += 1
            continue

        if state in ("string", "char"):
            quote = '"' if state == "string" else "'"
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                state = "code"
            i += 1
            continue

    raise ValueError("No matching brace")


def replace_cpp_function(text: str, start_marker: str, replacement: str, label: str) -> str:
    start = text.find(start_marker)
    if start < 0:
        fail(f"{label}: start marker not found")
    if text.find(start_marker, start + 1) >= 0:
        fail(f"{label}: start marker is not unique")
    open_brace = text.find("{", start + len(start_marker))
    if open_brace < 0:
        fail(f"{label}: opening brace not found")
    try:
        close_brace = find_matching_cpp_brace(text, open_brace)
    except ValueError as exc:
        fail(f"{label}: {exc}")
    return text[:start] + replacement.rstrip() + text[close_brace + 1 :]


def backup(path: Path) -> None:
    bak = path.with_suffix(path.suffix + ".pre4d")
    if not bak.exists():
        shutil.copy2(path, bak)


MANAGER_PREPARE = r'''rpc::ReportRecoveryCandidateReply::Result
RecoverySuccessionManager::PrepareHolderAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
    HolderAdmissionPlan *plan,
    rpc::RecoveryManifest *latest_manifest) {
  if (plan == nullptr || latest_manifest == nullptr || request.task_id().empty() ||
      !request.has_candidate_address() || !request.has_cached_manifest()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  *plan = HolderAdmissionPlan();
  latest_manifest->Clear();

  const rpc::Address &candidate_address = request.candidate_address();

  if (candidate_address.worker_id().empty() || candidate_address.node_id().empty() ||
      candidate_address.ip_address().empty() || candidate_address.port() <= 0) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);
  if (task_it == task_states_.end() || !task_it->second.task_spec.has_value()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  const TaskRecoveryState &task_state = task_it->second;
  latest_manifest->CopyFrom(task_state.manifest);

  if (task_state.manifest.tombstoned()) {
    return rpc::ReportRecoveryCandidateReply::TOMBSTONED;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(task_state.manifest, 0);
  if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
    return rpc::ReportRecoveryCandidateReply::WRONG_COORDINATOR;
  }

  if (request.cached_manifest().version().generation() >
      task_state.manifest.version().generation()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  if (ContainsWorker(task_state.manifest, candidate_address)) {
    return rpc::ReportRecoveryCandidateReply::ACCEPTED;
  }

  if (task_state.manifest.frozen()) {
    return rpc::ReportRecoveryCandidateReply::FROZEN;
  }

  const uint32_t confirmed_non_owner_holders =
      task_state.manifest.succession_size() > 0
          ? static_cast<uint32_t>(task_state.manifest.succession_size() - 1)
          : 0;

  const auto per_task_it = holder_reservation_by_task_.find(task_id);
  const size_t pending_count =
      per_task_it == holder_reservation_by_task_.end()
          ? 0
          : per_task_it->second.size();

  if (confirmed_non_owner_holders + pending_count >=
      task_state.manifest.target_holder_count()) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  // Patch 4D: a task may have several provisional reservations. Reject only
  // duplicate/failure-domain candidates; do not reject merely because another
  // rank is currently being installed.
  for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
    if (!holder.failure_domain_id().empty() &&
        holder.failure_domain_id() == candidate_address.node_id()) {
      return rpc::ReportRecoveryCandidateReply::NO_SLOT;
    }
  }

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      static_cast<void>(rank);
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      if (SameWorker(pending.candidate_address, candidate_address)) {
        // The original report RPC for this candidate is still responsible for
        // completing the admission. Treat duplicate reports as already accepted.
        return rpc::ReportRecoveryCandidateReply::ACCEPTED;
      }

      if (!pending.candidate_address.node_id().empty() &&
          pending.candidate_address.node_id() == candidate_address.node_id()) {
        return rpc::ReportRecoveryCandidateReply::NO_SLOT;
      }
    }
  }

  // Construct the speculative prefix from the committed manifest plus all
  // earlier reservations. Every proposed manifest is therefore contiguous:
  // [A,H1], [A,H1,H2], ... even while H1..HR installations overlap.
  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(task_state.manifest);

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      rpc::RecoveryHolder *holder = proposed_manifest.add_succession();
      holder->mutable_address()->CopyFrom(pending.candidate_address);
      holder->set_rank(rank);
      holder->set_failure_domain_id(pending.candidate_address.node_id());
    }
  }

  const uint32_t proposed_rank =
      static_cast<uint32_t>(proposed_manifest.succession_size());

  rpc::RecoveryHolder *new_holder = proposed_manifest.add_succession();
  new_holder->mutable_address()->CopyFrom(candidate_address);
  new_holder->set_rank(proposed_rank);
  new_holder->set_failure_domain_id(candidate_address.node_id());

  proposed_manifest.mutable_version()->set_generation(
      task_state.manifest.version().generation() + pending_count + 1);

  const uint32_t holders_after_admission =
      static_cast<uint32_t>(proposed_manifest.succession_size() - 1);
  if (holders_after_admission >= proposed_manifest.target_holder_count()) {
    proposed_manifest.set_frozen(true);
  }

  const std::string reservation_id = UniqueID::FromRandom().Binary();

  HolderReservation reservation;
  reservation.task_id = task_id;
  reservation.candidate_address.CopyFrom(candidate_address);
  reservation.proposed_manifest.CopyFrom(proposed_manifest);
  reservation.proposed_rank = proposed_rank;

  holder_reservations_[reservation_id] = std::move(reservation);
  holder_reservation_by_task_[task_id][proposed_rank] = reservation_id;

  plan->reservation_id = reservation_id;
  plan->candidate_address.CopyFrom(candidate_address);
  plan->candidate_already_stores_task_spec = request.already_stores_task_spec();

  if (!plan->candidate_already_stores_task_spec) {
    if (profiling_enabled_) {
      const auto copy_start = std::chrono::steady_clock::now();

      plan->task_spec.CopyFrom(task_it->second.task_spec.value());
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);

      const auto copy_end = std::chrono::steady_clock::now();
      const uint64_t copy_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(copy_end - copy_start)
              .count());

      ++profile_.owner_task_spec_copy_count;
      profile_.owner_task_spec_copy_time_ns += copy_ns;
    } else {
      plan->task_spec.CopyFrom(task_it->second.task_spec.value());
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);
    }
  }

  plan->proposed_manifest.CopyFrom(proposed_manifest);
  return rpc::ReportRecoveryCandidateReply::ACCEPTED;
}'''


MANAGER_COMMIT = r'''bool RecoverySuccessionManager::CommitHolderAdmission(
    const std::string &reservation_id, rpc::RecoveryManifest *committed_manifest) {
  if (reservation_id.empty() || committed_manifest == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return false;
  }

  const HolderReservation &reservation = reservation_it->second;
  const TaskID task_id = reservation.task_id;
  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    EraseHolderReservationLocked(reservation_id);
    return false;
  }

  const rpc::RecoveryManifest &current = task_it->second.manifest;
  const rpc::RecoveryManifest &proposed = reservation.proposed_manifest;

  // Patch 4D: only the next contiguous rank may become durable. Install RPCs
  // may complete in any order, but commits must remain H1,H2,...
  if (reservation.proposed_rank !=
          static_cast<uint32_t>(current.succession_size()) ||
      proposed.succession_size() != current.succession_size() + 1 ||
      proposed.version().generation() != current.version().generation() + 1) {
    return false;
  }

  for (int index = 0; index < current.succession_size(); ++index) {
    if (current.succession(index).SerializeAsString() !=
        proposed.succession(index).SerializeAsString()) {
      return false;
    }
  }

  UpdateManifestForTaskLocked(task_id, proposed, true);

  if (profiling_enabled_) {
    ++profile_.holder_admissions_committed;
    ++profile_.manifest_generations_committed;

    if (proposed.version().generation() > profile_.max_generation) {
      profile_.max_generation = proposed.version().generation();
    }

    const uint64_t non_owner_holders =
        proposed.succession_size() > 0
            ? static_cast<uint64_t>(proposed.succession_size() - 1)
            : 0;

    if (non_owner_holders > profile_.max_non_owner_holders) {
      profile_.max_non_owner_holders = non_owner_holders;
    }

    if (proposed.frozen()) {
      ++profile_.frozen_commits;
    }
  }

  committed_manifest->CopyFrom(proposed);
  EraseHolderReservationLocked(reservation_id);
  return true;
}'''


MANAGER_ABORT = r'''void RecoverySuccessionManager::AbortHolderAdmission(
    const std::string &reservation_id) {
  if (reservation_id.empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t failed_rank = reservation_it->second.proposed_rank;

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it == holder_reservation_by_task_.end()) {
    holder_reservations_.erase(reservation_it);
    return;
  }

  // Patch 4D conservative failure rule: a missing lower rank invalidates every
  // speculative suffix reservation because their proposed manifests include it.
  std::vector<std::string> suffix;
  for (auto it = task_index_it->second.lower_bound(failed_rank);
       it != task_index_it->second.end(); ++it) {
    suffix.push_back(it->second);
  }

  for (const std::string &id : suffix) {
    EraseHolderReservationLocked(id);
  }
}'''


MANAGER_ERASE = r'''void RecoverySuccessionManager::EraseHolderReservationLocked(
    const std::string &reservation_id) {
  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t rank = reservation_it->second.proposed_rank;

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it != holder_reservation_by_task_.end()) {
    auto rank_it = task_index_it->second.find(rank);
    if (rank_it != task_index_it->second.end() && rank_it->second == reservation_id) {
      task_index_it->second.erase(rank_it);
    }
    if (task_index_it->second.empty()) {
      holder_reservation_by_task_.erase(task_index_it);
    }
  }

  holder_reservations_.erase(reservation_it);
}'''


MANAGER_ALLOW_RETRY = r'''void RecoverySuccessionManager::AllowCandidateReportRetry(
    const TaskID &task_id) {
  if (task_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  candidate_reports_sent_.erase(task_id);
}'''


CORE_HELPERS = r'''void CoreWorker::SendRecoveryHolderRollback(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
    const rpc::RecoveryManifest &committed_manifest) {
  if (state == nullptr || committed_manifest.task_id().empty() ||
      state->candidate_address.worker_id().empty()) {
    return;
  }

  rpc::CommitRecoveryManifestRequest request;
  request.mutable_manifest()->CopyFrom(committed_manifest);

  auto client = core_worker_client_pool_->GetOrConnect(state->candidate_address);

  uint64_t rpc_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    recovery_succession_manager_->RecordHolderCommitRpcSent(
        static_cast<uint64_t>(committed_manifest.ByteSizeLong()));
    rpc_start_ns = RecoveryProfileNowNs();
  }

  client->CommitRecoveryManifest(
      std::move(request),
      [manager = recovery_succession_manager_, rpc_start_ns](
          const Status &status, rpc::CommitRecoveryManifestReply &&reply) {
        static_cast<void>(reply);
        if (rpc_start_ns != 0) {
          manager->RecordHolderCommitRpcLatency(RecoveryProfileNowNs() - rpc_start_ns);
        }
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Patch 4D provisional-holder rollback RPC failed: "
                         << status;
        }
      });
}

void CoreWorker::AbortRecoveryHolderAdmissionSuffix(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &failed_state,
    rpc::ReportRecoveryCandidateReply::Result failed_result,
    const rpc::RecoveryManifest &committed_manifest) {
  if (failed_state == nullptr) {
    return;
  }

  // First remove the owner-side reservations. Manager semantics remove the
  // failed rank and every speculative rank above it.
  recovery_succession_manager_->AbortHolderAdmission(failed_state->reservation_id);

  std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> aborted;
  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    const auto task_it = recovery_holder_admission_states_.find(failed_state->task_id);
    if (task_it != recovery_holder_admission_states_.end()) {
      auto &task_state = task_it->second;
      for (auto it = task_state.pending_by_rank.lower_bound(failed_state->rank);
           it != task_state.pending_by_rank.end();) {
        it->second->aborted = true;
        it->second->abort_manifest.CopyFrom(committed_manifest);
        aborted.push_back(it->second);
        it = task_state.pending_by_rank.erase(it);
      }
      if (task_state.witness_publish_rank >= failed_state->rank) {
        task_state.witness_publish_rank = 0;
      }
      if (task_state.pending_by_rank.empty()) {
        recovery_holder_admission_states_.erase(task_it);
      }
    }
  }

  for (const auto &state : aborted) {
    if (state->reply != nullptr) {
      state->reply->set_result(
          state->rank == failed_state->rank
              ? failed_result
              : rpc::ReportRecoveryCandidateReply::NO_SLOT);
      if (!committed_manifest.task_id().empty()) {
        state->reply->mutable_latest_manifest()->CopyFrom(committed_manifest);
      }
    }

    // Failure-only cleanup. A higher-rank InstallRecoveryHolder may already
    // have completed. Roll it back to the last committed prefix so it does not
    // remain a permanently orphaned provisional holder.
    SendRecoveryHolderRollback(state, committed_manifest);

    state->send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

void CoreWorker::TryAdvanceRecoveryHolderAdmissions(const TaskID &task_id) {
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
}

void CoreWorker::FinishRecoveryHolderAdmission(
    std::shared_ptr<PendingRecoveryHolderAdmission> state) {
  if (state == nullptr) {
    return;
  }

  auto manager = recovery_succession_manager_;
  uint64_t witness_publish_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    witness_publish_start_ns = RecoveryProfileNowNs();
  }

  PublishRecoveryManifestToWitnesses(
      state->proposed_manifest,
      [this, manager, state = std::move(state), witness_publish_start_ns](
          bool witness_stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (witness_publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - witness_publish_start_ns);
        }

        if (!witness_stored) {
          rpc::RecoveryManifest rollback_manifest;
          if (newer_manifest.has_value()) {
            rollback_manifest.CopyFrom(newer_manifest.value());
            manager->ApplyCommittedManifest(newer_manifest.value());
          } else {
            rollback_manifest.CopyFrom(state->latest_manifest);
          }

          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::STALE_MANIFEST,
              rollback_manifest);
          return;
        }

        // Preserve the existing post-witness/pre-commit failure injection.
        if (state->candidate_needs_commit_rpc &&
            RayConfig::instance().recovery_succession_test_fail_after_witness_ack()) {
          RAY_LOG(WARNING).WithField(state->task_id)
              << "TEST ONLY: injected recovery succession failure after "
                 "witness ACK before candidate commit";
          state->send_reply_callback(
              Status::IOError(
                  "Injected recovery succession failure after witness ACK "
                  "before candidate commit"),
              nullptr,
              nullptr);
          return;
        }

        rpc::RecoveryManifest committed_manifest;
        if (!manager->CommitHolderAdmission(state->reservation_id, &committed_manifest)) {
          // This should not occur on the normal Patch-4D path because only the
          // lowest installed rank reaches this function. Fail the speculative
          // suffix rather than committing out of order.
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

        RAY_LOG(INFO).WithField(state->task_id)
            << "Patch 4D: committed ordered recovery succession rank "
            << state->rank << " with " << committed_manifest.succession_size()
            << " total members";

        // Patch 4B-2 remains in force: successful normal admission does not
        // send an explicit CommitRecoveryManifest RPC. The report reply carries
        // the committed manifest to this candidate.
        state->send_reply_callback(Status::OK(), nullptr, nullptr);

        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          const auto task_it = recovery_holder_admission_states_.find(state->task_id);
          if (task_it != recovery_holder_admission_states_.end()) {
            auto &task_state = task_it->second;
            const auto rank_it = task_state.pending_by_rank.find(state->rank);
            if (rank_it != task_state.pending_by_rank.end() &&
                rank_it->second->reservation_id == state->reservation_id) {
              task_state.pending_by_rank.erase(rank_it);
            }
            if (task_state.witness_publish_rank == state->rank) {
              task_state.witness_publish_rank = 0;
            }
            if (task_state.pending_by_rank.empty()) {
              recovery_holder_admission_states_.erase(task_it);
            }
          }
        }

        TryAdvanceRecoveryHolderAdmissions(state->task_id);
      });
}'''


CORE_REPORT = r'''void CoreWorker::HandleReportRecoveryCandidate(
    rpc::ReportRecoveryCandidateRequest request,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!recovery_succession_enabled_ ||
      recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    reply->set_result(rpc::ReportRecoveryCandidateReply::DISABLED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const uint64_t admission_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  auto manager = recovery_succession_manager_;
  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
  rpc::RecoveryManifest latest_manifest;

  const auto result = manager->PrepareHolderAdmission(
      request, &admission_plan, &latest_manifest);

  if (recovery_succession_profiling_enabled_) {
    const bool accepted_new_holder =
        result == rpc::ReportRecoveryCandidateReply::ACCEPTED &&
        !admission_plan.reservation_id.empty();
    manager->RecordCandidateReport(accepted_new_holder);
  }

  reply->set_result(result);
  if (!latest_manifest.task_id().empty()) {
    reply->mutable_latest_manifest()->CopyFrom(latest_manifest);
  }

  // ACCEPTED with no reservation means already committed or already pending.
  if (result != rpc::ReportRecoveryCandidateReply::ACCEPTED ||
      admission_plan.reservation_id.empty()) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const std::string reservation_id = admission_plan.reservation_id;
  const TaskID task_id = TaskID::FromBinary(admission_plan.proposed_manifest.task_id());

  rpc::Address candidate_address;
  candidate_address.CopyFrom(admission_plan.candidate_address);

  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(admission_plan.proposed_manifest);

  const rpc::RecoveryHolder *candidate_holder = nullptr;
  for (const rpc::RecoveryHolder &holder : proposed_manifest.succession()) {
    if (holder.address().worker_id() == candidate_address.worker_id()) {
      candidate_holder = &holder;
      break;
    }
  }

  if (candidate_holder == nullptr) {
    manager->AbortHolderAdmission(reservation_id);
    reply->set_result(rpc::ReportRecoveryCandidateReply::STALE_MANIFEST);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  auto state = std::make_shared<PendingRecoveryHolderAdmission>();
  state->reservation_id = reservation_id;
  state->task_id = task_id;
  state->rank = candidate_holder->rank();
  state->candidate_address.CopyFrom(candidate_address);
  state->candidate_needs_commit_rpc =
      !admission_plan.candidate_already_stores_task_spec;
  state->latest_manifest.CopyFrom(latest_manifest);
  state->proposed_manifest.CopyFrom(proposed_manifest);
  state->admission_start_ns = admission_start_ns;
  state->reply = reply;
  state->send_reply_callback = std::move(send_reply_callback);

  {
    absl::MutexLock lock(&recovery_holder_admission_mutex_);
    auto &task_state = recovery_holder_admission_states_[task_id];
    const auto inserted = task_state.pending_by_rank.emplace(state->rank, state);
    if (!inserted.second) {
      manager->AbortHolderAdmission(reservation_id);
      state->reply->set_result(rpc::ReportRecoveryCandidateReply::NO_SLOT);
      state->send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
  }

  if (admission_plan.candidate_already_stores_task_spec) {
    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      state->installed = true;
    }
    TryAdvanceRecoveryHolderAdmissions(task_id);
    return;
  }

  rpc::InstallRecoveryHolderRequest install_request;
  install_request.set_task_id(proposed_manifest.task_id());
  install_request.set_reservation_id(reservation_id);
  install_request.set_proposed_rank(state->rank);

  if (recovery_succession_profiling_enabled_) {
    const uint64_t task_spec_copy_start_ns = RecoveryProfileNowNs();
    install_request.mutable_task_spec()->CopyFrom(admission_plan.task_spec);
    manager->RecordOwnerTaskSpecCopyLatency(
        RecoveryProfileNowNs() - task_spec_copy_start_ns);
  } else {
    install_request.mutable_task_spec()->CopyFrom(admission_plan.task_spec);
  }

  install_request.mutable_proposed_manifest()->CopyFrom(proposed_manifest);

  auto candidate_client = core_worker_client_pool_->GetOrConnect(candidate_address);

  uint64_t install_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    manager->RecordHolderInstallRpcSent(
        static_cast<uint64_t>(install_request.task_spec().ByteSizeLong()),
        static_cast<uint64_t>(install_request.proposed_manifest().ByteSizeLong()));
    install_start_ns = RecoveryProfileNowNs();
  }

  // Patch 4D: this RPC is no longer serialized behind the previous holder's
  // witness publication/commit. H1..HR installs may all be in flight.
  candidate_client->InstallRecoveryHolder(
      std::move(install_request),
      [this, manager, state, install_start_ns](
          const Status &status, rpc::InstallRecoveryHolderReply &&install_reply) mutable {
        if (install_start_ns != 0) {
          manager->RecordHolderInstallRpcLatency(
              RecoveryProfileNowNs() - install_start_ns);
        }

        bool already_aborted = false;
        rpc::RecoveryManifest abort_manifest;
        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          already_aborted = state->aborted;
          if (already_aborted) {
            abort_manifest.CopyFrom(state->abort_manifest);
          }
        }

        if (already_aborted) {
          // The lower-rank failure may have raced with this Install RPC. If the
          // candidate stored the provisional lineage after the first cleanup,
          // send cleanup again now that installation has definitively finished.
          if (status.ok() && install_reply.stored()) {
            SendRecoveryHolderRollback(state, abort_manifest);
          }
          return;
        }

        if (!status.ok() || !install_reply.stored() ||
            install_reply.reservation_id() != state->reservation_id) {
          AbortRecoveryHolderAdmissionSuffix(
              state,
              rpc::ReportRecoveryCandidateReply::NO_SLOT,
              state->latest_manifest);
          return;
        }

        {
          absl::MutexLock lock(&recovery_holder_admission_mutex_);
          if (state->aborted) {
            abort_manifest.CopyFrom(state->abort_manifest);
            already_aborted = true;
          } else {
            state->installed = true;
          }
        }

        if (already_aborted) {
          SendRecoveryHolderRollback(state, abort_manifest);
          return;
        }

        TryAdvanceRecoveryHolderAdmissions(state->task_id);
      });
}'''


def patch_manager_h(text: str) -> str:
    if PATCH_MARKER in text:
        return text

    text = replace_once(
        text,
        "#include <functional>\n#include <optional>",
        "#include <functional>\n#include <map>\n#include <optional>",
        "manager.h include <map>",
    )

    text = replace_once(
        text,
        "  /// Removes a failed provisional reservation.\n"
        "  void AbortHolderAdmission(const std::string &reservation_id);",
        "  /// Patch 4D: removes a failed provisional reservation and every\n"
        "  /// speculative reservation at a higher rank for the same task.\n"
        "  void AbortHolderAdmission(const std::string &reservation_id);\n\n"
        "  /// Allows a borrower whose candidate report was rejected/aborted to\n"
        "  /// report itself again on a later ObjectRef delivery.\n"
        "  void AllowCandidateReportRetry(const TaskID &task_id);",
        "manager.h abort declaration",
    )

    text = replace_once(
        text,
        "  struct HolderReservation {\n"
        "    TaskID task_id;\n"
        "    rpc::Address candidate_address;\n"
        "    rpc::RecoveryManifest proposed_manifest;\n"
        "  };",
        "  struct HolderReservation {\n"
        "    TaskID task_id;\n"
        "    rpc::Address candidate_address;\n"
        "    rpc::RecoveryManifest proposed_manifest;\n"
        "    uint32_t proposed_rank = 0;  // Patch 4D: speculative contiguous rank.\n"
        "  };",
        "manager.h HolderReservation",
    )

    text = replace_once(
        text,
        "  /// At most one provisional holder reservation is permitted per task.\n"
        "  absl::flat_hash_map<TaskID, std::string> holder_reservation_by_task_\n"
        "      ABSL_GUARDED_BY(mutex_);",
        "  /// Patch 4D: multiple provisional reservations may coexist per task.\n"
        "  /// The ordered rank map is the speculative prefix H1..HR.\n"
        "  absl::flat_hash_map<TaskID, std::map<uint32_t, std::string>>\n"
        "      holder_reservation_by_task_ ABSL_GUARDED_BY(mutex_);",
        "manager.h reservation index",
    )

    text = text.replace(
        "/// Stores lineage, succession, and holder-admission state for the\n",
        f"/// {PATCH_MARKER}.\n/// Stores lineage, succession, and holder-admission state for the\n",
        1,
    )
    return text


def patch_manager_cc(text: str) -> str:
    if PATCH_MARKER in text:
        return text

    text = replace_cpp_function(
        text,
        "rpc::ReportRecoveryCandidateReply::Result\nRecoverySuccessionManager::PrepareHolderAdmission(",
        MANAGER_PREPARE,
        "manager.cc PrepareHolderAdmission",
    )
    text = replace_cpp_function(
        text,
        "bool RecoverySuccessionManager::CommitHolderAdmission(",
        MANAGER_COMMIT,
        "manager.cc CommitHolderAdmission",
    )
    text = replace_cpp_function(
        text,
        "void RecoverySuccessionManager::AbortHolderAdmission(",
        MANAGER_ABORT,
        "manager.cc AbortHolderAdmission",
    )
    # Add the candidate retry helper immediately after AbortHolderAdmission.
    abort_pos = text.find(MANAGER_ABORT)
    if abort_pos < 0:
        fail("manager.cc patched AbortHolderAdmission not found")
    abort_open = text.find("{", abort_pos)
    abort_close = find_matching_cpp_brace(text, abort_open)
    text = text[: abort_close + 1] + "\n\n" + MANAGER_ALLOW_RETRY + text[abort_close + 1 :]
    text = replace_cpp_function(
        text,
        "void RecoverySuccessionManager::EraseHolderReservationLocked(",
        MANAGER_ERASE,
        "manager.cc EraseHolderReservationLocked",
    )

    # Tombstone cleanup used to assume a single reservation id.
    old_tombstone = '''  const auto reservation_it =
      holder_reservation_by_task_.find(task_id);

  if (reservation_it != holder_reservation_by_task_.end()) {
    const std::string reservation_id = reservation_it->second;
    EraseHolderReservationLocked(reservation_id);
  }
'''
    new_tombstone = '''  const auto reservation_it = holder_reservation_by_task_.find(task_id);
  if (reservation_it != holder_reservation_by_task_.end()) {
    std::vector<std::string> reservation_ids;
    reservation_ids.reserve(reservation_it->second.size());
    for (const auto &[rank, reservation_id] : reservation_it->second) {
      static_cast<void>(rank);
      reservation_ids.push_back(reservation_id);
    }
    for (const std::string &reservation_id : reservation_ids) {
      EraseHolderReservationLocked(reservation_id);
    }
  }
'''
    text = replace_once(text, old_tombstone, new_tombstone, "manager.cc tombstone reservations")

    # Failure-only rollback support: an aborted speculative candidate can be
    # reset to the last committed prefix even though that prefix has a lower
    # generation than its speculative local manifest.
    old_apply = '''    if (comparison < 0) {
      return false;
    }

    if (comparison == 0 &&
'''
    new_apply = '''    if (comparison < 0) {
      // Patch 4D failure-only rollback. A speculative higher-rank holder may
      // have installed a future manifest before a lower rank failed. The
      // coordinator cleans that candidate up by sending the last committed
      // prefix through the existing CommitRecoveryManifest RPC. Accept this
      // older prefix only for an uncommitted provisional holder that is NOT a
      // member of the committed prefix.
      if (!task_it->second.manifest_committed &&
          !ContainsWorker(manifest, self_address_)) {
        UpdateManifestForTaskLocked(task_id, manifest, true);
        candidate_reports_sent_.erase(task_id);
        return true;
      }
      return false;
    }

    if (comparison == 0 &&
'''
    # There are several comparison<0 blocks in the file, so scope the change to
    # ApplyCommittedManifest by replacing within its function only.
    marker = "bool RecoverySuccessionManager::ApplyCommittedManifest("
    start = text.find(marker)
    if start < 0:
        fail("manager.cc ApplyCommittedManifest not found")
    open_brace = text.find("{", start)
    close_brace = find_matching_cpp_brace(text, open_brace)
    apply_block = text[start : close_brace + 1]
    apply_block = replace_once(
        apply_block, old_apply, new_apply, "manager.cc provisional rollback"
    )
    text = text[:start] + apply_block + text[close_brace + 1 :]

    text = text.replace(
        "namespace ray::core {\n",
        f"namespace ray::core {{\n\n// {PATCH_MARKER}.\n",
        1,
    )
    return text


def patch_core_h(text: str) -> str:
    if PATCH_MARKER in text:
        return text

    text = replace_once(
        text,
        "#include <functional>\n#include <memory>",
        "#include <functional>\n#include <map>\n#include <memory>",
        "core_worker.h include <map>",
    )

    old_finish = '''  void FinishRecoveryHolderAdmission(
    std::string reservation_id,
    TaskID task_id,
    rpc::Address candidate_address,
    bool candidate_needs_commit_rpc,
    rpc::RecoveryManifest latest_manifest,
    rpc::RecoveryManifest proposed_manifest,
    uint64_t admission_start_ns,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback);
'''
    new_finish = '''  // Patch 4D: InstallRecoveryHolder RPCs may complete concurrently, while
  // witness publication and durable commits remain strictly rank ordered.
  struct PendingRecoveryHolderAdmission {
    std::string reservation_id;
    TaskID task_id;
    uint32_t rank = 0;
    rpc::Address candidate_address;
    bool candidate_needs_commit_rpc = true;
    rpc::RecoveryManifest latest_manifest;
    rpc::RecoveryManifest proposed_manifest;
    uint64_t admission_start_ns = 0;
    rpc::ReportRecoveryCandidateReply *reply = nullptr;
    rpc::SendReplyCallback send_reply_callback;
    bool installed = false;
    bool aborted = false;
    rpc::RecoveryManifest abort_manifest;
  };

  struct RecoveryHolderAdmissionTaskState {
    // Zero means no publication is active. Otherwise this is the rank whose
    // manifest is currently being published/committed.
    uint32_t witness_publish_rank = 0;
    std::map<uint32_t, std::shared_ptr<PendingRecoveryHolderAdmission>> pending_by_rank;
  };

  void FinishRecoveryHolderAdmission(
      std::shared_ptr<PendingRecoveryHolderAdmission> state);

  void TryAdvanceRecoveryHolderAdmissions(const TaskID &task_id);

  void AbortRecoveryHolderAdmissionSuffix(
      const std::shared_ptr<PendingRecoveryHolderAdmission> &failed_state,
      rpc::ReportRecoveryCandidateReply::Result failed_result,
      const rpc::RecoveryManifest &committed_manifest);

  void SendRecoveryHolderRollback(
      const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
      const rpc::RecoveryManifest &committed_manifest);
'''
    text = replace_once(text, old_finish, new_finish, "core_worker.h Finish declaration")

    old_members = '''  /// Distributed recovery succession state. Null when the feature is disabled.
  std::shared_ptr<RecoverySuccessionManager> recovery_succession_manager_;

  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;
'''
    new_members = '''  /// Distributed recovery succession state. Null when the feature is disabled.
  std::shared_ptr<RecoverySuccessionManager> recovery_succession_manager_;

  // Patch 4D owner-side continuation queue. It is deliberately separate from
  // mutex_: RPC callbacks may finish out of order, and this mutex protects only
  // the tiny admission scheduler state.
  mutable absl::Mutex recovery_holder_admission_mutex_;
  absl::flat_hash_map<TaskID, RecoveryHolderAdmissionTaskState>
      recovery_holder_admission_states_
          ABSL_GUARDED_BY(recovery_holder_admission_mutex_);

  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;
'''
    text = replace_once(text, old_members, new_members, "core_worker.h Patch 4D members")

    text = text.replace(
        "namespace ray::core {\n",
        f"namespace ray::core {{\n\n// {PATCH_MARKER}.\n",
        1,
    )
    return text


def patch_core_cc(text: str) -> str:
    if PATCH_MARKER in text:
        return text

    text = replace_cpp_function(
        text,
        "void CoreWorker::FinishRecoveryHolderAdmission(",
        CORE_HELPERS,
        "core_worker.cc FinishRecoveryHolderAdmission",
    )
    text = replace_cpp_function(
        text,
        "void CoreWorker::HandleReportRecoveryCandidate(",
        CORE_REPORT,
        "core_worker.cc HandleReportRecoveryCandidate",
    )

    old_candidate_callback = '''            if (candidate_reply.has_latest_manifest()) {
              manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
            }
'''
    new_candidate_callback = '''            if (candidate_reply.has_latest_manifest()) {
              manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
            }

            // Patch 4D: transient admission failure must not permanently suppress
            // future candidate reports from this borrower. A late speculative
            // install that is rolled back will clear the bit again on rollback.
            if (candidate_reply.result() ==
                    rpc::ReportRecoveryCandidateReply::NO_SLOT ||
                candidate_reply.result() ==
                    rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
              manager->AllowCandidateReportRetry(reported_task_id);
            }
'''
    text = replace_once(
        text,
        old_candidate_callback,
        new_candidate_callback,
        "core_worker.cc candidate retry callback",
    )

    text = text.replace(
        "namespace ray::core {\n",
        f"namespace ray::core {{\n\n// {PATCH_MARKER}.\n",
        1,
    )
    return text


def main() -> None:
    paths = [MANAGER_H, MANAGER_CC, CORE_H, CORE_CC]
    originals = {path: read_required(path) for path in paths}

    if all(PATCH_MARKER in originals[path] for path in paths):
        print("Patch 4D is already applied to all four files; nothing to do.")
        return
    if any(PATCH_MARKER in originals[path] for path in paths):
        fail("Patch 4D marker found in only some files; refusing a partial re-apply.")

    patched = {
        MANAGER_H: patch_manager_h(originals[MANAGER_H]),
        MANAGER_CC: patch_manager_cc(originals[MANAGER_CC]),
        CORE_H: patch_core_h(originals[CORE_H]),
        CORE_CC: patch_core_cc(originals[CORE_CC]),
    }

    # Cheap structural sanity checks before touching the tree.
    required_fragments = {
        MANAGER_H: [
            "std::map<uint32_t, std::string>",
            "proposed_rank = 0",
        ],
        MANAGER_CC: [
            "pending_count",
            "lower_bound(failed_rank)",
            "Patch 4D failure-only rollback",
            "AllowCandidateReportRetry",
        ],
        CORE_H: [
            "PendingRecoveryHolderAdmission",
            "recovery_holder_admission_states_",
        ],
        CORE_CC: [
            "TryAdvanceRecoveryHolderAdmissions",
            "SendRecoveryHolderRollback",
            "H1..HR installs may all be in flight",
        ],
    }
    for path, fragments in required_fragments.items():
        for fragment in fragments:
            if fragment not in patched[path]:
                fail(f"Internal patch validation failed for {path}: missing {fragment!r}")

    for path in paths:
        backup(path)
    for path in paths:
        path.write_text(patched[path])

    print("Applied Patch 4D successfully.")
    print("Changed:")
    for path in paths:
        print(f"  {path.relative_to(ROOT)}")
    print()
    print("Backups (created only if absent): *.pre4d")
    print()
    print("Next steps:")
    print("  1. Rebuild your Ray editable install as usual.")
    print("  2. Run a 1-repetition concurrent Benchmark 21 smoke test.")
    print("  3. Require copy_count_valid=1 and native_target_reached=1.")
    print("  4. Then compare 1 MiB formation time against the pre-4D result.")


if __name__ == "__main__":
    main()
