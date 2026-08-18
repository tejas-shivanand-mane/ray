#!/usr/bin/env python3
"""Apply Patch 4E: cross-task batching for Recovery Succession control RPCs.

Run from the root of the custom Ray repository after Patch 4D + its SIGSEGV hotfix:

    python gossip_benchmarks/apply_patch4e_batched_recovery_control.py

Patch 4E is a *physical RPC coalescing* optimization only.

It adds:
  * ReportRecoveryCandidateBatch RPC.
  * InstallRecoveryHolderBatch RPC.
  * A small sender-side coalescing window for candidate reports to the same owner.
  * Byte-bounded install batches so large TaskSpecs do not form giant gRPC messages.

It intentionally preserves all logical protocol semantics:
  * one logical candidate report per candidate/task,
  * one logical TaskSpec installation per admitted holder,
  * Patch-4D concurrent provisional installs,
  * strictly ordered per-task witness publication / durable commits,
  * the same target holder count, witness count, lineage bytes, and recovery semantics,
  * Patch-4B-2's no-extra-commit-RPC successful path,
  * Patch-4D conservative suffix rollback on an admission failure.

The coalescer defaults are intentionally conservative:
  * candidate batch window: 500 us,
  * candidate batch max: 64 logical reports,
  * install batch max: 64 logical installs,
  * install batch byte cap: 4 MiB.

The script is fail-fast. It creates .pre4e backups and refuses to patch a tree
that is not the expected post-4D/hotfix shape.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path.cwd()

PROTO = ROOT / "src/ray/protobuf/core_worker.proto"
CLIENT_IFACE = ROOT / "src/ray/core_worker_rpc_client/core_worker_client_interface.h"
CLIENT_H = ROOT / "src/ray/core_worker_rpc_client/core_worker_client.h"
FAKE_CLIENT_H = ROOT / "src/ray/core_worker_rpc_client/fake_core_worker_client.h"
PROXY_H = ROOT / "src/ray/core_worker/core_worker_rpc_proxy.h"
CORE_H = ROOT / "src/ray/core_worker/core_worker.h"
CORE_CC = ROOT / "src/ray/core_worker/core_worker.cc"

FILES = [PROTO, CLIENT_IFACE, CLIENT_H, FAKE_CLIENT_H, PROXY_H, CORE_H, CORE_CC]

PATCH4D_MARKER = "Patch 4D: pipelined holder admission"
PATCH4E_MARKER = "Patch 4E: batched recovery control RPCs"


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
    bak = path.with_suffix(path.suffix + ".pre4e")
    if not bak.exists():
        shutil.copy2(path, bak)


def patch_proto(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    old_report_tail = '''message ReportRecoveryCandidateReply {
  enum Result {
    ACCEPTED = 0;
    NO_SLOT = 1;
    FROZEN = 2;
    STALE_MANIFEST = 3;
    WRONG_COORDINATOR = 4;
    TOMBSTONED = 5;
    DISABLED = 6;
  }

  Result result = 1;
  RecoveryManifest latest_manifest = 2;
}

message InstallRecoveryHolderRequest {'''

    new_report_tail = '''message ReportRecoveryCandidateReply {
  enum Result {
    ACCEPTED = 0;
    NO_SLOT = 1;
    FROZEN = 2;
    STALE_MANIFEST = 3;
    WRONG_COORDINATOR = 4;
    TOMBSTONED = 5;
    DISABLED = 6;
  }

  Result result = 1;
  RecoveryManifest latest_manifest = 2;
}

// Patch 4E: physically coalesce independent logical candidate reports.
// replies[i] corresponds to requests[i].
message ReportRecoveryCandidateBatchRequest {
  repeated ReportRecoveryCandidateRequest requests = 1;
}

message ReportRecoveryCandidateBatchReply {
  repeated ReportRecoveryCandidateReply replies = 1;
}

message InstallRecoveryHolderRequest {'''
    text = replace_once(text, old_report_tail, new_report_tail, "proto candidate batch messages")

    old_install_tail = '''message InstallRecoveryHolderReply {
  bool stored = 1;
  bytes reservation_id = 2;
}

message CommitRecoveryManifestRequest {'''

    new_install_tail = '''message InstallRecoveryHolderReply {
  bool stored = 1;
  bytes reservation_id = 2;
}

// Patch 4E: physically coalesce independent logical holder installations.
// replies[i] corresponds to requests[i].
message InstallRecoveryHolderBatchRequest {
  repeated InstallRecoveryHolderRequest requests = 1;
}

message InstallRecoveryHolderBatchReply {
  repeated InstallRecoveryHolderReply replies = 1;
}

message CommitRecoveryManifestRequest {'''
    text = replace_once(text, old_install_tail, new_install_tail, "proto install batch messages")

    old_service_report = '''  rpc ReportRecoveryCandidate(ReportRecoveryCandidateRequest)
      returns (ReportRecoveryCandidateReply);

  rpc InstallRecoveryHolder(InstallRecoveryHolderRequest)'''
    new_service_report = '''  rpc ReportRecoveryCandidate(ReportRecoveryCandidateRequest)
      returns (ReportRecoveryCandidateReply);

  rpc ReportRecoveryCandidateBatch(ReportRecoveryCandidateBatchRequest)
      returns (ReportRecoveryCandidateBatchReply);

  rpc InstallRecoveryHolder(InstallRecoveryHolderRequest)'''
    text = replace_once(text, old_service_report, new_service_report, "proto report batch RPC")

    old_service_install = '''  rpc InstallRecoveryHolder(InstallRecoveryHolderRequest)
      returns (InstallRecoveryHolderReply);

  rpc CommitRecoveryManifest(CommitRecoveryManifestRequest)'''
    new_service_install = '''  rpc InstallRecoveryHolder(InstallRecoveryHolderRequest)
      returns (InstallRecoveryHolderReply);

  rpc InstallRecoveryHolderBatch(InstallRecoveryHolderBatchRequest)
      returns (InstallRecoveryHolderBatchReply);

  rpc CommitRecoveryManifest(CommitRecoveryManifestRequest)'''
    text = replace_once(text, old_service_install, new_service_install, "proto install batch RPC")

    text = text.replace("// Recovery succession\n", f"// Recovery succession\n// {PATCH4E_MARKER}.\n", 1)
    return text


def patch_client_interface(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    old = '''  virtual void ReportRecoveryCandidate(
      ReportRecoveryCandidateRequest &&request,
      const ClientCallback<ReportRecoveryCandidateReply> &callback) = 0;

  virtual void InstallRecoveryHolder(
      InstallRecoveryHolderRequest &&request,
      const ClientCallback<InstallRecoveryHolderReply> &callback) = 0;
'''
    new = '''  virtual void ReportRecoveryCandidate(
      ReportRecoveryCandidateRequest &&request,
      const ClientCallback<ReportRecoveryCandidateReply> &callback) = 0;

  virtual void ReportRecoveryCandidateBatch(
      ReportRecoveryCandidateBatchRequest &&request,
      const ClientCallback<ReportRecoveryCandidateBatchReply> &callback) = 0;

  virtual void InstallRecoveryHolder(
      InstallRecoveryHolderRequest &&request,
      const ClientCallback<InstallRecoveryHolderReply> &callback) = 0;

  virtual void InstallRecoveryHolderBatch(
      InstallRecoveryHolderBatchRequest &&request,
      const ClientCallback<InstallRecoveryHolderBatchReply> &callback) = 0;
'''
    text = replace_once(text, old, new, "client interface batch methods")
    text = text.replace("  // Recovery succession RPCs.\n", f"  // Recovery succession RPCs.\n  // {PATCH4E_MARKER}.\n", 1)
    return text


def patch_client_h(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    old = '''  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   ReportRecoveryCandidate,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   InstallRecoveryHolder,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)
'''
    new = '''  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   ReportRecoveryCandidate,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  // Patch 4E: one physical RPC may carry many independent logical reports.
  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   ReportRecoveryCandidateBatch,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   InstallRecoveryHolder,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)

  VOID_RETRYABLE_RPC_CLIENT_METHOD(retryable_grpc_client_,
                                   CoreWorkerService,
                                   InstallRecoveryHolderBatch,
                                   grpc_client_,
                                   /*method_timeout_ms*/ -1,
                                   override)
'''
    text = replace_once(text, old, new, "core worker client batch macros")
    text = text.replace("namespace rpc {\n", f"namespace rpc {{\n\n// {PATCH4E_MARKER}.\n", 1)
    return text


def patch_fake_client(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    old = '''  void ReportRecoveryCandidate(
      ReportRecoveryCandidateRequest &&request,
      const ClientCallback<ReportRecoveryCandidateReply> &callback) override {}

  void InstallRecoveryHolder(
      InstallRecoveryHolderRequest &&request,
      const ClientCallback<InstallRecoveryHolderReply> &callback) override {}
'''
    new = '''  void ReportRecoveryCandidate(
      ReportRecoveryCandidateRequest &&request,
      const ClientCallback<ReportRecoveryCandidateReply> &callback) override {}

  void ReportRecoveryCandidateBatch(
      ReportRecoveryCandidateBatchRequest &&request,
      const ClientCallback<ReportRecoveryCandidateBatchReply> &callback) override {}

  void InstallRecoveryHolder(
      InstallRecoveryHolderRequest &&request,
      const ClientCallback<InstallRecoveryHolderReply> &callback) override {}

  void InstallRecoveryHolderBatch(
      InstallRecoveryHolderBatchRequest &&request,
      const ClientCallback<InstallRecoveryHolderBatchReply> &callback) override {}
'''
    text = replace_once(text, old, new, "fake core worker client batch methods")
    text = text.replace("namespace rpc {\n", f"namespace rpc {{\n\n// {PATCH4E_MARKER}.\n", 1)
    return text


def patch_proxy(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    old = '''  RAY_CORE_WORKER_RPC_PROXY(ReportRecoveryCandidate)
  RAY_CORE_WORKER_RPC_PROXY(InstallRecoveryHolder)
  RAY_CORE_WORKER_RPC_PROXY(CommitRecoveryManifest)
'''
    new = '''  RAY_CORE_WORKER_RPC_PROXY(ReportRecoveryCandidate)
  RAY_CORE_WORKER_RPC_PROXY(ReportRecoveryCandidateBatch)
  RAY_CORE_WORKER_RPC_PROXY(InstallRecoveryHolder)
  RAY_CORE_WORKER_RPC_PROXY(InstallRecoveryHolderBatch)
  RAY_CORE_WORKER_RPC_PROXY(CommitRecoveryManifest)
'''
    text = replace_once(text, old, new, "core worker proxy batch handlers")
    text = text.replace("namespace core {\n", f"namespace core {{\n\n// {PATCH4E_MARKER}.\n", 1)
    return text


def patch_core_h(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    if PATCH4D_MARKER not in text:
        fail("core_worker.h is missing Patch 4D")

    old_handlers = '''  /// Handles a worker volunteering as a recovery holder.
  void HandleReportRecoveryCandidate(rpc::ReportRecoveryCandidateRequest request,
                                     rpc::ReportRecoveryCandidateReply *reply,
                                     rpc::SendReplyCallback send_reply_callback);

  /// Installs lineage and a provisional manifest on a holder.
  void HandleInstallRecoveryHolder(rpc::InstallRecoveryHolderRequest request,
                                   rpc::InstallRecoveryHolderReply *reply,
                                   rpc::SendReplyCallback send_reply_callback);
'''
    new_handlers = '''  /// Handles a worker volunteering as a recovery holder.
  void HandleReportRecoveryCandidate(rpc::ReportRecoveryCandidateRequest request,
                                     rpc::ReportRecoveryCandidateReply *reply,
                                     rpc::SendReplyCallback send_reply_callback);

  /// Patch 4E: batched form of HandleReportRecoveryCandidate. Logical replies
  /// preserve request order.
  void HandleReportRecoveryCandidateBatch(
      rpc::ReportRecoveryCandidateBatchRequest request,
      rpc::ReportRecoveryCandidateBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback);

  /// Installs lineage and a provisional manifest on a holder.
  void HandleInstallRecoveryHolder(rpc::InstallRecoveryHolderRequest request,
                                   rpc::InstallRecoveryHolderReply *reply,
                                   rpc::SendReplyCallback send_reply_callback);

  /// Patch 4E: batched form of HandleInstallRecoveryHolder. Logical replies
  /// preserve request order.
  void HandleInstallRecoveryHolderBatch(
      rpc::InstallRecoveryHolderBatchRequest request,
      rpc::InstallRecoveryHolderBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback);
'''
    text = replace_once(text, old_handlers, new_handlers, "core_worker.h public batch handlers")

    old_private_block = '''  // Patch 4D: InstallRecoveryHolder RPCs may complete concurrently, while
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

    new_private_block = '''  // Patch 4E sender-side coalescing state. The queue is per coordinator so
  // unrelated owners never share a physical RPC.
  struct PendingRecoveryCandidateReport {
    TaskID task_id;
    rpc::ReportRecoveryCandidateRequest request;
  };

  struct RecoveryCandidateBatchQueue {
    rpc::Address coordinator_address;
    std::deque<PendingRecoveryCandidateReport> pending;
  };

  void QueueRecoveryCandidateReport(
      rpc::Address coordinator_address,
      rpc::ReportRecoveryCandidateRequest request);

  void FlushRecoveryCandidateReportBatch(const std::string &coordinator_worker_id);

  // Patch 4D: InstallRecoveryHolder RPCs may complete concurrently, while
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
    uint64_t install_start_ns = 0;
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

  struct PreparedRecoveryHolderInstall {
    std::shared_ptr<PendingRecoveryHolderAdmission> state;
    rpc::InstallRecoveryHolderRequest request;
  };

  std::optional<PreparedRecoveryHolderInstall> PrepareRecoveryCandidateAdmission(
      const rpc::ReportRecoveryCandidateRequest &request,
      rpc::ReportRecoveryCandidateReply *reply,
      rpc::SendReplyCallback send_reply_callback);

  void DispatchRecoveryHolderInstall(
      PreparedRecoveryHolderInstall prepared);

  void DispatchRecoveryHolderInstallBatch(
      std::vector<PreparedRecoveryHolderInstall> prepared);

  void HandleRecoveryHolderInstallResult(
      const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
      const Status &status,
      rpc::InstallRecoveryHolderReply install_reply);

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
    text = replace_once(text, old_private_block, new_private_block, "core_worker.h 4E private helpers")

    old_members = '''  // Patch 4D owner-side continuation queue. It is deliberately separate from
  // mutex_: RPC callbacks may finish out of order, and this mutex protects only
  // the tiny admission scheduler state.
  mutable absl::Mutex recovery_holder_admission_mutex_;
  absl::flat_hash_map<TaskID, RecoveryHolderAdmissionTaskState>
      recovery_holder_admission_states_
          ABSL_GUARDED_BY(recovery_holder_admission_mutex_);

  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;
'''
    new_members = '''  // Patch 4E candidate-report microbatch queues. This lock protects only queue
  // metadata and is never held while sending an RPC.
  mutable absl::Mutex recovery_candidate_batch_mutex_;
  absl::flat_hash_map<std::string, RecoveryCandidateBatchQueue>
      recovery_candidate_batch_queues_
          ABSL_GUARDED_BY(recovery_candidate_batch_mutex_);

  // Patch 4D owner-side continuation queue. It is deliberately separate from
  // mutex_: RPC callbacks may finish out of order, and this mutex protects only
  // the tiny admission scheduler state.
  mutable absl::Mutex recovery_holder_admission_mutex_;
  absl::flat_hash_map<TaskID, RecoveryHolderAdmissionTaskState>
      recovery_holder_admission_states_
          ABSL_GUARDED_BY(recovery_holder_admission_mutex_);

  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;
'''
    text = replace_once(text, old_members, new_members, "core_worker.h 4E queue members")

    text = text.replace(
        f"// {PATCH4D_MARKER}.\n",
        f"// {PATCH4D_MARKER}.\n// {PATCH4E_MARKER}.\n",
        1,
    )
    return text


CORE_REPORT_AND_BATCH = r'''std::optional<CoreWorker::PreparedRecoveryHolderInstall>
CoreWorker::PrepareRecoveryCandidateAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (reply == nullptr) {
    send_reply_callback(Status::Invalid("null recovery candidate reply"), nullptr, nullptr);
    return std::nullopt;
  }

  if (!recovery_succession_enabled_ ||
      recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    reply->set_result(rpc::ReportRecoveryCandidateReply::DISABLED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return std::nullopt;
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
    return std::nullopt;
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
    return std::nullopt;
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
      return std::nullopt;
    }
  }

  if (admission_plan.candidate_already_stores_task_spec) {
    {
      absl::MutexLock lock(&recovery_holder_admission_mutex_);
      state->installed = true;
    }
    TryAdvanceRecoveryHolderAdmissions(task_id);
    return std::nullopt;
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

  if (recovery_succession_profiling_enabled_) {
    // Keep the existing logical accounting. Patch 4E changes physical RPC count,
    // not the number of logical holder installations or lineage bytes.
    manager->RecordHolderInstallRpcSent(
        static_cast<uint64_t>(install_request.task_spec().ByteSizeLong()),
        static_cast<uint64_t>(install_request.proposed_manifest().ByteSizeLong()));
  }

  PreparedRecoveryHolderInstall prepared;
  prepared.state = std::move(state);
  prepared.request.Swap(&install_request);
  return prepared;
}

void CoreWorker::HandleRecoveryHolderInstallResult(
    const std::shared_ptr<PendingRecoveryHolderAdmission> &state,
    const Status &status,
    rpc::InstallRecoveryHolderReply install_reply) {
  if (state == nullptr) {
    return;
  }

  auto manager = recovery_succession_manager_;
  if (state->install_start_ns != 0) {
    manager->RecordHolderInstallRpcLatency(
        RecoveryProfileNowNs() - state->install_start_ns);
    state->install_start_ns = 0;
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
    // The lower-rank failure may have raced with this install. If the candidate
    // stored the provisional lineage after the first cleanup, clean it again.
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
}

void CoreWorker::DispatchRecoveryHolderInstall(
    PreparedRecoveryHolderInstall prepared) {
  if (prepared.state == nullptr) {
    return;
  }

  auto state = prepared.state;
  auto candidate_client =
      core_worker_client_pool_->GetOrConnect(state->candidate_address);

  if (recovery_succession_profiling_enabled_) {
    state->install_start_ns = RecoveryProfileNowNs();
  }

  candidate_client->InstallRecoveryHolder(
      std::move(prepared.request),
      [this, state](const Status &status,
                    rpc::InstallRecoveryHolderReply &&install_reply) mutable {
        HandleRecoveryHolderInstallResult(
            state, status, std::move(install_reply));
      });
}

void CoreWorker::DispatchRecoveryHolderInstallBatch(
    std::vector<PreparedRecoveryHolderInstall> prepared) {
  if (prepared.empty()) {
    return;
  }

  size_t begin = 0;
  while (begin < prepared.size()) {
    size_t end = begin;
    uint64_t bytes = 0;

    while (end < prepared.size() &&
           end - begin < kRecoveryInstallBatchMaxItems) {
      const uint64_t next_bytes =
          static_cast<uint64_t>(prepared[end].request.ByteSizeLong());

      if (end > begin && bytes + next_bytes > kRecoveryInstallBatchMaxBytes) {
        break;
      }

      bytes += next_bytes;
      ++end;
    }

    // Always make progress even when one TaskSpec itself exceeds the byte cap.
    if (end == begin) {
      ++end;
    }

    if (end - begin == 1) {
      DispatchRecoveryHolderInstall(std::move(prepared[begin]));
      begin = end;
      continue;
    }

    rpc::InstallRecoveryHolderBatchRequest batch_request;
    batch_request.mutable_requests()->Reserve(static_cast<int>(end - begin));

    std::vector<std::shared_ptr<PendingRecoveryHolderAdmission>> states;
    states.reserve(end - begin);

    for (size_t i = begin; i < end; ++i) {
      states.push_back(prepared[i].state);
      batch_request.add_requests()->Swap(&prepared[i].request);
    }

    const rpc::Address candidate_address = states.front()->candidate_address;
    auto candidate_client = core_worker_client_pool_->GetOrConnect(candidate_address);

    const uint64_t install_start_ns =
        recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;
    if (install_start_ns != 0) {
      for (const auto &state : states) {
        state->install_start_ns = install_start_ns;
      }
    }

    RAY_LOG(DEBUG) << "Patch 4E sending install batch with "
                   << states.size() << " logical installs and ~"
                   << bytes << " serialized request bytes";

    candidate_client->InstallRecoveryHolderBatch(
        std::move(batch_request),
        [this, states = std::move(states)](
            const Status &status,
            rpc::InstallRecoveryHolderBatchReply &&batch_reply) mutable {
          const bool shape_ok =
              status.ok() &&
              batch_reply.replies_size() == static_cast<int>(states.size());

          const Status item_status =
              shape_ok
                  ? Status::OK()
                  : (status.ok()
                         ? Status::IOError(
                               "InstallRecoveryHolderBatch reply size mismatch")
                         : status);

          for (size_t i = 0; i < states.size(); ++i) {
            rpc::InstallRecoveryHolderReply item_reply;
            if (shape_ok) {
              item_reply.CopyFrom(batch_reply.replies(static_cast<int>(i)));
            } else {
              item_reply.set_stored(false);
              item_reply.set_reservation_id(states[i]->reservation_id);
            }

            HandleRecoveryHolderInstallResult(
                states[i], item_status, std::move(item_reply));
          }
        });

    begin = end;
  }
}

void CoreWorker::HandleReportRecoveryCandidate(
    rpc::ReportRecoveryCandidateRequest request,
    rpc::ReportRecoveryCandidateReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto prepared = PrepareRecoveryCandidateAdmission(
      request, reply, std::move(send_reply_callback));
  if (prepared.has_value()) {
    DispatchRecoveryHolderInstall(std::move(prepared.value()));
  }
}

void CoreWorker::HandleReportRecoveryCandidateBatch(
    rpc::ReportRecoveryCandidateBatchRequest request,
    rpc::ReportRecoveryCandidateBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (reply == nullptr) {
    send_reply_callback(Status::Invalid("null recovery candidate batch reply"),
                        nullptr,
                        nullptr);
    return;
  }

  const int count = request.requests_size();
  if (count == 0) {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  reply->mutable_replies()->Reserve(count);
  std::vector<rpc::ReportRecoveryCandidateReply *> item_replies;
  item_replies.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    item_replies.push_back(reply->add_replies());
  }

  struct CompletionState {
    absl::Mutex mutex;
    int remaining ABSL_GUARDED_BY(mutex);
    bool sent ABSL_GUARDED_BY(mutex) = false;
    Status first_error ABSL_GUARDED_BY(mutex) = Status::OK();
    rpc::SendReplyCallback callback;

    CompletionState(int count, rpc::SendReplyCallback cb)
        : remaining(count), callback(std::move(cb)) {}
  };

  auto completion =
      std::make_shared<CompletionState>(count, std::move(send_reply_callback));

  auto item_done = [completion](const Status &status, auto, auto) mutable {
    bool finish = false;
    Status final_status = Status::OK();

    {
      absl::MutexLock lock(&completion->mutex);
      if (!status.ok() && completion->first_error.ok()) {
        completion->first_error = status;
      }

      --completion->remaining;
      RAY_CHECK_GE(completion->remaining, 0);

      if (completion->remaining == 0 && !completion->sent) {
        completion->sent = true;
        final_status = completion->first_error;
        finish = true;
      }
    }

    if (finish) {
      completion->callback(final_status, nullptr, nullptr);
    }
  };

  // A batch normally comes from one candidate worker and one owner, but group
  // by candidate worker defensively before issuing holder-install batches.
  std::map<std::string, std::vector<PreparedRecoveryHolderInstall>> install_groups;

  for (int i = 0; i < count; ++i) {
    auto prepared = PrepareRecoveryCandidateAdmission(
        request.requests(i), item_replies[static_cast<size_t>(i)], item_done);

    if (!prepared.has_value()) {
      continue;
    }

    const std::string candidate_worker_id =
        prepared->state->candidate_address.worker_id();
    install_groups[candidate_worker_id].push_back(std::move(prepared.value()));
  }

  for (auto &[candidate_worker_id, group] : install_groups) {
    static_cast<void>(candidate_worker_id);
    DispatchRecoveryHolderInstallBatch(std::move(group));
  }
}'''


CORE_INSTALL_AND_BATCH = r'''void CoreWorker::HandleInstallRecoveryHolder(
    rpc::InstallRecoveryHolderRequest request,
    rpc::InstallRecoveryHolderReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  reply->set_reservation_id(request.reservation_id());

  if (!recovery_succession_enabled_ ||
      recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    reply->set_stored(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const bool stored = recovery_succession_manager_->InstallRecoveryHolder(request);
  reply->set_stored(stored);

  if (stored) {
    RAY_LOG(INFO).WithField(TaskID::FromBinary(request.task_id()))
        << "Stored provisional recovery holder at rank "
        << request.proposed_rank();
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleInstallRecoveryHolderBatch(
    rpc::InstallRecoveryHolderBatchRequest request,
    rpc::InstallRecoveryHolderBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const int count = request.requests_size();
  reply->mutable_replies()->Reserve(count);

  const bool enabled =
      recovery_succession_enabled_ &&
      !recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr;

  for (int i = 0; i < count; ++i) {
    const rpc::InstallRecoveryHolderRequest &item_request = request.requests(i);
    rpc::InstallRecoveryHolderReply *item_reply = reply->add_replies();
    item_reply->set_reservation_id(item_request.reservation_id());

    const bool stored =
        enabled && recovery_succession_manager_->InstallRecoveryHolder(item_request);
    item_reply->set_stored(stored);

    if (stored) {
      RAY_LOG(DEBUG).WithField(TaskID::FromBinary(item_request.task_id()))
          << "Patch 4E batch stored provisional recovery holder at rank "
          << item_request.proposed_rank();
    }
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}'''


CORE_BATCH_SENDER = r'''void CoreWorker::QueueRecoveryCandidateReport(
    rpc::Address coordinator_address,
    rpc::ReportRecoveryCandidateRequest request) {
  if (request.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  // Preserve deterministic failure-injection semantics. A batch has one gRPC
  // status for all logical items, so the post-witness/pre-commit test continues
  // to use the original single-item RPC path.
  if (RayConfig::instance().recovery_succession_test_fail_after_witness_ack() ||
      coordinator_address.worker_id().empty()) {
    auto manager = recovery_succession_manager_;
    auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);
    client->ReportRecoveryCandidate(
        std::move(request),
        [manager, task_id](const Status &status,
                           rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (!status.ok()) {
            RAY_LOG(DEBUG).WithField(task_id)
                << "Recovery candidate report failed: " << status;
            return;
          }

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_id);
          }
        });
    return;
  }

  const std::string coordinator_worker_id = coordinator_address.worker_id();
  bool schedule_flush = false;

  {
    absl::MutexLock lock(&recovery_candidate_batch_mutex_);
    auto [it, inserted] = recovery_candidate_batch_queues_.try_emplace(
        coordinator_worker_id);
    RecoveryCandidateBatchQueue &queue = it->second;

    if (inserted) {
      queue.coordinator_address.CopyFrom(coordinator_address);
      schedule_flush = true;
    }

    PendingRecoveryCandidateReport pending;
    pending.task_id = task_id;
    pending.request.Swap(&request);
    queue.pending.push_back(std::move(pending));
  }

  if (schedule_flush) {
    io_service_.post(
        [this, coordinator_worker_id]() {
          FlushRecoveryCandidateReportBatch(coordinator_worker_id);
        },
        "CoreWorker.FlushRecoveryCandidateReportBatch",
        kRecoveryCandidateBatchDelayUs);
  }
}

void CoreWorker::FlushRecoveryCandidateReportBatch(
    const std::string &coordinator_worker_id) {
  rpc::Address coordinator_address;
  std::vector<PendingRecoveryCandidateReport> items;
  bool schedule_next = false;

  {
    absl::MutexLock lock(&recovery_candidate_batch_mutex_);
    const auto it = recovery_candidate_batch_queues_.find(coordinator_worker_id);
    if (it == recovery_candidate_batch_queues_.end()) {
      return;
    }

    RecoveryCandidateBatchQueue &queue = it->second;
    coordinator_address.CopyFrom(queue.coordinator_address);

    const size_t take =
        std::min(kRecoveryCandidateBatchMaxItems, queue.pending.size());
    items.reserve(take);
    for (size_t i = 0; i < take; ++i) {
      items.push_back(std::move(queue.pending.front()));
      queue.pending.pop_front();
    }

    if (queue.pending.empty()) {
      recovery_candidate_batch_queues_.erase(it);
    } else {
      schedule_next = true;
    }
  }

  if (schedule_next) {
    // The first window already performed the coalescing. Drain a backlog on
    // successive event-loop turns without adding another fixed delay.
    io_service_.post(
        [this, coordinator_worker_id]() {
          FlushRecoveryCandidateReportBatch(coordinator_worker_id);
        },
        "CoreWorker.FlushRecoveryCandidateReportBatch");
  }

  if (items.empty()) {
    return;
  }

  auto manager = recovery_succession_manager_;
  auto client = core_worker_client_pool_->GetOrConnect(coordinator_address);

  if (items.size() == 1) {
    TaskID task_id = items.front().task_id;
    rpc::ReportRecoveryCandidateRequest single_request;
    single_request.Swap(&items.front().request);

    client->ReportRecoveryCandidate(
        std::move(single_request),
        [manager, task_id](const Status &status,
                           rpc::ReportRecoveryCandidateReply &&candidate_reply) {
          if (!status.ok()) {
            RAY_LOG(DEBUG).WithField(task_id)
                << "Recovery candidate report failed: " << status;
            return;
          }

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_id);
          }
        });
    return;
  }

  rpc::ReportRecoveryCandidateBatchRequest batch_request;
  batch_request.mutable_requests()->Reserve(static_cast<int>(items.size()));
  std::vector<TaskID> task_ids;
  task_ids.reserve(items.size());

  for (auto &item : items) {
    task_ids.push_back(item.task_id);
    batch_request.add_requests()->Swap(&item.request);
  }

  RAY_LOG(DEBUG) << "Patch 4E sending candidate batch with "
                 << task_ids.size() << " logical reports";

  client->ReportRecoveryCandidateBatch(
      std::move(batch_request),
      [manager, task_ids = std::move(task_ids)](
          const Status &status,
          rpc::ReportRecoveryCandidateBatchReply &&batch_reply) mutable {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Recovery candidate batch failed: " << status;
          return;
        }

        const int reply_count = batch_reply.replies_size();
        const size_t matched =
            std::min(task_ids.size(), static_cast<size_t>(reply_count));

        for (size_t i = 0; i < matched; ++i) {
          const auto &candidate_reply = batch_reply.replies(static_cast<int>(i));

          if (candidate_reply.has_latest_manifest()) {
            manager->ApplyCommittedManifest(candidate_reply.latest_manifest());
          }

          if (candidate_reply.result() == rpc::ReportRecoveryCandidateReply::NO_SLOT ||
              candidate_reply.result() ==
                  rpc::ReportRecoveryCandidateReply::STALE_MANIFEST) {
            manager->AllowCandidateReportRetry(task_ids[i]);
          }
        }

        if (matched != task_ids.size()) {
          RAY_LOG(WARNING)
              << "Patch 4E candidate batch reply size mismatch: expected "
              << task_ids.size() << ", got " << reply_count;

          // Missing logical replies must not permanently suppress future
          // candidate reports from those tasks.
          for (size_t i = matched; i < task_ids.size(); ++i) {
            manager->AllowCandidateReportRetry(task_ids[i]);
          }
        }
      });
}'''


def patch_core_cc(text: str) -> str:
    if PATCH4E_MARKER in text:
        return text

    if PATCH4D_MARKER not in text:
        fail("core_worker.cc is missing Patch 4D")

    unsafe = "[this, manager, state = std::move(state), witness_publish_start_ns]"
    safe = "[this, manager, state, witness_publish_start_ns]"
    if unsafe in text:
        fail(
            "The Patch-4D SIGSEGV hotfix is not applied. Run fix_patch4d_sigsegv.py first."
        )
    if safe not in text:
        fail("Could not verify the Patch-4D SIGSEGV hotfix capture")

    text = replace_once(
        text,
        "constexpr size_t kDefaultSerializationCacheCap = 500;",
        '''constexpr size_t kDefaultSerializationCacheCap = 500;

// Patch 4E physical batching knobs. These alter only transport coalescing,
// never logical holder/witness semantics.
constexpr size_t kRecoveryCandidateBatchMaxItems = 64;
constexpr int64_t kRecoveryCandidateBatchDelayUs = 500;
constexpr size_t kRecoveryInstallBatchMaxItems = 64;
constexpr uint64_t kRecoveryInstallBatchMaxBytes = 4ULL * 1024ULL * 1024ULL;''',
        "core_worker.cc 4E constants",
    )

    old_push_loop = '''    for (auto &candidate_report : candidate_reports) {
      const TaskID reported_task_id =
          TaskID::FromBinary(candidate_report.request.task_id());

      auto candidate_client =
          core_worker_client_pool_->GetOrConnect(candidate_report.coordinator_address);

      candidate_client->ReportRecoveryCandidate(
          std::move(candidate_report.request),
          [manager = recovery_succession_manager_, reported_task_id](
              const Status &status, rpc::ReportRecoveryCandidateReply &&candidate_reply) {
            if (!status.ok()) {
              RAY_LOG(DEBUG).WithField(reported_task_id) << "Recovery candidate report "
                                                            "failed: "
                                                         << status;
              return;
            }

            if (candidate_reply.has_latest_manifest()) {
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
          });
    }
'''

    new_push_loop = '''    for (auto &candidate_report : candidate_reports) {
      // Patch 4E: queue by coordinator and physically coalesce independent
      // logical reports across tasks. The task itself is not blocked on this RPC.
      QueueRecoveryCandidateReport(
          std::move(candidate_report.coordinator_address),
          std::move(candidate_report.request));
    }
'''
    text = replace_once(text, old_push_loop, new_push_loop, "core_worker.cc candidate sender batching")

    # Insert the sender-side queue/flush methods immediately before the owner-side
    # ReportRecoveryCandidate handler, then replace that handler with the refactored
    # single + batch implementation.
    marker = "void CoreWorker::HandleReportRecoveryCandidate("
    pos = text.find(marker)
    if pos < 0:
        fail("core_worker.cc HandleReportRecoveryCandidate not found")
    text = text[:pos] + CORE_BATCH_SENDER.rstrip() + "\n\n" + text[pos:]

    text = replace_cpp_function(
        text,
        "void CoreWorker::HandleReportRecoveryCandidate(",
        CORE_REPORT_AND_BATCH,
        "core_worker.cc report candidate 4E",
    )

    text = replace_cpp_function(
        text,
        "void CoreWorker::HandleInstallRecoveryHolder(",
        CORE_INSTALL_AND_BATCH,
        "core_worker.cc install holder 4E",
    )

    text = text.replace(
        f"// {PATCH4D_MARKER}.\n",
        f"// {PATCH4D_MARKER}.\n// {PATCH4E_MARKER}.\n",
        1,
    )
    return text


def validate_patched(texts: dict[Path, str]) -> None:
    required = {
        PROTO: [
            "message ReportRecoveryCandidateBatchRequest",
            "message InstallRecoveryHolderBatchRequest",
            "rpc ReportRecoveryCandidateBatch",
            "rpc InstallRecoveryHolderBatch",
        ],
        CLIENT_IFACE: ["ReportRecoveryCandidateBatch", "InstallRecoveryHolderBatch"],
        CLIENT_H: ["ReportRecoveryCandidateBatch", "InstallRecoveryHolderBatch"],
        FAKE_CLIENT_H: ["ReportRecoveryCandidateBatch", "InstallRecoveryHolderBatch"],
        PROXY_H: [
            "RAY_CORE_WORKER_RPC_PROXY(ReportRecoveryCandidateBatch)",
            "RAY_CORE_WORKER_RPC_PROXY(InstallRecoveryHolderBatch)",
        ],
        CORE_H: [
            "HandleReportRecoveryCandidateBatch",
            "HandleInstallRecoveryHolderBatch",
            "RecoveryCandidateBatchQueue",
            "PreparedRecoveryHolderInstall",
        ],
        CORE_CC: [
            "kRecoveryCandidateBatchDelayUs = 500",
            "QueueRecoveryCandidateReport(",
            "FlushRecoveryCandidateReportBatch(",
            "HandleReportRecoveryCandidateBatch(",
            "HandleInstallRecoveryHolderBatch(",
            "DispatchRecoveryHolderInstallBatch(",
        ],
    }

    for path, needles in required.items():
        for needle in needles:
            if needle not in texts[path]:
                fail(f"post-patch validation failed: {needle!r} missing from {path}")

    # Patch 4B-2 and the 4D hotfix must still be present.
    core = texts[CORE_CC]
    if "[this, manager, state, witness_publish_start_ns]" not in core:
        fail("post-patch validation lost the Patch-4D SIGSEGV hotfix")

    # Successful admission must still be reply-based; an explicit commit RPC is
    # allowed only in the Patch-4D rollback helper.
    if "Patch 4B-2 remains in force" not in core:
        fail("post-patch validation could not find Patch 4B-2 success-path invariant")


def check_extra_interface_implementers() -> None:
    try:
        proc = subprocess.run(
            ["git", "grep", "-l", "public CoreWorkerClientInterface", "--", "*.h"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return

    if proc.returncode not in (0, 1):
        return

    expected = {
        "src/ray/core_worker_rpc_client/core_worker_client.h",
        "src/ray/core_worker_rpc_client/fake_core_worker_client.h",
    }
    found = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    extra = sorted(found - expected)
    if extra:
        print("WARNING: additional direct CoreWorkerClientInterface implementers found:")
        for path in extra:
            print(f"  {path}")
        print("They may also need no-op implementations of the two new batch methods.")


def main() -> None:
    originals = {path: read_required(path) for path in FILES}

    # Refuse partial reapplication.
    marked = [PATCH4E_MARKER in originals[path] for path in FILES]
    if all(marked):
        print("Patch 4E is already applied to all expected files; nothing to do.")
        return
    if any(marked):
        fail("Patch 4E marker found in only some files; refusing a partial re-apply.")

    if PATCH4D_MARKER not in originals[CORE_H] or PATCH4D_MARKER not in originals[CORE_CC]:
        fail("Patch 4D must be applied before Patch 4E.")

    if "state = std::move(state), witness_publish_start_ns" in originals[CORE_CC]:
        fail("Apply the Patch-4D SIGSEGV hotfix before Patch 4E.")

    check_extra_interface_implementers()

    patched = dict(originals)
    patched[PROTO] = patch_proto(patched[PROTO])
    patched[CLIENT_IFACE] = patch_client_interface(patched[CLIENT_IFACE])
    patched[CLIENT_H] = patch_client_h(patched[CLIENT_H])
    patched[FAKE_CLIENT_H] = patch_fake_client(patched[FAKE_CLIENT_H])
    patched[PROXY_H] = patch_proxy(patched[PROXY_H])
    patched[CORE_H] = patch_core_h(patched[CORE_H])
    patched[CORE_CC] = patch_core_cc(patched[CORE_CC])

    validate_patched(patched)

    # Only write after every transformation and validation succeeded.
    for path in FILES:
        backup(path)
    for path in FILES:
        path.write_text(patched[path])

    print("Applied Patch 4E successfully.")
    print()
    print("Modified files:")
    for path in FILES:
        print(f"  {path.relative_to(ROOT)}")
    print()
    print("Backups use the .pre4e suffix.")
    print()
    print("Patch 4E transport settings:")
    print("  candidate microbatch delay: 500 us")
    print("  candidate batch max items: 64")
    print("  install batch max items: 64")
    print("  install batch max bytes: 4 MiB")
    print()
    print("Next:")
    print("  git diff --check")
    print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")
    print("  rerun Benchmark 09 with the exact post-4D command")


if __name__ == "__main__":
    main()
