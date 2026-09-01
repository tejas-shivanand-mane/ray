// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "ray/common/id.h"
#include "ray/core_worker/reference_counter_interface.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker_rpc_client/core_worker_client_pool.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

using ReportLocalityDataCallback =
    std::function<void(const ObjectID &, const absl::flat_hash_set<NodeID> &, uint64_t)>;

using RecoveryMetadataCallback =
    std::function<void(const ObjectID &, const rpc::RecoveryObjectMetadata &)>;

using RecoveryReentryCallback =
    std::function<void(const ObjectID &, std::function<void(bool)>)>;

// Resolve values for futures that were given to us before the value
// was available. This class is thread-safe.
class FutureResolver {
 public:
  FutureResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                 std::shared_ptr<ReferenceCounterInterface> ref_counter,
                 ReportLocalityDataCallback report_locality_data_callback,
                 std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
                 rpc::Address rpc_address)
      : in_memory_store_(std::move(store)),
        reference_counter_(std::move(ref_counter)),
        report_locality_data_callback_(std::move(report_locality_data_callback)),
        owner_clients_(std::move(core_worker_client_pool)),
        rpc_address_(std::move(rpc_address)) {}

  /// Installs a callback for recovery metadata received from owners.
  ///
  /// This is configured once during CoreWorker initialization.
  void SetRecoveryMetadataCallback(RecoveryMetadataCallback callback) {
    recovery_metadata_callback_ = std::move(callback);
  }

  /// Installs the Fixed-R recovery re-entry hook.
  ///
  /// FutureResolver intentionally does not depend on CoreWorker. The owning
  /// CoreWorkerProcess installs this callback only for the witness-holder
  /// baseline, keeping the dependency direction CoreWorker -> FutureResolver.
  void SetRecoveryReentryCallback(RecoveryReentryCallback callback) {
    recovery_reentry_callback_ = std::move(callback);
  }

  /// Resolve the value for a future. This will periodically contact the given
  /// owner until the owner dies or the owner has finished creating the object.
  /// In either case, this will put an OBJECT_IN_PLASMA error as the future's
  /// value.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] owner_address The address of the task or actor that owns the
  /// future.
  void ResolveFutureAsync(const ObjectID &object_id, const rpc::Address &owner_address);

  /// Process a resolved future. This can be used if we already have the objec
  /// status and don't need to ask the owner for it right away.
  ///
  /// \param[in] object_id The ID of the future to resolve.
  /// \param[in] status Any error code from the owner obtaining the object status.
  /// \param[in] object_status The object status.
  void ProcessResolvedObject(const ObjectID &object_id,
                             const rpc::Address &owner_address,
                             const Status &status,
                             const rpc::GetObjectStatusReply &object_status);

 private:
  /// Record the first owner contacted for an unresolved ObjectID. If a later
  /// ResolveFutureAsync call uses a different owner, that owner is a recovery
  /// successor. This lets an already-blocked ray.get distinguish failure of the
  /// original owner from failure of an acting recovery owner without changing
  /// the ordinary owner-failure path.
  bool RecordAndIsRecoverySuccessor(const ObjectID &object_id,
                                    const rpc::Address &owner_address);

  /// Forget owner-transition state once resolution is terminal or this worker
  /// itself becomes the acting recovery owner.
  void ClearRecoveryOwnerTracking(const ObjectID &object_id);

  /// Used to store values of resolved futures.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used to record nested ObjectRefs of resolved futures.
  std::shared_ptr<ReferenceCounterInterface> reference_counter_;

  /// Used to report locality data received during future resolution.
  ReportLocalityDataCallback report_locality_data_callback_;

  /// Pool of owner core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> owner_clients_;

  /// Address of our RPC server. Used to notify borrowed objects' owners of our
  /// address, so the owner can contact us to ask when our reference to the
  /// object has gone out of scope.
  rpc::Address rpc_address_;

  /// Called when GetObjectStatus returns recovery metadata.
  RecoveryMetadataCallback recovery_metadata_callback_;

  /// Installed only for Fixed-R. Its presence also gates all owner-transition
  /// bookkeeping, so disabled and adaptive Succession modes pay no map/mutex cost.
  RecoveryReentryCallback recovery_reentry_callback_;

  /// Original owner observed by FutureResolver for each unresolved object.
  /// Binary strings keep this cold-path state independent of extra hash/Bazel
  /// dependencies in the standalone future_resolver target.
  std::mutex recovery_owner_mutex_;
  std::unordered_map<std::string, std::string> initial_owner_by_object_;

  /// Suppresses duplicate re-entry while one acting-owner recovery attempt is
  /// already in flight for this ObjectID.
  std::unordered_set<std::string> recovery_reentry_in_flight_;
};

}  // namespace core
}  // namespace ray
