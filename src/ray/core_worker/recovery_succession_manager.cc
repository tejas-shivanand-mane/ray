#include "ray/core_worker/recovery_succession_manager.h"

namespace ray {
namespace core {

bool RecoverySuccessionManager::IsEligibleTask(
    const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK &&
         !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() &&
         task_spec.max_retries() != 0;
}

// Other RecoverySuccessionManager methods...

bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id,
    rpc::RecoveryObjectMetadata *metadata) const {
  absl::MutexLock lock(&mutex_);

  const auto it = object_recovery_metadata_.find(object_id);
  if (it == object_recovery_metadata_.end()) {
    return false;
  }

  metadata->CopyFrom(it->second);
  return true;
}



void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr) {
    return;
  }

  for (auto &arg : *task_spec->mutable_args()) {
    // Dependency passed directly by ObjectRef.
    if (arg.has_object_ref()) {
      auto *object_ref = arg.mutable_object_ref();
      const ObjectID object_id =
          ObjectID::FromBinary(object_ref->object_id());

      PopulateRecoveryMetadata(
          object_id,
          object_ref->mutable_recovery_metadata());
    }

    // ObjectRefs nested inside a pass-by-value argument.
    for (auto &nested_ref :
         *arg.mutable_nested_inlined_refs()) {
      const ObjectID nested_id =
          ObjectID::FromBinary(nested_ref.object_id());

      PopulateRecoveryMetadata(
          nested_id,
          nested_ref.mutable_recovery_metadata());
    }
  }
}




}  // namespace core
}  // namespace ray