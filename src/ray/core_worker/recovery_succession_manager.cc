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

}  // namespace core
}  // namespace ray