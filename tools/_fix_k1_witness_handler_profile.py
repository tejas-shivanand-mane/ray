#!/usr/bin/env python3
from pathlib import Path

path = Path("src/ray/raylet/node_manager.cc")
text = path.read_text()

def one(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old[:120]!r}")
    text = text.replace(old, new, 1)

one(
    '#include <google/protobuf/util/message_differencer.h>\n\n#include "absl/cleanup/cleanup.h"\n#include "absl/strings/str_format.h"\n',
    '#include <google/protobuf/util/message_differencer.h>\n\n#include "absl/strings/str_format.h"\n',
)

one(
    '''  const bool profile_witness =\n      RayConfig::instance().enable_recovery_succession_profiling();\n  const uint64_t handler_start_ns =\n      profile_witness ? RecoveryWitnessProfileNowNs() : 0;\n  absl::Cleanup record_handler_time = [reply, handler_start_ns]() {\n    if (handler_start_ns != 0) {\n      reply->set_witness_handler_time_ns(\n          RecoveryWitnessProfileNowNs() - handler_start_ns);\n    }\n  };\n\n  if (!RayConfig::instance().enable_recovery_succession()) {\n''',
    '''  const bool profile_witness =\n      RayConfig::instance().enable_recovery_succession_profiling();\n\n  if (!RayConfig::instance().enable_recovery_succession()) {\n''',
)

one(
    '''    HandleUpdateRecoveryWitness(\n        std::move(item_request),\n        item_reply,\n        [](Status, std::function<void()>, std::function<void()>) {});\n    if (item_start_ns != 0) {\n      item_reply->set_witness_batch_queue_time_ns(\n          item_start_ns - batch_start_ns);\n    }\n''',
    '''    HandleUpdateRecoveryWitness(\n        std::move(item_request),\n        item_reply,\n        [](Status, std::function<void()>, std::function<void()>) {});\n    if (item_start_ns != 0) {\n      const uint64_t item_end_ns = RecoveryWitnessProfileNowNs();\n      item_reply->set_witness_handler_time_ns(item_end_ns - item_start_ns);\n      item_reply->set_witness_batch_queue_time_ns(\n          item_start_ns - batch_start_ns);\n    }\n''',
)

path.write_text(text)
print("Fixed witness handler profiling placement.")
