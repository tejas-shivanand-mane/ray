#!/usr/bin/env python3
"""Extend the applied H1 K=1 piggyback patch to two transient piggybacks.

Run this only after apply_h1_transient_piggyback.py has already patched the local
recovery_succession_manager.cc and that version has been benchmarked.

The first two downstream metadata builds may carry the producer replay TaskSpec.
The existing first_holder_piggyback_sent bit means "one piggyback has already
been emitted": after the first send the transient recipe is retained, and after
the second send it is released. Candidate admission and witness confirmation are
unchanged; a piggyback holder is still provisional until its witness-backed
manifest is confirmed.

This is a K=1 performance experiment. It does not alter Fixed-R, Frontier,
certificate admission, target R/W, or recovery/replay authorization rules.
"""
from __future__ import annotations

from pathlib import Path

PATH = Path("src/ray/core_worker/recovery_succession_manager.cc")

OLD = '''        if (!state.first_holder_piggyback_sent &&
            state.manifest_committed &&
            !state.manifest.tombstoned() &&
            !state.manifest.frozen() &&
            state.manifest.succession_size() == 1 &&
            state.manifest.task_id() == source->task_id() &&
            state.task_spec.has_value() &&
            state.task_spec->task_id() == source->task_id()) {
          const auto piggyback_start = std::chrono::steady_clock::now();

          rpc::TaskSpec piggyback_task_spec;
          piggyback_task_spec.CopyFrom(state.task_spec.value());
          ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);
          piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(state.manifest);

          std::string serialized_task_spec;
          if (piggyback_task_spec.SerializeToString(&serialized_task_spec) &&
              !serialized_task_spec.empty()) {
            out->set_first_holder_task_spec(serialized_task_spec);
            state.first_holder_piggyback_sent = true;
            state.task_spec.reset();

            if (profiling_enabled_) {
'''

NEW = '''        if (state.manifest_committed &&
            !state.manifest.tombstoned() &&
            !state.manifest.frozen() &&
            state.manifest.task_id() == source->task_id() &&
            state.task_spec.has_value() &&
            state.task_spec->task_id() == source->task_id()) {
          const auto piggyback_start = std::chrono::steady_clock::now();

          rpc::TaskSpec piggyback_task_spec;
          piggyback_task_spec.CopyFrom(state.task_spec.value());
          ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);
          piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(state.manifest);

          std::string serialized_task_spec;
          if (piggyback_task_spec.SerializeToString(&serialized_task_spec) &&
              !serialized_task_spec.empty()) {
            out->set_first_holder_task_spec(serialized_task_spec);

            // first_holder_piggyback_sent is reused as a one-bit two-shot
            // counter. Keep the transient recipe after the first piggyback so
            // one more borrower can receive it; release it after the second.
            if (state.first_holder_piggyback_sent) {
              state.task_spec.reset();
            } else {
              state.first_holder_piggyback_sent = true;
            }

            if (profiling_enabled_) {
'''

OLD_COMMENT = '''    // Ordinary adaptive K=1 H1 fast path. Lazy activation staged one sanitized
    // producer TaskSpec in the existing task state. Exactly one downstream
    // metadata build claims it under mutex_, transports it in the already
    // supported Patch-4F field, then releases the owner-side duplicate. The
    // receiver remains provisional and must still verify witness durability.
'''

NEW_COMMENT = '''    // Ordinary adaptive K=1 holder fast path. Lazy activation staged one
    // sanitized producer TaskSpec in the existing task state. The first two
    // downstream metadata builds may transport it in the already-supported
    // Patch-4F field; the duplicate is released after the second send. Every
    // receiver remains provisional and must still verify witness durability.
'''


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def main() -> None:
    text = PATH.read_text()
    text = replace_once(text, OLD_COMMENT, NEW_COMMENT, "H1 fast-path comment")
    text = replace_once(text, OLD, NEW, "H1 one-shot transport block")
    PATH.write_text(text)
    print(f"Extended transient piggyback to two sends in {PATH}")


if __name__ == "__main__":
    main()
