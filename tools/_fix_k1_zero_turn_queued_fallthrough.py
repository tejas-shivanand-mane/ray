#!/usr/bin/env python3
from pathlib import Path

path = Path("src/ray/raylet_rpc_client/raylet_client.cc")
text = path.read_text()
old = '''    if (use_k1_zero_turn_microbatch) {
      state->pending.emplace_back(std::move(item));
      if (!state->in_flight && !state->flush_scheduled) {
        state->flush_scheduled = true;
        schedule_flush = true;
      }
    } else {
'''
new = '''    if (use_k1_zero_turn_microbatch) {
      state->pending.emplace_back(std::move(item));
      if (!state->in_flight && !state->flush_scheduled) {
        state->flush_scheduled = true;
        schedule_flush = true;
      } else {
        // An in-flight batch completion or an already-posted zero-turn flush
        // owns this queued update. Do not fall through to the legacy immediate
        // dispatch tail, where no local batch exists.
        return;
      }
    } else {
'''
count = text.count(old)
if count != 1:
    raise RuntimeError(f"expected exactly one target block, found {count}")
path.write_text(text.replace(old, new, 1))
print("Fixed queued K1 witness update fallthrough.")
