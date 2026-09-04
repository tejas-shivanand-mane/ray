#!/usr/bin/env python3
from pathlib import Path

p = Path("tools/_apply_k1_cq_witness_drain.py")
text = p.read_text()

needle = '''def replace_one(path: str, old: str, new: str) -> None:\n    p = Path(path)\n    text = p.read_text()\n    count = text.count(old)\n    if count != 1:\n        raise RuntimeError(f"{path}: expected one occurrence, found {count}: {old[:120]!r}")\n    p.write_text(text.replace(old, new, 1))\n'''
replacement = needle + '''\n\ndef replace_first(path: str, old: str, new: str) -> None:\n    p = Path(path)\n    text = p.read_text()\n    if old not in text:\n        raise RuntimeError(f"{path}: missing expected first occurrence: {old[:120]!r}")\n    p.write_text(text.replace(old, new, 1))\n'''
if text.count(needle) != 1:
    raise RuntimeError("could not insert replace_first helper")
text = text.replace(needle, replacement, 1)

old_call = '''replace_one(\n    "src/ray/rpc/grpc_client.h",\n    \'\'\'      client_call_manager_.GetMainService().post(\\n          [callback]() {\\n            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),\\n                     Reply());\\n          },\\n          "RpcChaos");\\n\'\'\',\n'''
new_call = old_call.replace("replace_one(", "replace_first(", 1)
if text.count(old_call) != 1:
    raise RuntimeError(f"expected one request-failure applicator call, found {text.count(old_call)}")
text = text.replace(old_call, new_call, 1)
p.write_text(text)
print("Updated CQ-drain applicator to replace only the request-failure post block.")
