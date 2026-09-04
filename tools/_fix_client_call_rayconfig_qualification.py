#!/usr/bin/env python3
from pathlib import Path

p = Path("src/ray/rpc/client_call.h")
text = p.read_text()
old = "if (RayConfig::instance().enable_recovery_succession_profiling()) {"
new = "if (::RayConfig::instance().enable_recovery_succession_profiling()) {"
count = text.count(old)
if count != 1:
    raise RuntimeError(f"expected exactly one unqualified RayConfig profiling call, found {count}")
text = text.replace(old, new, 1)
p.write_text(text)
print("Qualified recovery profiling config as ::RayConfig::instance().")
