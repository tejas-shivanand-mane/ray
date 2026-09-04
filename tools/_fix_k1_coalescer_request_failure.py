#!/usr/bin/env python3
from pathlib import Path
p = Path("src/ray/rpc/grpc_client.h")
s = p.read_text()
old = '''      client_call_manager_.GetMainService().post(\n          [callback]() {\n            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),\n                     Reply());\n          },\n          "RpcChaos");\n    } else if (failure == testing::RpcFailure::Response) {\n'''
new = '''      client_call_manager_.GetMainService().post(\n          [callback]() {\n            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),\n                     Reply());\n          },\n          "RpcChaos");\n      // There is no real CompletionQueue event for a request-side injected\n      // failure. If this call owns a transport-lane CQ hook, advance that lane\n      // immediately after handing the logical failure callback to the main loop.\n      if (completion_queue_hook) {\n        completion_queue_hook();\n      }\n    } else if (failure == testing::RpcFailure::Response) {\n'''
if s.count(old) != 1:
    raise RuntimeError(f"expected one request-failure block, found {s.count(old)}")
p.write_text(s.replace(old, new, 1))
