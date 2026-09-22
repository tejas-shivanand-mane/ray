"""Execute a script normally, preserving argv and sibling imports."""

import json
import os
import runpy
import sys
from pathlib import Path


if __name__ == "__main__":
    script = Path(sys.argv[1]).resolve()
    sys.argv = [str(script), *sys.argv[2:]]
    sys.path.insert(0, str(script.parent))
    runpy.run_path(str(script), run_name="__main__")
    # Optional no-fault measurement audit, outside benchmark timers. No polling
    # actor/callback is installed in either arm of the performance comparison.
    audit_path = os.environ.get("RAY_RECOVERY_STATE_REPORT")
    if audit_path:
        import ray
        from ray.data import DataContext
        from ray.experimental.recovery import system_config

        if not ray.is_initialized():
            raise RuntimeError("Cannot audit recovery settings after ray.shutdown()")
        context = DataContext.get_current()
        native = ray._private.state.state.get_system_config()
        runtime = ray.get_runtime_context()
        Path(audit_path).write_text(json.dumps({
            "native_settings": {key: native.get(key) for key in system_config()},
            "data_recovery_enabled": context.enable_fixed_r_task_recovery,
            "launcher_observer_enabled": any(
                getattr(cls, "_fixed_r_launcher_observer", False)
                for cls in context.custom_execution_callback_classes
            ),
            "driver_node_id": runtime.get_node_id(),
            "ray_version": ray.__version__, "ray_commit": ray.__commit__,
        }, indent=2))
