from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))

replace_once(
    "src/ray/core_worker/core_worker.h",
    '''  const bool recovery_succession_profiling_enabled_;\n\n  // Diagnostic-only normal-task submit-stage timers. These are CoreWorker-local\n''',
    '''  const bool recovery_succession_profiling_enabled_;\n\n  // Unlike the recovery profiler above, submit-stage profiling is also useful\n  // for the recovery-disabled control. It follows the profiling config directly\n  // and never enables any recovery behavior.\n  const bool normal_submit_stage_profiling_enabled_;\n\n  // Diagnostic-only normal-task submit-stage timers. These are CoreWorker-local\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''      recovery_succession_profiling_enabled_(\n          recovery_succession_enabled_ &&\n          RayConfig::instance().enable_recovery_succession_profiling()),\n      recovery_succession_manager_(nullptr),\n''',
    '''      recovery_succession_profiling_enabled_(\n          recovery_succession_enabled_ &&\n          RayConfig::instance().enable_recovery_succession_profiling()),\n      normal_submit_stage_profiling_enabled_(\n          RayConfig::instance().enable_recovery_succession_profiling()),\n      recovery_succession_manager_(nullptr),\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  const bool profile_normal_submit = recovery_succession_profiling_enabled_;\n''',
    '''  const bool profile_normal_submit = normal_submit_stage_profiling_enabled_;\n''')

print("fixed disabled-control submit-stage profiling")
