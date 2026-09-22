"""Opt-in Fixed-R recovery for supported Data pipelines and Train ingestion.

Call enable() after ray.init() and before constructing Datasets or Trainers.
The driver, required workers and GCS storage must survive head replacement.
This API does not recover lost actors, model state or arbitrary Ray programs.
"""

import math
import os

import ray

__all__ = ["enable", "disable", "system_config"]


def system_config():
    """Return native settings to apply when starting a recovery-enabled cluster.

    Merge these into the head's ``--system-config`` JSON (or ``_system_config``
    for a local Cluster). Persistent GCS storage and head supervision are
    deployment responsibilities, configured separately. The experimental
    ``ray.experimental.recovery._head_supervisor`` module provides a bounded
    one-head replacement workflow with external Redis and deployment hooks.
    """
    return {
        "enable_recovery_succession": True,
        "enable_recovery_witness_holder_baseline": True,
        "enable_recovery_streaming_fixed_r": True,
        "recovery_succession_target_holder_count": 2,
        "recovery_succession_witness_count": 2,
        "recovery_frontier_group_size": 1,
        "recovery_baseline_perf_protect_every_n": 1,
    }


def enable(*, timeout_s=120, context=None):
    """Enable streaming recovery before constructing new Datasets/Trainers.

    Uses the current DataContext unless an explicit context is supplied. Ray
    Data and Train propagate that context to their execution coordinators.
    Requires a connected non-head driver and at least two surviving CPU nodes.
    Unsupported plans fail before dispatch; actor maps require actor survival.
    Returns the configured context. Existing Datasets keep their own snapshots.
    """
    from ray.data import DataContext
    from ray.data._internal.execution.streaming_recovery import CONFIG_KEY, get_config

    if not ray.is_initialized():
        raise RuntimeError("Call ray.init(address=...) before recovery.enable()")
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("Recovery timeout_s must be positive and finite")
    settings = ray._private.state.state.get_system_config()
    required = system_config()
    mismatches = [key for key, value in required.items()
                  if settings.get(key, value if not key.startswith("enable_") else False) != value]
    if mismatches:
        raise ValueError(
            "Start the cluster with recovery.system_config(); incompatible settings: "
            + ", ".join(mismatches)
        )
    target = context if context is not None else DataContext.get_current()
    candidate = target.copy()
    # Re-enabling a configured context is idempotent, including after head
    # replacement. A new cluster/session requires a fresh context or disable().
    cached = candidate.get_config(CONFIG_KEY)
    if cached is not None and (
        not cached.automatic_outputs or not cached.dynamic_task_outputs or cached.mode != "fixed_r"
    ):
        raise ValueError("Clear the explicit experiment configuration with recovery.disable() first")
    if cached is not None and cached.timeout_s != timeout_s:
        raise ValueError("Disable recovery before changing the cached timeout")
    candidate.enable_fixed_r_task_recovery = True
    candidate.fixed_r_task_recovery_output_mode = "streaming"
    candidate.fixed_r_task_recovery_timeout_s = timeout_s
    get_config(candidate)
    # Commit only after validation, preserving callers' current-context identity.
    target.__dict__.update(candidate.__dict__)
    return target


def disable(*, context=None):
    """Disable recovery for subsequently constructed Datasets/Trainers.

    Clears cached/explicit configuration and restores the prior eager-free
    setting. Does not mutate already-created Datasets or interrupt running work.
    Native cluster settings are fixed at startup; use a fresh cluster with them
    disabled for a complete native disabled-overhead baseline.
    """
    from ray.data import DataContext
    from ray.data._internal.execution.streaming_recovery import clear_config

    target = context if context is not None else DataContext.get_current()
    target.enable_fixed_r_task_recovery = False
    clear_config(target)
    target.custom_execution_callback_classes = [
        cls for cls in target.custom_execution_callback_classes
        if not getattr(cls, "_fixed_r_launcher_observer", False)
    ]
    return target


def _enable_from_environment():
    """Called only by the opt-in ray.init hook used by the launcher."""
    context = enable(timeout_s=float(os.environ.get("RAY_RECOVERY_TIMEOUT_S", "120")))
    monitor = os.environ.get("RAY_RECOVERY_MONITOR")
    if monitor:
        from ray.experimental.recovery._observe import MONITOR_KEY, TRIGGER_KEY, Observe

        trigger = os.environ.get("RAY_RECOVERY_FAILURE_TRIGGER", "output")
        if trigger not in ("output", "task-submission"):
            raise ValueError("Unknown RAY_RECOVERY_FAILURE_TRIGGER")
        # Store only a module-level class and plain settings in DataContext so
        # normal pickle (including Dataset schemas) remains supported.
        context.set_config(MONITOR_KEY, monitor)
        context.set_config(TRIGGER_KEY, trigger)
        if Observe not in context.custom_execution_callback_classes:
            context.custom_execution_callback_classes.append(Observe)
