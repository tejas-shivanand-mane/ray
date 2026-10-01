from contextlib import contextmanager
from dataclasses import dataclass
import math

import xgboost
from packaging.version import Version

from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.xgboost.config import XGBoostConfig as XGBoostConfigV1


@dataclass
class XGBoostConfig(XGBoostConfigV1):
    """XGBoost backend with optional CPU worker reuse on a training retry.

    ``selective_recovery`` preserves quiescent healthy actors and reruns the
    training function on all ranks with the latest Ray Train checkpoint.
    Training code must restore that checkpoint and rebuild its DMatrix. Use
    ``get_cached_input`` with immutable, fixed rank partitions to retain input.
    Ray Data shards are recreated on retry; streaming iterator state is not retained.
    Elastic world sizes and GPUs are excluded.
    FailureConfig still controls retries. Unsafe reuse falls back to a full
    group restart; it is never reported as selective recovery.
    """

    selective_recovery: bool = False
    recovery_timeout_s: float = 20.0

    def __post_init__(self):
        if not math.isfinite(self.recovery_timeout_s) or self.recovery_timeout_s <= 0:
            raise ValueError("recovery_timeout_s must be finite and positive")
        if self.selective_recovery and Version(xgboost.__version__) < Version("2.1.0"):
            raise ValueError("Selective recovery requires XGBoost >= 2.1.0")

    def to_dict(self):
        return {**super().to_dict(), "selective_recovery": self.selective_recovery,
                "recovery_timeout_s": self.recovery_timeout_s}

    def prepare_worker_for_retry(self):
        from ray.train.v2.xgboost import recovery

        if not recovery._communicator_cleared:
            raise RuntimeError("XGBoost communicator cleanup was not confirmed")

    @property
    def train_func_context(self):
        distributed_context = super(XGBoostConfig, self).train_func_context

        @contextmanager
        def collective_communication_context():
            # The distributed_context is only needed in distributed mode
            if get_train_fn_utils().is_distributed():
                from ray.train.v2.xgboost import recovery

                recovery._communicator_cleared = False
                try:
                    with distributed_context():
                        yield
                finally:
                    # XGBoost uses thread-local state. Inspect it on the actual
                    # training thread before allowing the actor to be reused.
                    recovery._communicator_cleared = not xgboost.collective.is_distributed()
            else:
                yield

        return collective_communication_context
