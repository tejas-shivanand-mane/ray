"""Opt-in, benchmark-local controller tracing; no changes to recovery policy."""
import functools
import inspect
import itertools
import os
import time


def install(recorder):
    import ray
    from ray.train.v2._internal.execution.controller.controller import TrainController
    from ray.train.v2._internal.execution.worker_group.worker_group import WorkerGroup
    from ray.train.v2._internal.execution.worker_group.execution_group import ReplicaGroup
    from ray.train.v2._internal.execution.worker_group.placement_group_handle import DefaultPlacementGroupHandle
    from ray.train.v2._internal.execution.worker_group import state, execution_group
    if getattr(TrainController, '_benchmark_trace_installed', False):
        return
    TrainController._benchmark_trace_installed = True
    counter = itertools.count()

    def wrap(original, name):
        def emit(phase, token, **extra):
            recorder.add.remote({'kind': 'recovery_trace', 'name': name, 'phase': phase,
                                 'span': token, 'time': time.monotonic(), 'pid': os.getpid(), **extra})

        if inspect.iscoroutinefunction(original):
            @functools.wraps(original)
            async def traced(*args, **kwargs):
                token = f'{os.getpid()}-{next(counter)}'
                emit('begin', token)
                try:
                    return await original(*args, **kwargs)
                finally:
                    emit('end', token)
        else:
            @functools.wraps(original)
            def traced(*args, **kwargs):
                token = f'{os.getpid()}-{next(counter)}'
                emit('begin', token)
                try:
                    return original(*args, **kwargs)
                finally:
                    emit('end', token)
        return traced

    for cls, names in (
        (TrainController, ('_poll_workers', '_execute_failure_decision', '_replace_bad_workers',
                           '_start_worker_group', '_shutdown_worker_group')),
        (WorkerGroup, ('poll_status', 'replace_replica_group', '_create_workers',
                       '_init_train_context', '_init_train_context_on_workers')),
        (ReplicaGroup, ('shutdown', 'start_training')),
        (DefaultPlacementGroupHandle, ('wait',)),
    ):
        for name in names:
            descriptor = inspect.getattr_static(cls, name)
            original = descriptor.__func__ if isinstance(descriptor, staticmethod) else descriptor
            replacement = wrap(original, f'{cls.__name__}.{name}')
            setattr(cls, name, staticmethod(replacement) if isinstance(descriptor, staticmethod) else replacement)
    # execution_group imports this function directly, so patch both bindings.
    traced_shutdown = wrap(state._shutdown_workers, 'workers.graceful_shutdown')
    state._shutdown_workers = traced_shutdown
    execution_group._shutdown_workers = traced_shutdown
    ray.get(recorder.add.remote({'kind': 'trace_installed', 'time': time.monotonic()}))
