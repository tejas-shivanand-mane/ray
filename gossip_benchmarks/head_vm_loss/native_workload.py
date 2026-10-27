"""Owner-loss workload. Run only on the surviving support VM, by the pilot."""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import traceback

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from evidence import write_json


@ray.remote(max_retries=2)
def produce(index, folder, size, gate_timeout):
    root = Path(folder)
    # Separate per-attempt files avoid concurrent append races across NFS clients.
    old = list(root.glob(f"start-{index}-*.json"))
    token = f"{os.getpid()}-{time.time_ns()}"
    write_json(root / f"start-{index}-{token}.json", dict(
        index=index, pid=os.getpid(), node=ray.get_runtime_context().get_node_id(),
        replay=bool(old), unix_s=time.time()))
    if not old:
        deadline = time.monotonic() + gate_timeout
        while not (root / f"release-{index}").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Original task gate stayed closed")
            time.sleep(.05)
    value = index.to_bytes(8, "little") + bytes([index % 251]) * (size - 8)
    write_json(root / f"finish-{index}-{token}.json", dict(index=index, unix_s=time.time()))
    return value


@ray.remote(max_restarts=0, max_task_retries=0)
class Owner:
    def create(self, executor, borrowers, cfg):
        from ray._private.worker import global_worker
        global_worker.core_worker.reset_recovery_succession_profile()
        self.refs = [produce.options(num_cpus=.1, scheduling_strategy=
            NodeAffinitySchedulingStrategy(executor, soft=False)).remote(
                i, cfg['folder'], cfg['payload_bytes'], cfg['timeout_s'] * 2)
            for i in range(cfg['tasks'])]
        held = ray.get([b.hold.remote(self.refs) for b in borrowers])
        return [r.hex() for r in self.refs], held

    def profile(self):
        from ray._private.worker import global_worker
        return dict(global_worker.core_worker.get_recovery_succession_profile())


@ray.remote(max_restarts=0, max_task_retries=0)
class Borrower:
    def hold(self, refs):
        self.refs = refs
        return [r.hex() for r in refs]

    def read(self, index, timeout):
        ref = self.refs[index]
        value = ray.get(ref, timeout=timeout)
        return dict(id=ref.hex(), digest=hashlib.sha256(value).hexdigest())


def snapshot():
    from ray._raylet import GcsClient
    from ray._private import ray_constants
    client = GcsClient(address=ray.get_runtime_context().gcs_address)
    return dict(cluster_id=client.cluster_id.hex(), session=client.internal_kv_get(
        b'session_name', ray_constants.KV_NAMESPACE_SESSION).decode(),
        driver_node=ray.get_runtime_context().get_node_id(),
        job_id=ray.get_runtime_context().get_job_id(),
        nodes=[dict(id=n['NodeID'], ip=n['NodeManagerAddress'], alive=n['Alive'])
               for n in ray.nodes()])


def main(cfg):
    root = Path(cfg['folder'])
    start = time.monotonic()
    result = dict(status='running', correct=False, reads=[], parameters=cfg)
    try:
        ray.init(address=cfg['address'], _node_ip_address=cfg['support_ip'],
                 log_to_driver=False)
        native = ray._private.state.state.get_system_config()
        result['observed_native_config'] = native
        if (native.get('enable_recovery_succession') != cfg['enabled'] or
                native.get('enable_recovery_witness_holder_baseline') != cfg['enabled']):
            raise RuntimeError('Running native configuration differs from requested arm')
        before = snapshot()
        result['before'] = before
        nodes = {n['ip']: n['id'] for n in before['nodes'] if n['alive']}
        if before['driver_node'] != nodes[cfg['support_ip']]:
            raise RuntimeError('Driver is not attached to surviving support node')
        owner = Owner.options(num_cpus=0, scheduling_strategy=
            NodeAffinitySchedulingStrategy(nodes[cfg['head_ip']], soft=False)).remote()
        borrowers = [Borrower.options(num_cpus=0, scheduling_strategy=
            NodeAffinitySchedulingStrategy(nodes[ip], soft=False)).remote()
            for ip in cfg['executor_ips']]
        ids, held = ray.get(owner.create.remote(nodes[cfg['executor_ips'][0]], borrowers, cfg), timeout=60)
        if held != [ids, ids] or len(set(ids)) != cfg['tasks']:
            raise RuntimeError('Borrowers did not receive identical distinct ObjectIDs')
        deadline = time.monotonic() + 60
        while True:
            profile = ray.get(owner.profile.remote(), timeout=10)
            started = all(list(root.glob(f'start-{i}-*.json')) for i in range(cfg['tasks']))
            durable = not cfg['enabled'] or (
                profile.get('witness_update_rpcs_completed', 0) >= 2 * cfg['tasks']
                and all(profile.get(k + '_sent', 0) == profile.get(k + '_completed', 0)
                        for k in ('holder_install_rpcs', 'holder_commit_rpcs',
                                  'witness_update_rpcs', 'candidate_rpc_logical_reports',
                                  'candidate_rpc_physical_rpcs')))
            if started and durable:
                break
            if time.monotonic() > deadline:
                raise TimeoutError('Tasks/protection did not become ready: ' + repr(profile))
            time.sleep(.1)
        result['owner_profile'] = profile
        result['object_ids'] = ids
        if len(list(root.glob('start-*.json'))) != cfg['tasks']:
            raise RuntimeError('Unexpected replay before injection')

        def consume(i):
            replies = ray.get([b.read.remote(i, 45) for b in borrowers], timeout=50)
            expected = hashlib.sha256(i.to_bytes(8, 'little') +
                bytes([i % 251]) * (cfg['payload_bytes'] - 8)).hexdigest()
            if any(r != dict(id=ids[i], digest=expected) for r in replies):
                raise ValueError('Output/ObjectID mismatch')
            result['reads'].append(dict(index=i, elapsed_s=time.monotonic()-start))
            write_json(root / 'progress.json', result)

        (root / 'release-0').touch()
        consume(0)
        write_json(root / 'ready.json', dict(before=before, owner_profile=profile,
            elapsed_s=time.monotonic()-start, object_ids=ids))
        wait_start = time.monotonic()
        while not (root / 'continue.json').exists():
            if time.monotonic()-start > cfg['timeout_s']:
                raise TimeoutError('Supervisor did not release observation')
            time.sleep(.1)
        result['supervisor_gate_wait_s'] = time.monotonic()-wait_start
        if not cfg['fault']:
            for i in range(1, cfg['tasks']):
                (root / f'release-{i}').touch()
        for i in range(1, cfg['tasks']):
            consume(i)
        after = snapshot()
        result['after'] = after
        for key in ('cluster_id', 'session', 'driver_node', 'job_id'):
            if before[key] != after[key]:
                raise ValueError('Recovery changed ' + key)
        alive = {n['id'] for n in after['nodes'] if n['alive']}
        survivors = {nodes[ip] for ip in [cfg['support_ip'], *cfg['executor_ips']]}
        if not survivors <= alive:
            raise ValueError('A required original worker was lost')
        if cfg['fault'] and (nodes[cfg['head_ip']] in alive or
                not any(n['alive'] and n['ip'] == cfg['replacement_ip'] for n in after['nodes'])):
            raise ValueError('Head replacement not reflected in GCS')
        result.update(status='completed', correct=True)
    except Exception as exc:
        result.update(status='failed', error=str(exc), traceback=traceback.format_exc())
    finally:
        result['elapsed_s'] = time.monotonic()-start
        result['elapsed_excluding_supervisor_gate_s'] = (
            result['elapsed_s']-result.get('supervisor_gate_wait_s', 0))
        result['starts'] = [json.loads(p.read_text()) for p in root.glob('start-*.json')]
        result['finishes'] = [json.loads(p.read_text()) for p in root.glob('finish-*.json')]
        result['reexecutions'] = max(0, len(result['starts'])-cfg['tasks'])
        result['checkpoint_bytes'] = 0
        result['lost_final_outputs'] = cfg['tasks']-len(result['reads'])
        # This diagnostic gate forces unfinished work; not production throughput.
        write_json(root / 'result.json', result)
        ray.shutdown()


if __name__ == '__main__':
    main(json.loads(Path(sys.argv[1]).read_text()))
