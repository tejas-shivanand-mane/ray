"""Matched Fashion-MNIST training trials with a head-owned task lineage."""
import hashlib
import json
import math
from pathlib import Path
import sys
import time
import traceback

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from evidence import write_json
from native_gate import request
from native_ml_task import train_trial
from native_workload import snapshot


@ray.remote(max_retries=2)
def train(index, cfg):
    return train_trial(index, cfg['ml'], cfg['gate_url'], cfg['timeout_s']*2)


@ray.remote(max_restarts=0, max_task_retries=0)
class Owner:
    def create(self, executors, borrowers, cfg):
        from ray._private.worker import global_worker
        global_worker.core_worker.reset_recovery_succession_profile()
        self.refs = [train.options(num_cpus=1, scheduling_strategy=
            NodeAffinitySchedulingStrategy(node, soft=False)).remote(i, cfg)
            for i, node in enumerate(executors)]
        held = ray.get([b.hold.remote(self.refs) for b in borrowers])
        return [ref.hex() for ref in self.refs], held

    def profile(self):
        from ray._private.worker import global_worker
        return dict(global_worker.core_worker.get_recovery_succession_profile())


@ray.remote(max_restarts=0, max_task_retries=0)
class Borrower:
    def hold(self, refs):
        self.refs = refs
        return [ref.hex() for ref in refs]

    def read(self, index, timeout):
        output = ray.get(self.refs[index], timeout=timeout)
        if (hashlib.sha256(output['weights']).hexdigest() != output['metrics']['weights_sha256'] or
                len(output['weights']) != output['metrics']['weights_bytes']):
            raise ValueError('Trained weight payload corruption')
        return dict(id=self.refs[index].hex(), **{k: v for k, v in output.items() if k != 'weights'})


def main(cfg):
    root, start = Path(cfg['folder']), time.monotonic()
    result = dict(status='running', correct=False, reads=[], trials=[], parameters=cfg,
                  valid_overhead_sample=False, workload='Fashion-MNIST CNN: two independent training trials')
    try:
        ray.init(address=cfg['address'], _node_ip_address=cfg['support_ip'], log_to_driver=False)
        native = ray._private.state.state.get_system_config()
        result['observed_native_config'] = native
        expected = dict(cfg['detection_config'], enable_recovery_succession=cfg['enabled'],
                        enable_recovery_witness_holder_baseline=cfg['enabled'])
        if any(native.get(k) != v for k, v in expected.items()):
            raise ValueError('Running configuration differs from requested ML arm')
        before = snapshot()
        result['before'] = before
        nodes = {n['ip']: n['id'] for n in before['nodes'] if n['alive']}
        if before['driver_node'] != nodes[cfg['support_ip']]:
            raise ValueError('ML driver must survive on support VM')
        owner = Owner.options(num_cpus=0, scheduling_strategy=
            NodeAffinitySchedulingStrategy(nodes[cfg['head_ip']], soft=False)).remote()
        borrowers = [Borrower.options(num_cpus=0, scheduling_strategy=
            NodeAffinitySchedulingStrategy(nodes[ip], soft=False)).remote() for ip in cfg['executor_ips']]
        ids, held = ray.get(owner.create.remote([nodes[ip] for ip in cfg['executor_ips']], borrowers, cfg), timeout=45)
        if held != [ids, ids] or len(set(ids)) != 2:
            raise ValueError('Both borrowers must hold both original training ObjectRefs')
        result['object_ids'] = ids
        deadline = time.monotonic()+75
        while True:
            profile = ray.get(owner.profile.remote(), timeout=10)
            gates = [list(root.glob(f'training_gate-{i}-*.json')) for i in range(2)]
            durable = not cfg['enabled'] or profile.get('witness_update_rpcs_completed', 0) >= 4
            if all(gates) and durable:
                break
            if time.monotonic() > deadline:
                raise TimeoutError('Training/protection did not reach the injection point')
            time.sleep(.2)
        if len(list(root.glob('start-*.json'))) != 2:
            raise ValueError('Unexpected replay before injection')
        result['owner_profile'] = profile
        result['training_before_gate'] = [json.loads(paths[0].read_text()) for paths in gates]
        write_json(root/'ready.json', dict(before=before, object_ids=ids, owner_profile=profile,
            training_before_gate=result['training_before_gate'], elapsed_s=time.monotonic()-start))
        gate_start = time.monotonic()
        while not (root/'continue.json').exists():
            if time.monotonic()-start >= cfg['timeout_s']:
                raise TimeoutError('Supervisor did not release training observation')
            time.sleep(.2)
        result['supervisor_gate_wait_s'] = time.monotonic()-gate_start
        if not cfg['fault']:
            for i in range(2):
                request(cfg['gate_url'], 'release', index=i)
        # Actors execute methods serially. Start with a different trial on each
        # borrower so both owner-loss recovery paths can run concurrently.
        pending = {b.read.remote(i, cfg['timeout_s']): (i, j)
                   for j, b in enumerate(borrowers) for i in ((0, 1) if j == 0 else (1, 0))}
        outputs = {}
        while pending:
            if time.monotonic()-start >= cfg['timeout_s']:
                raise TimeoutError('ML observation deadline reached')
            # The supervisor observes replayed optimizer progress, not just a
            # submitted task or restored GCS, as evidence of training resumption.
            if cfg['fault'] and not result.get('training_resumed'):
                replay_progress = [json.loads(p.read_text()) for p in root.glob('training_progress-*.json')]
                replay_progress = [p for p in replay_progress if p['replay'] and p['step'] >= 1]
                if replay_progress:
                    result['training_resumed'] = replay_progress
                    write_json(root/'progress.json', result)
            ready, _ = ray.wait(list(pending), num_returns=1, timeout=1)
            for ref in ready:
                i, borrower = pending.pop(ref)
                value = ray.get(ref)
                if value['id'] != ids[i]:
                    raise ValueError('Recovery changed training ObjectID')
                outputs[i, borrower] = value
                if (i, 0) in outputs and (i, 1) in outputs:
                    if outputs[i, 0] != outputs[i, 1]:
                        raise ValueError('Borrowers obtained different training results')
                    result['reads'].append(dict(index=i, elapsed_s=time.monotonic()-start))
                    result['trials'].append(value)
                    write_json(root/'progress.json', result)
        result['trials'].sort(key=lambda t: t['metrics']['index'])
        for trial in result['trials']:
            m = trial['metrics']
            if (m['steps'] != cfg['ml']['steps'] or m['input_sha256'] != cfg['ml']['input_sha256'] or
                    not math.isfinite(m['test_loss']) or m['test_accuracy'] < cfg['ml']['minimum_accuracy'] or
                    m['last_loss'] >= m['first_loss']):
                raise ValueError('Training quality/step/input check failed')
        stable = [t['metrics'] for t in result['trials']]
        reference_path = root.parent/'ml-reference.json'
        if reference_path.exists():
            reference = json.loads(reference_path.read_text())
            if reference['ml'] != cfg['ml'] or reference['trials'] != stable:
                raise ValueError('Final model/optimizer/metrics differ from plain healthy reference')
            result['reference_label'] = reference['label']
        elif not cfg['fault'] and not cfg['enabled'] and not cfg['infrastructure']:
            write_json(reference_path, dict(ml=cfg['ml'], trials=stable, label=root.name))
            result['reference_label'] = root.name
        else:
            raise ValueError('Plain healthy reference is missing; keep this negative outcome')
        after = snapshot()
        result['after'] = after
        for key in ('cluster_id', 'session', 'driver_node', 'job_id'):
            if before[key] != after[key]:
                raise ValueError('Recovery changed '+key)
        alive = {n['id'] for n in after['nodes'] if n['alive']}
        if not {nodes[ip] for ip in [cfg['support_ip'], *cfg['executor_ips']]} <= alive:
            raise ValueError('Required surviving node identity lost')
        if cfg['fault']:
            if (nodes[cfg['head_ip']] in alive or not any(n['alive'] and
                    n['ip'] == cfg['replacement_ip'] for n in after['nodes'])):
                raise ValueError('Head replacement not reflected in GCS')
            for i in range(2):
                finishes = [json.loads(p.read_text()) for p in root.glob(f'finish-{i}-*.json')]
                if not any(f['replay'] for f in finishes) or (root/f'release-{i}.json').exists():
                    raise ValueError('Missing replay evidence or original training gate released')
        result['minimum_training_met'] = all(t['training_s'] >= cfg['ml']['minimum_training_s']
                                              for t in result['trials'])
        result['training_critical_s'] = max(t['training_s'] for t in result['trials'])
        result['training_cpu_s'] = sum(t['cpu_s'] for t in result['trials'])
        result['training_examples_per_s'] = sum(t['metrics']['examples'] for t in result['trials']) / result['training_critical_s']
        result['selected_trial'] = max(stable, key=lambda t: t['validation_accuracy'])['index']
        result.update(status='completed', correct=True,
                      valid_overhead_sample=not cfg['fault'] and result['minimum_training_met'])
        if not result['minimum_training_met']:
            result['duration_warning'] = 'Below 60-second floor: recalibrate higher under a NEW suite; never pad individual arms'
    except Exception as exc:
        result.update(status='failed', error=str(exc), traceback=traceback.format_exc())
    finally:
        result['elapsed_s'] = time.monotonic()-start
        result['elapsed_excluding_supervisor_gate_s'] = result['elapsed_s']-result.get('supervisor_gate_wait_s', 0)
        result['starts'] = [json.loads(p.read_text()) for p in root.glob('start-*.json')]
        result['finishes'] = [json.loads(p.read_text()) for p in root.glob('finish-*.json')]
        result['reexecutions'] = max(0, len(result['starts'])-2)
        result['lost_final_outputs'] = 2-len(result['reads'])
        result['checkpoint_bytes'] = 0
        attempts = {}
        for pattern in ('training_progress-*.json', 'training_gate-*.json'):
            for path in root.glob(pattern):
                event = json.loads(path.read_text())
                token = path.stem.rsplit('-', 1)[1]
                if event['step'] >= attempts.get(token, {}).get('step', 0):
                    attempts[token] = event
        events = list(attempts.values())
        result['training_progress'] = events
        result['observed_training_cpu_s_lower_bound'] = sum(e['cpu_s'] for e in events)
        result['observed_optimizer_steps_lower_bound'] = sum(e['step'] for e in events)
        result['abandoned_optimizer_steps'] = sum(e['step'] for e in events if not e['replay']) if cfg['fault'] else 0
        write_json(root/'result.json', result)
        ray.shutdown()


if __name__ == '__main__':
    main(json.loads(Path(sys.argv[1]).read_text()))
