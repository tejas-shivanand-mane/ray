"""Real TorchFT DDP with head-owned normal-task data shards.

Two surviving training actors and Lighthouse are owned by the support driver.
Fixed-R protects shard-producing tasks, not actor/model state. No failure gate,
application resubmission, checkpoint restart, or forced worker failure is used.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import traceback
import uuid

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from evidence import state_digest, write_json
from native_gate import request
from native_ml_task import file_hash
from native_workload import snapshot


def preflight():
    import importlib.metadata
    import inspect
    import torch
    import psutil
    from torchft import Manager, Optimizer, DistributedDataParallel, ProcessGroupGloo
    from torchft.checkpointing.pg_transport import PGTransport
    from torchft.coordination import LighthouseServer
    required = {'store_addr', 'hostname', 'lighthouse_addr', 'use_async_quorum'}
    if not required <= set(inspect.signature(Manager).parameters):
        raise RuntimeError('Installed TorchFT Manager API differs from this pilot')
    distributions = importlib.metadata.packages_distributions().get('torchft', [])
    return dict(torch=torch.__version__, ray=ray.__version__, psutil=psutil.__version__,
                torchft={name: importlib.metadata.version(name) for name in distributions},
                manager_api=sorted(required), checked='imports/API only; no training')


@ray.remote(max_retries=2)
def prepare_shard(index, cfg):
    import numpy as np
    import torch
    torch.set_num_threads(1)
    ml, ft = cfg['ml'], cfg['torchft']
    token = uuid.uuid4().hex
    entry = request(cfg['gate_url'], 'start', index=index, token=token,
                    pid=os.getpid(), node=ray.get_runtime_context().get_node_id())
    if file_hash(ml['data_path']) != ml['input_sha256']:
        raise ValueError('Input file changed')
    data = torch.load(ml['data_path'], map_location='cpu', weights_only=True)
    order = np.random.default_rng(ml['seed']).permutation(len(data['train_y']))
    ids = np.array_split(order, 2*ft['shards_per_rank'])[index]
    x = data['train_x'][ids].numpy().astype('float32')[:, None] / 255.0
    y = data['train_y'][ids].numpy().astype('int64')
    digest = hashlib.sha256(x.tobytes()+y.tobytes()).hexdigest()
    request(cfg['gate_url'], 'finish', index=index, token=token)
    return dict(x=x, y=y, sha256=digest, index=index,
                replay=entry['replay'], bytes=x.nbytes+y.nbytes)


@ray.remote(max_restarts=0, max_task_retries=0)
class InputOwner:
    def create(self, cfg, executor_nodes):
        from ray._private.worker import global_worker
        global_worker.core_worker.reset_recovery_succession_profile()
        n = cfg['torchft']['shards_per_rank']
        self.refs = [prepare_shard.options(num_cpus=1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(executor_nodes[i//n], soft=False)
        ).remote(i, cfg) for i in range(2*n)]
        return self.refs  # Nested refs; ownership stays in this actor on the head.

    def profile(self):
        from ray._private.worker import global_worker
        return dict(global_worker.core_worker.get_recovery_succession_profile())


@ray.remote(max_restarts=0, max_task_retries=0)
class Lighthouse:
    def __init__(self, ip):
        from torchft.coordination import LighthouseServer
        self.server = LighthouseServer(bind=ip+':0', min_replicas=2)

    def address(self):
        return self.server.address()


@ray.remote(max_restarts=0, max_task_retries=0)
class Replica:
    def run(self, rank, cfg, refs, lighthouse):
        from datetime import timedelta
        import torch
        from torch import nn
        import torch.distributed as dist
        from torchft import Manager, Optimizer, DistributedDataParallel, ProcessGroupGloo
        from torchft.checkpointing.pg_transport import PGTransport

        # Same synchronous, failure-aware CPU hook as the existing Ray TorchFT
        # step-aligned example. The Manager still decides whether updates commit.
        class FailureAwareDDP(DistributedDataParallel):
            @staticmethod
            def _comm_hook(state, bucket):
                buffer = bucket.buffer()
                state.allreduce(buffer).wait()
                future = torch.futures.Future()
                future.set_result(buffer)
                return future

        def event(kind, **fields):
            request(cfg['gate_url'], 'torchft_event', rank=rank, kind=kind, **fields)

        ml, ft = cfg['ml'], cfg['torchft']
        ip = ray.util.get_node_ip_address()
        if ip != cfg['executor_ips'][rank]:
            raise ValueError('Training actor is on the wrong VM')
        torch.set_num_threads(1)
        torch.set_num_interop_threads(1)
        torch.use_deterministic_algorithms(True)
        torch.manual_seed(ml['seed'])  # Identical initial model on both replicas.
        import psutil
        interfaces = [name for name, addresses in psutil.net_if_addrs().items()
                      if any(a.address == ip for a in addresses)]
        if len(interfaces) != 1:
            raise ValueError('Cannot identify the private-IP network interface')
        os.environ['GLOO_SOCKET_IFNAME'] = interfaces[0]
        os.environ['TORCHFT_LIGHTHOUSE'] = lighthouse
        timeout = timedelta(seconds=ft['collective_timeout_s'])
        manager = None
        start = time.monotonic()
        input_wait_s = compute_s = cpu_s = 0.0
        commits = attempts = heals = 0
        try:
            # One rank per replica group; two groups participate in TorchFT DDP.
            store = dist.TCPStore(ip, 0, 1, True, timeout=timeout)
            dist.init_process_group('gloo', store=store, rank=0, world_size=1,
                                    timeout=timeout)
            raw_model = nn.Sequential(
                nn.Conv2d(1, 32, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
                nn.Conv2d(32, 64, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
                nn.Flatten(), nn.Linear(64*7*7, 128), nn.ReLU(), nn.Linear(128, 10))
            raw_optimizer = torch.optim.Adam(raw_model.parameters(), lr=ml['learning_rates'][0])

            def load_state(value):
                nonlocal heals
                raw_model.load_state_dict(value['model'])
                raw_optimizer.load_state_dict(value['optim'])
                heals += 1
                event('peer_state_loaded', count=heals)

            def get_state():
                return dict(model=raw_model.state_dict(), optim=raw_optimizer.state_dict())

            pg = ProcessGroupGloo(timeout=timeout)
            transport = PGTransport(pg, timeout=timeout, device=torch.device('cpu'))
            manager = Manager(pg=pg, min_replica_size=2, rank=0, world_size=1,
                replica_id=f'headloss_{rank}', hostname=ip, store_addr=f'{ip}:{store.port}',
                lighthouse_addr=lighthouse, state_dict=get_state, load_state_dict=load_state,
                checkpoint_transport=transport, use_async_quorum=False, timeout=timeout,
                quorum_timeout=timeout)
            model = FailureAwareDDP(manager, raw_model)
            optimizer = Optimizer(manager, raw_optimizer)
            event('started', pid=os.getpid(), node=ray.get_runtime_context().get_node_id(),
                  lighthouse=lighthouse, versions=preflight())
            # Keep only the current input shard as a materialized value. Future
            # shards remain normal Ray ObjectRefs; no artificial unavailability.
            current_index, shard = None, None
            shard_count = ft['shards_per_rank']
            loss_first, loss_last = [], []
            while True:
                optimizer.zero_grad()
                manager.wait_quorum()
                step = manager.current_step()
                if manager.num_participants() != 2:
                    raise RuntimeError('Expected the same two-replica quorum')
                if step >= ml['steps']:
                    break  # Terminal quorum before either peer shuts down.
                if time.monotonic()-start > cfg['timeout_s']-30:
                    raise TimeoutError('Training observation budget exhausted')
                index = (step // ft['steps_per_shard']) % shard_count
                if index != current_index:
                    tick = time.monotonic()
                    shard = ray.get(refs[rank*shard_count+index], timeout=ft['input_timeout_s'])
                    input_wait_s += time.monotonic()-tick
                    if hashlib.sha256(shard['x'].tobytes()+shard['y'].tobytes()).hexdigest() != shard['sha256']:
                        raise ValueError('Input shard checksum mismatch')
                    current_index = index
                    event('input', step=step, shard=shard['index'], sha256=shard['sha256'],
                          replay=shard['replay'], payload_bytes=shard['bytes'])
                ids = (step*ml['batch_size'] + torch.arange(ml['batch_size'])) % len(shard['y'])
                x = torch.from_numpy(shard['x'])[ids]
                y = torch.from_numpy(shard['y'])[ids]
                tick, cpu_tick = time.monotonic(), time.process_time()
                loss = nn.functional.cross_entropy(model(x), y)
                loss.backward()
                optimizer.step()
                compute_s += time.monotonic()-tick
                cpu_s += time.process_time()-cpu_tick
                attempts += 1
                if manager.current_step() > step:
                    commits += 1
                    scalar = float(loss.detach())
                    if not math.isfinite(scalar):
                        raise ValueError('Nonfinite loss')
                    if len(loss_first) < 10:
                        loss_first.append(scalar)
                    loss_last = (loss_last+[scalar])[-10:]
                if manager.current_step() == 1 or manager.current_step() % ft['progress_every'] == 0:
                    event('progress', step=manager.current_step(), compute_s=compute_s,
                          cpu_s=cpu_s, input_wait_s=input_wait_s, commits=commits, attempts=attempts)
            event('progress', step=manager.current_step(), compute_s=compute_s,
                  cpu_s=cpu_s, input_wait_s=input_wait_s, commits=commits, attempts=attempts)
            training_wall_s = time.monotonic()-start
            # Evaluate local immutable test data, never an application checkpoint.
            if file_hash(ml['data_path']) != ml['input_sha256']:
                raise ValueError('Evaluation input changed')
            data = torch.load(ml['data_path'], map_location='cpu', weights_only=True)
            raw_model.eval()
            correct = 0
            with torch.no_grad():
                for offset in range(0, len(data['test_y']), ml['batch_size']):
                    x = data['test_x'][offset:offset+ml['batch_size']].unsqueeze(1).float()/255
                    y = data['test_y'][offset:offset+ml['batch_size']]
                    correct += int((raw_model(x).argmax(1) == y).sum())
            weights = b''.join(t.detach().cpu().contiguous().numpy().tobytes()
                              for _, t in sorted(raw_model.state_dict().items()))
            metrics = dict(steps=manager.current_step(), model_digest=state_digest(raw_model.state_dict()),
                optimizer_digest=state_digest(raw_optimizer.state_dict()),
                test_accuracy=correct/len(data['test_y']), weights_sha256=hashlib.sha256(weights).hexdigest())
            event('finished', **metrics, compute_s=compute_s, training_wall_s=training_wall_s)
            return dict(rank=rank, metrics=metrics, weights=weights, compute_s=compute_s,
                training_wall_s=training_wall_s, cpu_s=cpu_s, input_wait_s=input_wait_s,
                committed_updates=commits, attempted_updates=attempts, peer_state_loads=heals,
                first_loss=sum(loss_first)/len(loss_first), last_loss=sum(loss_last)/len(loss_last))
        except Exception as exc:
            event('failed', error=str(exc), traceback=traceback.format_exc(),
                  step=manager.current_step() if manager is not None else 0)
            raise
        finally:
            if manager is not None:
                manager.shutdown()
            if dist.is_initialized():
                dist.destroy_process_group()


def events(root):
    return [[json.loads(line) for line in (root/f'torchft-events-{rank}.jsonl').read_text().splitlines()]
            if (root/f'torchft-events-{rank}.jsonl').exists() else [] for rank in (0, 1)]


def main(cfg):
    root, start = Path(cfg['folder']), time.monotonic()
    result = dict(status='running', correct=False, reads=[], replicas=[], valid_overhead_sample=False,
                  workload='TorchFT DDP CNN with head-owned normal-task input shards')
    actors = []
    try:
        ray.init(address=cfg['address'], _node_ip_address=cfg['support_ip'], log_to_driver=False)
        native = ray._private.state.state.get_system_config()
        for key, value in dict(cfg['detection_config'], enable_recovery_succession=cfg['enabled'],
                enable_recovery_witness_holder_baseline=cfg['enabled']).items():
            if native.get(key) != value:
                raise ValueError('Native configuration mismatch: '+key)
        result['observed_native_config'] = native
        before = snapshot()
        result['before'] = before
        nodes = {n['ip']: n['id'] for n in before['nodes'] if n['alive']}
        if before['driver_node'] != nodes[cfg['support_ip']]:
            raise ValueError('Driver must run on support VM')
        placement = lambda ip: NodeAffinitySchedulingStrategy(nodes[ip], soft=False)
        owner = InputOwner.options(num_cpus=0, scheduling_strategy=placement(cfg['head_ip'])).remote()
        actors.append(owner)
        refs = ray.get(owner.create.remote(cfg, [nodes[ip] for ip in cfg['executor_ips']]), timeout=60)
        result['object_ids'] = [r.hex() for r in refs]
        lighthouse = Lighthouse.options(num_cpus=0, scheduling_strategy=placement(cfg['support_ip'])).remote(cfg['support_ip'])
        actors.append(lighthouse)
        address = ray.get(lighthouse.address.remote(), timeout=45)
        result['lighthouse_address'] = address
        replicas = [Replica.options(num_cpus=1, scheduling_strategy=placement(ip)).remote()
                    for ip in cfg['executor_ips']]
        actors += replicas
        pending = {actor.run.remote(rank, cfg, refs, address): rank for rank, actor in enumerate(replicas)}
        ready_sent = False
        while pending:
            if time.monotonic()-start > cfg['timeout_s']-20:
                raise TimeoutError('TorchFT workload deadline reached')
            progress = [json.loads((root/f'torchft-progress-{r}.json').read_text())
                        if (root/f'torchft-progress-{r}.json').exists() else {} for r in (0, 1)]
            if not ready_sent and all(p.get('step', 0) >= cfg['torchft']['fault_step'] for p in progress):
                profile = ray.get(owner.profile.remote(), timeout=15)
                if not cfg['enabled'] or profile.get('witness_update_rpcs_completed', 0) >= 2*len(refs):
                    result['owner_profile'] = profile
                    write_json(root/'ready.json', dict(before=before, owner_profile=profile,
                        training_progress=progress, elapsed_s=time.monotonic()-start,
                        injection='training continues; no release gate'))
                    ready_sent = True
            if (root/'continue.json').exists():
                # This is observed progress after controller recovery, not a claim
                # that training had stopped. Detailed event timestamps decide that.
                result['training_resumed'] = [p for p in progress
                    if p.get('step', 0) > cfg['torchft']['fault_step']]
            write_json(root/'progress.json', dict(reads=result['reads'],
                training_resumed=result.get('training_resumed', []), training_progress=progress))
            done, _ = ray.wait(list(pending), num_returns=1, timeout=.5)
            for ref in done:
                rank = pending.pop(ref)
                value = ray.get(ref)
                if hashlib.sha256(value.pop('weights')).hexdigest() != value['metrics']['weights_sha256']:
                    raise ValueError('Output payload checksum mismatch')
                result['replicas'].append(value)
                result['reads'].append(dict(index=rank, elapsed_s=time.monotonic()-start))
        if not ready_sent:
            raise ValueError('Training completed before the requested injection point')
        while not (root/'continue.json').exists():
            if time.monotonic()-start > cfg['timeout_s']-20:
                raise TimeoutError('Controller did not complete the requested scenario')
            time.sleep(.2)
        result['replicas'].sort(key=lambda r: r['rank'])
        a, b = result['replicas']
        if a['metrics'] != b['metrics']:
            raise ValueError('TorchFT replicas disagree on final model/optimizer/metrics')
        if (a['metrics']['steps'] != cfg['ml']['steps'] or
                a['metrics']['test_accuracy'] < cfg['ml']['minimum_accuracy'] or
                any(r['last_loss'] >= r['first_loss'] for r in result['replicas'])):
            raise ValueError('Training quality or step check failed')
        reference_path = root.parent/'torchft-reference.json'
        if reference_path.exists():
            reference = json.loads(reference_path.read_text())
            if reference['ml'] != cfg['ml'] or reference['torchft'] != cfg['torchft'] or reference['metrics'] != a['metrics']:
                raise ValueError('Final training state differs from the healthy OFF reference')
        elif not cfg['enabled'] and not cfg['fault']:
            write_json(reference_path, dict(ml=cfg['ml'], torchft=cfg['torchft'], metrics=a['metrics']))
        else:
            raise ValueError('Healthy OFF reference missing; keep this negative outcome')
        result['after'] = after = snapshot()
        if any(before[k] != after[k] for k in ('cluster_id', 'session', 'driver_node', 'job_id')):
            raise ValueError('GCS session or driver identity changed')
        alive = {n['id'] for n in after['nodes'] if n['alive']}
        if not {nodes[ip] for ip in [cfg['support_ip'], *cfg['executor_ips']]} <= alive:
            raise ValueError('A required survivor changed')
        if cfg['fault'] and (nodes[cfg['head_ip']] in alive or not any(
                n['alive'] and n['ip'] == cfg['replacement_ip'] for n in after['nodes'])):
            raise ValueError('Head replacement not confirmed in GCS')
        result['minimum_training_met'] = all(r['compute_s'] >= cfg['ml']['minimum_training_s'] for r in result['replicas'])
        result['training_critical_s'] = max(r['training_wall_s'] for r in result['replicas'])
        result.update(status='completed', correct=True,
                      valid_overhead_sample=not cfg['fault'] and result['minimum_training_met'])
    except Exception as exc:
        result.update(status='failed', error=str(exc), traceback=traceback.format_exc())
    finally:
        result['elapsed_s'] = time.monotonic()-start
        result['events'] = events(root)
        result['starts'] = [json.loads(p.read_text()) for p in root.glob('start-*.json')]
        result['reexecutions'] = sum(s['replay'] for s in result['starts'])
        result['checkpoint_bytes'] = 0
        result['note'] = 'No model checkpoint restart; Fixed-R replays input tasks only. No training gate.'
        write_json(root/'result.json', result)
        for actor in actors:
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass
        ray.shutdown()


if __name__ == '__main__':
    if sys.argv[1] == 'preflight':
        print(json.dumps(preflight()))
    else:
        main(json.loads(Path(sys.argv[1]).read_text()))
