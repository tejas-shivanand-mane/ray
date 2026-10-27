"""Fashion-MNIST CNN kernel, input preparation and owner-run calibration.

Two independent training trials, not DDP or mutable actor-state recovery.
Training clocks cover only real minibatch/optimizer iterations, never gate waits.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import uuid

from evidence import state_digest, write_json
from native_gate import request

_THREADS_SET = False


def file_hash(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def prepare_data(cfg):
    import torch
    import torchvision
    from torchvision.datasets import FashionMNIST
    # Use the dataset publisher's HTTPS mirror; retain torchvision's checksums.
    FashionMNIST.mirrors = ['https://raw.githubusercontent.com/zalandoresearch/fashion-mnist/master/data/fashion/']
    destination = Path(cfg['data_path'])
    destination.parent.mkdir(parents=True, exist_ok=True)
    train = FashionMNIST(cfg['download_root'], train=True, download=True)
    test = FashionMNIST(cfg['download_root'], train=False, download=True)
    # Fixed published train/test split; subset sizes are explicit in the notebook.
    data = dict(train_x=train.data[:cfg['train_examples']].clone(),
                train_y=train.targets[:cfg['train_examples']].clone(),
                validation_x=train.data[cfg['train_examples']:cfg['train_examples']+cfg['validation_examples']].clone(),
                validation_y=train.targets[cfg['train_examples']:cfg['train_examples']+cfg['validation_examples']].clone(),
                test_x=test.data[:cfg['test_examples']].clone(),
                test_y=test.targets[:cfg['test_examples']].clone())
    temporary = destination.with_suffix('.tmp')
    torch.save(data, temporary)
    temporary.replace(destination)
    return dict(dataset='FashionMNIST', input_sha256=file_hash(destination),
                data_path=str(destination), torch=torch.__version__,
                torchvision=torchvision.__version__,
                train_examples=len(data['train_y']), validation_examples=len(data['validation_y']),
                test_examples=len(data['test_y']))


def train_trial(index, cfg, gate_url=None, gate_timeout=600, calibration_s=None):
    global _THREADS_SET
    import torch
    from torch import nn
    torch.set_num_threads(1)
    if not _THREADS_SET:
        torch.set_num_interop_threads(1)
        _THREADS_SET = True
    torch.use_deterministic_algorithms(True)
    torch.manual_seed(cfg['seed'] + index)
    if file_hash(cfg['data_path']) != cfg['input_sha256']:
        raise ValueError('Fashion-MNIST input file changed')
    data = torch.load(cfg['data_path'], map_location='cpu', weights_only=True)
    model = nn.Sequential(
        nn.Conv2d(1, 32, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
        nn.Conv2d(32, 64, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
        nn.Flatten(), nn.Linear(64 * 7 * 7, 128), nn.ReLU(), nn.Linear(128, 10))
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg['learning_rates'][index])
    order = torch.randperm(len(data['train_y']), generator=torch.Generator().manual_seed(cfg['seed']))
    batch = cfg['batch_size']
    batch_offset = torch.arange(batch)
    token, replay = uuid.uuid4().hex, False
    if gate_url:
        import ray
        entry = request(gate_url, 'start', index=index, token=token, pid=os.getpid(),
                        node=ray.get_runtime_context().get_node_id())
        replay = entry['replay']
    training_s, cpu_s, step = 0.0, 0.0, 0
    first_losses, last_losses = [], []
    while step < cfg['steps']:
        tick, cpu_tick = time.perf_counter(), time.process_time()
        ids = order[(step * batch + batch_offset) % len(order)]
        x = data['train_x'][ids].unsqueeze(1).float().div_(255)
        y = data['train_y'][ids]
        optimizer.zero_grad(set_to_none=True)
        loss = nn.functional.cross_entropy(model(x), y)
        loss.backward()
        optimizer.step()
        loss_value = float(loss.detach())
        training_s += time.perf_counter() - tick
        cpu_s += time.process_time() - cpu_tick
        if not math.isfinite(loss_value):
            raise ValueError('Non-finite training loss')
        step += 1
        if step <= 10:
            first_losses.append(loss_value)
        last_losses = (last_losses + [loss_value])[-10:]
        if calibration_s is not None and training_s >= calibration_s:
            break
        if gate_url and (step == 1 or step % 50 == 0 or step == cfg['steps']):
            request(gate_url, 'training_progress', index=index, token=token,
                    step=step, train_s=training_s, cpu_s=cpu_s)
        if gate_url and not replay and step == cfg['gate_step']:
            request(gate_url, 'training_gate', index=index, token=token,
                    step=step, train_s=training_s, cpu_s=cpu_s)
            end = time.monotonic() + gate_timeout
            while not request(gate_url, 'released', index=index)['released']:
                if time.monotonic() >= end:
                    raise TimeoutError('Training gate was never released')
                time.sleep(.2)
    if calibration_s is not None:
        return dict(index=index, steps=step, training_s=training_s, cpu_s=cpu_s,
                    seconds_per_step=training_s / step)
    model.eval()
    evaluations = {}
    for split in ('validation', 'test'):
        correct, total_loss = 0, 0.0
        with torch.no_grad():
            for offset in range(0, len(data[split+'_y']), batch):
                x = data[split+'_x'][offset:offset+batch].unsqueeze(1).float().div_(255)
                y = data[split+'_y'][offset:offset+batch]
                logits = model(x)
                correct += int((logits.argmax(1) == y).sum())
                total_loss += float(nn.functional.cross_entropy(logits, y, reduction='sum'))
        evaluations[split+'_accuracy'] = correct/len(data[split+'_y'])
        evaluations[split+'_loss'] = total_loss/len(data[split+'_y'])
    # Return the trained weights, not just a success marker. The borrowers verify
    # their bytes and compare stable model/optimizer digests with the healthy run.
    weights = b''.join(t.detach().cpu().contiguous().numpy().tobytes()
                       for _, t in sorted(model.state_dict().items()))
    metrics = dict(index=index, steps=step, examples=step*batch,
        input_sha256=cfg['input_sha256'], model_digest=state_digest(model.state_dict()),
        optimizer_digest=state_digest(optimizer.state_dict()),
        weights_sha256=hashlib.sha256(weights).hexdigest(), weights_bytes=len(weights),
        first_loss=sum(first_losses)/len(first_losses), last_loss=sum(last_losses)/len(last_losses), **evaluations)
    if gate_url:
        request(gate_url, 'finish', index=index, token=token)
    return dict(weights=weights, metrics=metrics, training_s=training_s, cpu_s=cpu_s,
                replay=replay, attempt_token=token)


if __name__ == '__main__':
    action, config_path, output = sys.argv[1:]
    config = json.loads(Path(config_path).read_text())
    if action == 'prepare':
        result = prepare_data(config)
    elif action == 'calibrate':
        result = train_trial(config['trial_index'], config, calibration_s=config['calibration_s'])
    else:
        raise ValueError('Unknown ML preparation action')
    write_json(output, result)
