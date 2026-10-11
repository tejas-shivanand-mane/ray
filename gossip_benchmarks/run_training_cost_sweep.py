"""Run matched checkpoint-interval experiments with genuine CPU MLP computation.

This is a controlled synthetic workload, not evidence of production/GPU speedup.
The child harness owns fault injection, timeouts, and correctness validation.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--intervals', type=int, nargs='+', default=[30, 60, 120])
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--steps', type=int, default=300)
    parser.add_argument('--fault-step', type=int, default=239)
    parser.add_argument('--model-width', type=int, default=2048)
    parser.add_argument('--batch-size', type=int, default=256)
    parser.add_argument('--timeout-s', type=int, default=1800)
    args = parser.parse_args()
    if min(args.intervals + [args.repeats, args.steps, args.model_width,
                             args.batch_size, args.timeout_s]) <= 0:
        parser.error('All sizes, intervals, repeats and timeouts must be positive')
    if not 0 < args.fault_step < args.steps or any(args.fault_step % i == 0 for i in args.intervals):
        parser.error('Fault must be within training and between checkpoints for every interval')
    if len(set(args.intervals)) != len(args.intervals):
        parser.error('Intervals must be distinct')
    output = args.output.expanduser().resolve()
    directory = output.parent / (output.stem + '-reports')
    if output.exists() or directory.exists():
        parser.error('Choose a fresh output name')
    directory.mkdir(parents=True)
    report = {'profile': 'training-cost-sweep', 'parameters': {
        k: v for k, v in vars(args).items() if k != 'output'}, 'reports': [],
        'scope': 'Synthetic CPU MLP, real forward/backward computation, zero startup sleep; '
                 'two logical worker nodes on one host, surviving head/storage'}

    def save():
        temporary = output.with_suffix(output.suffix + '.tmp')
        temporary.write_text(json.dumps(report, indent=2))
        temporary.replace(output)

    save()
    for interval in args.intervals:
        path = directory / f'checkpoint-{interval}.json'
        command = [sys.executable, str(Path(__file__).with_name('run_step_input_experiment.py')),
                   '--comparison', 'ordinary-ray', '--startup-delay-s', '0',
                   '--checkpoint-every', str(interval), '--output', str(path)]
        for name in ('repeats', 'steps', 'fault_step', 'model_width', 'batch_size', 'timeout_s'):
            command += ['--' + name.replace('_', '-'), str(getattr(args, name))]
        print(f'Checkpoint interval {interval}; rollback {args.fault_step % interval} updates', flush=True)
        code = subprocess.call(command)
        entry = {'interval': interval, 'path': str(path), 'exit_code': code}
        if path.exists():
            entry['report'] = json.loads(path.read_text())
        report['reports'].append(entry)
        save()
        if code != 0:
            raise SystemExit(f'Child failed; partial evidence saved to {output}')
    report['status'] = 'finished'
    save()
    print(output)


if __name__ == '__main__':
    main()
