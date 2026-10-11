"""Plot opt-in recovery traces. All clocks are monotonic on one physical host."""
import argparse
import json
from pathlib import Path
from _support.ordinary_ray_comparison import recovery_errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.expanduser().read_text())
    if not report.get('parameters', {}).get('profile_recovery'):
        parser.error('Report was not collected with --profile-recovery')
    samples = [s for s in report['samples'] if s.get('fault')]
    if not samples:
        parser.error('No failure samples')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(len(samples), 1, figsize=(15, 6 * len(samples)), squeeze=False)
    for ax, sample in zip(axes[:, 0], samples):
        title = f'{sample["mode"]}, repetition {sample["repeat"] + 1}'
        if not sample.get('matches_reference') or recovery_errors(sample, report['parameters']):
            ax.set_title(title + ': invalid or incomplete run')
            continue
        events = sorted(sample['events'], key=lambda e: e['time'])
        fault = sample['fault_time']
        progress = [e for e in events if e['kind'] == 'update' and e['step'] > report['parameters']['fault_step']
                    and e['time'] >= fault]
        finish = max(min(e['time'] for e in progress if e['rank'] == r) for r in (0, 1))
        starts = {}
        spans = []
        for event in events:
            if event['kind'] == 'recovery_trace':
                if event['phase'] == 'begin':
                    starts[event['span']] = event
                elif event['span'] in starts:
                    begin = starts.pop(event['span'])
                    spans.append((begin['name'], begin['time'], event['time']))
            elif event['kind'] == 'step_synchronization':
                spans.append((f'Worker {event["rank"]}: zero_grad + quorum', event['time'], event['end_time']))
        for rank in (0, 1):
            for prefix, label in (('torchft_setup', 'TorchFT setup'), ('peer_state_load', 'Apply peer state')):
                begin = None
                for event in events:
                    if event.get('rank') != rank:
                        continue
                    if event['kind'] == prefix + '_begin':
                        begin = event['time']
                    elif event['kind'] == prefix + '_end' and begin is not None:
                        spans.append((f'Worker {rank}: {label}', begin, event['time']))
                        begin = None
        spans = [(name, max(begin, fault), min(end, finish)) for name, begin, end in spans
                 if end > fault and begin < finish]
        lanes = list(dict.fromkeys(name for name, _, _ in sorted(spans, key=lambda v: v[1])))
        for name, begin, end in spans:
            y = lanes.index(name)
            ax.broken_barh([(begin - fault, max(end - begin, .005))], (y - .35, .7), facecolors='#4477AA')
        for kind, label, color in (
                ('driver_observed_node_dead', 'Driver sees node dead', '#999999'),
                ('controller_observed_failure', 'Controller sees worker failure', '#AA3377'),
                ('start', 'Replacement training starts', '#EE7733')):
            matching = [e for e in events if e['kind'] == kind and fault <= e['time'] <= finish]
            if matching:
                timestamp = matching[0]['time'] - fault
                ax.axvline(timestamp, color=color, linestyle='--', label=f'{label}: {timestamp:.2f}s')
        ax.axvline(finish - fault, color='#228833', label=f'Both ranks make new progress: {finish - fault:.2f}s')
        ax.set_yticks(range(len(lanes)), lanes)
        ax.invert_yaxis()
        ax.set_xlim(0, (finish - fault) * 1.06)
        ax.set_xlabel('Seconds since node-kill request')
        ax.set_title(title)
        ax.grid(axis='x', alpha=.2)
        ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=8)
        print(title)
        for name in lanes:
            durations = [end - begin for n, begin, end in spans if n == name]
            print(f'  {name}: {sum(durations):.3f}s inclusive across {len(durations)} calls')
        if any(e['time'] < finish for e in starts.values()):
            print('  Warning: incomplete controller spans; inspect raw events')
    fig.suptitle('Recovery phase profile — unchanged timeouts and recovery policy')
    fig.text(.5, .015, 'Nested spans overlap: do not add rows. Actor creation includes placement rescheduling and metadata readiness.\n'
             'Quorum waits include peer healing, not a measurement of network transfer alone. '
             'Driver node-table observation is not the exact GCS detection time.\n'
             'Single-host clocks; diagnostic tracing adds overhead. Compare only equally instrumented runs.', ha='center', fontsize=9)
    fig.tight_layout(rect=(0, .08, 1, .96))
    output = args.output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    fig.savefig(output.with_suffix('.pdf'))
    plt.close(fig)
    print(output)


if __name__ == '__main__':
    main()
