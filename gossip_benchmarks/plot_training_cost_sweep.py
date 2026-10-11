"""Plot validated checkpoint-interval sweep evidence, without launching training."""
import argparse
import json
from pathlib import Path
import statistics
from _support.ordinary_ray_comparison import recovery_errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    sweep = json.loads(args.report.expanduser().read_text())
    if sweep.get('profile') != 'training-cost-sweep':
        parser.error('Expected training-cost-sweep evidence')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    valid_counts = []
    for entry in sorted(sweep['reports'], key=lambda e: e['interval']):
        report = entry.get('report', {})
        parameters = report.get('parameters', {})
        interval = entry['interval']
        values = {key: [] for key in ('healthy_ordinary', 'healthy_aligned',
                                     'fault_ordinary', 'fault_aligned', 'reduction', 'overhead')}
        for repeat in range(sweep['parameters']['repeats']):
            samples = [s for s in report.get('samples', []) if s['repeat'] == repeat]
            by_mode = {(s['mode'], s['fault']): s for s in samples}
            if (len(samples) != 4 or set(by_mode) != {
                    ('ordinary', False), ('ordinary', True), ('aligned', False), ('aligned', True)}
                    or not all(s.get('matches_reference') is True and not recovery_errors(s, parameters)
                               for s in samples)):
                continue
            for mode in ('ordinary', 'aligned'):
                for fault, label in ((False, 'healthy'), (True, 'fault')):
                    values[f'{label}_{mode}'].append(by_mode[(mode, fault)]['fit_s'])
            baseline = by_mode[('ordinary', True)]['fit_s']
            values['reduction'].append(100 * (1 - by_mode[('aligned', True)]['fit_s'] / baseline))
            values['overhead'].append(100 * (by_mode[('aligned', False)]['fit_s'] /
                                             by_mode[('ordinary', False)]['fit_s'] - 1))
        valid_counts.append(f'{interval}: {len(values["reduction"])}/{sweep["parameters"]["repeats"]}')
        for ax, keys in ((axes[0, 0], ['healthy_ordinary', 'healthy_aligned']),
                         (axes[0, 1], ['fault_ordinary', 'fault_aligned']),
                         (axes[1, 0], ['reduction']), (axes[1, 1], ['overhead'])):
            for key in keys:
                points = values[key]
                color = '#CC6677' if key.endswith('ordinary') else '#4477AA'
                offset = (-.8 if key.endswith('ordinary') else .8) if len(keys) == 2 else 0
                if points:
                    ax.scatter([interval + offset] * len(points), points, color=color, alpha=.45, s=24)
                    ax.scatter(interval + offset, statistics.median(points), color=color, marker='_', s=180)
    for ax, title, ylabel in zip(axes.flat,
            ['Healthy completion time', 'Completion time with worker-node loss',
             'Fault-runtime reduction (positive is better)', 'Healthy overhead (negative is better)'],
            ['Seconds', 'Seconds', 'Percent', 'Percent']):
        ax.set(title=title, ylabel=ylabel, xlabel='Checkpoint interval (updates)')
        ax.set_xticks(sorted(sweep['parameters']['intervals']))
        ax.grid(alpha=.2)
    for ax in axes[0]:
        ax.set_ylim(bottom=0)
        ax.plot([], [], 'o', color='#CC6677', label='Ordinary Ray')
        ax.plot([], [], 'o', color='#4477AA', label='TorchFT + aligned input')
        ax.legend()
    for ax in axes[1]:
        ax.axhline(0, color='black', linewidth=.8)
    p = sweep['parameters']
    fig.suptitle(f'Real compute, synthetic CPU MLP: width {p["model_width"]}, batch {p["batch_size"]}; no startup sleep')
    fig.text(.5, .025, f'{p["steps"]} updates; node lost before update {p["fault_step"]}. '
             'Points: paired runs; horizontal marks: medians.\n'
             'Validated pairs by interval: ' + '; '.join(valid_counts) + '\n'
             'Only complete correctness/recovery-validated pairs included. Combined-system comparison; '
             'not production or GPU evidence.', ha='center', fontsize=9)
    fig.tight_layout(rect=(0, .11, 1, .95))
    output = args.output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    fig.savefig(output.with_suffix('.pdf'))
    plt.close(fig)
    print(output)


if __name__ == '__main__':
    main()
