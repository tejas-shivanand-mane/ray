"""Owner-run summary and optional point plot. Failed rows remain explicit."""
import argparse
import json
from pathlib import Path


def summarize(report):
    rows = report['samples']
    for row in rows:
        result = row.get('result', {})
        print(row['label'], row['status'], 'correct=', result.get('correct', False),
              'elapsed_s=', result.get('elapsed_s'),
              'reexecutions=', result.get('reexecutions'),
              'error=', row.get('error', result.get('error', '')))
    by = {r['label']: r for r in rows}
    for base, candidate, label in (
        ('plain-healthy', 'off-healthy', 'Redis/proxy infrastructure'),
        ('off-healthy', 'on-healthy', 'Native Fixed-R on shared infrastructure'),
        ('plain-healthy', 'on-healthy', 'Combined enablement')):
        a, b = (by.get(k, {}).get('result', {}) for k in (base, candidate))
        if a.get('correct') and b.get('correct'):
            av, bv = (r['elapsed_excluding_supervisor_gate_s'] for r in (a, b))
            print(f'{label}: {(bv/av-1)*100:+.2f}% completion-time change '
                  '(single replicate, supervisor gate excluded; includes setup/diagnostics)')
        else:
            print(label+': unavailable; requires two correct healthy observations')
    print('Whole-VM cumulative CPU/network/disk snapshots are in each row. '
          'Head final counters are unavailable after VM loss. These are not task-only costs.')


def plot(report, output):
    import matplotlib.pyplot as plt
    rows = report['samples']
    fig, ax = plt.subplots(figsize=(9, 4))
    for i, row in enumerate(rows):
        result = row.get('result', {})
        if result.get('correct'):
            ax.scatter(result['elapsed_s'], i, marker='o', color='tab:blue')
        else:
            ax.text(.02, i, row['status'], transform=ax.get_yaxis_transform(), color='tab:red')
    ax.set_yticks(range(len(rows)), [r['label'] for r in rows])
    ax.set_xlabel('Measured driver completion time (s), including supervisor gate')
    ax.set_title('Head VM loss pilot — successful completions only')
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(output, dpi=160)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--plot', type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    summarize(report)
    if args.plot:
        plot(report, args.plot)
