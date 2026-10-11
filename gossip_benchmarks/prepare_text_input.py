"""Prepare raw compressed text batches; tokenization happens during training.

Default source: scikit-learn's 20 Newsgroups training split (downloaded on demand).
Alternatively provide JSONL records with text and integer label fields.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import random


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-directory', type=Path, required=True)
    parser.add_argument('--jsonl', type=Path)
    parser.add_argument('--steps', type=int, default=120)
    parser.add_argument('--batch-size', type=int, default=32)
    args = parser.parse_args()
    if min(args.steps, args.batch_size) <= 0:
        parser.error('Steps and batch size must be positive')
    directory = args.output_directory.expanduser().resolve()
    if directory.exists():
        parser.error('Use a fresh preparation directory')
    if args.jsonl:
        records = [json.loads(line) for line in args.jsonl.expanduser().read_text().splitlines() if line.strip()]
        source = 'user JSONL'
    else:
        from sklearn.datasets import fetch_20newsgroups
        data = fetch_20newsgroups(subset='train', remove=('headers', 'footers', 'quotes'))
        records = [{'text': t, 'label': int(y)} for t, y in zip(data.data, data.target)]
        source = '20 Newsgroups train, headers/footers/quotes removed'
    if any(not isinstance(r.get('text'), str) or type(r.get('label')) is not int
           or r['label'] < 0 for r in records):
        parser.error('Each record must have string text and nonnegative integer label')
    labels = sorted({r['label'] for r in records})
    if labels != list(range(len(labels))) or len(labels) < 2:
        parser.error('Labels must be contiguous integers starting at zero, with at least two classes')
    needed = args.steps * 2 * args.batch_size
    if len(records) < needed:
        parser.error(f'Require {needed} records; found {len(records)}. Reduce steps/batch size; no implicit repetition')
    random.Random(42).shuffle(records)
    directory.mkdir(parents=True)
    manifest = {'format': 'text-input-v1', 'source': source, 'steps': args.steps,
                'batch_size': args.batch_size, 'world_size': 2, 'num_classes': len(labels),
                'shuffle_seed': 42, 'records': needed, 'chunks': []}
    for chunk_index in range(args.steps * 2):
        start = chunk_index * args.batch_size
        raw = ''.join(json.dumps({'id': i, 'text': records[i]['text'], 'label': records[i]['label']}) + '\n'
                      for i in range(start, start + args.batch_size)).encode('utf-8')
        payload = gzip.compress(raw, mtime=0)
        name = f'chunk-{chunk_index:06d}.jsonl.gz'
        (directory / name).write_bytes(payload)
        manifest['chunks'].append({'file': name, 'sha256': hashlib.sha256(payload).hexdigest()})
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(directory / 'manifest.json')


if __name__ == '__main__':
    main()
