"""Seekable compressed text shards with deterministic online preprocessing."""
import gzip
import hashlib
import json
from pathlib import Path
import re
import time
import zlib


def load_manifest(path, steps, batch_size):
    path = Path(path).expanduser().resolve()
    manifest = json.loads(path.read_text())
    if (manifest.get('format') != 'text-input-v1' or manifest['batch_size'] != batch_size
            or manifest['steps'] != steps or manifest['world_size'] != 2):
        raise ValueError('Prepared text manifest must match steps, batch-size and two workers')
    if len(manifest['chunks']) != steps * 2 or manifest['num_classes'] < 2:
        raise ValueError('Invalid text manifest')
    # Fail before launching Ray if preparation is missing or damaged.
    for chunk in manifest['chunks']:
        payload = (path.parent / chunk['file']).read_bytes()
        if hashlib.sha256(payload).hexdigest() != chunk['sha256']:
            raise ValueError(f'Compressed source checksum mismatch: {chunk["file"]}')
    return manifest, path.parent


def digest(batch):
    import numpy as np
    h = hashlib.sha256()
    for key in ('id', 'x', 'y'):
        h.update(np.ascontiguousarray(batch[key]).tobytes())
    return h.hexdigest()


def read_batch(root, manifest, chunk_index):
    import numpy as np
    started = time.monotonic()
    chunk = manifest['chunks'][chunk_index]
    payload = (Path(root) / chunk['file']).read_bytes()
    records = [json.loads(line) for line in gzip.decompress(payload).splitlines()]
    x = np.zeros((len(records), 256), dtype=np.float32)
    tokens_count = 0
    for row, record in enumerate(records):
        tokens = re.findall(r'\b\w+\b', record['text'].lower())
        tokens_count += len(tokens)
        # Actual tokenization and unigram/bigram feature hashing, with no sleeps
        # or repeated dummy computation to manufacture an expensive pipeline.
        for token in tokens:
            x[row, zlib.crc32(token.encode('utf-8')) % 256] += 1
        for left, right in zip(tokens, tokens[1:]):
            x[row, zlib.crc32((left + '\x00' + right).encode('utf-8')) % 256] += 1
    np.log1p(x, out=x)
    x /= np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)
    batch = {'id': np.asarray([r['id'] for r in records], dtype=np.int64),
             'x': x, 'y': np.asarray([r['label'] for r in records], dtype=np.int64)}
    expected = list(range(chunk_index * manifest['batch_size'],
                          (chunk_index + 1) * manifest['batch_size']))
    if batch['id'].tolist() != expected:
        raise ValueError('Source chunk IDs do not match its cursor')
    event = {'kind': 'preprocess', 'chunk': chunk_index, 'records': len(records),
             'tokens': tokens_count, 'compressed_bytes': len(payload),
             'seconds': time.monotonic() - started}
    return batch, event
