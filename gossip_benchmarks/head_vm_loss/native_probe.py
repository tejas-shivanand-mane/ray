"""Bounded probes and native log collection; never starts or resets Ray."""
import json
import sys
import time


def identity(address):
    from ray._raylet import GcsClient
    from ray._private import ray_constants
    client = GcsClient(address=address)
    session = client.internal_kv_get(b'session_name', ray_constants.KV_NAMESPACE_SESSION, timeout=5)
    if not session:
        raise RuntimeError('No session name yet')
    return dict(cluster_id=client.cluster_id.hex(), session=session.decode(),
        nodes=[dict(id=nid.hex(), ip=n.node_manager_address, alive=n.state == 0,
                    head=n.is_head_node)
               for nid, n in client.get_all_node_info(timeout=5).items()])


def logs(destination):
    from pathlib import Path
    from evidence import write_json
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    source = Path('/tmp/ray/session_latest/logs').resolve()
    candidates = sorted(set(p for pattern in ('raylet*', 'gcs_server*', 'python-core-worker*',
        'python-core-driver*', 'worker-*.err') for p in source.glob(pattern) if p.is_file()))
    manifest = dict(source=str(source), source_exists=source.is_dir(),
                    files=[], limit_bytes_per_file=65536)
    for path in candidates[:128]:
        try:
            with path.open('rb') as stream:
                stream.seek(0, 2)
                size = stream.tell()
                stream.seek(max(0, size-65536))
                (target/path.name).write_bytes(stream.read())
            manifest['files'].append(dict(name=path.name, total_bytes=size))
        except OSError as exc:
            manifest['files'].append(dict(name=path.name, error=str(exc)))
    manifest['omitted_files'] = max(0, len(candidates)-128)
    write_json(target/'manifest.json', manifest)
    return manifest


def counters():
    import psutil
    import shutil
    from pathlib import Path
    return dict(unix_s=time.time(), cpu=psutil.cpu_times()._asdict(),
        network={k: v._asdict() for k, v in psutil.net_io_counters(pernic=True).items()},
        disk={k: v._asdict() for k, v in psutil.disk_io_counters(perdisk=True).items()},
        shared_bytes=sum(p.stat().st_size for p in Path('/srv/ray-headloss').rglob('*') if p.is_file()),
        free_bytes=shutil.disk_usage('/srv/ray-headloss').free,
        scope='whole VM cumulative counters, includes instrumentation and other activity')


if __name__ == '__main__':
    if sys.argv[1] == 'identity':
        result = identity(sys.argv[2])
    elif sys.argv[1] == 'logs':
        result = logs(sys.argv[2])
    elif sys.argv[1] == 'counters':
        result = counters()
    else:
        raise ValueError('Unknown probe operation')
    print(json.dumps(result))
