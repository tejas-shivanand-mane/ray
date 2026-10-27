"""Bounded by its caller's SSH timeout; read-only identity/counter probes."""
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
        nodes=[dict(id=nid.hex(), ip=n.node_manager_address, alive=n.state == 0)
               for nid, n in client.get_all_node_info(timeout=5).items()])


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
    print(json.dumps(identity(sys.argv[2]) if sys.argv[1] == 'identity' else counters()))
