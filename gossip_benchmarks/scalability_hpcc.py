import argparse
import os
import csv
import socket
import time
import ray
import numpy as np

NODES_PER_DRIVER = 4
CHAIN_LENGTH = 2
TASKS_PER_NODE_PER_BATCH = 2000

OWNERSHIP = "ownership"
SMALL_ARG = "small"
LARGE_ARG = "large"


def get_node_ids(local_ip):
    node_resources = []
    nodes = ray.nodes()
    for node in nodes:
        if not node["Alive"]:
            continue
        for r in node["Resources"]:
            if "node" in r and local_ip not in r:
                node_resources.append(r)
    return node_resources


def get_local_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def timeit(fn, trials=5, multiplier=1):
    # Two warmup runs to stabilize JIT/caching
    for w in range(2):
        start = time.time()
        fn()
        elapsed = time.time() - start
        print(f"Warmup {w+1} finished in {elapsed:.2f}s")
        if w == 0 and elapsed > 30:
            print("Warmup took >30s, skipping second warmup")
            break

    stats = []
    for i in range(trials):
        start = time.time()
        fn()
        end = time.time()
        elapsed = end - start
        throughput = multiplier / elapsed
        print(f"Trial {i+1}/{trials}: {elapsed:.2f}s, throughput: {throughput:.1f} tasks/s")
        stats.append(throughput)
        time.sleep(5 if args.colocated else 2)  # let object store settle between trials

    median_tp = float(np.median(stats))
    std_tp    = float(np.std(stats))
    print(f"Median throughput: {median_tp:.2f} +- {std_tp:.2f} tasks/s")

    if args.output:
        file_exists = os.path.exists(args.output)
        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'arg_size', 'colocated', 'num_nodes', 'throughput', 'std']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'system': args.system,
                'arg_size': args.arg_size,
                'colocated': args.colocated,
                'num_nodes': args.num_nodes,
                'throughput': median_tp,
                'std': std_tp,
            })


@ray.remote
def f_small(*args):
    return b"hi"


@ray.remote
def f_large(*args):
    return np.zeros(1 * 1024 * 1024, dtype=np.uint8)


def do_batch(use_small, node_ids, args=None):
    if args is None:
        args = {node_id: None for node_id in node_ids}
    f = f_small if use_small else f_large
    results = {}
    for node_id in node_ids:
        f_node = f.options(resources={node_id: 0.0001})
        batch = [f_node.remote(args[node_id]) for _ in range(TASKS_PER_NODE_PER_BATCH)]
        results[node_id] = f_node.remote(*batch)
    return results


def main(opts):
    ray.init(address="auto")

    # Reduce batch size for large objects to avoid OOM
    global TASKS_PER_NODE_PER_BATCH
    if args.arg_size == "large":
        TASKS_PER_NODE_PER_BATCH = 200
        print(f"Large objects: reduced TASKS_PER_NODE_PER_BATCH to {TASKS_PER_NODE_PER_BATCH}")

    local_ip = get_local_ip_address()

    print(f"Driver local IP: {local_ip}")

    node_ids = get_node_ids(local_ip)
    target_nodes = args.num_nodes

    wait_count = 0
    while len(node_ids) < target_nodes:
        print(f"{len(node_ids)} / {target_nodes} worker nodes joined, waiting 5s...")
        time.sleep(5)
        node_ids = get_node_ids(local_ip)
        wait_count += 1
        if wait_count > 60:
            raise RuntimeError(f"Timeout: have {len(node_ids)}, need {target_nodes}")

    print(f"All {len(node_ids)} worker nodes joined. Starting benchmark...")
    time.sleep(5)
    node_ids = get_node_ids(local_ip)

    assert args.num_nodes % NODES_PER_DRIVER == 0, \
        f"num_nodes ({args.num_nodes}) must be divisible by NODES_PER_DRIVER ({NODES_PER_DRIVER})"

    num_drivers = args.num_nodes // NODES_PER_DRIVER
    worker_node_ids = list(node_ids)[:args.num_nodes]
    driver_node_ids = list(node_ids)[args.num_nodes:args.num_nodes + num_drivers]

    use_small = args.arg_size == "small"

    @ray.remote(num_cpus=4 if not args.colocated else 0)
    class Driver:
        def __init__(self, node_ids):
            self.node_ids = node_ids

        def do_batch(self):
            prev = None
            for _ in range(CHAIN_LENGTH):
                prev = do_batch(use_small, self.node_ids, args=prev)
            ray.get(list(prev.values()))

        def ready(self):
            pass

    drivers = []
    for i in range(num_drivers):
        if args.colocated:
            node_id = f"node:{local_ip}"
        else:
            node_id = driver_node_ids[i]
        resources = {node_id: 0.001}
        worker_nodes = worker_node_ids[i * NODES_PER_DRIVER:(i + 1) * NODES_PER_DRIVER]
        d = Driver.options(resources=resources).remote(worker_nodes)
        ray.get(d.ready.remote())
        drivers.append(d)

    timeit(
        lambda: ray.get([d.do_batch.remote() for d in drivers]),
        multiplier=len(worker_node_ids) * TASKS_PER_NODE_PER_BATCH * CHAIN_LENGTH, trials=7 if args.colocated else 5
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--arg-size", type=str, required=True)
    parser.add_argument("--num-nodes", type=int, required=True)
    parser.add_argument("--colocated", type=str, required=True)
    parser.add_argument("--system", type=str, default="ownership")
    parser.add_argument("--output", type=str, required=False)
    args = parser.parse_args()
    args.colocated = args.colocated.lower() == "true"
    print(f"Running: system={args.system}, arg_size={args.arg_size}, "
          f"num_nodes={args.num_nodes}, colocated={args.colocated}")
    main(args)