import argparse
import json
import os
import csv
import socket
import time
import datetime
import ray
import numpy as np

NODES_PER_DRIVER = 4
CHAIN_LENGTH = 1
TASKS_PER_NODE_PER_BATCH = 800

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
    # HPCC-compatible IP detection
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def timeit(fn, trials=1, multiplier=1):
    # Warmup
    start = time.time()
    fn()
    print("Warmup finished in", time.time() - start)

    stats = []
    for i in range(trials):
        start = time.time()
        fn()
        end = time.time()
        elapsed = end - start
        throughput = multiplier / elapsed
        print(f"Trial {i+1}/{trials}: {elapsed:.2f}s, throughput: {throughput:.1f} tasks/s")
        stats.append(throughput)

    print(f"Avg throughput: {round(np.mean(stats), 2)} +- {round(np.std(stats), 2)} tasks/s")

    if args.output:
        file_exists = os.path.exists(args.output)
        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'arg_size', 'colocated', 'num_nodes', 'throughput']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for throughput in stats:
                writer.writerow({
                    'system': args.system,
                    'arg_size': args.arg_size,
                    'colocated': args.colocated,
                    'num_nodes': args.num_nodes,
                    'throughput': throughput,
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

    local_ip = get_local_ip_address()
    print(f"Driver local IP: {local_ip}")

    node_ids = get_node_ids(local_ip)
    target_nodes = args.num_nodes

    # Wait for enough worker nodes
    wait_count = 0
    while len(node_ids) < target_nodes:
        print(f"{len(node_ids)} / {target_nodes} worker nodes joined, waiting 5s...")
        time.sleep(5)
        node_ids = get_node_ids(local_ip)
        wait_count += 1
        if wait_count > 60:
            print(f"Timeout waiting for nodes. Have {len(node_ids)}, need {target_nodes}")
            raise RuntimeError("Not enough nodes joined")

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
        multiplier=len(worker_node_ids) * TASKS_PER_NODE_PER_BATCH * CHAIN_LENGTH
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
