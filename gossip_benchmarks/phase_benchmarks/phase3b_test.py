import time

import ray
from ray.cluster_utils import Cluster


cluster = Cluster()

cluster.add_node(
    num_cpus=1,
    resources={"head_node": 1},
    _system_config={
        "enable_recovery_succession": False,
    },
)

cluster.add_node(
    num_cpus=1,
    resources={"producer_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"consumer_node": 1},
)

ray.init(address=cluster.address)


@ray.remote(max_retries=1)
def produce():
    return 21


@ray.remote(max_retries=1)
def consume(value):
    return value * 2


produced = produce.options(
    resources={"producer_node": 0.01},
).remote()

consumed = consume.options(
    resources={"consumer_node": 0.01},
).remote(produced)

assert ray.get(consumed) == 42

# Allow asynchronous manifest-commit RPCs to finish.
time.sleep(2)

print("Phase 3B task execution passed.")

ray.shutdown()
cluster.shutdown()