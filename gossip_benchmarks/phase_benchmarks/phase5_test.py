import time

import ray
from ray.cluster_utils import Cluster


cluster = Cluster()

cluster.add_node(
    num_cpus=1,
    resources={"head_node": 1},
    _system_config={
        "enable_recovery_succession": True,
        "recovery_succession_witness_count": 2,
        "object_timeout_milliseconds": 200,
    },
)

cluster.add_node(
    num_cpus=1,
    resources={"owner_node": 1},
)

producer_node = cluster.add_node(
    num_cpus=1,
    resources={"producer_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"holder_a_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"holder_b_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"borrower_node": 1},
)

ray.init(address=cluster.address)


@ray.remote(max_retries=2)
def produce():
    return 42


@ray.remote(
    resources={"owner_node": 0.01},
    max_restarts=0,
)
class Owner:
    def create(self):
        ref = produce.options(
            resources={"producer_node": 0.01},
        ).remote()

        # Keep the ObjectRef nested so Ray does not resolve it.
        return [ref]


@ray.remote(
    resources={"holder_a_node": 0.01},
)
class HolderA:
    def borrow(self, wrapped_ref):
        return ray.get(wrapped_ref[0])


@ray.remote(
    resources={"holder_b_node": 0.01},
)
class HolderB:
    def borrow(self, wrapped_ref):
        return ray.get(wrapped_ref[0])


@ray.remote(
    resources={"borrower_node": 0.01},
)
class Borrower:
    def hold(self, wrapped_ref):
        self.ref = wrapped_ref[0]
        return True

    def read(self):
        return ray.get(self.ref)


try:
    owner = Owner.remote()

    nested = ray.get(owner.create.remote())
    produced_ref = nested[0]

    assert ray.get(produced_ref) == 42

    holder_a = HolderA.remote()
    holder_b = HolderB.remote()
    borrower = Borrower.remote()

    assert ray.get(
        holder_a.borrow.remote([produced_ref])
    ) == 42

    assert ray.get(
        holder_b.borrow.remote([produced_ref])
    ) == 42

    assert ray.get(
        borrower.hold.remote([produced_ref])
    )

    # Allow holder admission and witness publication to finish.
    time.sleep(5)

    ray.kill(owner, no_restart=True)

    cluster.remove_node(
        producer_node,
        allow_graceful=True,
    )

    result = ray.get(
        borrower.read.remote(),
        timeout=30,
    )

    assert result == 42

    print("Phase 5 owner-death recovery passed.")

finally:
    ray.shutdown()
    cluster.shutdown()