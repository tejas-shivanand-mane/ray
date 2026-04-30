import ray
import numpy as np
import time
import os

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

ray.init(address="auto", log_to_driver=True)

@ray.remote(max_retries=0)
def generate_data(seed):
    print(f"[generate_data] EXECUTING seed={seed} pid={os.getpid()}")
    time.sleep(20)  # Slow — still running when node dies
    np.random.seed(seed)
    return np.random.rand(100, 100)

@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    result = float(np.sum(data))
    print(f"[compute_sum] sum={result:.2f} pid={os.getpid()}")
    return result

@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch(self, seed):
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        print(f"[Owner] dispatched pid={os.getpid()}")
        return result_ref

    def ping(self):
        return os.getpid()

# Step 1: actor dispatches — generate_data takes 20s
print("Step 1: actor dispatching tasks on worker_a...")
owner = Owner.remote()
ray.get(owner.ping.remote())
result_ref = ray.get(owner.dispatch.remote(seed=42))
print(f"Got result_ref: {result_ref}")

# Step 2: signal kill immediately — generate_data still running
print("Step 2: signaling docker stop worker_a immediately...")
with open("/tmp/ready_to_kill.txt", "w") as f:
    f.write("kill")
print("Waiting 10s for worker_a container to die...")
time.sleep(10)

# Step 3: access result
# generate_data never completed on worker_a
# gossip recovery must resubmit and run on different node
print("Step 3: accessing result after worker_a node killed...")
print("Watch for [generate_data] EXECUTING AGAIN on different pid")
try:
    val = ray.get(result_ref, timeout=90)
    print(f"Result: {val:.2f}")
    print("PASS — node failure recovery worked!")
except ray.exceptions.OwnerDiedError:
    print("FAIL — OwnerDiedError")
except Exception as e:
    print(f"FAIL — {type(e).__name__}: {e}")

ray.shutdown()
