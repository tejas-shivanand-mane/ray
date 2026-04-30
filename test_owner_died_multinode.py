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

# Step 1: actor dispatches both tasks and returns result_ref
print("Step 1: actor dispatching tasks...")
owner = Owner.remote()
ray.get(owner.ping.remote())
result_ref = ray.get(owner.dispatch.remote(seed=42))
print(f"Got result_ref: {result_ref}")

# Step 2: immediately kill actor before compute_sum completes
# compute_sum needs generate_data's result — actor still owns it
print("Step 2: immediately killing actor...")
ray.kill(owner)
print("Actor killed")
time.sleep(3)

# Step 3: get result
print("Step 3: getting result after actor killed...")
print("Watch for GOSSIP_RECOVER in logs")
try:
    val = ray.get(result_ref, timeout=60)
    print(f"Result: {val:.2f}")
    print("PASS — gossip recovery worked!")
except ray.exceptions.OwnerDiedError:
    print("FAIL — OwnerDiedError")
except Exception as e:
    print(f"FAIL — {type(e).__name__}: {e}")

ray.shutdown()
