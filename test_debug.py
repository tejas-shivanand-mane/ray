import ray
import numpy as np
import time
import os

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

ray.init(address="ray://127.0.0.1:10001", log_to_driver=True)

passed = []
failed = []

def check(name, result, expected=True):
    if result == expected:
        print(f"  ✓ PASS: {name}")
        passed.append(name)
    else:
        print(f"  ✗ FAIL: {name}")
        failed.append(name)

@ray.remote(resources={"worker_a": 1})
class Owner:
    def create(self):
        data = np.random.rand(100, 100)
        ref = ray.put(data)
        print(f"[Owner] Created pid={os.getpid()}")
        return ref

@ray.remote(resources={"worker_b": 1})
def use(data):
    result = float(np.sum(data))
    print(f"[use] Sum={result:.2f}")
    return result

# Test 1
print("=== TEST 1 ===")
owner = Owner.remote()
ref = ray.get(owner.create.remote())
val_before = ray.get(use.remote(ref))
print(f"Before kill: {val_before:.2f}")

# Kill and recover
print("Killing owner...")
ray.kill(owner)
time.sleep(5)

print("Accessing after kill...")
try:
    val_after = ray.get(use.remote(ref))
    print(f"After kill: {val_after:.2f}")
    check("Object recovered", True)
except ray.exceptions.OwnerDiedError:
    check("Object recovered", False)
    print("OwnerDiedError")
except Exception as e:
    print(f"Other error: {type(e).__name__}: {e}")

# Test 2 — does a second Owner after kill work?
print("\n=== TEST 2 ===")
owner2 = Owner.remote()
ref2 = ray.get(owner2.create.remote())
val2 = ray.get(use.remote(ref2))
print(f"Test 2 value: {val2:.2f}")
check("Second owner works", val2 > 0)
ray.kill(owner2)

print(f"\nPassed: {passed}")
print(f"Failed: {failed}")

ray.shutdown()
