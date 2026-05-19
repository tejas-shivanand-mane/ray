import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True,
                        choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    ray.init(address="auto", log_to_driver=True)

    # Wait for head + worker_a + worker_b
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    # ── Step 1: actor dispatches — generate_data takes 20s ───────────────────
    print("\n" + "="*60)
    print("Step 1: actor dispatching tasks on worker_a...")
    print("="*60)
    owner = Owner.remote()
    ray.get(owner.ping.remote())
    dispatch_start = time.time()
    result_ref = ray.get(owner.dispatch.remote(seed=42))
    print(f"Got result_ref: {result_ref}")
    print(f"generate_data is now running on worker_a (sleeping 20s)...")

    # ── Step 2: signal kill immediately — generate_data still running ─────────
    print("\n" + "="*60)
    print("Step 2: signaling kill of worker_a immediately...")
    print("="*60)
    kill_time = time.time()
    with open(SIGNAL_FILE, "w") as f:
        f.write("kill")
    print(f"Kill signal written at t={kill_time - dispatch_start:.1f}s")
    print("generate_data is mid-execution on worker_a — it will die")
    print("Waiting 10s for worker_a to die...")
    time.sleep(10)

    # ── Step 3: access result ─────────────────────────────────────────────────
    print("\n" + "="*60)
    print("Step 3: accessing result after worker_a killed...")
    print("="*60)
    print("Without gossip: OwnerDiedError — result permanently lost")
    print("With gossip:    generate_data resubmitted on a NEW node")
    print("Watch for [generate_data] EXECUTING AGAIN on a different pid")
    print("Waiting for result (timeout=90s)...")

    success       = False
    recovery_time = None
    total_time    = None

    try:
        val      = ray.get(result_ref, timeout=90)
        end_time = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time

        print(f"\nResult: {val:.2f}")
        print("\n" + "="*60)
        print("PASS — node failure recovery worked!")
        print(f"  System:        {args.system}")
        print(f"  Total time:    {total_time:.2f}s")
        print(f"  Recovery time: {recovery_time:.2f}s "
              f"(from kill signal to result)")
        print("="*60)
        success = True

    except ray.exceptions.OwnerDiedError:
        end_time      = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time
        print("\n" + "="*60)
        print("FAIL — OwnerDiedError")
        print(f"  System: {args.system}")
        print(f"  No gossip recovery — result permanently lost")
        print("="*60)

    except Exception as e:
        end_time      = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time
        print("\n" + "="*60)
        print(f"FAIL — {type(e).__name__}: {e}")
        print("="*60)

    # ── Save result ───────────────────────────────────────────────────────────
    file_exists = os.path.exists(args.output)
    with open(args.output, 'a') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['system', 'total_time',
                             'recovery_time', 'success'])
        writer.writerow([args.system,
                         round(total_time, 3),
                         round(recovery_time, 3),
                         success])

    print(f"\nResult saved to {args.output}")
    ray.shutdown()


if __name__ == "__main__":
    main()