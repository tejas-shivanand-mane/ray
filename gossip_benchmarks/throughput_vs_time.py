import ray
import numpy as np
import time
import os
import csv
import threading
import collections

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"
RESULT_FILE = "/rhome/tmane002/work/ray/gossip_benchmarks/throughput_vs_time.csv"

# ── Ray remote functions ──────────────────────────────────────────────────────

@ray.remote(max_retries=0)
def generate_data(seed):
    time.sleep(0.5)  # simulate work
    np.random.seed(seed)
    return np.random.rand(10, 10)

@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))

@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def __init__(self):
        self.count = 0

    def dispatch(self, seed):
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        return result_ref

    def ping(self):
        return os.getpid()

# ── Throughput tracker ────────────────────────────────────────────────────────

class ThroughputTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.completions = collections.deque()  # timestamps of completions
        self.start_time = time.time()
        self.records = []  # (elapsed_s, throughput_per_s)
        self.running = True

    def record_completion(self):
        with self.lock:
            self.completions.append(time.time())

    def monitor(self, interval=1.0):
        """Background thread: compute throughput every interval seconds."""
        while self.running:
            time.sleep(interval)
            now = time.time()
            window_start = now - interval
            with self.lock:
                # Count completions in last interval
                count = sum(1 for t in self.completions if t >= window_start)
                elapsed = now - self.start_time
            throughput = count / interval
            self.records.append((round(elapsed, 2), round(throughput, 2)))
            print(f"  t={elapsed:.1f}s  throughput={throughput:.1f} tasks/s")

    def stop(self):
        self.running = False

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ray.init(address="auto", log_to_driver=False)

    # Clean up signal file
    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    # Wait for both workers
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    # Create owner on worker_a
    owner = Owner.remote()
    ray.get(owner.ping.remote())
    print("Owner created on worker_a")

    tracker = ThroughputTracker()

    # Start throughput monitor thread
    monitor_thread = threading.Thread(target=tracker.monitor, daemon=True)
    monitor_thread.start()

    # ── Phase 1: steady state (15s) ──
    print("\nPhase 1: steady state...")
    kill_time = None
    futures = []
    start = time.time()

    seed = 0
    kill_signaled = False

    while time.time() - start < 60:  # run for 60s total
        elapsed = time.time() - start

        # Signal kill at 15s
        if elapsed >= 15 and not kill_signaled:
            print(f"\nPhase 2: signaling kill at t={elapsed:.1f}s...")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True
            kill_time = elapsed

        # Keep submitting tasks
        try:
            result_ref = ray.get(owner.dispatch.remote(seed=seed), timeout=2)
            futures.append((seed, result_ref, time.time()))
            seed += 1
        except Exception as e:
            print(f"  dispatch failed at t={elapsed:.1f}s: {type(e).__name__}")
            time.sleep(0.5)
            continue

        # Collect completed results
        done = []
        for s, ref, submit_time in futures:
            try:
                val = ray.get(ref, timeout=0.01)
                tracker.record_completion()
                done.append((s, ref, submit_time))
            except ray.exceptions.GetTimeoutError:
                pass
            except Exception:
                done.append((s, ref, submit_time))  # remove failed

        for item in done:
            futures.remove(item)

        time.sleep(0.1)

    tracker.stop()

    # Save results
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['elapsed_s', 'throughput', 'kill_time'])
        for elapsed, tp in tracker.records:
            writer.writerow([elapsed, tp, kill_time])

    print(f"\nResults saved to {RESULT_FILE}")
    ray.shutdown()

if __name__ == "__main__":
    main()import ray
import numpy as np
import time
import os
import csv
import threading
import collections

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"
RESULT_FILE = "/rhome/tmane002/work/ray/gossip_benchmarks/throughput_vs_time.csv"

# ── Ray remote functions ──────────────────────────────────────────────────────

@ray.remote(max_retries=0)
def generate_data(seed):
    time.sleep(0.5)  # simulate work
    np.random.seed(seed)
    return np.random.rand(10, 10)

@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))

@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def __init__(self):
        self.count = 0

    def dispatch(self, seed):
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        return result_ref

    def ping(self):
        return os.getpid()

# ── Throughput tracker ────────────────────────────────────────────────────────

class ThroughputTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.completions = collections.deque()  # timestamps of completions
        self.start_time = time.time()
        self.records = []  # (elapsed_s, throughput_per_s)
        self.running = True

    def record_completion(self):
        with self.lock:
            self.completions.append(time.time())

    def monitor(self, interval=1.0):
        """Background thread: compute throughput every interval seconds."""
        while self.running:
            time.sleep(interval)
            now = time.time()
            window_start = now - interval
            with self.lock:
                # Count completions in last interval
                count = sum(1 for t in self.completions if t >= window_start)
                elapsed = now - self.start_time
            throughput = count / interval
            self.records.append((round(elapsed, 2), round(throughput, 2)))
            print(f"  t={elapsed:.1f}s  throughput={throughput:.1f} tasks/s")

    def stop(self):
        self.running = False

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ray.init(address="auto", log_to_driver=False)

    # Clean up signal file
    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    # Wait for both workers
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    # Create owner on worker_a
    owner = Owner.remote()
    ray.get(owner.ping.remote())
    print("Owner created on worker_a")

    tracker = ThroughputTracker()

    # Start throughput monitor thread
    monitor_thread = threading.Thread(target=tracker.monitor, daemon=True)
    monitor_thread.start()

    # ── Phase 1: steady state (15s) ──
    print("\nPhase 1: steady state...")
    kill_time = None
    futures = []
    start = time.time()

    seed = 0
    kill_signaled = False

    while time.time() - start < 60:  # run for 60s total
        elapsed = time.time() - start

        # Signal kill at 15s
        if elapsed >= 15 and not kill_signaled:
            print(f"\nPhase 2: signaling kill at t={elapsed:.1f}s...")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True
            kill_time = elapsed

        # Keep submitting tasks
        try:
            result_ref = ray.get(owner.dispatch.remote(seed=seed), timeout=2)
            futures.append((seed, result_ref, time.time()))
            seed += 1
        except Exception as e:
            print(f"  dispatch failed at t={elapsed:.1f}s: {type(e).__name__}")
            time.sleep(0.5)
            continue

        # Collect completed results
        done = []
        for s, ref, submit_time in futures:
            try:
                val = ray.get(ref, timeout=0.01)
                tracker.record_completion()
                done.append((s, ref, submit_time))
            except ray.exceptions.GetTimeoutError:
                pass
            except Exception:
                done.append((s, ref, submit_time))  # remove failed

        for item in done:
            futures.remove(item)

        time.sleep(0.1)

    tracker.stop()

    # Save results
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['elapsed_s', 'throughput', 'kill_time'])
        for elapsed, tp in tracker.records:
            writer.writerow([elapsed, tp, kill_time])

    print(f"\nResults saved to {RESULT_FILE}")
    ray.shutdown()

if __name__ == "__main__":
    main()