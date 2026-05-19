import ray
import numpy as np
import time
import os
import csv
import threading
import collections

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE  = "/rhome/tmane002/ready_to_kill.txt"
RESULT_FILE  = "/rhome/tmane002/thput_lat_with_gossip.csv"
INTERVAL     = 1.0    # sampling interval in seconds
TOTAL_TIME   = 120    # total experiment duration in seconds
KILL_AT      = 60     # kill owner at this elapsed time
TASK_SLEEP   = 0.2    # simulate work per task (seconds)
BATCH_SIZE   = 500    # tasks pre-dispatched before kill


# ── Ray remote functions ──────────────────────────────────────────────────────

@ray.remote(max_retries=0)
def generate_data(seed):
    time.sleep(TASK_SLEEP)
    np.random.seed(seed)
    return np.random.rand(10, 10)

@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))

@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch_batch(self, seeds):
        """
        Pre-dispatch a batch of tasks and return all result refs.
        This matches the gossip recovery scenario: owner dispatches,
        then dies — gossip must recover the pending refs.
        """
        result_refs = []
        for seed in seeds:
            ref = generate_data.remote(seed=seed)
            result_ref = compute_sum.remote(ref)
            result_refs.append(result_ref)
        return result_refs

    def ping(self):
        return os.getpid()


# ── Throughput + latency tracker ──────────────────────────────────────────────

class Tracker:
    """Records per-interval throughput and average latency."""

    def __init__(self):
        self.lock        = threading.Lock()
        self.completions = []   # list of (finish_time, latency_s)
        self.start_time  = time.time()
        self.records     = []   # (elapsed_s, throughput, avg_latency_ms)
        self.running     = True

    def record(self, latency_s):
        with self.lock:
            self.completions.append((time.time(), latency_s))

    def monitor(self):
        """Background thread: compute throughput/latency every INTERVAL seconds."""
        while self.running:
            time.sleep(INTERVAL)
            now          = time.time()
            window_start = now - INTERVAL
            elapsed      = round(now - self.start_time, 2)

            with self.lock:
                window = [(t, l) for t, l in self.completions
                          if t >= window_start]

            if window:
                throughput  = len(window) / INTERVAL
                avg_latency = np.mean([l for _, l in window]) * 1000  # ms
            else:
                throughput  = 0.0
                avg_latency = float('nan')

            self.records.append((elapsed,
                                  round(throughput, 2),
                                  round(avg_latency, 2)))
            print(f"  t={elapsed:6.1f}s  "
                  f"throughput={throughput:6.1f} tasks/s  "
                  f"latency={avg_latency:7.1f} ms")

    def stop(self):
        self.running = False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ray.init(address="auto", log_to_driver=False)

    # Clean up signal file from any previous run
    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    # Wait for head + worker_a + worker_b
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    # Create owner on worker_a
    owner = Owner.remote()
    ray.get(owner.ping.remote())
    print("Owner ready on worker_a")

    # ── Pre-dispatch ALL tasks before kill ────────────────────────────────────
    # Matches gossip recovery scenario exactly:
    #   1. Owner dispatches tasks → result_refs created and owned by worker_a
    #   2. Owner (worker_a) killed mid-experiment
    #   3. Without gossip: refs → OwnerDiedError → throughput=0
    #   4. With gossip: gossip resubmits pending tasks → throughput recovers
    print(f"\nPre-dispatching {BATCH_SIZE} tasks via owner on worker_a...")
    dispatch_start = time.time()
    all_result_refs = ray.get(
        owner.dispatch_batch.remote(list(range(BATCH_SIZE))))
    print(f"Dispatched {len(all_result_refs)} tasks "
          f"in {time.time() - dispatch_start:.1f}s")

    # Record submit time for latency measurement
    submit_time = time.time()
    futures = {ref: submit_time for ref in all_result_refs}

    # ── Start tracker ─────────────────────────────────────────────────────────
    tracker = Tracker()
    monitor_thread = threading.Thread(target=tracker.monitor, daemon=True)
    monitor_thread.start()

    start         = time.time()
    kill_signaled = False

    print(f"\nCollecting results for {TOTAL_TIME}s, "
          f"killing owner at t={KILL_AT}s...\n")

    while time.time() - start < TOTAL_TIME:
        elapsed = time.time() - start

        # Signal kill at KILL_AT seconds
        if elapsed >= KILL_AT and not kill_signaled:
            print(f"\n>>> Signaling kill at t={elapsed:.1f}s <<<\n")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # Collect completed futures (non-blocking polling)
        done_refs = []
        for ref, ref_submit_time in list(futures.items()):
            try:
                ray.get(ref, timeout=0.01)
                latency = time.time() - ref_submit_time
                tracker.record(latency)
                done_refs.append(ref)

            except ray.exceptions.GetTimeoutError:
                # Still pending — check again next loop
                pass

            except ray.exceptions.OwnerDiedError:
                # Owner died and no gossip recovery — ref permanently lost
                print(f"  OwnerDiedError — ref lost (no gossip recovery)")
                done_refs.append(ref)

            except ray.exceptions.RayActorError:
                # Actor (owner) crashed
                done_refs.append(ref)

            except Exception as e:
                print(f"  Unexpected error: {type(e).__name__}: {e}")
                done_refs.append(ref)

        for ref in done_refs:
            futures.pop(ref, None)

        # Stop early if all futures resolved
        if not futures:
            print(f"All {BATCH_SIZE} futures resolved at t={elapsed:.1f}s")
            break

        time.sleep(0.05)

    tracker.stop()
    monitor_thread.join(timeout=2)

    remaining = len(futures)
    if remaining > 0:
        print(f"{remaining} futures unresolved at end of experiment "
              f"(expected for no-gossip run)")

    # ── Save CSV ──────────────────────────────────────────────────────────────
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['elapsed_s', 'throughput', 'latency_ms', 'kill_time'])
        for elapsed, tp, lat in tracker.records:
            writer.writerow([elapsed, tp, lat, KILL_AT])

    print(f"\nSaved {len(tracker.records)} records to {RESULT_FILE}")
    ray.shutdown()


if __name__ == "__main__":
    main()