import ray
import numpy as np
import time
import os
import csv
import threading
import collections

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE   = "/rhome/tmane002/ready_to_kill.txt"
RESULT_FILE   = "/rhome/tmane002/results/throughput_latency_vs_time.csv"
INTERVAL      = 1.0   # sampling interval in seconds
TOTAL_TIME    = 120   # total experiment duration
KILL_AT       = 60    # kill owner at this elapsed time
TASK_SLEEP    = 0.2   # simulate work per task

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
    def dispatch(self, seed):
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        return result_ref

    def ping(self):
        return os.getpid()


class Tracker:
    """Records per-interval throughput and latency."""
    def __init__(self):
        self.lock = threading.Lock()
        self.completions = []  # list of (finish_time, latency_s)
        self.start_time = time.time()
        self.records = []      # (elapsed_s, throughput, avg_latency_ms)
        self.running = True

    def record(self, latency_s):
        with self.lock:
            self.completions.append((time.time(), latency_s))

    def monitor(self):
        while self.running:
            time.sleep(INTERVAL)
            now = time.time()
            window_start = now - INTERVAL
            elapsed = round(now - self.start_time, 2)
            with self.lock:
                window = [(t, l) for t, l in self.completions
                          if t >= window_start]
            if window:
                throughput   = len(window) / INTERVAL
                avg_latency  = np.mean([l for _, l in window]) * 1000  # ms
            else:
                throughput  = 0.0
                avg_latency = float('nan')
            self.records.append((elapsed, round(throughput, 2),
                                 round(avg_latency, 2)))
            print(f"  t={elapsed:6.1f}s  "
                  f"throughput={throughput:6.1f} tasks/s  "
                  f"latency={avg_latency:7.1f} ms")

    def stop(self):
        self.running = False


def main():
    ray.init(address="auto", log_to_driver=False)

    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    # Wait for head + 2 workers
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    owner = Owner.remote()
    ray.get(owner.ping.remote())
    print("Owner ready on worker_a")

    tracker = Tracker()
    monitor_thread = threading.Thread(target=tracker.monitor, daemon=True)
    monitor_thread.start()

    start      = time.time()
    seed       = 0
    kill_signaled = False
    futures    = {}   # ref -> submit_time

    print(f"\nRunning for {TOTAL_TIME}s, kill at {KILL_AT}s...\n")

    while time.time() - start < TOTAL_TIME:
        elapsed = time.time() - start

        # Signal kill at KILL_AT seconds
        if elapsed >= KILL_AT and not kill_signaled:
            print(f"\n>>> Signaling kill at t={elapsed:.1f}s <<<\n")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # Submit new task
        try:
            submit_time = time.time()
            result_ref = ray.get(
                owner.dispatch.remote(seed=seed), timeout=2)
            futures[result_ref] = submit_time
            seed += 1
        except ray.exceptions.OwnerDiedError:
            print(f"  OwnerDiedError at t={elapsed:.1f}s — owner dead, no gossip recovery")
            # Keep running to record zeros for the rest of the experiment
            time.sleep(0.2)
            continue
        except ray.exceptions.RayActorError:
            print(f"  RayActorError at t={elapsed:.1f}s — actor dead")
            time.sleep(0.2)
            continue
        except Exception as e:
            time.sleep(0.2)
            continue

        # Collect completed futures (non-blocking)
        done_refs = []
        for ref, submit_time in list(futures.items()):
            try:
                ray.get(ref, timeout=0.01)
                latency = time.time() - submit_time
                tracker.record(latency)
                done_refs.append(ref)
            except ray.exceptions.GetTimeoutError:
                pass
            except ray.exceptions.OwnerDiedError:
                print(f"  future OwnerDiedError — dropping ref")
                done_refs.append(ref)
            except Exception:
                done_refs.append(ref)  # remove failed refs

        for ref in done_refs:
            futures.pop(ref, None)

        time.sleep(0.05)

    tracker.stop()
    monitor_thread.join(timeout=2)

    # Save CSV
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['elapsed_s', 'throughput', 'latency_ms', 'kill_time'])
        for elapsed, tp, lat in tracker.records:
            writer.writerow([elapsed, tp, lat, KILL_AT])

    print(f"\nSaved {len(tracker.records)} records to {RESULT_FILE}")
    ray.shutdown()


if __name__ == "__main__":
    main()