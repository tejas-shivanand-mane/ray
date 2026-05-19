import ray
import numpy as np
import time
import os
import csv
import threading

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE  = "/rhome/tmane002/ready_to_kill.txt"
RESULT_FILE  = "/rhome/tmane002/results/thput_lat_with_gossip.csv"

INTERVAL     = 1.0    # sampling interval in seconds
TOTAL_TIME   = 120    # total experiment duration in seconds
KILL_AT      = 60     # kill owner at this elapsed time in seconds
TASK_SLEEP   = 3.0    # seconds per task — long enough to be in-flight at kill
N_PARALLEL   = 16     # concurrent tasks to keep in flight at all times
              # steady throughput = N_PARALLEL / TASK_SLEEP ≈ 5 tasks/s


# ── Ray remote functions ──────────────────────────────────────────────────────

@ray.remote(max_retries=0)
def generate_data(seed):
    time.sleep(TASK_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(10, 10)

@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))

@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch(self, seed):
        """Dispatch one task pipeline and return result ref."""
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        return result_ref

    def ping(self):
        return os.getpid()


# ── Tracker ───────────────────────────────────────────────────────────────────

class Tracker:
    def __init__(self):
        self.lock        = threading.Lock()
        self.completions = []   # (finish_time, latency_s)
        self.start_time  = time.time()
        self.records     = []   # (elapsed_s, throughput, avg_latency_ms)
        self.running     = True
        self._in_flight  = 0

    def record(self, latency_s):
        with self.lock:
            self.completions.append((time.time(), latency_s))

    def set_in_flight(self, n):
        self._in_flight = n

    def monitor(self):
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
                avg_latency = np.mean([l for _, l in window]) * 1000
            else:
                throughput  = 0.0
                avg_latency = float('nan')
            self.records.append((elapsed,
                                  round(throughput, 2),
                                  round(avg_latency, 2)))
            print(f"  t={elapsed:6.1f}s  "
                  f"throughput={throughput:5.1f} tasks/s  "
                  f"latency={avg_latency:7.1f} ms  "
                  f"in_flight={self._in_flight}")

    def stop(self):
        self.running = False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ray.init(address="auto", log_to_driver=False)

    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    # Wait for head + worker_a + worker_b
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    owner = Owner.remote()
    ray.get(owner.ping.remote())
    print(f"Owner ready on worker_a")
    print(f"Config: TASK_SLEEP={TASK_SLEEP}s, N_PARALLEL={N_PARALLEL}, "
          f"expected steady throughput ~{N_PARALLEL/TASK_SLEEP:.1f} tasks/s")

    tracker = Tracker()
    monitor_thread = threading.Thread(target=tracker.monitor, daemon=True)
    monitor_thread.start()

    # futures: ref -> submit_time
    futures    = {}
    seed       = 0
    start      = time.time()
    kill_signaled = False
    owner_dead    = False

    print(f"\nRunning {TOTAL_TIME}s, killing owner at t={KILL_AT}s, "
          f"maintaining {N_PARALLEL} tasks in flight...\n")

    while time.time() - start < TOTAL_TIME:
        elapsed = time.time() - start

        # ── Signal kill ───────────────────────────────────────────────────────
        if elapsed >= KILL_AT and not kill_signaled:
            print(f"\n>>> Signaling kill at t={elapsed:.1f}s "
                  f"({len(futures)} tasks in flight) <<<\n")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # ── Collect completed futures (non-blocking) ──────────────────────────
        done_refs = []
        for ref, submit_t in list(futures.items()):
            try:
                ray.get(ref, timeout=0.001)
                tracker.record(time.time() - submit_t)
                done_refs.append(ref)
            except ray.exceptions.GetTimeoutError:
                pass  # still pending
            except ray.exceptions.OwnerDiedError:
                # No gossip recovery — ref permanently lost
                done_refs.append(ref)
            except ray.exceptions.RayActorError:
                owner_dead = True
                done_refs.append(ref)
            except Exception:
                done_refs.append(ref)

        for ref in done_refs:
            futures.pop(ref, None)

        tracker.set_in_flight(len(futures))

        # ── Dispatch new tasks to maintain N_PARALLEL in flight ───────────────
        # Only dispatch while owner is alive
        while len(futures) < N_PARALLEL and not owner_dead:
            try:
                submit_t   = time.time()
                result_ref = ray.get(
                    owner.dispatch.remote(seed), timeout=1.0)
                futures[result_ref] = submit_t
                seed += 1
            except ray.exceptions.RayActorError:
                print(f"  Owner died at t={elapsed:.1f}s — "
                      f"{len(futures)} tasks still in flight")
                owner_dead = True
                break
            except ray.exceptions.OwnerDiedError:
                owner_dead = True
                break
            except Exception:
                break

        time.sleep(0.02)

    tracker.stop()
    monitor_thread.join(timeout=2)

    unresolved = len(futures)
    if unresolved:
        print(f"\n{unresolved} futures unresolved at end — "
              f"expected for no-gossip run (owner died, refs lost)")
    else:
        print(f"\nAll futures resolved — "
              f"gossip recovery succeeded!")

    # Save CSV
    with open(RESULT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['elapsed_s', 'throughput', 'latency_ms', 'kill_time'])
        for elapsed, tp, lat in tracker.records:
            writer.writerow([elapsed, tp, lat, KILL_AT])

    print(f"Saved {len(tracker.records)} records to {RESULT_FILE}")
    ray.shutdown()


if __name__ == "__main__":
    main()