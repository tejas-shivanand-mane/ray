import ray
import numpy as np
import time
import os
import csv
import argparse
import threading

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"
TASK_SLEEP  = 20   # seconds per task — short for high throughput
KILL_AT     = 2    # kill owner at t=10s
TOTAL_TASKS = 120   # enough tasks to last ~60s at 4 CPUs / 2s = 2 tasks/s


@ray.remote(max_retries=0)
def generate_data(seed):
    time.sleep(TASK_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(100, 100)


@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))


@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch_many(self, seeds):
        """Dispatch all tasks at once and return result refs."""
        result_refs = []
        for seed in seeds:
            ref        = generate_data.remote(seed)
            result_ref = compute_sum.remote(ref)
            result_refs.append(result_ref)
        print(f"[Owner] dispatched {len(seeds)} tasks pid={os.getpid()}")
        return result_refs

    def ping(self):
        return os.getpid()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True,
                        choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    # Clean signal file
    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    ray.init(address="auto", log_to_driver=False)

    # Wait for head + worker_a + worker_b
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    # ── Step 1: dispatch all tasks via owner ──────────────────────────────────
    print(f"\nDispatching {TOTAL_TASKS} tasks via owner on worker_a...")
    owner = Owner.remote()
    ray.get(owner.ping.remote())

    experiment_start = time.time()
    all_refs = ray.get(
        owner.dispatch_many.remote(list(range(TOTAL_TASKS))))
    print(f"All {TOTAL_TASKS} tasks dispatched in "
          f"{time.time()-experiment_start:.2f}s")

    # ── Step 2: collect completions + signal kill at KILL_AT ──────────────────
    completion_times = []   # elapsed seconds when each task completed
    futures          = {ref: i for i, ref in enumerate(all_refs)}
    kill_signaled    = False

    print(f"Collecting results, killing owner at t={KILL_AT}s...\n")

    while futures:
        elapsed = time.time() - experiment_start

        # Signal kill at KILL_AT
        if elapsed >= KILL_AT and not kill_signaled:
            print(f">>> Signaling kill at t={elapsed:.1f}s "
                  f"({len(futures)} refs still pending) <<<")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # Poll all pending refs
        done = []
        for ref in list(futures.keys()):
            try:
                ray.get(ref, timeout=0.001)
                completion_times.append(time.time() - experiment_start)
                done.append(ref)
            except ray.exceptions.GetTimeoutError:
                pass
            except ray.exceptions.OwnerDiedError:
                # No gossip — permanently lost
                completion_times  # don't record — task failed
                done.append(ref)
            except Exception:
                done.append(ref)

        for ref in done:
            futures.pop(ref, None)

        # Stop after 90s to avoid hanging forever on no-gossip run
        if elapsed > 90:
            print(f"Timeout at t={elapsed:.1f}s — "
                  f"{len(futures)} refs unresolved")
            break

        time.sleep(0.02)

    total_elapsed = time.time() - experiment_start
    print(f"\nExperiment done in {total_elapsed:.1f}s")
    print(f"Tasks completed: {len(completion_times)} / {TOTAL_TASKS}")

    # ── Compute throughput per second ─────────────────────────────────────────
    max_t    = max(int(total_elapsed) + 1, KILL_AT + 30)
    bins     = list(range(0, max_t + 1))
    throughput_per_sec = []
    for t in bins:
        count = sum(1 for ct in completion_times
                    if t <= ct < t + 1)
        throughput_per_sec.append((t, count))

    # ── Save CSV ──────────────────────────────────────────────────────────────
    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['system', 'elapsed_s', 'throughput', 'kill_time'])
        for t, tp in throughput_per_sec:
            writer.writerow([args.system, t, tp, KILL_AT])

    print(f"Saved to {args.output}")
    ray.shutdown()


if __name__ == "__main__":
    main()