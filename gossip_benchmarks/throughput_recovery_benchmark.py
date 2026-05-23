import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_max_pending_lease_requests_per_scheduling_category"] = "200"

SIGNAL_FILE  = "/rhome/tmane002/ready_to_kill.txt"

# Task sleep — short enough for high throughput, long enough to have
# in-flight tasks when owner dies
TASK_SLEEP   = 2.0

# Phase durations
WARMUP_END   = 10    # t=0  to t=10:  warmup — owner dispatches, throughput builds
KILL_AT      = 15    # t=15: kill owner — all tasks dispatched in wave 2 are in-flight
TOTAL_END    = 60    # t=60: end experiment

# Tasks per wave
WAVE1_TASKS  = 80    # dispatched at t=0, complete before kill — establishes baseline
WAVE2_TASKS  = 80    # dispatched at t=12, all in-flight when owner dies at t=15


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
    def dispatch_wave(self, seeds):
        refs = []
        for seed in seeds:
            ref        = generate_data.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(result_ref)
        print(f"[Owner] dispatched wave of {len(seeds)} tasks pid={os.getpid()}")
        return refs

    def ping(self):
        return os.getpid()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True,
                        choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    ray.init(address="auto", log_to_driver=False)

    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined")

    owner = Owner.remote()
    ray.get(owner.ping.remote())

    experiment_start = time.time()
    completion_times = []
    all_futures      = {}
    kill_signaled    = False

    # ── Wave 1: dispatch before kill — establishes baseline throughput ────────
    print(f"\nWave 1: dispatching {WAVE1_TASKS} tasks (complete before kill)...")
    wave1_refs = ray.get(
        owner.dispatch_wave.remote(list(range(WAVE1_TASKS))))
    for ref in wave1_refs:
        all_futures[ref] = "wave1"
    print(f"Wave 1 dispatched at t={time.time()-experiment_start:.1f}s")

    # ── Wait until just before kill, then dispatch wave 2 ────────────────────
    while time.time() - experiment_start < KILL_AT - TASK_SLEEP:
        time.sleep(0.1)

    print(f"\nWave 2: dispatching {WAVE2_TASKS} tasks (will be in-flight at kill)...")
    wave2_refs = ray.get(
        owner.dispatch_wave.remote(
            list(range(WAVE1_TASKS, WAVE1_TASKS + WAVE2_TASKS))))
    for ref in wave2_refs:
        all_futures[ref] = "wave2"
    print(f"Wave 2 dispatched at t={time.time()-experiment_start:.1f}s")

    # ── Main collection loop ──────────────────────────────────────────────────
    print(f"\nCollecting results, killing owner at t={KILL_AT}s...")

    while all_futures:
        elapsed = time.time() - experiment_start

        if elapsed >= KILL_AT and not kill_signaled:
            pending_wave2 = sum(1 for v in all_futures.values() if v == "wave2")
            print(f">>> Signaling kill at t={elapsed:.1f}s "
                  f"({pending_wave2} wave2 refs pending) <<<")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        done = []
        ready, _ = ray.wait(
            list(all_futures.keys()),
            num_returns=min(len(all_futures), 32),
            timeout=0.05)

        for ref in ready:
            try:
                ray.get(ref, timeout=0)
                completion_times.append(time.time() - experiment_start)
            except ray.exceptions.OwnerDiedError:
                pass  # no gossip — lost permanently
            except Exception:
                pass
            done.append(ref)

        for ref in done:
            all_futures.pop(ref, None)

        if elapsed > TOTAL_END:
            remaining = len(all_futures)
            print(f"Timeout at t={elapsed:.1f}s — {remaining} refs unresolved")
            break

        time.sleep(0.01)

    total_elapsed = time.time() - experiment_start
    print(f"\nExperiment done in {total_elapsed:.1f}s")
    print(f"Tasks completed: {len(completion_times)} / "
          f"{WAVE1_TASKS + WAVE2_TASKS}")

    # ── Throughput per second ─────────────────────────────────────────────────
    max_t = max(int(total_elapsed) + 1, TOTAL_END)
    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['system', 'elapsed_s', 'throughput', 'kill_time'])
        for t in range(0, max_t + 1):
            count = sum(1 for ct in completion_times if t <= ct < t + 1)
            writer.writerow([args.system, t, count, KILL_AT])

    print(f"Saved to {args.output}")
    ray.shutdown()


if __name__ == "__main__":
    main()