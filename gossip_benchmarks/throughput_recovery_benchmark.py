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
WAVE1_SLEEP  = 1.0    # fast tasks — show pre-kill baseline
WAVE2_SLEEP  = 1.0   # slow tasks — in-flight at kill, recovered by gossip
WAVE1_TASKS  = 80     # dispatched at t=0, complete t=1-10
WAVE2_TASKS  = 300     # dispatched at t=10, in-flight when killed at t=13
WAVE2_START  = 8     # dispatch wave 2 at t=10
KILL_AT      = 13     # kill at t=13 — wave 2 is 3s into 20s sleep
TOTAL_END    = 50     # run long enough for gossip recovery


@ray.remote(max_retries=0)
def fast_task(seed):
    time.sleep(WAVE1_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(100, 100)


@ray.remote(max_retries=0)
def slow_task(seed):
    time.sleep(WAVE2_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(100, 100)


@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))


@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch_fast(self, seeds):
        refs = []
        for seed in seeds:
            ref = fast_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(result_ref)
        print(f"[Owner] dispatched {len(seeds)} fast tasks pid={os.getpid()}")
        return refs

    def dispatch_slow(self, seeds):
        refs = []
        for seed in seeds:
            ref = slow_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(result_ref)
        print(f"[Owner] dispatched {len(seeds)} slow tasks pid={os.getpid()}")
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
    wave2_dispatched = False

    # ── Dispatch wave 1 (fast tasks) at t=0 ──────────────────────────────────
    print(f"\nDispatching {WAVE1_TASKS} fast tasks at t=0...")
    wave1_refs = ray.get(owner.dispatch_fast.remote(
        list(range(WAVE1_TASKS))))
    for ref in wave1_refs:
        all_futures[ref] = "wave1"
    print(f"Wave 1 dispatched at t={time.time()-experiment_start:.1f}s")

    # ── Main loop: collect + dispatch wave 2 at WAVE2_START ──────────────────
    print(f"Collecting results. Wave 2 at t={WAVE2_START}s, "
          f"kill at t={KILL_AT}s...")


    while all_futures or not wave2_dispatched:
        elapsed = time.time() - experiment_start
        # Dispatch wave 2 at WAVE2_START
        if elapsed >= WAVE2_START and not wave2_dispatched:
            wave2_dispatched = True
            print(f"\nDispatching {WAVE2_TASKS} slow tasks at t={elapsed:.1f}s...")
            wave2_refs = ray.get(owner.dispatch_slow.remote(
                list(range(WAVE1_TASKS, WAVE1_TASKS + WAVE2_TASKS))))
            for ref in wave2_refs:
                all_futures[ref] = "wave2"
            print(f"Wave 2 dispatched — {len(wave2_refs)} slow tasks in flight")
            # Kill immediately after dispatch — tasks just started sleeping
            print(f">>> Killing immediately at t={time.time()-experiment_start:.1f}s <<<")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # Collect completions
        if all_futures:
            ready, _ = ray.wait(
                list(all_futures.keys()),
                num_returns=min(len(all_futures), 32),
                timeout=0.05)
            done = []
            for ref in ready:
                try:
                    ray.get(ref, timeout=0)
                    completion_times.append(time.time() - experiment_start)
                except ray.exceptions.OwnerDiedError:
                    pass
                except Exception:
                    pass
                done.append(ref)
            for ref in done:
                all_futures.pop(ref, None)

        if elapsed > TOTAL_END:
            print(f"Timeout at t={elapsed:.1f}s — "
                  f"{len(all_futures)} refs unresolved")
            break

        time.sleep(0.01)

    total_elapsed = time.time() - experiment_start
    total = WAVE1_TASKS + WAVE2_TASKS
    print(f"\nExperiment done in {total_elapsed:.1f}s")
    print(f"Tasks completed: {len(completion_times)} / {total}")

    # ── Save CSV ──────────────────────────────────────────────────────────────
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