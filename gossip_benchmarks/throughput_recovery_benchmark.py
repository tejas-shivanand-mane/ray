import ray
import numpy as np
import time
import os
import csv
import argparse
import threading

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_max_pending_lease_requests_per_scheduling_category"] = "200"

SIGNAL_FILE    = "/rhome/tmane002/ready_to_kill.txt"

# Wave 1: fast tasks dispatched at t=0, complete t=1-10, show pre-kill baseline
WAVE1_TASKS    = 80
WAVE1_SLEEP    = 1.0   # 1s sleep, 8 CPUs -> ~8 tasks/s

# Wave 2: slow tasks dispatched at t=0, all in-flight when owner dies
WAVE2_TASKS    = 80
WAVE2_SLEEP    = 20.0  # 20s sleep, killed at t=8, clearly in-flight

KILL_AT        = 8     # kill owner well into wave2 sleep
TOTAL_END      = 75    # run long enough to see gossip recovery complete


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
    def dispatch_waves(self, wave1_seeds, wave2_seeds):
        """Dispatch both waves and return all result refs."""
        refs = []
        # Wave 1: fast tasks
        for seed in wave1_seeds:
            ref        = fast_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(("wave1", result_ref))
        # Wave 2: slow tasks — all in-flight at kill time
        for seed in wave2_seeds:
            ref        = slow_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(("wave2", result_ref))
        print(f"[Owner] dispatched {len(wave1_seeds)} fast + "
              f"{len(wave2_seeds)} slow tasks pid={os.getpid()}")
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

    print(f"\nDispatching {WAVE1_TASKS} fast + {WAVE2_TASKS} slow tasks...")
    experiment_start = time.time()

    wave_and_refs = ray.get(owner.dispatch_waves.remote(
        list(range(WAVE1_TASKS)),
        list(range(WAVE1_TASKS, WAVE1_TASKS + WAVE2_TASKS))))

    dispatch_time = time.time() - experiment_start
    print(f"All tasks dispatched in {dispatch_time:.2f}s")

    # Build futures dict: ref -> wave label
    all_futures = {ref: wave for wave, ref in wave_and_refs}

    completion_times = []
    kill_signaled    = False
    lock             = threading.Lock()

    print(f"Collecting results, killing owner at t={KILL_AT}s...")

    while all_futures:
        elapsed = time.time() - experiment_start

        if elapsed >= KILL_AT and not kill_signaled:
            wave2_pending = sum(1 for v in all_futures.values()
                                if v == "wave2")
            print(f">>> Kill at t={elapsed:.1f}s "
                  f"({wave2_pending} slow tasks pending) <<<")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            kill_signaled = True

        # Collect completions
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
    total_tasks   = WAVE1_TASKS + WAVE2_TASKS
    print(f"\nExperiment done in {total_elapsed:.1f}s")
    print(f"Tasks completed: {len(completion_times)} / {total_tasks}")

    # Save CSV
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