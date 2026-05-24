import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_max_pending_lease_requests_per_scheduling_category"] = "200"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"

# -------------------------
# Benchmark parameters
# -------------------------
WAVE1_SLEEP = 1.0
WAVE2_SLEEP = 20.0

WAVE1_TASKS = 80
WAVE2_TASKS = 80

WAVE2_START = 8       # dispatch wave 2 at t=8
KILL_AT = 13          # kill owner at t=13, while wave 2 is still sleeping
TOTAL_END = 90        # allow enough time for gossip recovery


# IMPORTANT:
# All producer tasks are pinned to worker_b.
# This prevents ray stop on worker_a from killing the actual producer tasks.
@ray.remote(num_cpus=0, resources={"producer_b": 0.001}, max_retries=0)
def fast_task(seed):
    time.sleep(WAVE1_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(100, 100)


@ray.remote(num_cpus=0, resources={"producer_b": 0.001}, max_retries=0)
def slow_task(seed):
    time.sleep(WAVE2_SLEEP)
    np.random.seed(seed % 10000)
    return np.random.rand(100, 100)


# Consumer task is also pinned to worker_b.
@ray.remote(num_cpus=1, resources={"consumer_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))


# Owner actor is pinned to worker_a.
# This is the only component we want to kill.
@ray.remote(resources={"worker_a": 1}, max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch_fast(self, seeds):
        refs = []
        for seed in seeds:
            ref = fast_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(result_ref)

        print(f"[Owner] dispatched {len(seeds)} fast tasks pid={os.getpid()}", flush=True)
        return refs

    def dispatch_slow(self, seeds):
        refs = []
        for seed in seeds:
            ref = slow_task.remote(seed)
            result_ref = compute_sum.remote(ref)
            refs.append(result_ref)

        print(f"[Owner] dispatched {len(seeds)} slow tasks pid={os.getpid()}", flush=True)
        return refs

    def ping(self):
        return os.getpid()


def write_kill_signal():
    print(f">>> Writing kill signal: {SIGNAL_FILE}", flush=True)
    with open(SIGNAL_FILE, "w") as f:
        f.write("kill")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True, choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if os.path.exists(SIGNAL_FILE):
        os.remove(SIGNAL_FILE)

    ray.init(address="auto", log_to_driver=False)

    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        print("Waiting for all 3 nodes to join...", flush=True)
        time.sleep(1)
        nodes = ray.nodes()

    print("All nodes joined", flush=True)

    owner = Owner.remote()
    owner_pid = ray.get(owner.ping.remote())
    print(f"Owner actor started with pid={owner_pid}", flush=True)

    experiment_start = time.time()

    completion_records = []
    all_futures = {}

    wave2_dispatched = False
    kill_signaled = False

    # -------------------------
    # Dispatch wave 1 at t = 0
    # -------------------------
    print(f"\nDispatching {WAVE1_TASKS} fast tasks at t=0...", flush=True)

    wave1_refs = ray.get(owner.dispatch_fast.remote(list(range(WAVE1_TASKS))))
    for ref in wave1_refs:
        all_futures[ref] = "wave1"

    print(f"Wave 1 dispatched at t={time.time() - experiment_start:.2f}s", flush=True)

    print(
        f"\nCollecting results. "
        f"Wave 2 dispatch at t={WAVE2_START}s, "
        f"owner kill at t={KILL_AT}s, "
        f"timeout at t={TOTAL_END}s.",
        flush=True,
    )

    # -------------------------
    # Main loop
    # -------------------------
    while all_futures or not wave2_dispatched:
        elapsed = time.time() - experiment_start

        # Dispatch wave 2 at WAVE2_START.
        if elapsed >= WAVE2_START and not wave2_dispatched:
            wave2_dispatched = True

            print(f"\nDispatching {WAVE2_TASKS} slow tasks at t={elapsed:.2f}s...", flush=True)

            wave2_refs = ray.get(
                owner.dispatch_slow.remote(
                    list(range(WAVE1_TASKS, WAVE1_TASKS + WAVE2_TASKS))
                )
            )

            for ref in wave2_refs:
                all_futures[ref] = "wave2"

            print(
                f"Wave 2 dispatched at t={time.time() - experiment_start:.2f}s. "
                f"{len(wave2_refs)} slow tasks are pinned to worker_b.",
                flush=True,
            )

        # Kill owner at KILL_AT, separately from wave-2 dispatch.
        if elapsed >= KILL_AT and not kill_signaled:
            kill_signaled = True
            print(f"\n>>> Killing owner node at t={elapsed:.2f}s <<<", flush=True)
            write_kill_signal()

        # Collect ready final result refs.
        if all_futures:
            ready, _ = ray.wait(
                list(all_futures.keys()),
                num_returns=min(len(all_futures), 32),
                timeout=0.05,
            )

            for ref in ready:
                wave = all_futures[ref]

                try:
                    ray.get(ref, timeout=0)
                    completion_time = time.time() - experiment_start
                    completion_records.append((completion_time, wave))
                    all_futures.pop(ref, None)

                except ray.exceptions.OwnerDiedError:
                    # Do NOT remove the ref.
                    # With gossip recovery, it may become resolvable later.
                    pass

                except ray.exceptions.GetTimeoutError:
                    pass

                except Exception as e:
                    print(
                        f"[WARN] Unexpected exception for {wave} ref at "
                        f"t={time.time() - experiment_start:.2f}s: "
                        f"{type(e).__name__}: {e}",
                        flush=True,
                    )
                    # Keep the ref for now.
                    pass

        if elapsed > TOTAL_END:
            print(
                f"\nTimeout at t={elapsed:.2f}s. "
                f"{len(all_futures)} refs still unresolved.",
                flush=True,
            )
            break

        time.sleep(0.01)

    total_elapsed = time.time() - experiment_start
    total_tasks = WAVE1_TASKS + WAVE2_TASKS

    wave1_done = sum(1 for _, wave in completion_records if wave == "wave1")
    wave2_done = sum(1 for _, wave in completion_records if wave == "wave2")

    print("\nExperiment done", flush=True)
    print(f"Elapsed time: {total_elapsed:.2f}s", flush=True)
    print(f"Total completed: {len(completion_records)} / {total_tasks}", flush=True)
    print(f"Wave 1 completed: {wave1_done} / {WAVE1_TASKS}", flush=True)
    print(f"Wave 2 completed: {wave2_done} / {WAVE2_TASKS}", flush=True)
    print(f"Unresolved refs: {len(all_futures)}", flush=True)

    # -------------------------
    # Save CSV
    # -------------------------
    max_t = max(int(total_elapsed) + 1, TOTAL_END)

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "system",
                "elapsed_s",
                "throughput",
                "cumulative_completed",
                "wave1_completed",
                "wave2_completed",
                "kill_time",
                "wave2_start",
                "wave2_sleep",
                "unresolved_at_end",
            ]
        )

        cumulative = 0
        cumulative_wave1 = 0
        cumulative_wave2 = 0

        for t in range(0, max_t + 1):
            completions_this_sec = [
                wave
                for completion_time, wave in completion_records
                if t <= completion_time < t + 1
            ]

            throughput = len(completions_this_sec)
            cumulative += throughput
            cumulative_wave1 += sum(1 for wave in completions_this_sec if wave == "wave1")
            cumulative_wave2 += sum(1 for wave in completions_this_sec if wave == "wave2")

            writer.writerow(
                [
                    args.system,
                    t,
                    throughput,
                    cumulative,
                    cumulative_wave1,
                    cumulative_wave2,
                    KILL_AT,
                    WAVE2_START,
                    WAVE2_SLEEP,
                    len(all_futures),
                ]
            )

    print(f"Saved to {args.output}", flush=True)

    ray.shutdown()


if __name__ == "__main__":
    main()