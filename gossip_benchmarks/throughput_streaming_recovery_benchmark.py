import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_max_pending_lease_requests_per_scheduling_category"] = "1000"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"

# -------------------------
# Benchmark parameters
# -------------------------
TASK_RATE = 2
WORKLOAD_START = 5
WORKLOAD_DURATION = 90
KILL_AT = 30
TOTAL_END = 140

TOTAL_TASKS = TASK_RATE * WORKLOAD_DURATION

NUM_PRODUCER_ACTORS = 8
DATA_SHAPE = (400, 400)
CONSUMER_SLEEP = 0.3


@ray.remote(num_cpus=1, resources={"producer_b": 1})
class ProducerActor:
    def produce_at(self, seed, target_time_abs):
        now = time.time()
        sleep_time = max(0.0, target_time_abs - now)
        time.sleep(sleep_time)

        np.random.seed(seed % 10000)
        return np.random.rand(*DATA_SHAPE)


@ray.remote(num_cpus=1, resources={"consumer_b": 1}, max_retries=0)
def compute_sum(data):
    result = float(np.sum(data))
    time.sleep(CONSUMER_SLEEP)
    return result


@ray.remote(resources={"worker_a": 1}, max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch_stream(self, producer_actors, num_tasks, task_rate, workload_start_abs):
        refs = []
        interval = 1.0 / task_rate

        for i in range(num_tasks):
            target_time_abs = workload_start_abs + i * interval

            producer = producer_actors[i % len(producer_actors)]
            producer_ref = producer.produce_at.remote(i, target_time_abs)

            result_ref = compute_sum.remote(producer_ref)
            refs.append(result_ref)

        print(
            f"[Owner] dispatched {num_tasks} tasks at {task_rate} tasks/s, "
            f"pid={os.getpid()}",
            flush=True,
        )

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

    print("Cluster resources:", ray.cluster_resources(), flush=True)
    print("Available resources:", ray.available_resources(), flush=True)

    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        print("Waiting for all 3 nodes to join...", flush=True)
        time.sleep(1)
        nodes = ray.nodes()

    print("All nodes joined", flush=True)

    # Producer actors live on worker_b and survive owner failure.
    producer_actors = [ProducerActor.remote() for _ in range(NUM_PRODUCER_ACTORS)]

    owner = Owner.remote()
    owner_pid = ray.get(owner.ping.remote())
    print(f"Owner actor started with pid={owner_pid}", flush=True)

    experiment_start = time.time()
    workload_start_abs = experiment_start + WORKLOAD_START

    print(
        f"\nDispatching actor-based streaming workload:\n"
        f"  total tasks       = {TOTAL_TASKS}\n"
        f"  target rate       = {TASK_RATE} tasks/s\n"
        f"  producer actors   = {NUM_PRODUCER_ACTORS}\n"
        f"  workload starts   = t={WORKLOAD_START}s\n"
        f"  workload duration = {WORKLOAD_DURATION}s\n"
        f"  owner killed at   = t={KILL_AT}s\n",
        flush=True,
    )

    result_refs = ray.get(
        owner.dispatch_stream.remote(
            producer_actors,
            TOTAL_TASKS,
            TASK_RATE,
            workload_start_abs,
        )
    )

    all_futures = {ref: i for i, ref in enumerate(result_refs)}
    completion_records = []
    kill_signaled = False

    while all_futures:
        elapsed = time.time() - experiment_start

        if elapsed >= KILL_AT and not kill_signaled:
            kill_signaled = True
            print(f"\n>>> Killing owner at t={elapsed:.2f}s <<<", flush=True)
            write_kill_signal()

        ready, _ = ray.wait(
            list(all_futures.keys()),
            num_returns=min(len(all_futures), 32),
            timeout=0.05,
        )

        for ref in ready:
            task_index = all_futures[ref]

            try:
                ray.get(ref, timeout=0)
                completion_time = time.time() - experiment_start
                completion_records.append((completion_time, task_index))
                all_futures.pop(ref, None)

            except ray.exceptions.OwnerDiedError:
                # Keep waiting; gossip recovery may make this ref resolvable later.
                pass

            except ray.exceptions.GetTimeoutError:
                pass

            except Exception as e:
                print(
                    f"[WARN] Unexpected exception at "
                    f"t={time.time() - experiment_start:.2f}s "
                    f"for task {task_index}: {type(e).__name__}: {e}",
                    flush=True,
                )

        if elapsed > TOTAL_END:
            print(
                f"\nTimeout at t={elapsed:.2f}s. "
                f"{len(all_futures)} refs still unresolved.",
                flush=True,
            )
            break

        time.sleep(0.01)

    total_elapsed = time.time() - experiment_start
    completed = len(completion_records)
    unresolved = len(all_futures)

    pre_failure_completed = sum(1 for t, _ in completion_records if t < KILL_AT)
    post_failure_completed = sum(1 for t, _ in completion_records if t >= KILL_AT)

    print("\nExperiment done", flush=True)
    print(f"Elapsed time: {total_elapsed:.2f}s", flush=True)
    print(f"Total completed: {completed} / {TOTAL_TASKS}", flush=True)
    print(f"Pre-failure completed: {pre_failure_completed}", flush=True)
    print(f"Post-failure completed: {post_failure_completed}", flush=True)
    print(f"Unresolved refs: {unresolved}", flush=True)

    max_t = max(int(total_elapsed) + 1, TOTAL_END)

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow(
            [
                "system",
                "elapsed_s",
                "throughput",
                "cumulative_completed",
                "kill_time",
                "workload_start",
                "workload_duration",
                "target_rate",
                "total_tasks",
                "unresolved_at_end",
            ]
        )

        cumulative = 0

        for sec in range(0, max_t + 1):
            throughput = sum(
                1
                for completion_time, _ in completion_records
                if sec <= completion_time < sec + 1
            )

            cumulative += throughput

            writer.writerow(
                [
                    args.system,
                    sec,
                    throughput,
                    cumulative,
                    KILL_AT,
                    WORKLOAD_START,
                    WORKLOAD_DURATION,
                    TASK_RATE,
                    TOTAL_TASKS,
                    unresolved,
                ]
            )

    print(f"Saved to {args.output}", flush=True)
    ray.shutdown()


if __name__ == "__main__":
    main()