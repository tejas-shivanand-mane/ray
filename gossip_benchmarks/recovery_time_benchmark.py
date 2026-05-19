import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE = "/rhome/tmane002/ready_to_kill.txt"
RESTARTED_FILE = "/rhome/tmane002/recon_restarted.txt"


def make_generate_data(task_duration):
    @ray.remote(max_retries=0)
    def generate_data(seed):
        time.sleep(task_duration)
        np.random.seed(seed)
        return np.random.rand(100, 100)
    return generate_data


@ray.remote(resources={"worker_b": 1}, max_retries=0)
def compute_sum(data):
    return float(np.sum(data))


@ray.remote(resources={"worker_a": 1},
            max_restarts=0, max_task_retries=0)
class Owner:
    def dispatch(self, seed, task_duration):
        from recovery_time_benchmark import make_generate_data
        generate_data = make_generate_data(task_duration)
        ref = generate_data.remote(seed=seed)
        result_ref = compute_sum.remote(ref)
        return result_ref

    def ping(self):
        return os.getpid()


def run_trial(task_duration, seed, system):
    """
    Run one trial of the recovery benchmark.
    Returns (total_time, recovery_time, success).
    """
    # Clean up signal files
    for f in [SIGNAL_FILE, RESTARTED_FILE]:
        if os.path.exists(f):
            os.remove(f)

    # Step 1: dispatch
    owner = Owner.remote()
    ray.get(owner.ping.remote())

    dispatch_start = time.time()
    result_ref = ray.get(owner.dispatch.remote(seed, task_duration))
    print(f"  Dispatched, generate_data sleeping {task_duration}s...")

    # Step 2: kill immediately after dispatch
    # generate_data is now running on worker_a
    time.sleep(2)  # small delay to ensure task is running
    kill_time = time.time()
    print(f"  Signaling kill at t={kill_time - dispatch_start:.1f}s...")
    with open(SIGNAL_FILE, "w") as f:
        f.write("kill")

    # Step 3: measure recovery
    print(f"  Waiting for recovery (timeout=120s)...")
    try:
        val = ray.get(result_ref, timeout=120)
        recovery_end = time.time()
        total_time    = recovery_end - dispatch_start
        recovery_time = recovery_end - kill_time
        print(f"  PASS — result={val:.2f}, "
              f"total={total_time:.1f}s, recovery={recovery_time:.1f}s")
        return total_time, recovery_time, True

    except ray.exceptions.OwnerDiedError:
        end_time = time.time()
        print(f"  FAIL — OwnerDiedError (no gossip recovery)")
        return end_time - dispatch_start, end_time - kill_time, False

    except Exception as e:
        end_time = time.time()
        print(f"  FAIL — {type(e).__name__}: {e}")
        return end_time - dispatch_start, end_time - kill_time, False

    finally:
        # Clean up owner
        try:
            ray.kill(owner)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True,
                        choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--task-durations", type=int, nargs="+",
                        default=[5, 10, 20, 30])
    args = parser.parse_args()

    ray.init(address="auto", log_to_driver=False)

    # Wait for head + worker_a + worker_b
    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined\n")

    file_exists = os.path.exists(args.output)
    with open(args.output, 'a') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['system', 'task_duration',
                             'trial', 'total_time',
                             'recovery_time', 'success'])

        for task_duration in args.task_durations:
            print(f"=== task_duration={task_duration}s ===")
            for trial in range(args.trials):
                print(f"  Trial {trial+1}/{args.trials}")
                total, recovery, success = run_trial(
                    task_duration, seed=trial, system=args.system)
                writer.writerow([args.system, task_duration,
                                 trial+1, round(total, 3),
                                 round(recovery, 3), success])
                f.flush()
                # Wait for worker_a to restart before next trial
                print(f"  Waiting 15s for worker_a to restart...")
                time.sleep(15)

    print(f"\nResults saved to {args.output}")
    ray.shutdown()


if __name__ == "__main__":
    main()