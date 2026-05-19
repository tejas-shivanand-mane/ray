import ray
import numpy as np
import time
import os
import csv
import argparse

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

SIGNAL_FILE    = "/rhome/tmane002/ready_to_kill.txt"
RESTARTED_FILE = "/rhome/tmane002/recon_restarted.txt"


def make_remote_functions(task_duration):
    """Create remote functions with the given task duration."""

    @ray.remote(max_retries=0)
    def generate_data(seed):
        print(f"[generate_data] EXECUTING seed={seed} "
              f"pid={os.getpid()} duration={task_duration}s")
        time.sleep(task_duration)
        np.random.seed(seed)
        return np.random.rand(100, 100)

    @ray.remote(resources={"worker_b": 1}, max_retries=0)
    def compute_sum(data):
        result = float(np.sum(data))
        print(f"[compute_sum] sum={result:.2f} pid={os.getpid()}")
        return result

    @ray.remote(resources={"worker_a": 1},
                max_restarts=0, max_task_retries=0)
    class Owner:
        def dispatch(self, seed):
            ref = generate_data.remote(seed=seed)
            result_ref = compute_sum.remote(ref)
            print(f"[Owner] dispatched pid={os.getpid()}")
            return result_ref

        def ping(self):
            return os.getpid()

    return Owner


def run_single_trial(task_duration, seed, system):
    """
    Runs exactly the original gossip recovery test.
    Returns (total_time, recovery_time, success).
    """
    for f in [SIGNAL_FILE, RESTARTED_FILE]:
        if os.path.exists(f):
            os.remove(f)

    Owner = make_remote_functions(task_duration)

    # Step 1: dispatch
    print(f"  Step 1: dispatching (generate_data sleeps {task_duration}s)...")
    owner = Owner.remote()
    ray.get(owner.ping.remote())
    dispatch_start = time.time()
    result_ref = ray.get(owner.dispatch.remote(seed=seed))
    print(f"  Got result_ref — generate_data running on worker_a")

    # Step 2: kill immediately — generate_data still running
    kill_time = time.time()
    print(f"  Step 2: signaling kill at t={kill_time-dispatch_start:.1f}s...")
    with open(SIGNAL_FILE, "w") as f:
        f.write("kill")
    print(f"  Waiting 10s for worker_a to die...")
    time.sleep(10)

    # Step 3: access result
    print(f"  Step 3: waiting for result (gossip must resubmit)...")
    try:
        val      = ray.get(result_ref, timeout=120)
        end_time = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time
        print(f"  PASS result={val:.2f} "
              f"total={total_time:.1f}s recovery={recovery_time:.1f}s")
        return total_time, recovery_time, True

    except ray.exceptions.OwnerDiedError:
        end_time      = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time
        print(f"  FAIL OwnerDiedError — no gossip recovery")
        return total_time, recovery_time, False

    except Exception as e:
        end_time      = time.time()
        total_time    = end_time - dispatch_start
        recovery_time = end_time - kill_time
        print(f"  FAIL {type(e).__name__}: {e}")
        return total_time, recovery_time, False

    finally:
        try:
            ray.kill(owner)
        except Exception:
            pass


def wait_for_worker_a(timeout=90):
    """Wait until worker_a resource is available in the cluster."""
    print(f"  Waiting for worker_a to be available...")
    waited = 0
    while waited < timeout:
        nodes = ray.nodes()
        alive = [n for n in nodes
                 if n["Alive"] and
                 "worker_a" in n.get("Resources", {})]
        if alive:
            print(f"  worker_a is available")
            return True
        time.sleep(3)
        waited += 3
    print(f"  Timeout waiting for worker_a")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", required=True,
                        choices=["gossip", "no_gossip"])
    parser.add_argument("--output", required=True)
    parser.add_argument("--trials", type=int, default=3,
                        help="Number of trials per task duration")
    parser.add_argument("--task-durations", type=int, nargs="+",
                        default=[5, 10, 20, 30],
                        help="Task sleep durations in seconds")
    args = parser.parse_args()

    ray.init(address="auto", log_to_driver=True)

    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 3:
        time.sleep(1)
        nodes = ray.nodes()
    print("All nodes joined\n")

    file_exists = os.path.exists(args.output)
    with open(args.output, 'a') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['system', 'task_duration', 'trial',
                             'total_time', 'recovery_time', 'success'])

        for task_duration in args.task_durations:
            print(f"\n{'='*60}")
            print(f"task_duration={task_duration}s  "
                  f"trials={args.trials}  system={args.system}")
            print(f"{'='*60}")

            for trial in range(1, args.trials + 1):
                print(f"\n--- Trial {trial}/{args.trials} ---")

                wait_for_worker_a()

                total, recovery, success = run_single_trial(
                    task_duration=task_duration,
                    seed=trial * 100 + task_duration,
                    system=args.system)

                writer.writerow([args.system, task_duration, trial,
                                 round(total, 3), round(recovery, 3),
                                 success])
                csvfile.flush()

                if trial < args.trials:
                    print(f"  Waiting for worker_a restart...")
                    waited = 0
                    while not os.path.exists(RESTARTED_FILE) and waited < 90:
                        time.sleep(2)
                        waited += 2
                    if os.path.exists(RESTARTED_FILE):
                        os.remove(RESTARTED_FILE)
                        print(f"  worker_a restarted OK")
                    else:
                        print(f"  Restart timeout — continuing")
                    time.sleep(5)

        print(f"\n{'='*60}")
        print(f"All trials complete. System={args.system}")
        print(f"{'='*60}")

    print(f"\nResults saved to {args.output}")
    ray.shutdown()




if __name__ == "__main__":
    main()