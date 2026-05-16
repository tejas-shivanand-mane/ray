import time
import numpy as np
import threading
import os
import csv
import ray

JOB_TIME = 20
NUM_TRIALS = 5  # run each config multiple times for stability

SIGNAL_FILE = "/rhome/tmane002/recon_kill_signal.txt"
RESTARTED_FILE = "/rhome/tmane002/recon_restarted.txt"

@ray.remote
def chain(i, dep, delay_ms):
    time.sleep(delay_ms / 1000.0)
    return dep

@ray.remote
def small_dep():
    return 1

@ray.remote
def large_dep():
    return np.zeros(10 * 1024 * 1024, dtype=np.uint8)  # 10 MiB

def run(delay_ms, large, num_rounds):
    f = large_dep if large else small_dep
    start = time.time()
    dep = f.remote()
    for i in range(num_rounds):
        dep = chain.remote(i, dep, delay_ms)
    ray.get(dep)
    return time.time() - start

def run_trial(args, num_rounds):
    if args.failure:
        sleep = JOB_TIME / 2

        # Clean up signal files
        for f in [SIGNAL_FILE, RESTARTED_FILE]:
            if os.path.exists(f):
                os.remove(f)

        def kill():
            time.sleep(sleep)
            print("Writing kill signal...")
            with open(SIGNAL_FILE, "w") as f:
                f.write("kill")
            waited = 0
            while not os.path.exists(RESTARTED_FILE) and waited < 30:
                time.sleep(1)
                waited += 1
            print("Worker restarted signal received")

        t = threading.Thread(target=kill)
        t.start()
        duration = run(args.delay_ms, args.large, num_rounds)
        t.join()
    else:
        duration = run(args.delay_ms, args.large, num_rounds)
    return duration

def main(args):
    ray.init(address="auto")

    nodes = ray.nodes()
    while len([n for n in nodes if n["Alive"]]) < 2:
        time.sleep(1)
        print("{} nodes found, waiting...".format(len(nodes)))
        nodes = ray.nodes()
    print("All nodes joined")

    num_rounds = int(JOB_TIME / (args.delay_ms / 1000))
    num_rounds = max(num_rounds, 2)
    print(f"Running {num_rounds} rounds of {args.delay_ms}ms each, {NUM_TRIALS} trials")

    durations = []
    for trial in range(NUM_TRIALS):
        print(f"Trial {trial+1}/{NUM_TRIALS}...")
        duration = run_trial(args, num_rounds)
        durations.append(duration)
        print(f"  duration={duration:.2f}s")
        time.sleep(5)  # let object store settle between trials

    median_dur = float(np.median(durations))
    std_dur    = float(np.std(durations))
    print(f"delay_ms={args.delay_ms} large={args.large} failure={args.failure} "
          f"median={median_dur:.2f}s std={std_dur:.2f}s")

    if args.output:
        file_exists = os.path.exists(args.output)
        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'large', 'delay_ms', 'duration', 'std', 'failure']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'system': 'ownership',
                'large': args.large,
                'delay_ms': args.delay_ms,
                'duration': median_dur,
                'std': std_dur,
                'failure': args.failure,
            })

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--delay-ms", required=True, type=int)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    main(args)