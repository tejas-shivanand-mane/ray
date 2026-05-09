import ray
import numpy as np
import time
import os
import json

os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"

ray.init(address="auto", log_to_driver=False)

@ray.remote(max_retries=0)
def small_task(x):
    return x + 1

@ray.remote(max_retries=0)
def data_task(seed):
    np.random.seed(seed)
    return np.random.rand(100, 100)

@ray.remote(max_retries=0)
def sum_task(data):
    return float(np.sum(data))

DURATION = 30
BATCH    = 50

def benchmark_small_tasks(warmup=200):
    print(f"\n{'='*55}")
    print(f"Benchmark 1: Small Task Throughput  ({DURATION}s)")
    print(f"{'='*55}")
    ray.get([small_task.remote(i) for i in range(warmup)])
    n, i = 0, 0
    deadline = time.perf_counter() + DURATION
    start = time.perf_counter()
    while time.perf_counter() < deadline:
        ray.get([small_task.remote(i + j) for j in range(BATCH)])
        n += BATCH; i += BATCH
    elapsed = time.perf_counter() - start
    throughput = n / elapsed
    latency_ms = (elapsed / n) * 1000
    print(f"  Tasks completed: {n}")
    print(f"  Total time:      {elapsed:.3f}s")
    print(f"  Throughput:      {throughput:.1f} tasks/sec")
    print(f"  Avg latency:     {latency_ms:.3f} ms/task")
    return {"throughput": throughput, "latency_ms": latency_ms, "n": n}

def benchmark_pipeline(warmup=30):
    print(f"\n{'='*55}")
    print(f"Benchmark 2: Pipeline Throughput  ({DURATION}s)")
    print(f"{'='*55}")
    ray.get([sum_task.remote(data_task.remote(i)) for i in range(warmup)])
    n, i = 0, 0
    deadline = time.perf_counter() + DURATION
    start = time.perf_counter()
    while time.perf_counter() < deadline:
        ray.get([sum_task.remote(data_task.remote(i + j)) for j in range(BATCH)])
        n += BATCH; i += BATCH
    elapsed = time.perf_counter() - start
    throughput = n / elapsed
    latency_ms = (elapsed / n) * 1000
    print(f"  Pipelines completed: {n}")
    print(f"  Total time:          {elapsed:.3f}s")
    print(f"  Throughput:          {throughput:.1f} pipelines/sec")
    print(f"  Avg latency:         {latency_ms:.3f} ms/pipeline")
    return {"throughput": throughput, "latency_ms": latency_ms, "n": n}

def benchmark_serial_latency(warmup=50):
    print(f"\n{'='*55}")
    print(f"Benchmark 3: Serial Latency  ({DURATION}s)")
    print(f"{'='*55}")
    for i in range(warmup):
        ray.get(small_task.remote(i))
    latencies = []
    deadline = time.perf_counter() + DURATION
    i = 0
    while time.perf_counter() < deadline:
        t0 = time.perf_counter()
        ray.get(small_task.remote(i))
        latencies.append((time.perf_counter() - t0) * 1000)
        i += 1
    print(f"  Tasks completed: {len(latencies)}")
    print(f"  Mean latency:    {np.mean(latencies):.3f} ms")
    print(f"  P50 latency:     {np.percentile(latencies, 50):.3f} ms")
    print(f"  P99 latency:     {np.percentile(latencies, 99):.3f} ms")
    print(f"  Min latency:     {np.min(latencies):.3f} ms")
    print(f"  Max latency:     {np.max(latencies):.3f} ms")
    return {
        "mean_ms": float(np.mean(latencies)),
        "p50_ms":  float(np.percentile(latencies, 50)),
        "p99_ms":  float(np.percentile(latencies, 99)),
        "min_ms":  float(np.min(latencies)),
        "max_ms":  float(np.max(latencies)),
        "n":       len(latencies),
    }

label = os.environ.get("GOSSIP_LABEL", "true")
print(f"\n{'#'*55}")
print(f"  gossip_recovery_enabled = {label}")
print(f"{'#'*55}")

results = {
    "label": label,
    "small_tasks":    benchmark_small_tasks(),
    "pipeline":       benchmark_pipeline(),
    "serial_latency": benchmark_serial_latency(),
}

outfile = f"/tmp/benchmark_{label}.json"
with open(outfile, "w") as f:
    json.dump(results, f, indent=2)
print(f"\nResults saved to {outfile}")

ray.shutdown()
