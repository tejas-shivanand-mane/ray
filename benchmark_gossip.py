"""
Gossip Recovery Overhead Benchmark
Measures steady-state throughput and latency impact of gossip
with gossip_recovery_enabled=true vs false
"""
import ray
import numpy as np
import time
import os
import json
import sys

os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"

ray.init(address="auto", log_to_driver=False)

# ─────────────────────────────────────────────
# Task definitions
# ─────────────────────────────────────────────

@ray.remote(max_retries=0)
def small_task(x):
    """Minimal task — isolates gossip store/send overhead."""
    return x + 1

@ray.remote(max_retries=0)
def data_task(seed):
    """100x100 numpy task — realistic compute workload."""
    np.random.seed(seed)
    return np.random.rand(100, 100)

@ray.remote(max_retries=0)
def sum_task(data):
    """Dependent task — tests gossip overhead on chained tasks."""
    return float(np.sum(data))

# ─────────────────────────────────────────────
# Benchmark 1: Small task throughput
# Measures gossip store + send overhead on tiny tasks
# ─────────────────────────────────────────────

def benchmark_small_tasks(n_tasks=500, warmup=50):
    print(f"\n{'='*55}")
    print(f"Benchmark 1: Small Task Throughput  (N={n_tasks})")
    print(f"{'='*55}")

    # Warmup
    ray.get([small_task.remote(i) for i in range(warmup)])

    # Measure — submit all at once (max parallelism)
    start = time.perf_counter()
    refs = [small_task.remote(i) for i in range(n_tasks)]
    ray.get(refs)
    elapsed = time.perf_counter() - start

    throughput = n_tasks / elapsed
    latency_ms = (elapsed / n_tasks) * 1000
    print(f"  Total time:   {elapsed:.3f}s")
    print(f"  Throughput:   {throughput:.1f} tasks/sec")
    print(f"  Avg latency:  {latency_ms:.3f} ms/task")
    return {"throughput": throughput, "latency_ms": latency_ms}

# ─────────────────────────────────────────────
# Benchmark 2: Pipeline throughput
# data_task → sum_task chains
# Tests gossip overhead when passing refs between tasks
# ─────────────────────────────────────────────

def benchmark_pipeline(n_pipelines=100, warmup=10):
    print(f"\n{'='*55}")
    print(f"Benchmark 2: Pipeline Throughput  (N={n_pipelines})")
    print(f"{'='*55}")

    # Warmup
    ray.get([sum_task.remote(data_task.remote(i)) for i in range(warmup)])

    # Measure — submit all pipelines in parallel
    start = time.perf_counter()
    refs = [sum_task.remote(data_task.remote(i)) for i in range(n_pipelines)]
    ray.get(refs)
    elapsed = time.perf_counter() - start

    throughput = n_pipelines / elapsed
    latency_ms = (elapsed / n_pipelines) * 1000
    print(f"  Total time:   {elapsed:.3f}s")
    print(f"  Throughput:   {throughput:.1f} pipelines/sec")
    print(f"  Avg latency:  {latency_ms:.3f} ms/pipeline")
    return {"throughput": throughput, "latency_ms": latency_ms}

# ─────────────────────────────────────────────
# Benchmark 3: Serial latency
# Submit one task at a time, measure round-trip latency
# Most sensitive to per-task gossip overhead
# ─────────────────────────────────────────────

def benchmark_serial_latency(n_tasks=100, warmup=20):
    print(f"\n{'='*55}")
    print(f"Benchmark 3: Serial Latency  (N={n_tasks})")
    print(f"{'='*55}")

    # Warmup
    for i in range(warmup):
        ray.get(small_task.remote(i))

    # Measure — one task at a time
    latencies = []
    for i in range(n_tasks):
        start = time.perf_counter()
        ray.get(small_task.remote(i))
        latencies.append((time.perf_counter() - start) * 1000)

    print(f"  Mean latency: {np.mean(latencies):.3f} ms")
    print(f"  P50 latency:  {np.percentile(latencies, 50):.3f} ms")
    print(f"  P99 latency:  {np.percentile(latencies, 99):.3f} ms")
    print(f"  Min latency:  {np.min(latencies):.3f} ms")
    print(f"  Max latency:  {np.max(latencies):.3f} ms")
    return {
        "mean_ms": np.mean(latencies),
        "p50_ms": np.percentile(latencies, 50),
        "p99_ms": np.percentile(latencies, 99),
        "min_ms": np.min(latencies),
        "max_ms": np.max(latencies),
    }

# ─────────────────────────────────────────────
# Run all benchmarks
# ─────────────────────────────────────────────

label = sys.argv[1] if len(sys.argv) > 1 else "unknown"
print(f"\n{'#'*55}")
print(f"  gossip_recovery_enabled = {label}")
print(f"{'#'*55}")

results = {
    "label": label,
    "small_tasks": benchmark_small_tasks(n_tasks=500),
    "pipeline": benchmark_pipeline(n_pipelines=100),
    "serial_latency": benchmark_serial_latency(n_tasks=100),
}

outfile = f"/tmp/benchmark_{label}.json"
with open(outfile, "w") as f:
    json.dump(results, f, indent=2)
print(f"\nResults saved to {outfile}")

ray.shutdown()
