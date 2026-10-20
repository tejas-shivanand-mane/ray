"""Published algorithm kernels and explicitly labelled model adaptations.

No external dependencies, training, or automatic experiment execution on import.
Sources, equations and deviations are documented in LITERATURE_BENCHMARK.md.
"""

import itertools
import math


SOURCES = {
    "young_daly": "https://arxiv.org/pdf/1302.3752",
    "checkfreq": "https://www.usenix.org/system/files/fast21-mohan.pdf",
    "han": "https://henricasanova.github.io/files/papers/han_tc2018.pdf",
}
INTERVAL_METHODS = (
    "young_interval_adapted",
    "daly_interval_adapted",
    "checkfreq_bound_adapted",
)
OVERHEAD_BOUNDS = (0.01, 0.035, 0.05, 0.10)


def checkfreq_algorithm1(ti, tw, tc, tg, ts, size, used, capacity, overhead):
    """FAST'21 Algorithm 1; round its real-valued interval up to iterations."""
    if ti <= 0 or overhead <= 0:
        raise ValueError("Positive iteration time and overhead bound required")
    cpu_stall = max(0.0, tc - (ti - tw))
    if capacity - used > size and tg <= cpu_stall:
        stall, mode = tg, "GPU"
    else:
        stall, mode = cpu_stall, "CPU"
    interval = max(1, math.ceil((tc + ts - stall) / ti),
                   math.ceil(stall / (overhead * ti)))
    return dict(interval=interval, mode=mode, stall_s=stall)


def interval_candidates(problem, schedules, checkpoints, scenarios, reference):
    """Return allowed row IDs and raw rule diagnostics, using planning data only.

    Young/Daly: evaluate their continuous approximation on the shared discrete
    interval menu. CheckFreq: synchronous specialization of Algorithm 1; choose
    the shortest available interval satisfying its bound. Both are adaptations.
    Placement is optimized externally using the benchmark's common objective.
    """
    tasks, machines, estimates = problem
    menus = {t: sorted({c[t] for c in checkpoints}) for t in tasks}
    lookup = {tuple(c[t] for t in tasks): i for i, c in enumerate(checkpoints)}
    probabilities = {
        m: sum(s["probability"] for s in scenarios
               if s["failures"].get(m, math.inf) <= reference)
        for m in machines
    }
    # Match marginal probability of >=1 loss over the common reference window.
    # The evaluator remains a finite-scenario model, NOT a Poisson process.
    rates = {m: -math.log1p(-p) / reference for m, p in probabilities.items()}
    candidates = {m: {} for m in INTERVAL_METHODS}
    for sid, schedule in enumerate(schedules):
        for method in INTERVAL_METHODS:
            bounds = OVERHEAD_BOUNDS if method == "checkfreq_bound_adapted" else (None,)
            for bound in bounds:
                vector, diagnostics = {}, {}
                for task in tasks:
                    node = schedule["copies"][task][0]
                    profile = estimates[task][node]
                    step, save, rate = profile["epoch_s"], profile["save_s"], rates[node]
                    if bound is not None:
                        # No overlap in this simulator: Tc=C, Tw=Ti, Ts=0,
                        # GPU mode disabled. The exact kernel reduces to C/(p Ti).
                        rule = checkfreq_algorithm1(step, step, save, save, 0, 1, 0, 0, bound)
                        raw = rule["interval"]
                        eligible = [k for k in menus[task] if k >= raw]
                        chosen = min(eligible) if eligible else max(menus[task])
                        detail = dict(raw_interval=raw, overhead_bound=bound,
                                      bound_satisfied=save <= bound * chosen * step + 1e-12)
                    elif rate == 0:
                        chosen = max(menus[task])
                        detail = dict(raw_interval=None, equivalent_mtbf_s=None)
                    else:
                        mu = 1 / rate
                        effective_mu = mu + (profile["init_s"] if method == "daly_interval_adapted" else 0)
                        raw = math.sqrt(2 * save * effective_mu) / step
                        # Minimize the discrete form C/W + W/(2*effective_mu),
                        # rather than arbitrary nearest-integer snapping.
                        chosen = min(menus[task], key=lambda k: (
                            save / (k * step) + k * step / (2 * effective_mu), k
                        ))
                        detail = dict(raw_interval=raw, equivalent_mtbf_s=mu)
                    vector[task] = chosen
                    diagnostics[task] = dict(node=node, chosen_interval=chosen, **detail)
                cid = lookup[tuple(vector[t] for t in tasks)]
                rid = sid * len(checkpoints) + cid
                candidates[method].setdefault(rid, []).append(diagnostics)
    return dict(candidates=candidates, marginal_loss_probabilities=probabilities,
                equivalent_rates_per_s=rates)


def han_segment_times(work, edges, rate, downtime=0.0):
    """Han Eq. 2 for one topologically ordered superchain, zero-based nodes.

    edges: (producer, consumer, read/write seconds), including external input
    producer -1 and external output consumer len(work). Read all incoming edges
    and save all outgoing edges crossing a segment, including skip edges.
    """
    n = len(work)
    prefix = [0.0]
    for value in work:
        prefix.append(prefix[-1] + value)
    times = {}
    for start in range(n):
        for stop in range(start + 1, n + 1):
            read = sum(c for a, b, c in edges if a < start <= b < stop)
            save = sum(c for a, b, c in edges if start <= a < stop <= b)
            duration = read + prefix[stop] - prefix[start] + save
            times[start, stop] = (
                duration if rate == 0 else (1 / rate + downtime) * math.expm1(rate * duration)
            )
    return times


def han_checkpoint_dp(times, n, required=()):
    """Han Algorithm 2 recurrence and backtracking on a fixed superchain.

    Required boundaries are used only for our heterogeneous-placement adapter.
    With none supplied this is the paper's fixed-superchain recurrence.
    """
    best, previous = [0.0] + [math.inf] * n, [None] * (n + 1)
    for stop in range(1, n + 1):
        for start in range(stop):
            if any(start < cut < stop for cut in required):
                continue
            value = best[start] + times[start, stop]
            if value < best[stop]:
                best[stop], previous[stop] = value, start
    cuts, stop = [], n
    while stop:
        cuts.append(stop)
        stop = previous[stop]
    return dict(expected_time_s=best[n], checkpoint_after=[c - 1 for c in reversed(cuts)])


def han_panel(check_budget):
    """48 analytical chain cases, native expected-time objective (no deadline).

    This reproduces the checkpoint kernel, not Han's full M-SPG mapper. Our
    joint reference can place each checkpoint-delimited segment on any node.
    Homogeneous controls and a failure-blind mapping baseline expose placement
    advantages that have nothing to do with checkpoint selection.
    """
    results = []
    for n, affinity, save_fraction, risk, rate in itertools.product(
        (4, 8, 12), (False, True), (0.005, 0.10), ("equal", "mixed"), (0.0001, 0.002)
    ):
        check_budget()
        work = [30.0 * (1 + (i % 3) / 2) for i in range(n)]
        edges = [(-1, 0, 30 * save_fraction)] + [
            (i, i + 1, 30 * save_fraction) for i in range(n)
        ]
        speeds = [[1.5 if affinity and i % 3 == node else 1.0 for i in range(n)]
                  for node in range(3)]
        rates = [rate, rate, rate] if risk == "equal" else [rate / 20, rate, rate]
        tables = [han_segment_times([w / s for w, s in zip(work, speeds[node])],
                                    edges, rates[node], downtime=1.0) for node in range(3)]
        healthy_tables = [han_segment_times([w / s for w, s in zip(work, speeds[node])],
                                            edges, 0) for node in range(3)]

        def joint(tables):
            best, prev = [0.0] + [math.inf] * n, [None] * (n + 1)
            for stop in range(1, n + 1):
                for start in range(stop):
                    for node, table in enumerate(tables):
                        value = best[start] + table[start, stop]
                        if value < best[stop]:
                            best[stop], prev[stop] = value, (start, node)
            segments, stop = [], n
            while stop:
                start, node = prev[stop]
                segments.append(dict(start=start, stop=stop, node=node))
                stop = start
            return dict(expected_time_s=best[n], segments=list(reversed(segments)))

        # Failure-blind placement uses the same segments/IO model with lambda=0.
        healthy = joint(healthy_tables)
        placement = [None] * n
        for segment in healthy["segments"]:
            placement[segment["start"]:segment["stop"]] = [segment["node"]] * (segment["stop"] - segment["start"])
        required = [i for i in range(1, n) if placement[i] != placement[i - 1]]
        mapped = {(a, b): tables[placement[a]][a, b]
                  for a in range(n) for b in range(a + 1, n + 1)}
        baseline = han_checkpoint_dp(mapped, n, required)
        fixed = [dict(node=node, **han_checkpoint_dp(table, n)) for node, table in enumerate(tables)]
        best_fixed = min(fixed, key=lambda r: (r["expected_time_s"], r["node"]))
        optimum = joint(tables)
        results.append(dict(
            case_id=f"han-{len(results):03d}", tasks=n, affinity=affinity,
            save_fraction=save_fraction, risk=risk, base_rate_per_s=rate,
            work_s=work, edges=edges, speeds=speeds, rates_per_s=rates,
            downtime_s=1.0, healthy_mapping=placement,
            han_dp_after_healthy_mapping=baseline, han_dp_best_fixed_node=best_fixed,
            ckptall_after_healthy_mapping_s=sum(mapped[i, i + 1] for i in range(n)),
            ckptnone_best_fixed_node_s=min(table[0, n] for table in tables),
            joint_segment_dp=optimum,
            joint_gain_vs_healthy_mapping=1 - optimum["expected_time_s"] / baseline["expected_time_s"],
            joint_gain_vs_best_fixed=1 - optimum["expected_time_s"] / best_fixed["expected_time_s"],
        ))
    return dict(
        state="complete", cases=results, source=SOURCES["han"],
        objective="expected_makespan", statistical_repetitions=False,
        scope="Han Eq.2/Algorithm 2 kernel on chains; mapping adapters are ours, not full CKPTSOME.",
        assumptions="Poisson repeated failures; same-speed replacement after downtime; durable boundary outputs; no prices or deadline probabilities.",
    )
