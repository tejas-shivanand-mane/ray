"""Complete Matplotlib code for Benchmark 04; edit draw() in this file."""
from plots import pyplot


def prepare_data(rows, runs, methods):
    """Retain the existing bucket means and recovery-fraction calculation."""
    data = {}
    for method in methods:
        selected = [r for r in rows if r["method"] == method]
        xs = sorted({r["seconds_from_failure"] for r in selected})
        means = [
            sum(r["throughput_rps"] for r in selected if r["seconds_from_failure"] == x)
            / sum(r["seconds_from_failure"] == x for r in selected)
            for x in xs
        ]
        selected_runs = [r for r in runs if r["method"] == method]
        fractions = [
            100 * r["post_failure_successes"] / (r["tasks"] - r["before_tasks"])
            for r in selected_runs
        ]
        data[method] = {"x": xs, "throughput": means,
                        "recovery_pct": sum(fractions) / len(fractions) if fractions else None}
    return data


def draw(rows, runs, out, methods):
    """Edit this function freely: every Matplotlib operation is visible here."""
    # Legacy saved trials predate the failure_type field and killed a worker.
    # Keep them honestly labelled when applying these styles to old results.
    failure_types = {r.get("failure_type", "owner_worker") for r in runs}
    if len(failure_types) != 1:
        raise ValueError("Cannot combine owner-worker and owner-node failure trials")
    failure_type = next(iter(failure_types))
    failure_label = {"owner_node": "Owner-node", "owner_worker": "Owner-worker"}[failure_type]
    data = prepare_data(rows, runs, methods)
    plt = pyplot()

    # 1. Figure-wide defaults. Put any Matplotlib rcParams here.
    with plt.rc_context({}):
        # 2. Figure and axes. Replace with GridSpec, extra panels, etc.
        fig, (ax_throughput, ax_recovery) = plt.subplots(1, 2, figsize=(12, 4.5))

        # 3. Curves and bars. Edit calls independently for complete control.
        # Blue remains visible around the thinner orange dashes and through
        # their gaps. Hollow squares surround orange dots at coincident points.
        # All markers and curves use the actual data coordinates (no offsets).
        styles = {
            "disabled": {"label": "Disabled", "color": "#555555", "marker": "x",
                         "linestyle": ":", "linewidth": 2, "markersize": 6, "zorder": 2},
            "fixed_r": {"label": "Fixed-R", "color": "#0072B2", "marker": "s",
                        "linestyle": "-", "linewidth": 3.5, "markersize": 9,
                        "markerfacecolor": "none", "markeredgewidth": 1.5, "zorder": 3},
            "succession": {"label": "Succession", "color": "#D55E00", "marker": "o",
                          "linestyle": (0, (4, 3)), "linewidth": 1.8, "markersize": 4,
                          "zorder": 4},
        }
        throughput_artists = {}
        recovery_artists = {}
        for position, method in enumerate(methods):
            style = styles[method]
            throughput_artists[method] = ax_throughput.plot(
                data[method]["x"], data[method]["throughput"], **style)
            if data[method]["recovery_pct"] is not None:
                recovery_artists[method] = ax_recovery.bar(
                    position, data[method]["recovery_pct"], width=0.8,
                    color=style["color"], label=style["label"])
        kill_line = ax_throughput.axvline(
            0, color="black", linestyle="--", label=f"{failure_label} failure")

        # 4. Timeline axis. Add locators, formatters, limits, scales, etc. here.
        ax_throughput.set_xlabel(f"Seconds relative to {failure_label.lower()} failure")
        ax_throughput.set_ylabel("Throughput (objects/s)")
        ax_throughput.set_title("Paced finite backlog; curve ends when observation ends")
        ax_throughput.grid(False)

        # 5. Recovery axis. Integer positions correspond to the methods above.
        ax_recovery.set_xticks(
            list(range(len(methods))), [styles[method]["label"] for method in methods])
        ax_recovery.set_ylabel("Unfinished objects recovered (%)")
        ax_recovery.set_ylim(0, 105)
        ax_recovery.set_title("Same ObjectIDs; both borrowers succeed")
        ax_recovery.grid(False)

        # 6. Legends and annotations. Keep handles for further customization.
        throughput_legend = ax_throughput.legend(handlelength=3.5)
        title = fig.suptitle(
            f"{failure_label} failure, R=2 W=2; diagnostic, not steady-state throughput")

        # 7. Layout. Edit or replace this directly.
        fig.tight_layout()

        # 8. Final custom edits. Nothing below reapplies axes or layout settings.
        # Add inset/secondary axes, annotations, or modify any artist here.

        # 9. Exports. Change filenames, formats and savefig options directly.
        png = out / "04_owner_failure_throughput.png"
        pdf = out / "04_owner_failure_throughput.pdf"
        fig.savefig(png, dpi=180, bbox_inches="tight")
        fig.savefig(pdf, dpi=180, bbox_inches="tight")
        print(f"Plot: {png}", flush=True)
        print(f"Plot: {pdf}", flush=True)
        plt.close(fig)
