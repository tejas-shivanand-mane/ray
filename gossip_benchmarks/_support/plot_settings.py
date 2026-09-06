"""Edit plot appearance here, then use the benchmark's `plot` command.

No workload, aggregation, or confidence-interval settings live in this file.
Axis tick positions use plotted coordinates: B01 uses 0,1,... for ordered K;
B02 uses bytes; B04 uses seconds (left) and 0,1,2 method positions (right).
None means keep automatic/data-derived values. Tick labels must match ticks.
"""

# Matplotlib rcParams: applies inside a context only while drawing a figure.
# Examples: {"font.family": "DejaVu Sans", "font.size": 12}
RC_PARAMS = {}

COLORS = {"disabled": "#555555", "fixed_r": "#0072B2", "succession": "#D55E00"}
ERROR_BARS = {"linewidth": 1.8, "markersize": 5, "capsize": 3}


def axis(**overrides):
    """Shared defaults; each panel below can override any of these keys."""
    settings = {
        "xlabel": "", "ylabel": "", "title": "",
        "xlim": None, "ylim": None,
        "xscale": None, "yscale": None,
        "xscale_kwargs": {}, "yscale_kwargs": {},
        "xticks": None, "xticklabels": None,
        "yticks": None, "yticklabels": None,
        "xrotation": 0, "yrotation": 0,
        "label_fontsize": None, "title_fontsize": None, "tick_fontsize": 10,
        "grid": {"axis": "y", "alpha": 0.22},
        "hide_spines": ("top", "right"),
        "legend": {"fontsize": 9},  # None hides the legend.
    }
    settings.update(overrides)
    return settings


# Benchmark 01: placeholders in title/footer are filled from saved data.
FRONTIER = {
    "figsize": (12, 4.8),
    "formats": ("png", "pdf"), "dpi": 200,
    "filename": "fixed_vs_succession_k_padding_{padding}",
    "savefig": {"bbox_inches": "tight"},
    "title": "Fixed-R and Succession across K — object {payload_label}, "
             "TaskSpec padding {padding_label}",
    "title_kwargs": {"fontsize": 12},
    "footer": "R=2, W=2 · {repetitions} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
              "Overhead is paired within repetition; disabled is one shared reference, not a K sweep.",
    "footer_position": (0.5, 0.01),
    "footer_kwargs": {"ha": "center", "fontsize": 9},
    "tight_layout": {"rect": (0, 0.10, 1, 0.94)},
    "throughput": axis(xlabel="Frontier group size K",
                       ylabel="Application throughput (tasks/s)", ylim=(0, None)),
    "overhead": axis(xlabel="Frontier group size K",
                     ylabel="Throughput overhead versus disabled (%)"),
}
FRONTIER_SERIES = {
    "disabled": {"label": "Disabled (shared reference)", "color": COLORS["disabled"],
                 "marker": "", "linestyle": "--"},
    "fixed_r": {"label": "Fixed-R", "color": COLORS["fixed_r"],
                "marker": "o", "linestyle": "-"},
    "succession": {"label": "Succession", "color": COLORS["succession"],
                   "marker": "o", "linestyle": "-"},
}
FRONTIER_ZERO_LINE = {"color": COLORS["disabled"], "linestyle": "--", "label": "Disabled"}

# Benchmark 02: x ticks are bytes, even when labels say KiB or MiB.
OBJECT_SIZES = {
    "figsize": (13, 5),
    "formats": ("png", "pdf"), "dpi": 200,
    "filename": "object_size_comparison",
    "savefig": {"bbox_inches": "tight"},
    "title": "Object-size effect — fixed TaskSpec padding {padding_label}",
    "title_kwargs": {"fontsize": 13},
    "footer": "R=2, W=2 · K=1/32 · {repetitions} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
              "Each overhead uses the disabled run at the same object size and repetition.",
    "footer_position": (0.5, 0.01),
    "footer_kwargs": {"ha": "center", "fontsize": 9},
    "tight_layout": {"rect": (0, 0.12, 1, 0.94)},
    "throughput": axis(xlabel="Returned object payload size",
                       ylabel="Application throughput (tasks/s)", ylim=(0, None),
                       xscale="log", xscale_kwargs={"base": 2}, xrotation=25),
    "overhead": axis(xlabel="Returned object payload size",
                     ylabel="Throughput overhead versus same-size disabled (%)",
                     xscale="log", xscale_kwargs={"base": 2}, xrotation=25),
}
OBJECT_SIZE_SERIES = {
    "disabled": {"label": "Disabled", "color": COLORS["disabled"],
                 "marker": "s", "linestyle": "--"},
    "fixed_r": {"label": "Fixed-R K=1", "color": COLORS["fixed_r"],
                "marker": "o", "linestyle": "--"},
    "fixed_k32": {"label": "Fixed-R K=32", "color": COLORS["fixed_r"],
                  "marker": "o", "linestyle": "-"},
    "succession_k1": {"label": "Succession K=1", "color": COLORS["succession"],
                      "marker": "^", "linestyle": "--"},
    "succession_k32": {"label": "Succession K=32", "color": COLORS["succession"],
                       "marker": "^", "linestyle": "-"},
}

# Benchmark 03 currently writes profile data and logs, not figures.

# Benchmark 04: presentation only; --bucket-seconds controls data aggregation.
OWNER_FAILURE = {
    "figsize": (12, 4.5),
    "formats": ("png", "pdf"), "dpi": 180,
    "filename": "owner_failure",
    "savefig": {"bbox_inches": "tight"},
    "title": "Owner-worker failure, R=2 W=2; diagnostic, not steady-state throughput",
    "title_kwargs": {},
    "footer": "", "footer_position": (0.5, 0.01),
    "footer_kwargs": {"ha": "center", "fontsize": 9},
    "tight_layout": {},
    "throughput": axis(xlabel="Seconds relative to owner kill",
                       ylabel="Successful unique-object reads / s",
                       title="Paced finite backlog; curve ends when observation ends",
                       grid=None, hide_spines=(), tick_fontsize=None, legend={}),
    "recovery": axis(ylabel="Unfinished objects recovered (%)", ylim=(0, 105),
                     title="Same ObjectIDs; both borrowers succeed",
                     grid=None, hide_spines=(), tick_fontsize=None, legend=None),
}
OWNER_FAILURE_SERIES = {
    "disabled": {"label": "disabled", "color": COLORS["disabled"], "marker": "o"},
    "fixed_r": {"label": "fixed_r", "color": COLORS["fixed_r"], "marker": "o"},
    "succession": {"label": "succession", "color": COLORS["succession"], "marker": "o"},
}
OWNER_KILL_LINE = {"color": "black", "linestyle": "--", "label": "Owner kill"}
OWNER_BAR = {"width": 0.8}
