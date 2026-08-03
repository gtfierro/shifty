#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Summarize bench_evidence.sh output into the tables the evidence paper reports.

Reads the CSV written by ``benchmark/bench_evidence.sh`` and prints, per suite
and over the pooled corpus:

  * latency overhead of dual-evidence execution over conformance-only execution
    (per-model ratios; the headline is the geometric mean, since ratios compose
    multiplicatively and the corpus spans three orders of magnitude in size);
  * evidence nodes per selected pair;
  * serialized evidence bytes per selected pair;
  * pass/fail and selected-pair counts.

Text tables need no dependencies (``python3 summarize_evidence.py …``);
``--figures`` additionally needs matplotlib, which ``uv run`` installs from the
header above.

Usage:
  uv run benchmark/summarize_evidence.py results/evidence.csv \
      --per-model --latex paper/evidence.tex --figures paper/figures
"""

import argparse
import csv
import math
import statistics
import sys
from collections import defaultdict


def geomean(values: list[float]) -> float:
    values = [v for v in values if v > 0]
    if not values:
        return float("nan")
    return math.exp(sum(math.log(v) for v in values) / len(values))


def geosd(values: list[float]) -> float:
    """Geometric standard deviation: the multiplicative spread of a ratio.

    An arithmetic stddev over ratios is the wrong dispersion — a 2x speedup and
    a 0.5x slowdown are equal and opposite, but arithmetic treats them as 1.5
    apart. The GSD is read as ``geomean x/÷ gsd``.
    """
    values = [v for v in values if v > 0]
    if len(values) < 2:
        return float("nan")
    logs = [math.log(v) for v in values]
    mean = sum(logs) / len(logs)
    variance = sum((v - mean) ** 2 for v in logs) / (len(logs) - 1)
    return math.exp(math.sqrt(variance))


def load(path: str) -> list[dict]:
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    numeric = {
        "data_triples": int,
        "iters": int,
        "conformance_ms_mean": float,
        "conformance_ms_sd": float,
        "conformance_cv": float,
        "conformance_ms_median": float,
        "conformance_ms_min": float,
        "conformance_ms_mad": float,
        "evidence_ms_mean": float,
        "evidence_ms_sd": float,
        "evidence_cv": float,
        "evidence_ms_median": float,
        "evidence_ms_min": float,
        "evidence_ms_mad": float,
        "overhead_ratio_mean": float,
        "overhead_ratio_sd": float,
        "overhead_ratio_median": float,
        "overhead_ms": float,
        "serialize_ms_median": float,
        "evaluated_pairs": int,
        "pass_pairs": int,
        "fail_pairs": int,
        "authored_pairs": int,
        "authored_pass_pairs": int,
        "authored_fail_pairs": int,
        "statements": int,
        "evidence_nodes": int,
        "evidence_nodes_per_pair": float,
        "evidence_bytes": int,
        "evidence_bytes_per_pair": float,
        "run_bytes": int,
        "compact_bytes": int,
        "compact_bytes_no_catalog": int,
        "node_occurrences": int,
        "distinct_nodes": int,
        "node_redundancy": float,
        "term_occurrences": int,
        "distinct_terms": int,
        "term_redundancy": float,
    }
    for row in rows:
        # Sharing columns postdate the first published runs; a CSV without them
        # still summarizes, rather than failing on a column nothing plots.
        for key, cast in numeric.items():
            if key in row:
                row[key] = cast(row[key])
        row["conforms_conformance"] = row["conforms_conformance"] == "true"
        row["conforms_evidence"] = row["conforms_evidence"] == "true"
    return rows


def summarize(name: str, rows: list[dict]) -> dict:
    ratios = [row["overhead_ratio_mean"] for row in rows]
    pairs = sum(row["evaluated_pairs"] for row in rows)
    nodes = sum(row["evidence_nodes"] for row in rows)
    ev_bytes = sum(row["evidence_bytes"] for row in rows)
    return {
        "suite": name,
        "models": len(rows),
        "conformance_ms": sum(row["conformance_ms_mean"] for row in rows),
        "evidence_ms": sum(row["evidence_ms_mean"] for row in rows),
        "serialize_ms": sum(row["serialize_ms_median"] for row in rows),
        "ratio_geomean": geomean(ratios),
        "ratio_geosd": geosd(ratios),
        "ratio_median": statistics.median(ratios),
        # Within-model measurement noise, as the median coefficient of
        # variation over the corpus. This is the benchmark's own repeatability,
        # distinct from the across-model spread in overhead.
        "conformance_cv_median": statistics.median(
            row["conformance_cv"] for row in rows
        ),
        "evidence_cv_median": statistics.median(row["evidence_cv"] for row in rows),
        "min_iters": min(row["iters"] for row in rows),
        "ratio_min": min(ratios),
        "ratio_max": max(ratios),
        "pairs": pairs,
        "pass_pairs": sum(row["pass_pairs"] for row in rows),
        "fail_pairs": sum(row["fail_pairs"] for row in rows),
        "authored_pairs": sum(row["authored_pairs"] for row in rows),
        "conforming_models": sum(1 for row in rows if row["conforms_evidence"]),
        # Per-pair latency pools the corpus: total time over total pairs, so
        # large models weigh in proportion to the work they actually carry.
        "conformance_us_per_pair": (
            sum(row["conformance_ms_mean"] for row in rows) * 1e3 / pairs
            if pairs
            else float("nan")
        ),
        "evidence_us_per_pair": (
            sum(row["evidence_ms_mean"] for row in rows) * 1e3 / pairs
            if pairs
            else float("nan")
        ),
        "nodes": nodes,
        "nodes_per_pair": nodes / pairs if pairs else float("nan"),
        "bytes": ev_bytes,
        "bytes_per_pair": ev_bytes / pairs if pairs else float("nan"),
        "run_bytes": sum(row["run_bytes"] for row in rows),
        "compact_bytes": sum(row["compact_bytes"] for row in rows),
        "compact_bytes_no_catalog": sum(
            row["compact_bytes_no_catalog"] for row in rows
        ),
        "compact_per_pair": (
            sum(row["compact_bytes_no_catalog"] for row in rows) / pairs
            if pairs
            else float("nan")
        ),
        "compact_ratio": (
            sum(row["compact_bytes"] for row in rows)
            / max(1, sum(row["run_bytes"] for row in rows))
        ),
        "compact_ratio_no_catalog": (
            sum(row["compact_bytes_no_catalog"] for row in rows)
            / max(1, sum(row["run_bytes"] for row in rows))
        ),
        "mismatches": [
            row["model"]
            for row in rows
            if row["conforms_conformance"] != row["conforms_evidence"]
        ],
    }


def emit(summaries: list[dict], markdown: bool) -> None:
    columns = [
        ("suite", "suite", "{}"),
        ("models", "models", "{}"),
        ("pairs", "sel. pairs", "{}"),
        ("pass_pairs", "pass", "{}"),
        ("fail_pairs", "fail", "{}"),
        ("conformance_ms", "conf ms", "{:.1f}"),
        ("evidence_ms", "evid ms", "{:.1f}"),
        ("ratio_geomean", "overhead x", "{:.2f}"),
        ("ratio_geosd", "x/div", "{:.2f}"),
        ("ratio_min", "min x", "{:.2f}"),
        ("ratio_max", "max x", "{:.2f}"),
        ("conformance_us_per_pair", "conf us/pair", "{:.1f}"),
        ("evidence_us_per_pair", "evid us/pair", "{:.1f}"),
        ("nodes_per_pair", "nodes/pair", "{:.2f}"),
        ("bytes_per_pair", "bytes/pair", "{:.0f}"),
        ("compact_per_pair", "compact/pair", "{:.0f}"),
        ("serialize_ms", "ser ms", "{:.1f}"),
    ]
    rows = [[fmt.format(s[key]) for key, _, fmt in columns] for s in summaries]
    headers = [label for _, label, _ in columns]
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(len(headers))
    ]

    def line(cells: list[str]) -> str:
        padded = [cell.rjust(widths[i]) for i, cell in enumerate(cells)]
        return "| " + " | ".join(padded) + " |" if markdown else "  ".join(padded)

    print(line(headers))
    print(
        "|" + "|".join("-" * (w + 2) for w in widths) + "|"
        if markdown
        else "  ".join("-" * w for w in widths)
    )
    for row in rows:
        print(line(row))


def emit_per_model(rows: list[dict], markdown: bool) -> None:
    columns = [
        ("model", "model", "{}"),
        ("data_triples", "triples", "{}"),
        ("iters", "n", "{}"),
        ("evaluated_pairs", "pairs", "{}"),
        ("fail_pairs", "fail", "{}"),
        ("conformance_ms_mean", "conf ms", "{:.2f}"),
        ("conformance_ms_sd", "+/-", "{:.2f}"),
        ("evidence_ms_mean", "evid ms", "{:.2f}"),
        ("evidence_ms_sd", "+/-", "{:.2f}"),
        ("overhead_ratio_mean", "x", "{:.2f}"),
        ("overhead_ratio_sd", "+/-", "{:.2f}"),
        ("evidence_nodes_per_pair", "nodes/pair", "{:.2f}"),
        ("evidence_bytes_per_pair", "bytes/pair", "{:.0f}"),
    ]
    body = [[fmt.format(row[key]) for key, _, fmt in columns] for row in rows]
    headers = [label for _, label, _ in columns]
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in body)) for i in range(len(headers))
    ]

    def line(cells: list[str]) -> str:
        padded = [
            cell.ljust(widths[i]) if i == 0 else cell.rjust(widths[i])
            for i, cell in enumerate(cells)
        ]
        return "| " + " | ".join(padded) + " |" if markdown else "  ".join(padded)

    print(line(headers))
    print(
        "|" + "|".join("-" * (w + 2) for w in widths) + "|"
        if markdown
        else "  ".join("-" * w for w in widths)
    )
    for row in body:
        print(line(row))


SUITE_LABELS = {"brick": "Brick", "s223": "ASHRAE 223P", "lubm": "LUBM", "all": "All"}

# Categorical slots 1–3 of the validated reference palette. Three is the cap
# that clears the all-pairs colour-vision floors, which is the pairlist scatter
# plots need; a fourth suite folds into a facet rather than a new hue.
SUITE_COLORS = {"brick": "#2a78d6", "s223": "#eb6834", "lubm": "#1baf7a"}
GRID = "#d8d7d2"
INK = "#0b0b0b"
MUTED = "#52514e"


def latex_escape(text: str) -> str:
    for old, new in (("_", r"\_"), ("&", r"\&"), ("%", r"\%"), ("#", r"\#")):
        text = text.replace(old, new)
    return text


def emit_latex(summaries: list[dict], rows: list[dict], path: str) -> None:
    """Write booktabs tables: one summary, one per-model appendix."""
    lines = [
        "% Generated by benchmark/summarize_evidence.py -- do not edit by hand.",
        "% Requires \\usepackage{booktabs}",
        "",
        "\\begin{table}[t]",
        "  \\centering",
        "  \\caption{Cost of dual-evidence execution relative to conformance-only "
        "execution over the same prepared validator snapshot. Overhead is the "
        "geometric mean of per-model paired ratios, with its geometric standard "
        "deviation ($\\times\\!/\\!\\div$).}",
        "  \\label{tab:evidence-overhead}",
        "  \\begin{tabular}{lrrrrrrr}",
        "    \\toprule",
        "    Suite & Models & Pairs & \\multicolumn{2}{c}{Latency ($\\mu$s/pair)} "
        "& Overhead & \\multicolumn{2}{c}{Bytes/pair} \\\\",
        "    \\cmidrule(lr){4-5} \\cmidrule(lr){7-8}",
        "     & & & Conf. & Evid. & ($\\times$) & Full & Compact \\\\",
        "    \\midrule",
    ]
    for summary in summaries:
        label = SUITE_LABELS.get(summary["suite"], summary["suite"])
        if summary["suite"] == "all":
            lines.append("    \\midrule")
        lines.append(
            f"    {latex_escape(label)} & {summary['models']} & "
            f"{summary['pairs']:,} & "
            f"{summary['conformance_us_per_pair']:.1f} & "
            f"{summary['evidence_us_per_pair']:.1f} & "
            f"{summary['ratio_geomean']:.2f} $\\times\\!/\\!\\div$ "
            f"{summary['ratio_geosd']:.2f} & "
            f"{summary['bytes_per_pair']:,.0f} & "
            f"{summary['compact_per_pair']:,.0f} \\\\"
        )
    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
        "\\begin{table}[t]",
        "  \\centering",
        "  \\caption{Per-model evidence cost. Times are means over "
        f"{rows[0]['iters']} interleaved iterations, $\\pm$ one sample standard "
        "deviation.}",
        "  \\label{tab:evidence-per-model}",
        "  \\small",
        "  \\begin{tabular}{llrrrrr}",
        "    \\toprule",
        "    Suite & Model & Triples & Pairs & Conf. (ms) & Evid. (ms) & "
        "Overhead ($\\times$) \\\\",
        "    \\midrule",
    ]
    for row in rows:
        lines.append(
            f"    {latex_escape(SUITE_LABELS.get(row['suite'], row['suite']))} & "
            f"{latex_escape(row['model'].removesuffix('.ttl'))} & "
            f"{row['data_triples']:,} & {row['evaluated_pairs']:,} & "
            f"{row['conformance_ms_mean']:.1f} $\\pm$ {row['conformance_ms_sd']:.1f} & "
            f"{row['evidence_ms_mean']:.1f} $\\pm$ {row['evidence_ms_sd']:.1f} & "
            f"{row['overhead_ratio_mean']:.2f} $\\pm$ {row['overhead_ratio_sd']:.2f} \\\\"
        )
    lines += ["    \\bottomrule", "  \\end{tabular}", "\\end{table}", ""]

    with open(path, "w") as handle:
        handle.write("\n".join(lines))
    print(f"wrote {path}", file=sys.stderr)


def emit_figures(by_suite: dict, summaries: list[dict], directory: str) -> None:
    """Write the paper figures as vector PDF plus a PNG preview."""
    try:
        import matplotlib
    except ModuleNotFoundError:
        print(
            "--figures needs matplotlib: run this script with "
            "`uv run benchmark/summarize_evidence.py …`, which installs it.",
            file=sys.stderr,
        )
        raise SystemExit(2) from None

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from pathlib import Path

    Path(directory).mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 8,
            "axes.labelsize": 8,
            "axes.titlesize": 8,
            "legend.fontsize": 7,
            "xtick.labelsize": 7,
            "ytick.labelsize": 7,
            "axes.edgecolor": MUTED,
            "axes.labelcolor": INK,
            "text.color": INK,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "figure.dpi": 150,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.02,
        }
    )

    def new_axes(width=3.4, height=2.5):
        figure, axes = plt.subplots(figsize=(width, height))
        # Recessive grid, beneath the marks; no top/right spines.
        axes.grid(True, color=GRID, linewidth=0.5, zorder=0)
        axes.set_axisbelow(True)
        for side in ("top", "right"):
            axes.spines[side].set_visible(False)
        return figure, axes

    def label_diagonal(axes, span, text, at=0.16):
        """Label a y=x reference line at the angle it actually renders.

        The screen angle of y=x depends on the axes' aspect and limits, so a
        hard-coded rotation only looks right by accident.
        """
        figure = axes.get_figure()
        figure.canvas.draw()
        low, high = axes.transData.transform([(span[0], span[0]), (span[1], span[1])])
        angle = math.degrees(math.atan2(high[1] - low[1], high[0] - low[0]))
        position = math.exp(math.log(span[0]) + at * (math.log(span[1]) - math.log(span[0])))
        axes.annotate(
            text,
            xy=(position, position),
            color=MUTED,
            fontsize=6.5,
            rotation=angle,
            rotation_mode="anchor",
            ha="left",
            va="bottom",
            xytext=(0, 2),
            textcoords="offset points",
        )

    def save(figure, name):
        for suffix in ("pdf", "png"):
            figure.savefig(f"{directory}/{name}.{suffix}")
        plt.close(figure)
        print(f"wrote {directory}/{name}.pdf", file=sys.stderr)

    suites = list(by_suite)

    # --- Figure 1: what evidence costs, against what conformance costs -------
    # A relationship between two measures on the same scale: log-log scatter,
    # with y=x as the "free" reference. Distance above y=x *is* the overhead.
    figure, axes = new_axes()
    for suite in suites:
        group = by_suite[suite]
        axes.errorbar(
            [row["conformance_ms_mean"] for row in group],
            [row["evidence_ms_mean"] for row in group],
            xerr=[row["conformance_ms_sd"] for row in group],
            yerr=[row["evidence_ms_sd"] for row in group],
            fmt="o",
            markersize=4,
            linewidth=0,
            elinewidth=0.6,
            color=SUITE_COLORS.get(suite, INK),
            markeredgecolor="white",
            markeredgewidth=0.4,
            label=SUITE_LABELS.get(suite, suite),
            zorder=3,
        )
    limits = [
        min(row["conformance_ms_mean"] for row in rows_of(by_suite)) * 0.7,
        max(row["evidence_ms_mean"] for row in rows_of(by_suite)) * 1.4,
    ]
    axes.plot(limits, limits, color=MUTED, linewidth=0.8, linestyle="--", zorder=2)
    axes.set(
        xscale="log",
        yscale="log",
        xlim=limits,
        ylim=limits,
        xlabel="conformance-only (ms)",
        ylabel="dual evidence (ms)",
    )
    axes.legend(frameon=False, loc="upper left")
    label_diagonal(axes, limits, "no overhead", at=0.82)
    save(figure, "evidence_latency")

    # --- Figure 2: how overhead is distributed -------------------------------
    # A distribution, so an ECDF: every model is a step, and the reader can
    # take any quantile off it. Box plots hide n and invent conventions.
    figure, axes = new_axes()
    for suite in suites:
        ratios = sorted(row["overhead_ratio_mean"] for row in by_suite[suite])
        fractions = [(index + 1) / len(ratios) for index in range(len(ratios))]
        axes.step(
            ratios,
            fractions,
            where="post",
            color=SUITE_COLORS.get(suite, INK),
            linewidth=2,
            label=f"{SUITE_LABELS.get(suite, suite)} (n={len(ratios)})",
            zorder=3,
        )
    for summary in summaries:
        if summary["suite"] == "all":
            continue
        axes.axvline(
            summary["ratio_geomean"],
            color=SUITE_COLORS.get(summary["suite"], INK),
            linewidth=0.8,
            linestyle=":",
            zorder=2,
        )
    axes.set(
        xlabel="evidence overhead ($\\times$ conformance-only)",
        ylabel="fraction of models",
        ylim=(0, 1.02),
    )
    axes.legend(frameon=False, loc="lower right")
    save(figure, "evidence_overhead_ecdf")

    # --- Figure 3: does overhead grow with problem size? ---------------------
    figure, axes = new_axes()
    for suite in suites:
        group = by_suite[suite]
        axes.scatter(
            [row["evaluated_pairs"] for row in group],
            [row["overhead_ratio_mean"] for row in group],
            s=16,
            color=SUITE_COLORS.get(suite, INK),
            edgecolor="white",
            linewidth=0.4,
            label=SUITE_LABELS.get(suite, suite),
            zorder=3,
        )
    axes.set(
        xscale="log",
        xlabel="selected (statement, focus) pairs",
        ylabel="evidence overhead ($\\times$)",
    )
    axes.legend(frameon=False, loc="upper left")
    save(figure, "evidence_overhead_vs_size")

    # --- Figure 4: what compaction buys --------------------------------------
    figure, axes = new_axes()
    for suite in suites:
        group = by_suite[suite]
        axes.scatter(
            [row["evidence_bytes_per_pair"] for row in group],
            [
                row["compact_bytes_no_catalog"] / max(1, row["evaluated_pairs"])
                for row in group
            ],
            s=16,
            color=SUITE_COLORS.get(suite, INK),
            edgecolor="white",
            linewidth=0.4,
            label=SUITE_LABELS.get(suite, suite),
            zorder=3,
        )
    span = [
        min(row["evidence_bytes_per_pair"] for row in rows_of(by_suite)) * 0.5,
        max(row["evidence_bytes_per_pair"] for row in rows_of(by_suite)) * 1.5,
    ]
    axes.plot(span, span, color=MUTED, linewidth=0.8, linestyle="--", zorder=2)
    # Equal limits on both axes: vertical distance below the diagonal is then
    # exactly the saving, which is the whole point of the figure.
    axes.set(
        xscale="log",
        yscale="log",
        xlim=span,
        ylim=span,
        xlabel="full evidence (bytes/pair)",
        ylabel="compact encoding (bytes/pair)",
    )
    axes.legend(frameon=False, loc="upper left")
    label_diagonal(axes, span, "no saving", at=0.78)
    save(figure, "evidence_compaction")


def rows_of(by_suite: dict) -> list[dict]:
    return [row for group in by_suite.values() for row in group]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", help="output of bench_evidence.sh")
    parser.add_argument(
        "--per-model", action="store_true", help="also print the per-model table"
    )
    parser.add_argument(
        "--markdown", action="store_true", help="emit markdown tables"
    )
    parser.add_argument("--latex", metavar="FILE", help="write booktabs tables to FILE")
    parser.add_argument(
        "--figures", metavar="DIR", help="write paper figures (PDF+PNG) to DIR"
    )
    args = parser.parse_args()

    rows = load(args.csv)
    if not rows:
        print("no rows", file=sys.stderr)
        return 1

    by_suite = defaultdict(list)
    for row in rows:
        by_suite[row["suite"]].append(row)

    summaries = [summarize(suite, group) for suite, group in by_suite.items()]
    if len(summaries) > 1:
        summaries.append(summarize("all", rows))

    emit(summaries, args.markdown)

    for summary in summaries:
        if summary["mismatches"]:
            print(
                f"\nCONFORMANCE MISMATCH in {summary['suite']}: "
                + ", ".join(summary["mismatches"]),
                file=sys.stderr,
            )

    print()
    for summary in summaries:
        print(
            f"{summary['suite']}: measurement noise (median CV) "
            f"conformance {100 * summary['conformance_cv_median']:.1f}%, "
            f"evidence {100 * summary['evidence_cv_median']:.1f}%; "
            f"min iterations {summary['min_iters']}"
        )
    print()
    for summary in summaries:
        # Authored pairs exceed evaluated pairs when several authored statements
        # normalize together and share one evidence tree.
        fanout = (
            summary["authored_pairs"] / summary["pairs"] if summary["pairs"] else 0.0
        )
        print(
            f"{summary['suite']}: {summary['conforming_models']}/{summary['models']} "
            f"models conform; {summary['fail_pairs']} failing pairs; "
            f"authored fan-out {fanout:.2f}x; "
            f"total evidence {summary['bytes'] / 1e6:.1f} MB "
            f"({summary['run_bytes'] / 1e6:.1f} MB with constraint catalogs)"
        )
        print(
            f"{'':>{len(summary['suite'])}}  compact: "
            f"{summary['compact_bytes'] / 1e6:.1f} MB "
            f"({100 * (1 - summary['compact_ratio']):.0f}% smaller), "
            f"{summary['compact_bytes_no_catalog'] / 1e6:.1f} MB with the catalog "
            f"elided ({100 * (1 - summary['compact_ratio_no_catalog']):.0f}% smaller)"
        )

    if args.per_model:
        for suite, group in by_suite.items():
            print(f"\n### {suite}\n")
            emit_per_model(group, args.markdown)

    if args.latex:
        emit_latex(summaries, rows, args.latex)
    if args.figures:
        emit_figures(by_suite, summaries, args.figures)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
