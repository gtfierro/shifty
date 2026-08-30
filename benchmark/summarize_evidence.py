#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Summarize bench_evidence.sh output into the tables the evidence paper reports.

Reads the CSV written by ``benchmark/bench_evidence.sh`` and prints, per suite
and over the pooled corpus:

  * median latency for conformance-only, complete evidence, and failure-only
    on-demand evidence over ten rotated, paired rounds per model;
  * complete-run and compact-run bytes under matched catalog policies;
  * repeated evidence-node and RDF-term counts;
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


def median_mad(values: list[float]) -> tuple[float, float]:
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return median, mad


def verify_samples(row: dict) -> None:
    """Recompute every reported timing summary from the retained raw samples."""

    sample_fields = {
        "conformance": "conformance_ms_samples",
        "full_evidence": "full_evidence_ms_samples",
        "failure_discovery": "failure_discovery_ms_samples",
        "failure_explanation": "failure_explanation_ms_samples",
        "on_demand": "on_demand_ms_samples",
    }
    samples = {
        name: [float(value) for value in row[field].split(";")]
        for name, field in sample_fields.items()
    }
    expected = row["iters"]
    for name, values in samples.items():
        if len(values) != expected:
            raise ValueError(
                f"{row['model']}: {name} has {len(values)} samples, expected {expected}"
            )
        median, mad = median_mad(values)
        for suffix, measured, recomputed in (
            ("median", row[f"{name}_ms_median"], median),
            ("MAD", row[f"{name}_ms_mad"], mad),
        ):
            if not math.isclose(measured, recomputed, abs_tol=0.0002):
                raise ValueError(
                    f"{row['model']}: {name} {suffix} is {measured}, "
                    f"but raw samples give {recomputed}"
                )

    for discovery, explanation, total in zip(
        samples["failure_discovery"],
        samples["failure_explanation"],
        samples["on_demand"],
        strict=True,
    ):
        if not math.isclose(discovery + explanation, total, abs_tol=0.0002):
            raise ValueError(
                f"{row['model']}: on-demand sample is not discovery + explanation"
            )

    for name, numerators in (
        ("full_overhead", samples["full_evidence"]),
        ("on_demand_overhead", samples["on_demand"]),
    ):
        ratios = [
            numerator / max(1e-300, denominator)
            for numerator, denominator in zip(
                numerators, samples["conformance"], strict=True
            )
        ]
        median, mad = median_mad(ratios)
        if not math.isclose(row[f"{name}_median"], median, abs_tol=0.0002):
            raise ValueError(
                f"{row['model']}: {name} median does not match raw samples"
            )
        if not math.isclose(row[f"{name}_mad"], mad, abs_tol=0.0002):
            raise ValueError(f"{row['model']}: {name} MAD does not match raw samples")


def load(path: str) -> list[dict]:
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    numeric = {
        "data_triples": int,
        "iters": int,
        "conformance_ms_median": float,
        "conformance_ms_mad": float,
        "full_evidence_ms_median": float,
        "full_evidence_ms_mad": float,
        "full_overhead_median": float,
        "full_overhead_mad": float,
        "failure_discovery_ms_median": float,
        "failure_discovery_ms_mad": float,
        "failure_explanation_ms_median": float,
        "failure_explanation_ms_mad": float,
        "on_demand_ms_median": float,
        "on_demand_ms_mad": float,
        "on_demand_overhead_median": float,
        "on_demand_overhead_mad": float,
        "serialize_ms_median": float,
        "serialize_ms_mad": float,
        "evaluated_pairs": int,
        "pass_pairs": int,
        "fail_pairs": int,
        "fail_fraction": float,
        "authored_pairs": int,
        "authored_pass_pairs": int,
        "authored_fail_pairs": int,
        "statements": int,
        "evidence_nodes": int,
        "evidence_nodes_per_authored_pair": float,
        "evidence_bytes": int,
        "evidence_bytes_per_authored_pair": float,
        "full_run_bytes_with_catalog": int,
        "full_run_bytes_no_catalog": int,
        "compact_run_bytes_with_catalog": int,
        "compact_run_bytes_no_catalog": int,
        "node_occurrences": int,
        "distinct_nodes": int,
        "node_redundancy": float,
        "result_occurrences": int,
        "distinct_results": int,
        "result_redundancy": float,
        "support_occurrences": int,
        "distinct_support": int,
        "support_redundancy": float,
        "support_share": float,
        "term_occurrences": int,
        "distinct_terms": int,
        "term_redundancy": float,
        "normalized_requests": int,
        "duplicate_records": int,
        "divergent_duplicates": int,
        "canonical_occurrences": int,
        "shared_payloads": int,
        "shared_payload_fraction": float,
        "shared_canonical_occurrences": int,
        "shared_occurrence_fraction": float,
        "request_reaches": int,
        "requests_per_payload": float,
        "max_payload_requests": int,
        "distinct_keys": int,
        "key_redundancy": float,
        "multi_occurrence_keys": int,
        "divergent_keys": int,
        "divergence_fraction": float,
        "divergent_occurrences": int,
        "distinct_payloads_per_key": int,
        "keys_over_payload_cap": int,
        "both_polarity_addresses": int,
    }
    for row in rows:
        # Sharing columns postdate the first published runs; a CSV without them
        # still summarizes, rather than failing on a column nothing plots.
        for key, cast in numeric.items():
            if key in row:
                row[key] = cast(row[key])
        row["conforms_conformance"] = row["conforms_conformance"] == "true"
        row["conforms_evidence"] = row["conforms_evidence"] == "true"
        row["conforms_on_demand"] = row["conforms_on_demand"] == "true"
        verify_samples(row)
    return rows


SHARING_COLUMNS = (
    "result_occurrences",
    "distinct_results",
    "support_occurrences",
    "distinct_support",
    "normalized_requests",
    "duplicate_records",
    "divergent_duplicates",
    "canonical_occurrences",
    "shared_payloads",
    "shared_canonical_occurrences",
    "request_reaches",
    "distinct_keys",
    "multi_occurrence_keys",
    "divergent_keys",
    "divergent_occurrences",
    "keys_over_payload_cap",
    "both_polarity_addresses",
)


def sharing(rows: list[dict]) -> dict:
    """Suite totals for the sharing columns, and the ratios read off them.

    Distinct counts are summed across models rather than pooled, because the
    things being counted are not comparable between models: a `ShapeId` indexes
    one snapshot's arena and a term belongs to one graph. The sum is
    "distinct within a model, added up", which is what every per-model ratio
    here divides.

    Absent on CSVs written before these columns existed, in which case the
    sharing report is skipped rather than the whole summary failing.
    """
    if not all(column in rows[0] for column in SHARING_COLUMNS):
        return {}
    totals = {
        column: sum(row[column] for row in rows) for column in SHARING_COLUMNS
    }

    def ratio(part: str, whole: str) -> float:
        return totals[part] / totals[whole] if totals[whole] else 0.0

    return {
        **totals,
        "has_sharing": True,
        "max_payload_requests": max(row["max_payload_requests"] for row in rows),
        # The bracket: what hash-consing collapses now, and what a memo keyed on
        # the judgment could collapse if payloads never diverged.
        "payload_redundancy": ratio("result_occurrences", "distinct_results"),
        "key_redundancy": ratio("result_occurrences", "distinct_keys"),
        "support_redundancy": ratio("support_occurrences", "distinct_support"),
        "support_share": totals["support_occurrences"]
        / max(1, totals["result_occurrences"] + totals["support_occurrences"]),
        "shared_payload_fraction": ratio("shared_payloads", "distinct_results"),
        "shared_occurrence_fraction": ratio(
            "shared_canonical_occurrences", "canonical_occurrences"
        ),
        "requests_per_payload": ratio("request_reaches", "distinct_results"),
        "divergence_fraction": ratio("divergent_keys", "multi_occurrence_keys"),
        "divergent_occurrence_fraction": ratio(
            "divergent_occurrences", "result_occurrences"
        ),
        "authored_fanout": ratio("duplicate_records", "normalized_requests"),
    }


def emit_sharing(summaries: list[dict]) -> None:
    """Report sharing across independently addressable validation results.

    Three blocks, in the order the questions depend on each other: what the
    tagged-node counts are actually made of, how much is reached from more than
    one request, and whether a judgment determines its own evidence.
    """
    if not summaries or not summaries[0].get("has_sharing"):
        return
    for summary in summaries:
        print(f"\n### sharing: {summary['suite']}\n")
        print(
            f"  nodes         {summary['result_occurrences']:,} judgment "
            f"occurrences of {summary['distinct_results']:,} distinct "
            f"({summary['payload_redundancy']:.2f}x); "
            f"{summary['support_occurrences']:,} path-support occurrences of "
            f"{summary['distinct_support']:,} distinct "
            f"({summary['support_redundancy']:.2f}x)"
        )
        print(
            f"                path support is "
            f"{100 * summary['support_share']:.1f}% of tagged-node occurrences"
        )
        print(
            f"  addressing    {summary['normalized_requests']:,} normalized "
            f"requests behind "
            f"{summary['normalized_requests'] + summary['duplicate_records']:,} "
            f"authored records "
            f"(+{100 * summary['authored_fanout']:.1f}% duplication, "
            f"{summary['divergent_duplicates']} divergent)"
        )
        print(
            f"  cross-request {summary['shared_payloads']:,} of "
            f"{summary['distinct_results']:,} distinct judgments reached from "
            f"2+ requests ({100 * summary['shared_payload_fraction']:.1f}%), "
            f"covering {100 * summary['shared_occurrence_fraction']:.1f}% of "
            f"canonical occurrences"
        )
        print(
            f"                {summary['requests_per_payload']:.2f} requests per "
            f"distinct judgment; most shared reaches "
            f"{summary['max_payload_requests']:,}"
        )
        print(
            f"  divergence    {summary['divergent_keys']:,} of "
            f"{summary['multi_occurrence_keys']:,} repeated keys carry 2+ "
            f"payloads ({100 * summary['divergence_fraction']:.1f}%), "
            f"{100 * summary['divergent_occurrence_fraction']:.1f}% of "
            f"occurrences"
        )
        print(
            f"  bracket       {summary['payload_redundancy']:.2f}x collapsed "
            f"losslessly today, {summary['key_redundancy']:.2f}x if a memo keyed "
            f"on (constraint, node, polarity) could be trusted"
        )
        if summary["both_polarity_addresses"]:
            print(
                f"  WARNING       {summary['both_polarity_addresses']:,} "
                f"(constraint, node) addresses hold both polarities in one run",
                file=sys.stderr,
            )
        if summary["keys_over_payload_cap"]:
            print(
                f"                {summary['keys_over_payload_cap']:,} keys "
                f"exceeded the payload cap; per-key payload counts are floors"
            )


def summarize(name: str, rows: list[dict]) -> dict:
    full_ratios = [row["full_overhead_median"] for row in rows]
    on_demand_ratios = [row["on_demand_overhead_median"] for row in rows]
    pairs = sum(row["evaluated_pairs"] for row in rows)
    authored_pairs = sum(row["authored_pairs"] for row in rows)
    nodes = sum(row["evidence_nodes"] for row in rows)
    ev_bytes = sum(row["evidence_bytes"] for row in rows)
    full_with_catalog = sum(row["full_run_bytes_with_catalog"] for row in rows)
    full_no_catalog = sum(row["full_run_bytes_no_catalog"] for row in rows)
    compact_with_catalog = sum(row["compact_run_bytes_with_catalog"] for row in rows)
    compact_no_catalog = sum(row["compact_run_bytes_no_catalog"] for row in rows)
    return {
        "suite": name,
        "models": len(rows),
        "full_ratio_median": statistics.median(full_ratios),
        "on_demand_ratio_median": statistics.median(on_demand_ratios),
        "min_iters": min(row["iters"] for row in rows),
        "full_ratio_min": min(full_ratios),
        "full_ratio_max": max(full_ratios),
        "on_demand_ratio_min": min(on_demand_ratios),
        "on_demand_ratio_max": max(on_demand_ratios),
        "pairs": pairs,
        "pass_pairs": sum(row["pass_pairs"] for row in rows),
        "fail_pairs": sum(row["fail_pairs"] for row in rows),
        "fail_fraction": sum(row["fail_pairs"] for row in rows) / max(1, pairs),
        "authored_pairs": authored_pairs,
        "conforming_models": sum(1 for row in rows if row["conforms_evidence"]),
        "nodes": nodes,
        "nodes_per_authored_pair": nodes / authored_pairs
        if authored_pairs
        else float("nan"),
        "bytes": ev_bytes,
        "full_with_catalog": full_with_catalog,
        "full_no_catalog": full_no_catalog,
        "compact_with_catalog": compact_with_catalog,
        "compact_no_catalog": compact_no_catalog,
        "full_no_catalog_per_authored_pair": full_no_catalog / max(1, authored_pairs),
        "compact_no_catalog_per_authored_pair": compact_no_catalog
        / max(1, authored_pairs),
        "compact_ratio": compact_with_catalog / max(1, full_with_catalog),
        "compact_ratio_no_catalog": compact_no_catalog / max(1, full_no_catalog),
        "node_occurrences": sum(row["node_occurrences"] for row in rows),
        "distinct_nodes": sum(row["distinct_nodes"] for row in rows),
        "term_occurrences": sum(row["term_occurrences"] for row in rows),
        "distinct_terms": sum(row["distinct_terms"] for row in rows),
        **sharing(rows),
        "mismatches": [
            row["model"]
            for row in rows
            if not (
                row["conforms_conformance"]
                == row["conforms_evidence"]
                == row["conforms_on_demand"]
            )
        ],
    }


def emit(summaries: list[dict], markdown: bool) -> None:
    columns = [
        ("suite", "suite", "{}"),
        ("models", "models", "{}"),
        ("pairs", "eval. pairs", "{}"),
        ("fail_fraction", "fail frac.", "{:.3f}"),
        ("full_ratio_median", "full x", "{:.2f}"),
        ("on_demand_ratio_median", "on-dem. x", "{:.2f}"),
        ("full_no_catalog_per_authored_pair", "full B/auth.", "{:.0f}"),
        ("compact_no_catalog_per_authored_pair", "compact B/auth.", "{:.0f}"),
    ]
    rows = [[fmt.format(s[key]) for key, _, fmt in columns] for s in summaries]
    headers = [label for _, label, _ in columns]
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows))
        for i in range(len(headers))
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
        ("conformance_ms_median", "conf ms", "{:.2f}"),
        ("full_evidence_ms_median", "full ms", "{:.2f}"),
        ("full_overhead_median", "full x", "{:.2f}"),
        ("on_demand_ms_median", "on-dem. ms", "{:.2f}"),
        ("on_demand_overhead_median", "on-dem. x", "{:.2f}"),
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


SUITE_LABELS = {"brick": "Brick", "s223": "ASHRAE 223P", "all": "All"}

# Categorical slots 1–2 of the validated reference palette. Three is the cap
# that clears the all-pairs colour-vision floors, which is the pairlist scatter
# plots need; a fourth suite folds into a facet rather than a new hue.
SUITE_COLORS = {"brick": "#2a78d6", "s223": "#eb6834"}

# Inches, width by height. One column wide, and deliberately wider than tall:
# vertical space is what a two-column paper runs out of first. Override with
# --figure-size when the layout changes.
FIGURE_SIZE = (3.4, 1.85)
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
        (
            "  \\caption{Evidence cost over the same prepared validator snapshot. "
            "Latency columns report the median of the per-model median paired "
            "ratios. The compact/full column divides the total compact bytes by "
            "the total full bytes within each suite, with the constraint catalog "
            "elided on both sides.}"
        ),
        "  \\label{tab:evidence-overhead}",
        "  \\begin{tabular}{lrrrrrr}",
        "    \\toprule",
        (
            "    Suite & Models & Eval. pairs & Fail (\\%) & Full ($\\times$) "
            "& On-demand ($\\times$) & Total compact/full \\\\"
        ),
        "    \\midrule",
    ]
    for summary in summaries:
        label = SUITE_LABELS.get(summary["suite"], summary["suite"])
        if summary["suite"] == "all":
            lines.append("    \\midrule")
        lines.append(
            f"    {latex_escape(label)} & {summary['models']} & "
            f"{summary['pairs']:,} & "
            f"{100 * summary['fail_fraction']:.1f} & "
            f"{summary['full_ratio_median']:.2f} & "
            f"{summary['on_demand_ratio_median']:.2f} & "
            f"{summary['compact_ratio_no_catalog']:.2f} \\\\"
        )
    lines += [
        "    \\bottomrule",
        "  \\end{tabular}",
        "\\end{table}",
        "",
        "\\begin{table}[t]",
        "  \\centering",
        (
            "  \\caption{Per-model evidence cost. Times are medians over "
            f"{rows[0]['iters']} rotated, paired rounds."
            "}"
        ),
        "  \\label{tab:evidence-per-model}",
        "  \\small",
        "  \\begin{tabular}{llrrrrrr}",
        "    \\toprule",
        (
            "    Suite & Model & Triples & Pairs & Fail & Conf. (ms) & Full (ms) & "
            "On-demand (ms) \\\\"
        ),
        "    \\midrule",
    ]
    for row in rows:
        lines.append(
            f"    {latex_escape(SUITE_LABELS.get(row['suite'], row['suite']))} & "
            f"{latex_escape(row['model'].removesuffix('.ttl'))} & "
            f"{row['data_triples']:,} & {row['evaluated_pairs']:,} & "
            f"{row['fail_pairs']:,} & "
            f"{row['conformance_ms_median']:.1f} & "
            f"{row['full_evidence_ms_median']:.1f} & "
            f"{row['on_demand_ms_median']:.1f} \\\\"
        )
    lines += ["    \\bottomrule", "  \\end{tabular}", "\\end{table}", ""]

    with open(path, "w") as handle:
        handle.write("\n".join(lines))
    print(f"wrote {path}", file=sys.stderr)


def emit_figures(
    by_suite: dict,
    summaries: list[dict],
    directory: str,
    size: tuple[float, float] = FIGURE_SIZE,
) -> None:
    """Write the paper figures as vector PDF plus a PNG preview.

    `size` is inches, width by height. The default is one column wide and
    short enough to sit in a two-column layout without eating a third of the
    page; `--figure-size` overrides it when the surrounding text changes.
    """
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
    from pathlib import Path

    import matplotlib.pyplot as plt

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

    def new_axes(width=size[0], height=size[1]):
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
        position = math.exp(
            math.log(span[0]) + at * (math.log(span[1]) - math.log(span[0]))
        )
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
            [row["conformance_ms_median"] for row in group],
            [row["full_evidence_ms_median"] for row in group],
            xerr=[row["conformance_ms_mad"] for row in group],
            yerr=[row["full_evidence_ms_mad"] for row in group],
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
        min(row["conformance_ms_median"] for row in rows_of(by_suite)) * 0.7,
        max(row["full_evidence_ms_median"] for row in rows_of(by_suite)) * 1.4,
    ]
    axes.plot(limits, limits, color=MUTED, linewidth=0.8, linestyle="--", zorder=2)
    axes.set(
        xscale="log",
        yscale="log",
        xlim=limits,
        ylim=limits,
        xlabel="conformance-only (ms)",
        ylabel="full evidence (ms)",
    )
    axes.legend(frameon=False, loc="upper left")
    label_diagonal(axes, limits, "no overhead", at=0.82)
    save(figure, "evidence_latency")

    # --- Figure 2: how overhead is distributed -------------------------------
    # A distribution, so an ECDF: every model is a step, and the reader can
    # take any quantile off it. Box plots hide n and invent conventions.
    figure, axes = new_axes()
    for suite in suites:
        ratios = sorted(row["full_overhead_median"] for row in by_suite[suite])
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
            summary["full_ratio_median"],
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

    # --- Figure 3: when does on-demand evidence avoid full materialization? --
    figure, axes = new_axes()
    for suite in suites:
        group = by_suite[suite]
        axes.scatter(
            [100 * row["fail_fraction"] for row in group],
            [row["on_demand_overhead_median"] for row in group],
            s=16,
            color=SUITE_COLORS.get(suite, INK),
            edgecolor="white",
            linewidth=0.4,
            label=SUITE_LABELS.get(suite, suite),
            zorder=3,
        )
    axes.set(
        # Matplotlib renders its own text, so this is a plain string — the
        # LaTeX escaping the tables need would print the backslash.
        xlabel="failing evaluated pairs (%)",
        ylabel="on-demand overhead ($\\times$)",
    )
    axes.axhline(1, color=MUTED, linewidth=0.8, linestyle="--", zorder=2)
    axes.legend(frameon=False, loc="upper left")
    save(figure, "evidence_on_demand")

    # --- Figure 4: what compaction buys --------------------------------------
    figure, axes = new_axes()
    for suite in suites:
        group = by_suite[suite]
        axes.scatter(
            [
                row["full_run_bytes_no_catalog"] / max(1, row["authored_pairs"])
                for row in group
            ],
            [
                row["compact_run_bytes_no_catalog"] / max(1, row["authored_pairs"])
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
        min(
            row["full_run_bytes_no_catalog"] / max(1, row["authored_pairs"])
            for row in rows_of(by_suite)
        )
        * 0.5,
        max(
            row["full_run_bytes_no_catalog"] / max(1, row["authored_pairs"])
            for row in rows_of(by_suite)
        )
        * 1.5,
    ]
    axes.plot(span, span, color=MUTED, linewidth=0.8, linestyle="--", zorder=2)
    # Equal limits on both axes: vertical distance below the diagonal is then
    # exactly the saving, which is the whole point of the figure.
    axes.set(
        xscale="log",
        yscale="log",
        xlim=span,
        ylim=span,
        xlabel="full run (bytes/authored pair)",
        ylabel="compact run (bytes/authored pair)",
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
    parser.add_argument("--markdown", action="store_true", help="emit markdown tables")
    parser.add_argument("--latex", metavar="FILE", help="write booktabs tables to FILE")
    parser.add_argument(
        "--figures", metavar="DIR", help="write paper figures (PDF+PNG) to DIR"
    )
    parser.add_argument(
        "--figure-size",
        metavar="WxH",
        default="x".join(str(value) for value in FIGURE_SIZE),
        help="figure size in inches (default: %(default)s)",
    )
    args = parser.parse_args()

    try:
        width, height = (float(value) for value in args.figure_size.split("x"))
    except ValueError:
        print(f"--figure-size wants WxH in inches, got {args.figure_size}", file=sys.stderr)
        return 2

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
        # Authored pairs exceed evaluated pairs when several authored statements
        # normalize together and share one evidence tree.
        fanout = (
            summary["authored_pairs"] / summary["pairs"] if summary["pairs"] else 0.0
        )
        print(
            f"{summary['suite']}: {summary['conforming_models']}/{summary['models']} "
            f"models conform; {summary['fail_pairs']} failing pairs; "
            f"authored fan-out {fanout:.2f}x; "
            f"median full overhead {summary['full_ratio_median']:.2f}x; "
            f"median on-demand overhead {summary['on_demand_ratio_median']:.2f}x; "
            f"minimum {summary['min_iters']} rounds"
        )
        print(
            f"{'':>{len(summary['suite'])}}  compact: "
            f"{summary['compact_with_catalog'] / 1e6:.1f} MB "
            f"({100 * (1 - summary['compact_ratio']):.0f}% smaller), "
            f"{summary['compact_no_catalog'] / 1e6:.1f} MB with the catalog "
            f"elided ({100 * (1 - summary['compact_ratio_no_catalog']):.0f}% smaller)"
        )

    emit_sharing(summaries)

    if args.per_model:
        for suite, group in by_suite.items():
            print(f"\n### {suite}\n")
            emit_per_model(group, args.markdown)

    if args.latex:
        emit_latex(summaries, rows, args.latex)
    if args.figures:
        emit_figures(by_suite, summaries, args.figures, (width, height))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
