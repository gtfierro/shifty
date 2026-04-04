#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pandas",
#   "numpy",
#   "scipy",
#   "jinja2",
# ]
# ///
"""Compare two benchmark CSV files and generate a detailed comparison report.

This tool analyzes performance differences between baseline and optimized runs,
providing statistical significance testing and regression detection.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class PhaseComparison:
    """Comparison statistics for a single phase."""
    phase_name: str
    baseline_mean: float
    optimized_mean: float
    percent_change: float
    baseline_p50: float
    optimized_p50: float
    baseline_p95: float
    optimized_p95: float
    p_value: float
    is_significant: bool
    is_regression: bool


@dataclass
class PlatformComparison:
    """Comparison statistics for a platform across all models."""
    platform: str
    total_runs_baseline: int
    total_runs_optimized: int
    wall_time_mean_baseline: float
    wall_time_mean_optimized: float
    wall_time_percent_change: float
    wall_time_p50_baseline: float
    wall_time_p50_optimized: float
    wall_time_p95_baseline: float
    wall_time_p95_optimized: float
    wall_time_p_value: float
    wall_time_is_significant: bool
    wall_time_is_regression: bool
    phase_comparisons: List[PhaseComparison]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare two benchmark CSV files."
    )
    parser.add_argument(
        "baseline_csv",
        type=Path,
        help="Path to baseline benchmark CSV file.",
    )
    parser.add_argument(
        "optimized_csv",
        type=Path,
        help="Path to optimized benchmark CSV file.",
    )
    parser.add_argument(
        "--output-html",
        type=Path,
        help="Path for HTML comparison report.",
    )
    parser.add_argument(
        "--output-txt",
        type=Path,
        help="Path for text comparison report.",
    )
    parser.add_argument(
        "--significance-threshold",
        type=float,
        default=0.05,
        help="P-value threshold for statistical significance (default: 0.05).",
    )
    parser.add_argument(
        "--regression-threshold",
        type=float,
        default=5.0,
        help="Percent slowdown threshold for regression detection (default: 5.0).",
    )
    parser.add_argument(
        "--include-all-phases",
        action="store_true",
        help="Include all phase timings in the comparison.",
    )
    return parser.parse_args()


def load_csv(path: Path) -> pd.DataFrame:
    """Load and validate benchmark CSV file."""
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    df = pd.read_csv(path)
    required_cols = ["platform", "data_file", "triples", "run", "seconds"]
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"CSV missing required columns: {missing_cols}")

    return df


def calculate_phase_comparison(
    baseline: pd.Series,
    optimized: pd.Series,
    phase_name: str,
    significance_threshold: float,
    regression_threshold: float,
) -> PhaseComparison | None:
    """Calculate comparison statistics for a single phase."""
    # Filter out None/NaN values
    baseline_vals = baseline.dropna()
    optimized_vals = optimized.dropna()

    if len(baseline_vals) == 0 or len(optimized_vals) == 0:
        return None

    baseline_mean = baseline_vals.mean()
    optimized_mean = optimized_vals.mean()
    percent_change = ((optimized_mean - baseline_mean) / baseline_mean) * 100

    baseline_p50 = baseline_vals.median()
    optimized_p50 = optimized_vals.median()
    baseline_p95 = baseline_vals.quantile(0.95)
    optimized_p95 = optimized_vals.quantile(0.95)

    # Perform t-test if we have enough samples
    p_value = 1.0
    if len(baseline_vals) >= 2 and len(optimized_vals) >= 2:
        try:
            _, p_value = stats.ttest_ind(baseline_vals, optimized_vals)
        except Exception:
            p_value = 1.0

    is_significant = p_value < significance_threshold
    is_regression = percent_change > regression_threshold and is_significant

    return PhaseComparison(
        phase_name=phase_name,
        baseline_mean=baseline_mean,
        optimized_mean=optimized_mean,
        percent_change=percent_change,
        baseline_p50=baseline_p50,
        optimized_p50=optimized_p50,
        baseline_p95=baseline_p95,
        optimized_p95=optimized_p95,
        p_value=p_value,
        is_significant=is_significant,
        is_regression=is_regression,
    )


def compare_platforms(
    baseline_df: pd.DataFrame,
    optimized_df: pd.DataFrame,
    significance_threshold: float,
    regression_threshold: float,
    include_all_phases: bool,
) -> List[PlatformComparison]:
    """Compare benchmarks for each platform."""
    comparisons: List[PlatformComparison] = []

    platforms = set(baseline_df["platform"].unique()) & set(optimized_df["platform"].unique())

    for platform in sorted(platforms):
        baseline_platform = baseline_df[baseline_df["platform"] == platform]
        optimized_platform = optimized_df[optimized_df["platform"] == platform]

        # Wall time comparison
        wall_time_comp = calculate_phase_comparison(
            baseline_platform["seconds"],
            optimized_platform["seconds"],
            "wall_time",
            significance_threshold,
            regression_threshold,
        )

        if wall_time_comp is None:
            continue

        # Phase comparisons
        phase_cols = [
            ("graph_fetching_seconds", "Graph Fetching"),
            ("validate_seconds", "Validate"),
            ("inference_seconds", "Inference"),
            ("report_assembly_seconds", "Report Assembly"),
            ("target_collection_seconds", "Target Collection"),
            ("component_execution_seconds", "Component Execution"),
        ]

        phase_comparisons: List[PhaseComparison] = []
        for col, name in phase_cols:
            if col in baseline_platform.columns and col in optimized_platform.columns:
                comp = calculate_phase_comparison(
                    baseline_platform[col],
                    optimized_platform[col],
                    name,
                    significance_threshold,
                    regression_threshold,
                )
                if comp is not None and (include_all_phases or comp.is_significant):
                    phase_comparisons.append(comp)

        comparisons.append(PlatformComparison(
            platform=platform,
            total_runs_baseline=len(baseline_platform),
            total_runs_optimized=len(optimized_platform),
            wall_time_mean_baseline=wall_time_comp.baseline_mean,
            wall_time_mean_optimized=wall_time_comp.optimized_mean,
            wall_time_percent_change=wall_time_comp.percent_change,
            wall_time_p50_baseline=wall_time_comp.baseline_p50,
            wall_time_p50_optimized=wall_time_comp.optimized_p50,
            wall_time_p95_baseline=wall_time_comp.baseline_p95,
            wall_time_p95_optimized=wall_time_comp.optimized_p95,
            wall_time_p_value=wall_time_comp.p_value,
            wall_time_is_significant=wall_time_comp.is_significant,
            wall_time_is_regression=wall_time_comp.is_regression,
            phase_comparisons=phase_comparisons,
        ))

    return comparisons


def format_percent(value: float) -> str:
    """Format percent change with color indicator."""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def format_time(seconds: float) -> str:
    """Format time in human-readable format."""
    if seconds < 1.0:
        return f"{seconds * 1000:.1f} ms"
    else:
        return f"{seconds:.3f} s"


def generate_text_report(comparisons: List[PlatformComparison]) -> str:
    """Generate text comparison report."""
    lines = []
    lines.append("=" * 80)
    lines.append("BENCHMARK COMPARISON REPORT")
    lines.append("=" * 80)
    lines.append("")

    for comp in comparisons:
        lines.append(f"Platform: {comp.platform}")
        lines.append("-" * 80)
        lines.append(f"  Runs: {comp.total_runs_baseline} (baseline) vs {comp.total_runs_optimized} (optimized)")
        lines.append("")
        lines.append("  Wall Time:")
        lines.append(f"    Mean:   {format_time(comp.wall_time_mean_baseline)} → {format_time(comp.wall_time_mean_optimized)} ({format_percent(comp.wall_time_percent_change)})")
        lines.append(f"    p50:    {format_time(comp.wall_time_p50_baseline)} → {format_time(comp.wall_time_p50_optimized)}")
        lines.append(f"    p95:    {format_time(comp.wall_time_p95_baseline)} → {format_time(comp.wall_time_p95_optimized)}")
        lines.append(f"    p-value: {comp.wall_time_p_value:.4f} {'(significant)' if comp.wall_time_is_significant else '(not significant)'}")

        if comp.wall_time_is_regression:
            lines.append(f"    ⚠️  REGRESSION DETECTED: {format_percent(comp.wall_time_percent_change)} slowdown")

        if comp.phase_comparisons:
            lines.append("")
            lines.append("  Phase Breakdown:")
            for phase in comp.phase_comparisons:
                lines.append(f"    {phase.phase_name}:")
                lines.append(f"      Mean: {format_time(phase.baseline_mean)} → {format_time(phase.optimized_mean)} ({format_percent(phase.percent_change)})")
                if phase.is_regression:
                    lines.append(f"      ⚠️  REGRESSION: {format_percent(phase.percent_change)} slowdown")

        lines.append("")

    lines.append("=" * 80)
    return "\n".join(lines)


def generate_html_report(comparisons: List[PlatformComparison], baseline_path: Path, optimized_path: Path) -> str:
    """Generate HTML comparison report."""
    html_parts = []
    html_parts.append("""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Benchmark Comparison Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        h1 { color: #333; }
        h2 { color: #555; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; background-color: white; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .improvement { color: green; font-weight: bold; }
        .regression { color: red; font-weight: bold; }
        .neutral { color: #666; }
        .significant { background-color: #fff3cd; }
        .regression-warning { background-color: #f8d7da; color: #721c24; padding: 10px; margin: 10px 0; border-radius: 5px; }
        .summary { background-color: white; padding: 15px; margin: 20px 0; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>Benchmark Comparison Report</h1>
""")

    html_parts.append(f"""    <div class="summary">
        <p><strong>Baseline:</strong> {baseline_path}</p>
        <p><strong>Optimized:</strong> {optimized_path}</p>
    </div>
""")

    for comp in comparisons:
        html_parts.append(f"    <h2>{comp.platform}</h2>")

        if comp.wall_time_is_regression:
            html_parts.append(f"""    <div class="regression-warning">
        ⚠️ REGRESSION DETECTED: Wall time increased by {format_percent(comp.wall_time_percent_change)}
    </div>
""")

        html_parts.append("""    <table>
        <tr>
            <th>Metric</th>
            <th>Baseline</th>
            <th>Optimized</th>
            <th>Change</th>
            <th>p-value</th>
        </tr>
""")

        change_class = "improvement" if comp.wall_time_percent_change < 0 else "regression" if comp.wall_time_percent_change > 0 else "neutral"
        row_class = "significant" if comp.wall_time_is_significant else ""

        html_parts.append(f"""        <tr class="{row_class}">
            <td><strong>Wall Time (mean)</strong></td>
            <td>{format_time(comp.wall_time_mean_baseline)}</td>
            <td>{format_time(comp.wall_time_mean_optimized)}</td>
            <td class="{change_class}">{format_percent(comp.wall_time_percent_change)}</td>
            <td>{comp.wall_time_p_value:.4f}</td>
        </tr>
        <tr>
            <td>Wall Time (p50)</td>
            <td>{format_time(comp.wall_time_p50_baseline)}</td>
            <td>{format_time(comp.wall_time_p50_optimized)}</td>
            <td></td>
            <td></td>
        </tr>
        <tr>
            <td>Wall Time (p95)</td>
            <td>{format_time(comp.wall_time_p95_baseline)}</td>
            <td>{format_time(comp.wall_time_p95_optimized)}</td>
            <td></td>
            <td></td>
        </tr>
""")

        for phase in comp.phase_comparisons:
            change_class = "improvement" if phase.percent_change < 0 else "regression" if phase.percent_change > 0 else "neutral"
            row_class = "significant" if phase.is_significant else ""

            html_parts.append(f"""        <tr class="{row_class}">
            <td>{phase.phase_name}</td>
            <td>{format_time(phase.baseline_mean)}</td>
            <td>{format_time(phase.optimized_mean)}</td>
            <td class="{change_class}">{format_percent(phase.percent_change)}</td>
            <td>{phase.p_value:.4f}</td>
        </tr>
""")

        html_parts.append("    </table>\n")

    html_parts.append("""</body>
</html>
""")

    return "".join(html_parts)


def main() -> int:
    args = parse_args()

    try:
        baseline_df = load_csv(args.baseline_csv)
        optimized_df = load_csv(args.optimized_csv)

        comparisons = compare_platforms(
            baseline_df,
            optimized_df,
            args.significance_threshold,
            args.regression_threshold,
            args.include_all_phases,
        )

        if not comparisons:
            print("No matching platforms found between baseline and optimized CSVs.", file=sys.stderr)
            return 1

        # Generate text report
        text_report = generate_text_report(comparisons)
        print(text_report)

        if args.output_txt:
            args.output_txt.parent.mkdir(parents=True, exist_ok=True)
            args.output_txt.write_text(text_report)
            print(f"\nText report written to: {args.output_txt}")

        # Generate HTML report
        if args.output_html:
            html_report = generate_html_report(comparisons, args.baseline_csv, args.optimized_csv)
            args.output_html.parent.mkdir(parents=True, exist_ok=True)
            args.output_html.write_text(html_report)
            print(f"HTML report written to: {args.output_html}")

        # Return exit code based on regressions
        has_regression = any(c.wall_time_is_regression for c in comparisons)
        return 1 if has_regression else 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
