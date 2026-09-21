"""Render complete shared-dataset comparison JSON files as a Markdown report."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


def ratio(row: dict) -> float:
    return row["new"]["median_ms"] / row["old"]["median_ms"]


def mib(value: float | None) -> str:
    return "—" if value is None else f"{value / (1024 * 1024):.0f}"


def median_rss(values: list[int | None]) -> float | None:
    return (
        statistics.median(values)
        if all(value is not None for value in values)
        else None
    )


def render(inputs: list[dict]) -> str:
    lines = [
        "# Shared-dataset release comparison",
        "",
        "Each condition used a discarded warmup and five sequential, alternating",
        "samples per version. Times are wall-clock milliseconds; spread is the",
        "largest absolute deviation from the median. RSS is peak process memory.",
        "Inference and report equality means RDF graph isomorphism; validation",
        "text equality compares the human-readable CLI output verbatim.",
        "",
    ]
    for data in inputs:
        if not data.get("complete"):
            raise ValueError("comparison is incomplete")
        rows = data["conditions"]
        suite = Path(data["shapes"]).stem if data.get("shapes") else "generated"
        lines.extend(
            [
                f"## {suite}",
                "",
                (
                    f"Lockfile SHA-256: `{data['lock_sha256']}`. "
                    f"{data['samples_per_binary']} samples and "
                    f"{data['warmups_per_binary']} warmup(s) per version."
                ),
                "",
                (
                    "| Operation | Cases | Median new/old time | Worst new/old time | "
                    "Median old/new RSS (MiB) | Comparison mismatches |"
                ),
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for operation in ("infer", "validate", "report"):
            subset = [row for row in rows if row["operation"] == operation]
            if not subset:
                continue
            old_rss = [row["old"]["median_rss_bytes"] for row in subset]
            new_rss = [row["new"]["median_rss_bytes"] for row in subset]
            lines.append(
                f"| {operation} | {len(subset)} | "
                f"{statistics.median(map(ratio, subset)):.2f} | "
                f"{max(map(ratio, subset)):.2f} | "
                f"{mib(median_rss(old_rss))} / "
                f"{mib(median_rss(new_rss))} | "
                f"{sum(not row['semantic_equal'] for row in subset)} |"
            )
        lines.extend(
            [
                "",
                (
                    "| Case | Operation | Old median ± spread (ms) | "
                    "New median ± spread (ms) | New/old | Old/new RSS (MiB) | "
                    "Comparison equal |"
                ),
                "| --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in rows:
            old = row["old"]
            new = row["new"]
            case = row.get("case") or Path(row["model"]).stem
            lines.append(
                f"| {case} | {row['operation']} | "
                f"{old['median_ms']:.1f} ± {old['spread_ms']:.1f} | "
                f"{new['median_ms']:.1f} ± {new['spread_ms']:.1f} | "
                f"{ratio(row):.2f} | "
                f"{mib(old['median_rss_bytes'])} / "
                f"{mib(new['median_rss_bytes'])} | "
                f"{'yes' if row['semantic_equal'] else 'no'} |"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = render([json.loads(path.read_text()) for path in args.inputs])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report + "\n")


if __name__ == "__main__":
    main()
