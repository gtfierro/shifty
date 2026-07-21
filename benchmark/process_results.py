#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Parse benchmark results and produce benchmark_data.json for the docs chart.

Reads:
  benchmark/results/<version>/brick.txt
  benchmark/results/<version>/s223.txt

Writes:
  benchmark/results/benchmark_data.json

Usage:
  uv run benchmark/process_results.py
"""

from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR / "results"
BRICK_MODELS = SCRIPT_DIR / "brick" / "models"
S223_MODELS = SCRIPT_DIR / "s223" / "models"
OUTPUT_FILE = RESULTS_DIR / "benchmark_data.json"

_ROW_RE = re.compile(
    r"^(\S+\.ttl)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
)


def parse_bench_file(path: Path) -> dict[str, dict]:
    """Parse a bench_common.sh / bench_history.sh output file.

    Returns {model_name: {infer_ms, infer_sd, val_ms, val_sd, rep_ms, rep_sd}}.
    Only rows where val_ms > 0 are included.
    """
    models: dict[str, dict] = {}
    for line in path.read_text().splitlines():
        m = _ROW_RE.match(line.strip())
        if not m:
            continue
        name, infer_ms, infer_sd, val_ms, val_sd, rep_ms, rep_sd = m.groups()
        val = int(val_ms)
        if val == 0:
            continue
        models[name] = {
            "infer_ms": int(infer_ms),
            "infer_sd": int(infer_sd),
            "val_ms": val,
            "val_sd": int(val_sd),
            "rep_ms": int(rep_ms),
            "rep_sd": int(rep_sd),
        }
    return models


def geomean(values: list[float]) -> float:
    pos = [v for v in values if v > 0]
    if not pos:
        return 0.0
    return math.exp(sum(math.log(v) for v in pos) / len(pos))


def collect_versions() -> list[str]:
    dirs = [d.name for d in RESULTS_DIR.iterdir()
            if d.is_dir() and re.match(r"v\d+\.\d+\.\d+", d.name)]

    def ver_key(s: str):
        return tuple(int(x) for x in s.lstrip("v").split("."))

    dirs.sort(key=ver_key)
    return dirs


def build_timeseries(versions: list[str], dataset: str) -> dict:
    """Per-version distribution of validate_ms across all models.

    Each model is normalised against its own earliest recorded time, so the
    per-version spread reflects how *uniformly* a release moved the corpus
    rather than the (much larger) spread in model sizes.  The docs chart draws
    these as box-and-whisker per version with the geomean overlaid.
    """
    per_version: dict[str, dict[str, dict]] = {}
    for version in versions:
        bench_file = RESULTS_DIR / version / f"{dataset}.txt"
        if not bench_file.exists():
            continue
        models = parse_bench_file(bench_file)
        if models:
            per_version[version] = models

    ordered = [v for v in versions if v in per_version]

    # Baseline per model = its time in the earliest version that recorded it.
    # A model added mid-history therefore enters the chart at 1.0.
    baseline: dict[str, int] = {}
    for version in ordered:
        for name, row in per_version[version].items():
            baseline.setdefault(name, row["val_ms"])

    points: list[dict] = []
    for version in ordered:
        models = per_version[version]
        names, rel, abs_ms = [], [], []
        for name in sorted(models):
            base = baseline.get(name)
            if not base:
                continue
            names.append(name)
            rel.append(round(models[name]["val_ms"] / base, 4))
            abs_ms.append(models[name]["val_ms"])
        points.append({
            "version": version,
            "geomean_ms": round(geomean([r["val_ms"] for r in models.values()]), 1),
            "geomean_rel": round(geomean(rel), 4) if rel else None,
            "models": names,
            "rel": rel,
            "abs_ms": abs_ms,
        })

    return {
        "dataset": dataset,
        "baseline_version": ordered[0] if ordered else None,
        "points": points,
    }


def build_model_table(
    version: str,
    prev_version: str | None,
    dataset: str,
) -> list[dict]:
    """Per-model validate_ms with % change vs previous version."""
    bench_file = RESULTS_DIR / version / f"{dataset}.txt"
    if not bench_file.exists():
        return []
    curr_models = parse_bench_file(bench_file)

    prev_models: dict[str, dict] = {}
    if prev_version:
        prev_file = RESULTS_DIR / prev_version / f"{dataset}.txt"
        if prev_file.exists():
            prev_models = parse_bench_file(prev_file)

    rows = []
    for model_name in sorted(curr_models):
        row = curr_models[model_name]
        prev_row = prev_models.get(model_name)
        pct_change: float | None = None
        if prev_row and prev_row["val_ms"] > 0:
            pct_change = round(
                (row["val_ms"] - prev_row["val_ms"]) / prev_row["val_ms"] * 100, 1
            )
        rows.append({
            "model": model_name,
            "infer_ms": row["infer_ms"],
            "val_ms": row["val_ms"],
            "pct_change_val": pct_change,
        })

    # Summary row: geomean of val_ms and geomean of pct_change
    curr_gm = geomean([r["val_ms"] for r in curr_models.values()])
    prev_gm = (
        geomean([r["val_ms"] for r in prev_models.values()]) if prev_models else None
    )
    pct_summary: float | None = None
    if prev_gm and prev_gm > 0:
        pct_summary = round((curr_gm - prev_gm) / prev_gm * 100, 1)

    rows.append({
        "model": "__geomean__",
        "infer_ms": round(geomean([r["infer_ms"] for r in curr_models.values() if r["infer_ms"] > 0]) or 0),
        "val_ms": round(curr_gm),
        "pct_change_val": pct_summary,
    })
    return rows


def previous_version(versions: list[str], current: str) -> str | None:
    try:
        idx = versions.index(current)
    except ValueError:
        return None
    return versions[idx - 1] if idx > 0 else None


def main() -> None:
    versions = collect_versions()
    if not versions:
        print("No version result directories found under benchmark/results/", file=sys.stderr)
        sys.exit(1)

    print(f"Versions: {versions}", file=sys.stderr)

    current_version = versions[-1]
    prev_version = previous_version(versions, current_version)
    print(f"Current: {current_version}, previous: {prev_version}", file=sys.stderr)

    brick_ts = build_timeseries(versions, "brick")
    s223_ts = build_timeseries(versions, "s223")
    brick_table = build_model_table(current_version, prev_version, "brick")
    s223_table = build_model_table(current_version, prev_version, "s223")

    out = {
        "versions": versions,
        "current_version": current_version,
        "prev_version": prev_version,
        "timeseries": [brick_ts, s223_ts],
        "tables": {
            "brick": brick_table,
            "s223": s223_table,
        },
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w") as f:
        json.dump(out, f, indent=2)
    print(f"Written to {OUTPUT_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()
