#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["rdflib"]
# ///
"""Parse benchmark results and produce benchmark_data.json for the docs chart.

Wall-clock ``validate`` time is dominated by a fixed cost (preparing the shapes
graph) that is the same no matter how small the data graph is -- a 16-triple
Brick model still takes ~3.7 s.  Reporting raw ms, or naive triples/sec, mostly
measures model size rather than engine speed.  So per release we fit

    validate_ms = setup_ms + triples / throughput

across the whole corpus and chart the two coefficients separately: fixed setup
cost, and marginal validated triples/sec.  These separate "the engine starts up
faster" from "the engine validates faster", which the single wall-clock number
blurs together.

Reads:
  benchmark/results/<version>/brick.txt
  benchmark/results/<version>/s223.txt

Writes:
  benchmark/results/benchmark_data.json
  benchmark/results/model_triples.json   (cache; safe to delete)

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
TRIPLE_CACHE = RESULTS_DIR / "model_triples.json"

# Results directory for the current checkout; must match run_history.sh.
HEAD_LABEL = "HEAD"

MODEL_DIRS = {"brick": BRICK_MODELS, "s223": S223_MODELS}

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


def load_triple_counts() -> dict[str, int]:
    """Triple count per model, keyed "<dataset>/<file>.ttl".

    Parsing the corpus with rdflib takes ~a minute, and conf.py re-runs this
    script on every docs build, so results are cached and only re-parsed when a
    model's byte size changes.  Returns {} if rdflib is unavailable -- callers
    degrade to omitting the throughput series rather than failing the build.
    """
    cache: dict[str, dict] = {}
    if TRIPLE_CACHE.exists():
        try:
            cache = json.loads(TRIPLE_CACHE.read_text())
        except (json.JSONDecodeError, OSError):
            cache = {}

    stale = []
    for dataset, model_dir in MODEL_DIRS.items():
        if not model_dir.is_dir():
            continue
        for path in sorted(model_dir.glob("*.ttl")):
            key = f"{dataset}/{path.name}"
            entry = cache.get(key)
            if not entry or entry.get("bytes") != path.stat().st_size:
                stale.append((key, path))

    if stale:
        try:
            from rdflib import Graph
        except ImportError:
            print(
                f"rdflib not available; skipping triple counts for {len(stale)} "
                "model(s). Throughput series will be omitted.",
                file=sys.stderr,
            )
            return {k: v["triples"] for k, v in cache.items() if "triples" in v}

        print(f"Counting triples for {len(stale)} model(s)...", file=sys.stderr)
        for key, path in stale:
            graph = Graph()
            try:
                graph.parse(path)
            except Exception as exc:  # malformed model: skip, don't fail the build
                print(f"  {key}: parse failed ({exc})", file=sys.stderr)
                continue
            cache[key] = {"triples": len(graph), "bytes": path.stat().st_size}
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        TRIPLE_CACHE.write_text(json.dumps(cache, indent=1, sort_keys=True))

    return {k: v["triples"] for k, v in cache.items() if "triples" in v}


def fit_setup_throughput(
    samples: list[tuple[int, int]],
) -> tuple[float, float, float] | None:
    """Least-squares fit of validate_ms = setup_ms + triples / throughput.

    `samples` is [(triples, validate_ms), ...].  Returns
    (setup_ms, triples_per_sec, r_squared), or None when the fit is not
    meaningful (too few models, no size spread, or a non-positive slope).
    """
    if len(samples) < 5:
        return None
    xs = [float(t) for t, _ in samples]
    ys = [float(ms) for _, ms in samples]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx <= 0:
        return None
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / sxx
    if slope <= 0:
        return None
    intercept = mean_y - slope * mean_x

    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # slope is ms per triple; invert to triples per second.
    return round(intercept, 1), round(1000.0 / slope, 1), round(r2, 4)


def geomean(values: list[float]) -> float:
    pos = [v for v in values if v > 0]
    if not pos:
        return 0.0
    return math.exp(sum(math.log(v) for v in pos) / len(pos))


def collect_versions() -> list[str]:
    """Release directories in version order, with HEAD (if present) last.

    run_history.sh writes the current checkout to a `HEAD` directory so
    unreleased work shows up on the chart. It has no version number, and it is
    always newer than every tag, so it sorts after them rather than by name.
    """
    dirs = [d.name for d in RESULTS_DIR.iterdir()
            if d.is_dir() and (re.match(r"v\d+\.\d+\.\d+", d.name)
                               or d.name == HEAD_LABEL)]

    def ver_key(s: str):
        if s == HEAD_LABEL:
            return (1, ())
        return (0, tuple(int(x) for x in s.lstrip("v").split(".")))

    dirs.sort(key=ver_key)
    return dirs


def build_timeseries(
    versions: list[str], dataset: str, triples: dict[str, int]
) -> dict:
    """Per-version fixed setup cost and marginal throughput for one dataset.

    See the module docstring for why the two coefficients are charted instead
    of raw wall-clock time.  Points missing a fit (e.g. an in-progress run with
    too few models) still carry geomean_ms so nothing silently vanishes.
    """
    points: list[dict] = []
    for version in versions:
        bench_file = RESULTS_DIR / version / f"{dataset}.txt"
        if not bench_file.exists():
            continue
        models = parse_bench_file(bench_file)
        if not models:
            continue

        samples = [
            (triples[f"{dataset}/{name}"], row["val_ms"])
            for name, row in models.items()
            if triples.get(f"{dataset}/{name}")
        ]
        fit = fit_setup_throughput(samples)

        point = {
            "version": version,
            "geomean_ms": round(geomean([r["val_ms"] for r in models.values()]), 1),
            "n_models": len(models),
        }
        # The fit contributes only marginal throughput -- a headline
        # triples/sec figure. Its intercept is *not* used as setup; see
        # split_components for why.
        if fit:
            point["tps"], point["fit_r2"] = fit[1], fit[2]
        sizes = {n: triples[f"{dataset}/{n}"] for n in models
                 if triples.get(f"{dataset}/{n}")}
        point.update(split_components(models, sizes))
        points.append(point)

    return {"dataset": dataset, "points": points}


def split_components(models: dict[str, dict], sizes: dict[str, int]) -> dict:
    """Total corpus seconds split into setup / inference / validation.

    `validate` runs inference internally, so `val_ms` is cumulative and the
    phases separate by subtraction:

        setup      = infer_ms of the corpus's smallest model
        inference  = infer_ms - setup
        validation = val_ms   - infer_ms

    Setup is deliberately *not* taken from the ms-vs-triples fit.  That fit's
    intercept is `validate` extrapolated to zero triples, which already
    contains inference work -- it lands above the measured infer floor for
    every dataset and version here, which would make inference negative and the
    stack overshoot the real total.

    The corpus instead contains near-empty models (16 triples for Brick), whose
    inference time is almost entirely startup.  The *smallest* such model is
    used rather than whichever happened to run fastest: it is the same model
    every version, so run-to-run noise doesn't wander between versions and get
    misread as inference changing.  (Picking the per-version minimum instead
    roughly doubles the jitter and invents inference spikes -- e.g. s223 v0.2.5
    reads 20.7 s against ~15.5 s for both its neighbours.)

    Every component is measured, and the three sum to the recorded total
    exactly.  Runs predating the infer-timing change record infer_ms as 0;
    those yield inference=None and the chart falls back to a two-part stack.
    """
    total_val = sum(r["val_ms"] for r in models.values())
    out = {"total_s": round(total_val / 1000, 2)}

    if not all(r["infer_ms"] > 0 for r in models.values()):
        out["setup_ms"] = None
        out["setup_s"] = None
        out["infer_s"] = None
        out["validate_s"] = round(total_val / 1000, 2)
        return out

    sized = [n for n in models if sizes.get(n)]
    if sized:
        setup_ms = models[min(sized, key=lambda n: sizes[n])]["infer_ms"]
    else:  # no triple counts available: fall back to the fastest inference
        setup_ms = min(r["infer_ms"] for r in models.values())

    total_infer = sum(r["infer_ms"] for r in models.values())
    setup_total = setup_ms * len(models)

    out["setup_ms"] = setup_ms
    out["setup_s"] = round(setup_total / 1000, 2)
    # Clamp: the reference model is not guaranteed to be the fastest, so on a
    # noisy run the aggregate could dip a hair below zero.
    out["infer_s"] = round(max(total_infer - setup_total, 0) / 1000, 2)
    # `validate` is cumulative over `infer`, so this subtraction removes setup
    # and inference together -- nothing is double-counted.
    out["validate_s"] = round((total_val - total_infer) / 1000, 2)
    return out


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

    triples = load_triple_counts()
    brick_ts = build_timeseries(versions, "brick", triples)
    s223_ts = build_timeseries(versions, "s223", triples)
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
