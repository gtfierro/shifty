#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "matplotlib",
#   "pandas",
#   "rdflib",
# ]
# ///
"""Benchmark SHACL validation runtimes across shifty, pySHACL, and TopQuadrant.

Runs each validator over every model in a dataset suite (Brick and/or S223),
collects wall-clock timings across multiple runs, writes a CSV that
``compare_benchmarks.py`` can consume, and generates a scatter plot.

Usage:
    uv run benchmark/run_benchmarks.py                     # Brick + S223, 3 runs
    uv run benchmark/run_benchmarks.py --suite s223 -n 5
    uv run benchmark/run_benchmarks.py --skip-pyshacl --skip-topquadrant
    uv run benchmark/run_benchmarks.py --skip-build --output-csv results.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from rdflib import Graph
from rdflib.util import guess_format


REPO_ROOT = Path(__file__).resolve().parent.parent
LOGGER = logging.getLogger(__name__)

SUITES = {
    "brick": {
        "shapes": "brick/Brick-closure.ttl",
        "models": "brick/models/*.ttl",
    },
    "s223": {
        "shapes": "s223/223p-closure.ttl",
        "models": "s223/models/*.ttl",
    },
}


@dataclass
class Measurement:
    platform: str
    data_file: Path
    triples: int
    run: int
    seconds: float
    timed_out: bool = False
    failed: bool = False

    def __post_init__(self) -> None:
        suffix = " (timeout)" if self.timed_out else (" (failed)" if self.failed else "")
        LOGGER.info(
            "[%s] %s | %d triples | run %d | %.3f s%s",
            self.platform,
            self.data_file.name,
            self.triples,
            self.run,
            self.seconds,
            suffix,
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-n", "--runs", type=int, default=3,
                   help="Timed runs per model per platform (default: 3).")
    p.add_argument("--suite", choices=["brick", "s223", "both"], default="both",
                   help="Dataset suite to benchmark (default: both).")
    p.add_argument("--output-csv", default="benchmark/results.csv",
                   help="CSV output path.")
    p.add_argument("--plot-output", default="benchmark/results.png",
                   help="Plot output path (PNG).")
    p.add_argument("--skip-build", action="store_true",
                   help="Skip 'cargo build --release'.")
    p.add_argument("--skip-pyshacl", action="store_true",
                   help="Skip the pySHACL platform.")
    p.add_argument("--skip-topquadrant", action="store_true",
                   help="Skip the TopQuadrant platform.")
    p.add_argument("--timeout", type=float, default=None,
                   help="Per-run timeout in seconds (default: none).")
    p.add_argument("--uv", default="uv",
                   help="uv executable for Python-based validators.")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


# ── subprocess helpers ────────────────────────────────────────────────────────

def _kill(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=3)
    except (subprocess.TimeoutExpired, ProcessLookupError):
        try:
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except ProcessLookupError:
            pass


def run_checked(cmd: List[str], label: str) -> None:
    LOGGER.info("-> %s: %s", label, " ".join(cmd))
    proc = subprocess.run(cmd, cwd=REPO_ROOT)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def time_command(cmd: List[str], timeout: float | None) -> tuple[float, bool, bool]:
    """Return (seconds, timed_out, failed)."""
    kwargs: dict = dict(
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=(os.name == "posix"),
    )
    proc = subprocess.Popen(cmd, **kwargs)
    start = time.perf_counter()
    try:
        proc.wait(timeout=timeout)
        elapsed = time.perf_counter() - start
        if proc.returncode != 0:
            LOGGER.warning("Command failed (exit %d): %s", proc.returncode, " ".join(cmd))
            return elapsed, False, True
        return elapsed, False, False
    except subprocess.TimeoutExpired:
        elapsed = timeout or (time.perf_counter() - start)
        _kill(proc)
        proc.wait()
        LOGGER.warning("Timed out after %.1f s: %s", elapsed, " ".join(cmd))
        return elapsed, True, False


# ── data loading ─────────────────────────────────────────────────────────────

def count_triples(path: Path) -> int:
    g = Graph()
    g.parse(path, format=guess_format(str(path)))
    return len(g)


def collect_models(suite: str) -> list[tuple[Path, Path]]:
    """Return list of (shapes_file, model_file) pairs for the named suite."""
    cfg = SUITES[suite]
    shapes = (REPO_ROOT / cfg["shapes"]).resolve()
    if not shapes.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes}")
    models = sorted(REPO_ROOT.glob(cfg["models"]))
    if not models:
        raise FileNotFoundError(f"No models found at {REPO_ROOT / cfg['models']}")
    return [(shapes, m) for m in models]


# ── benchmark core ────────────────────────────────────────────────────────────

def build_commands(
    shapes: Path,
    data: Path,
    uv: str,
    skip_pyshacl: bool,
    skip_topquadrant: bool,
) -> dict[str, List[str]]:
    binary = REPO_ROOT / "target" / "release" / "shacl"
    cmds: dict[str, List[str]] = {
        "shifty": [str(binary), "validate", "--shapes", str(shapes), "--data", str(data)],
    }
    if not skip_pyshacl:
        cmds["pyshacl"] = [uv, "run", "scripts/pyshacl_bench.py", str(data), str(shapes)]
    if not skip_topquadrant:
        cmds["topquadrant"] = [uv, "run", "scripts/topquadrant.py", str(data), str(shapes)]
    return cmds


def benchmark(
    pairs: list[tuple[Path, Path]],
    runs: int,
    timeout: float | None,
    uv: str,
    skip_pyshacl: bool,
    skip_topquadrant: bool,
) -> List[Measurement]:
    measurements: List[Measurement] = []
    triple_cache: dict[Path, int] = {}

    for shapes, data in pairs:
        triples = triple_cache.setdefault(data, count_triples(data))
        cmds = build_commands(shapes, data, uv, skip_pyshacl, skip_topquadrant)

        for platform, cmd in cmds.items():
            for run_i in range(1, runs + 1):
                LOGGER.info("[%s] %s run %d/%d", platform, data.name, run_i, runs)
                seconds, timed_out, failed = time_command(cmd, timeout)
                m = Measurement(
                    platform=platform,
                    data_file=data.relative_to(REPO_ROOT),
                    triples=triples,
                    run=run_i,
                    seconds=seconds,
                    timed_out=timed_out,
                    failed=failed,
                )
                measurements.append(m)
                if timed_out or failed:
                    LOGGER.warning(
                        "[%s] %s %s on run %d; skipping remaining runs",
                        platform, data.name,
                        "timed out" if timed_out else "failed",
                        run_i,
                    )
                    break

    return measurements


# ── output ────────────────────────────────────────────────────────────────────

def save_csv(measurements: List[Measurement], path: Path) -> pd.DataFrame:
    df = pd.DataFrame([
        {
            "platform": m.platform,
            "data_file": str(m.data_file),
            "triples": m.triples,
            "run": m.run,
            "seconds": m.seconds,
            "timed_out": m.timed_out,
            "failed": m.failed,
        }
        for m in measurements
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    LOGGER.info("Wrote %s", path)
    return df


def plot(df: pd.DataFrame, path: Path, runs: int) -> None:
    stats = (
        df.groupby(["platform", "data_file", "triples"])["seconds"]
        .agg(["mean", "std"])
        .reset_index()
    )
    plt.figure(figsize=(8, 5))
    order = ["shifty", "pyshacl", "topquadrant"]
    extras = sorted(p for p in stats["platform"].unique() if p not in order)
    for platform in order + extras:
        sub = stats[stats["platform"] == platform].sort_values("triples")
        if sub.empty:
            continue
        plt.errorbar(
            sub["triples"], sub["mean"],
            yerr=sub["std"].fillna(0),
            marker="o", capsize=4, label=platform,
        )
    plt.xlabel("Triples in data graph")
    plt.ylabel(f"Mean runtime over {runs} run(s) (s)")
    plt.title("SHACL validation runtime by platform")
    plt.legend()
    plt.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path)
    LOGGER.info("Saved plot to %s", path)


def print_summary(df: pd.DataFrame) -> None:
    summary = (
        df.groupby("platform")
        .agg(mean=("seconds", "mean"), std=("seconds", "std"),
             timeouts=("timed_out", "sum"), failures=("failed", "sum"),
             runs=("timed_out", "count"))
        .sort_values("mean")
        .reset_index()
    )
    LOGGER.info("Summary (seconds):")
    for _, row in summary.iterrows():
        std = 0.0 if pd.isna(row["std"]) else row["std"]
        LOGGER.info(
            "  %-14s  mean=%.3f  std=%.3f  timeouts=%d/%d  failures=%d",
            row["platform"], row["mean"], std,
            int(row["timeouts"]), int(row["runs"]), int(row["failures"]),
        )


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(message)s",
    )

    if not args.skip_build:
        run_checked(["cargo", "build", "--release", "-p", "shacl-cli"], "cargo build")

    suites = ["brick", "s223"] if args.suite == "both" else [args.suite]
    pairs: list[tuple[Path, Path]] = []
    for suite in suites:
        try:
            pairs.extend(collect_models(suite))
        except FileNotFoundError as e:
            LOGGER.warning("Skipping suite %s: %s", suite, e)

    if not pairs:
        raise SystemExit("No benchmark data found. Run with --suite to specify.")

    measurements = benchmark(
        pairs=pairs,
        runs=args.runs,
        timeout=args.timeout,
        uv=args.uv,
        skip_pyshacl=args.skip_pyshacl,
        skip_topquadrant=args.skip_topquadrant,
    )

    df = save_csv(measurements, REPO_ROOT / args.output_csv)
    plot(df, REPO_ROOT / args.plot_output, args.runs)
    print_summary(df)


if __name__ == "__main__":
    main()
