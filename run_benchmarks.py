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

The script mirrors the old run_benchmarks.sh flow but adds:
- Repeated timed runs per data graph (configurable with -n/--runs)
- Triple counting using rdflib to size each data graph
- CSV output of all timings
- A plot showing mean runtime vs data graph size for each platform
- A textual summary of mean/stddev per platform
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List

import matplotlib

# Use a non-interactive backend so the script works in CI/headless environments.
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from rdflib import Graph
from rdflib.util import guess_format


REPO_ROOT = Path(__file__).resolve().parent


@dataclass
class Measurement:
    platform: str
    data_file: Path
    triples: int
    run: int
    seconds: float

    # log post-init
    def __post_init__(self) -> None:
        print(
            f"[{self.platform}] {self.data_file.name} | "
            f"{self.triples} triples | run {self.run} | {self.seconds:.3f} s"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark SHACL validation runtimes across implementations."
    )
    parser.add_argument(
        "-n",
        "--runs",
        type=int,
        default=3,
        help="Number of times to run each platform per data graph.",
    )
    parser.add_argument(
        "--models-glob",
        default="benchmark/s223/models/*.ttl",
        help="Glob for data graph files to benchmark (relative to repo root).",
    )
    parser.add_argument(
        "--shapes-file",
        default="benchmark/s223/223p.ttl",
        help="Shapes file passed to each validator.",
    )
    parser.add_argument(
        "--output-csv",
        default="benchmark/benchmark_results.csv",
        help="Path for the timing CSV output.",
    )
    parser.add_argument(
        "--plot-output",
        default="benchmark/benchmark_results.png",
        help="Path for the runtime plot (PNG).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip the initial 'cargo build -r' step for shifty.",
    )
    parser.add_argument(
        "--uv",
        default="uv",
        help="uv executable to use for Python-based benchmarks.",
    )
    return parser.parse_args()


def run_checked(command: List[str], label: str) -> None:
    """Run a command and raise on failure, surfacing stdout/stderr for debugging."""
    print(f"-> {label}: {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Command failed with exit code {result.returncode}")
        if result.stdout:
            print("stdout:\n", result.stdout)
        if result.stderr:
            print("stderr:\n", result.stderr, file=sys.stderr)
        result.check_returncode()


def time_command(command: List[str]) -> float:
    """Measure wall-clock runtime of a command, raising if it fails."""
    start = time.perf_counter()
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    duration = time.perf_counter() - start
    if result.returncode != 0:
        print(f"Command failed: {' '.join(command)}")
        if result.stdout:
            print("stdout:\n", result.stdout)
        if result.stderr:
            print("stderr:\n", result.stderr, file=sys.stderr)
        result.check_returncode()
    return duration


def count_triples(path: Path) -> int:
    fmt = guess_format(str(path))
    graph = Graph()
    graph.parse(path, format=fmt)
    return len(graph)


def collect_models(pattern: str) -> List[Path]:
    models = sorted(REPO_ROOT.glob(pattern))
    if not models:
        raise FileNotFoundError(
            f"No data graphs found for pattern '{pattern}' from {REPO_ROOT}"
        )
    return models


def benchmark_platforms(
    models: Iterable[Path],
    shapes_file: Path,
    runs: int,
    uv_exe: str,
    skip_build: bool,
) -> List[Measurement]:
    if not skip_build:
        run_checked(["cargo", "build", "-r"], "cargo build -r")
        run_checked(["./target/release/shifty", "generate-ir", "--shapes-file", str(shapes_file), "--output-file", "shapes.ir"], "shifty generate-ir")

    commands: dict[str, Callable[[Path], List[str]]] = {
        "shifty-pre": lambda data: [
            "./target/release/shifty",
            "validate",
            "--data-file",
            str(data),
            "--shacl-ir",
            "shapes.ir",
            "--run-inference",
        ],
        "shifty": lambda data: [
            "./target/release/shifty",
            "validate",
            "--data-file",
            str(data),
            "--shapes-file",
            str(shapes_file),
            "--run-inference",
        ],
        #"pyshacl": lambda data: [
        #    uv_exe,
        #    "run",
        #    "scripts/pyshacl_bench.py",
        #    str(data),
        #    str(shapes_file),
        #],
        "topquadrant": lambda data: [
            uv_exe,
            "run",
            "scripts/topquadrant.py",
            str(data),
            str(shapes_file),
        ],
        "bmotif-topquadrant": lambda data: [
            uv_exe,
            "run",
            "scripts/bmotif-topquadrant.py",
            str(data),
            str(shapes_file),
        ],
    }

    measurements: List[Measurement] = []
    triple_cache: dict[Path, int] = {}

    for data_file in models:
        triples = triple_cache.setdefault(data_file, count_triples(data_file))
        for run_index in range(1, runs + 1):
            for platform, command_builder in commands.items():
                print(
                    f"[{platform}] {data_file.name} run {run_index}/{runs} "
                    f"({triples} triples)"
                )
                seconds = time_command(command_builder(data_file))
                measurements.append(
                    Measurement(
                        platform=platform,
                        data_file=data_file.relative_to(REPO_ROOT),
                        triples=triples,
                        run=run_index,
                        seconds=seconds,
                    )
                )
    return measurements


def save_results(measurements: List[Measurement], csv_path: Path) -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "platform": m.platform,
                "data_file": str(m.data_file),
                "triples": m.triples,
                "run": m.run,
                "seconds": m.seconds,
            }
            for m in measurements
        ]
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Wrote timing data to {csv_path}")
    return df


def plot_results(df: pd.DataFrame, plot_path: Path, runs: int) -> None:
    plot_path.parent.mkdir(parents=True, exist_ok=True)

    # Compute mean/stddev per platform per dataset for plotting with error bars.
    stats_df = (
        df.groupby(["platform", "data_file", "triples"])["seconds"]
        .agg(["mean", "std"])
        .reset_index()
        .rename(columns={"mean": "seconds_mean", "std": "seconds_std"})
    )

    plt.figure(figsize=(8, 5))
    for platform in ["shifty-pre", "shifty", "pyshacl", "topquadrant", "bmotif-topquadrant"]:
        platform_df = stats_df[stats_df["platform"] == platform].sort_values(
            "triples"
        )
        if platform_df.empty:
            continue
        plt.errorbar(
            platform_df["triples"],
            platform_df["seconds_mean"],
            yerr=platform_df["seconds_std"].fillna(0.0),
            marker="o",
            capsize=4,
            label=platform,
        )

    plt.xlabel("Triples in data graph")
    plt.ylabel(f"Mean runtime over {runs} runs (s)")
    plt.title("SHACL validation runtime by platform")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path)
    print(f"Saved plot to {plot_path}")


def print_summary(df: pd.DataFrame) -> None:
    summary = (
        df.groupby("platform")["seconds"]
        .agg(["mean", "std"])
        .sort_values("mean")
        .reset_index()
    )
    print("\nRuntime summary (seconds):")
    for _, row in summary.iterrows():
        std = row["std"]
        std_val = 0.0 if pd.isna(std) else std
        print(f"  {row['platform']:12s} mean={row['mean']:.3f} std={std_val:.3f}")


def main() -> None:
    args = parse_args()
    models = collect_models(args.models_glob)

    shapes_file = (REPO_ROOT / args.shapes_file).resolve()
    if not shapes_file.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes_file}")

    measurements = benchmark_platforms(
        models=models,
        shapes_file=shapes_file,
        runs=args.runs,
        uv_exe=args.uv,
        skip_build=args.skip_build,
    )

    df = save_results(measurements, REPO_ROOT / args.output_csv)
    plot_results(df, REPO_ROOT / args.plot_output, args.runs)
    print_summary(df)


if __name__ == "__main__":
    main()
