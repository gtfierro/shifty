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
import logging
import os
import signal
import subprocess
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
LOGGER = logging.getLogger(__name__)


@dataclass
class Measurement:
    platform: str
    data_file: Path
    triples: int
    run: int
    seconds: float
    timed_out: bool = False

    # log post-init
    def __post_init__(self) -> None:
        timeout_suffix = " (timeout)" if self.timed_out else ""
        LOGGER.info(
            f"[{self.platform}] {self.data_file.name} | "
            f"{self.triples} triples | run {self.run} | {self.seconds:.3f} s"
            f"{timeout_suffix}"
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
        help="Skip initial build/compile steps for shifty and shacl-compiler.",
    )
    parser.add_argument(
        "--compiled-out-dir",
        default="target/compiled-shacl-benchmark",
        help="Output directory for the generated shacl-compiler benchmark executable.",
    )
    parser.add_argument(
        "--compiled-bin-name",
        default="shacl-compiled-benchmark",
        help="Binary name for the generated shacl-compiler benchmark executable.",
    )
    parser.add_argument(
        "--uv",
        default="uv",
        help="uv executable to use for Python-based benchmarks.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity for benchmark progress messages.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=None,
        help=(
            "Per-command timeout in seconds. "
            "When unset, commands run without a timeout."
        ),
    )
    args = parser.parse_args()
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than 0")
    return args


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(
        level=numeric_level,
        format="%(levelname)s %(message)s",
    )


def _terminate_subprocess(proc: subprocess.Popen[str]) -> None:
    """Best-effort termination for timed-out benchmark commands."""
    if proc.poll() is not None:
        return

    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
        proc.wait(timeout=3)
        return
    except (subprocess.TimeoutExpired, ProcessLookupError):
        pass

    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except ProcessLookupError:
        pass


def _run_command(
    command: List[str], timeout_seconds: float | None = None
) -> tuple[subprocess.Popen[str], str, str]:
    proc = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=(os.name == "posix"),
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_seconds)
        return proc, stdout, stderr
    except subprocess.TimeoutExpired:
        _terminate_subprocess(proc)
        stdout, stderr = proc.communicate()
        raise subprocess.TimeoutExpired(
            cmd=command,
            timeout=timeout_seconds,
            output=stdout,
            stderr=stderr,
        )


def run_checked(
    command: List[str], label: str, timeout_seconds: float | None = None
) -> None:
    """Run a command and raise on failure, surfacing stdout/stderr for debugging."""
    LOGGER.info("-> %s: %s", label, " ".join(command))
    try:
        proc, stdout, stderr = _run_command(command, timeout_seconds=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        timeout_display = (
            f"{timeout_seconds:g}" if timeout_seconds is not None else "unspecified"
        )
        LOGGER.error(
            "Command timed out after %s s (subprocess terminated): %s",
            timeout_display,
            " ".join(command),
        )
        if exc.stdout:
            LOGGER.debug("stdout:\n%s", exc.stdout)
        if exc.stderr:
            LOGGER.debug("stderr:\n%s", exc.stderr)
        raise

    if proc.returncode != 0:
        LOGGER.error("Command failed with exit code %s", proc.returncode)
        if stdout:
            LOGGER.debug("stdout:\n%s", stdout)
        if stderr:
            LOGGER.debug("stderr:\n%s", stderr)
        raise subprocess.CalledProcessError(
            proc.returncode,
            command,
            output=stdout,
            stderr=stderr,
        )


def time_command(
    command: List[str], timeout_seconds: float | None = None
) -> tuple[float, bool]:
    """Measure wall-clock runtime of a command.

    Returns:
        (duration_seconds, timed_out)
    """
    start = time.perf_counter()
    try:
        proc, stdout, stderr = _run_command(command, timeout_seconds=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        duration = time.perf_counter() - start
        timeout_display = f"{timeout_seconds:g}" if timeout_seconds is not None else "?"
        LOGGER.warning(
            "Command timed out after %s s; subprocess terminated; recording %.3f s and continuing: %s",
            timeout_display,
            duration,
            " ".join(command),
        )
        if exc.stdout:
            LOGGER.debug("stdout:\n%s", exc.stdout)
        if exc.stderr:
            LOGGER.debug("stderr:\n%s", exc.stderr)
        return duration, True

    duration = time.perf_counter() - start
    if proc.returncode != 0:
        LOGGER.error("Command failed: %s", " ".join(command))
        if stdout:
            LOGGER.debug("stdout:\n%s", stdout)
        if stderr:
            LOGGER.debug("stderr:\n%s", stderr)
        raise subprocess.CalledProcessError(
            proc.returncode,
            command,
            output=stdout,
            stderr=stderr,
        )
    return duration, False


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


def resolve_compiled_binary(out_dir: Path, bin_name: str) -> Path:
    candidates = [
        out_dir / "target" / "release" / bin_name,
        out_dir / bin_name,
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    candidate_list = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(
        f"Compiled shacl-compiler executable not found. Looked in: {candidate_list}"
    )


def benchmark_platforms(
    models: Iterable[Path],
    shapes_file: Path,
    runs: int,
    uv_exe: str,
    skip_build: bool,
    compiled_out_dir: Path,
    compiled_bin_name: str,
    timeout_seconds: float | None,
) -> List[Measurement]:
    if not skip_build:
        run_checked(
            ["cargo", "build", "-r"],
            "cargo build -r",
        )
        run_checked(
            [
                "./target/release/shifty",
                "generate-ir",
                "--shapes-file",
                str(shapes_file),
                "--output-file",
                "shapes.ir",
            ],
            "shifty generate-ir",
        )
        run_checked(
            [
                "cargo",
                "run",
                "-p",
                "cli",
                "--features",
                "shacl-compiler",
                "--",
                "compile",
                "--shapes-file",
                str(shapes_file),
                "--out-dir",
                str(compiled_out_dir),
                "--bin-name",
                compiled_bin_name,
                "--release",
                "--shifty-path",
                str(REPO_ROOT / "lib"),
            ],
            "shifty compile (shacl-compiler)",
        )

    compiled_binary = resolve_compiled_binary(compiled_out_dir, compiled_bin_name)
    LOGGER.info("Using shacl-compiler executable: %s", compiled_binary)

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
        "shacl-compiler": lambda data: [
            str(compiled_binary),
            str(data),
        ],
        "pyshacl": lambda data: [
            uv_exe,
            "run",
            "scripts/pyshacl_bench.py",
            str(data),
            str(shapes_file),
        ],
        "topquadrant": lambda data: [
            uv_exe,
            "run",
            "scripts/topquadrant.py",
            str(data),
            str(shapes_file),
        ],
        # "bmotif-topquadrant": lambda data: [
        #    uv_exe,
        #    "run",
        #    "scripts/bmotif-topquadrant.py",
        #    str(data),
        #    str(shapes_file),
        # ],
    }

    measurements: List[Measurement] = []
    triple_cache: dict[Path, int] = {}
    model_list = list(models)
    total_models = len(model_list)

    for model_index, data_file in enumerate(model_list, start=1):
        LOGGER.info("Model %s/%s: %s", model_index, total_models, data_file.name)
        triples = triple_cache.setdefault(data_file, count_triples(data_file))
        for platform, command_builder in commands.items():
            for run_index in range(1, runs + 1):
                LOGGER.info(
                    "[%s] %s run %s/%s (%s triples)",
                    platform,
                    data_file.name,
                    run_index,
                    runs,
                    triples,
                )
                seconds, timed_out = time_command(
                    command_builder(data_file),
                    timeout_seconds=timeout_seconds,
                )
                measurements.append(
                    Measurement(
                        platform=platform,
                        data_file=data_file.relative_to(REPO_ROOT),
                        triples=triples,
                        run=run_index,
                        seconds=seconds,
                        timed_out=timed_out,
                    )
                )
                if timed_out and run_index < runs:
                    LOGGER.warning(
                        "[%s] %s timed out on run %s/%s; skipping remaining runs for this benchmark",
                        platform,
                        data_file.name,
                        run_index,
                        runs,
                    )
                    break
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
                "timed_out": m.timed_out,
            }
            for m in measurements
        ]
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    LOGGER.info("Wrote timing data to %s", csv_path)
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
    preferred_order = [
        "shifty-pre",
        "shifty",
        "shacl-compiler",
        "pyshacl",
        "topquadrant",
        "bmotif-topquadrant",
    ]
    remaining_platforms = sorted(
        platform
        for platform in stats_df["platform"].unique()
        if platform not in preferred_order
    )
    for platform in preferred_order + remaining_platforms:
        platform_df = stats_df[stats_df["platform"] == platform].sort_values("triples")
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
    LOGGER.info("Saved plot to %s", plot_path)


def print_summary(df: pd.DataFrame) -> None:
    summary = (
        df.groupby("platform")
        .agg(
            mean=("seconds", "mean"),
            std=("seconds", "std"),
            timeouts=("timed_out", "sum"),
            runs=("timed_out", "count"),
        )
        .sort_values("mean")
        .reset_index()
    )
    LOGGER.info("Runtime summary (seconds):")
    for _, row in summary.iterrows():
        std = row["std"]
        std_val = 0.0 if pd.isna(std) else std
        LOGGER.info(
            "  %-12s mean=%.3f std=%.3f timeouts=%d/%d",
            row["platform"],
            row["mean"],
            std_val,
            int(row["timeouts"]),
            int(row["runs"]),
        )


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
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
        compiled_out_dir=(REPO_ROOT / args.compiled_out_dir).resolve(),
        compiled_bin_name=args.compiled_bin_name,
        timeout_seconds=args.timeout_seconds,
    )

    df = save_results(measurements, REPO_ROOT / args.output_csv)
    plot_results(df, REPO_ROOT / args.plot_output, args.runs)
    print_summary(df)


if __name__ == "__main__":
    main()
