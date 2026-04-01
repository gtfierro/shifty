#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "matplotlib",
#   "numpy",
#   "pandas",
#   "rdflib",
# ]
# ///
"""Benchmark SHACL validation runtimes using hyperfine.

This script mirrors `run_benchmarks.py` but delegates timing to hyperfine,
exports raw hyperfine JSON artifacts, and generates an HTML report that embeds
plots produced via hyperfine's helper scripts.
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import shlex
import shutil
import statistics
import subprocess
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
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

HYPERFINE_SCRIPT_BASE = (
    "https://raw.githubusercontent.com/sharkdp/hyperfine/master/scripts"
)
PLOT_SCRIPT_FILES = ("plot_whisker.py", "plot_histogram.py")


@dataclass
class Measurement:
    platform: str
    data_file: Path
    triples: int
    run: int
    seconds: float
    timed_out: bool = False

    def __post_init__(self) -> None:
        timeout_suffix = " (timeout)" if self.timed_out else ""
        LOGGER.info(
            f"[{self.platform}] {self.data_file.name} | "
            f"{self.triples} triples | run {self.run} | {self.seconds:.3f} s"
            f"{timeout_suffix}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark SHACL validation runtimes via hyperfine."
    )
    parser.add_argument(
        "-n",
        "--runs",
        type=int,
        default=3,
        help="Number of timed runs hyperfine should execute per benchmark command.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="Number of warmup runs for each hyperfine command.",
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
        default="benchmark/hyperfine_benchmark_results.csv",
        help="Path for the timing CSV output.",
    )
    parser.add_argument(
        "--plot-output",
        default="benchmark/hyperfine_benchmark_results.png",
        help="Path for the runtime-vs-size plot (PNG).",
    )
    parser.add_argument(
        "--html-output",
        default="benchmark/hyperfine_report.html",
        help="Path for the generated HTML report.",
    )
    parser.add_argument(
        "--hyperfine-json-dir",
        default="benchmark/hyperfine-json",
        help="Directory for raw hyperfine JSON artifacts.",
    )
    parser.add_argument(
        "--hyperfine-plots-dir",
        default="benchmark/hyperfine-plots",
        help="Directory for plots produced by hyperfine helper scripts.",
    )
    parser.add_argument(
        "--hyperfine-scripts-dir",
        default="benchmark/hyperfine-tools",
        help="Local directory to store/download hyperfine helper scripts.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip initial build/compile steps for shifty and shacl-compiler2.",
    )
    parser.add_argument(
        "--compiled-out-dir",
        default="target/compiled-shacl-benchmark",
        help="Output directory for the generated shacl-compiler2 benchmark executable.",
    )
    parser.add_argument(
        "--compiled-bin-name",
        default="shacl-compiled-benchmark",
        help="Binary name for the generated shacl-compiler2 benchmark executable.",
    )
    parser.add_argument(
        "--uv",
        default="uv",
        help="uv executable to use for Python-based benchmarks.",
    )
    parser.add_argument(
        "--hyperfine",
        default="hyperfine",
        help="hyperfine executable to use.",
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
        help="Per-command timeout in seconds (applied via GNU timeout wrapper).",
    )
    args = parser.parse_args()

    if args.runs <= 0:
        parser.error("--runs must be greater than 0")
    if args.warmup < 0:
        parser.error("--warmup must be >= 0")
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than 0")

    return args


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    logging.basicConfig(level=numeric_level, format="%(levelname)s %(message)s")


def run_checked(command: List[str], label: str) -> None:
    LOGGER.info("-> %s: %s", label, " ".join(command))
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        LOGGER.error("Command failed with exit code %s", result.returncode)
        if result.stdout:
            LOGGER.debug("stdout:\n%s", result.stdout)
        if result.stderr:
            LOGGER.debug("stderr:\n%s", result.stderr)
        result.check_returncode()


def collect_models(pattern: str) -> List[Path]:
    models = sorted(REPO_ROOT.glob(pattern))
    if not models:
        raise FileNotFoundError(
            f"No data graphs found for pattern '{pattern}' from {REPO_ROOT}"
        )
    return models


def count_triples(path: Path) -> int:
    fmt = guess_format(str(path))
    graph = Graph()
    graph.parse(path, format=fmt)
    return len(graph)


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
        f"Compiled shacl-compiler2 executable not found. Looked in: {candidate_list}"
    )


def resolve_compiled_shape_graph(out_dir: Path) -> Path:
    candidate = out_dir / "shape_graph.ttl"
    if candidate.exists() and candidate.is_file():
        return candidate
    raise FileNotFoundError(
        f"Compiled shacl-compiler2 shape graph not found. Expected: {candidate}"
    )


def resolve_compiled_shape_ir(out_dir: Path) -> Path:
    candidates = [
        out_dir / "src" / "generated" / "shape_ir.json",
        out_dir / "shape_ir.json",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    candidate_list = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(
        f"Compiled shacl-compiler2 shape IR not found. Looked in: {candidate_list}"
    )


def ensure_hyperfine_scripts(scripts_dir: Path) -> dict[str, Path]:
    scripts_dir.mkdir(parents=True, exist_ok=True)
    resolved: dict[str, Path] = {}
    for script_name in PLOT_SCRIPT_FILES:
        path = scripts_dir / script_name
        if not path.exists():
            url = f"{HYPERFINE_SCRIPT_BASE}/{script_name}"
            LOGGER.info("Downloading hyperfine helper script: %s", url)
            urllib.request.urlretrieve(url, path)
        resolved[script_name] = path
    return resolved


def run_hyperfine(
    hyperfine_exe: str,
    command: List[str],
    command_name: str,
    runs: int,
    warmup: int,
    timeout_seconds: float | None,
    export_json: Path,
) -> tuple[list[float], list[int]]:
    export_json.parent.mkdir(parents=True, exist_ok=True)
    command_str = shlex.join(command)

    if timeout_seconds is not None:
        timeout_exe = shutil.which("timeout")
        if timeout_exe is None:
            raise RuntimeError(
                "--timeout-seconds requires the 'timeout' command (GNU coreutils)"
            )
        timeout_spec = f"{timeout_seconds:g}s"
        command_str = (
            f"{shlex.quote(timeout_exe)} --signal=TERM --kill-after=5s "
            f"{timeout_spec} {command_str}"
        )

    hf_cmd = [
        hyperfine_exe,
        "--runs",
        str(runs),
        "--warmup",
        str(warmup),
        "--export-json",
        str(export_json),
        "--ignore-failure",
        "--command-name",
        command_name,
        command_str,
    ]

    LOGGER.info("-> hyperfine [%s]: %s", command_name, command_str)
    result = subprocess.run(
        hf_cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        LOGGER.debug("hyperfine stdout:\n%s", result.stdout)
    if result.stderr:
        LOGGER.debug("hyperfine stderr:\n%s", result.stderr)

    times: list[float] = []
    exit_codes: list[int] = []
    if export_json.exists():
        data = json.loads(export_json.read_text(encoding="utf-8"))
        results = data.get("results", [])
        if results:
            raw_times = results[0].get("times", [])
            if isinstance(raw_times, list):
                times = [float(value) for value in raw_times]
            raw_codes = results[0].get("exit_codes", [])
            if isinstance(raw_codes, list):
                exit_codes = [int(value) for value in raw_codes]

    if result.returncode != 0 and not export_json.exists():
        LOGGER.error("hyperfine failed for command '%s'", command_name)
        result.check_returncode()

    return times, exit_codes


def benchmark_platforms(
    models: Iterable[Path],
    shapes_file: Path,
    runs: int,
    warmup: int,
    uv_exe: str,
    hyperfine_exe: str,
    skip_build: bool,
    compiled_out_dir: Path,
    compiled_bin_name: str,
    timeout_seconds: float | None,
    hyperfine_json_dir: Path,
) -> List[Measurement]:
    if not skip_build:
        run_checked(["cargo", "build", "-r"], "cargo build -r")
        run_checked(
            [
                "cargo",
                "run",
                "-p",
                "cli",
                "--features",
                "shacl-compiler2",
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
            "shifty compile (shacl-compiler2)",
        )

    compiled_binary = resolve_compiled_binary(compiled_out_dir, compiled_bin_name)
    compiled_shape_graph = resolve_compiled_shape_graph(compiled_out_dir)
    compiled_shape_ir = resolve_compiled_shape_ir(compiled_out_dir)
    LOGGER.info("Using shacl-compiler2 executable: %s", compiled_binary)
    LOGGER.info("Using compiler-expanded shape graph: %s", compiled_shape_graph)
    LOGGER.info("Using compiler-expanded shape IR: %s", compiled_shape_ir)

    commands: dict[str, Callable[[Path], List[str]]] = {
        "shifty-pre": lambda data: [
            "./target/release/shifty",
            "validate",
            "--data-file",
            str(data),
            "--shacl-ir",
            str(compiled_shape_ir),
            "--no-imports",
            "--run-inference",
        ],
        "shifty": lambda data: [
            "./target/release/shifty",
            "validate",
            "--data-file",
            str(data),
            "--shapes-file",
            str(compiled_shape_graph),
            "--no-imports",
            "--run-inference",
        ],
        "shacl-compiler2": lambda data: [
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
    }

    measurements: List[Measurement] = []
    triple_cache: dict[Path, int] = {}
    model_list = list(models)
    total_models = len(model_list)

    for model_index, data_file in enumerate(model_list, start=1):
        LOGGER.info("Model %s/%s: %s", model_index, total_models, data_file.name)
        triples = triple_cache.setdefault(data_file, count_triples(data_file))
        model_slug = data_file.stem

        for platform, command_builder in commands.items():
            command = command_builder(data_file)
            json_path = hyperfine_json_dir / f"{model_slug}__{platform}.json"
            times, exit_codes = run_hyperfine(
                hyperfine_exe=hyperfine_exe,
                command=command,
                command_name=f"{platform}:{data_file.name}",
                runs=runs,
                warmup=warmup,
                timeout_seconds=timeout_seconds,
                export_json=json_path,
            )

            run_count = max(len(times), len(exit_codes))
            for run_index in range(1, run_count + 1):
                code = (
                    exit_codes[run_index - 1] if run_index - 1 < len(exit_codes) else 0
                )
                timed_out = code == 124
                if run_index - 1 < len(times):
                    seconds = times[run_index - 1]
                elif timed_out and timeout_seconds is not None:
                    seconds = float(timeout_seconds)
                else:
                    seconds = 0.0

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


def build_platform_distribution_json(df: pd.DataFrame, output_path: Path) -> Path:
    results = []
    for platform in sorted(df["platform"].unique()):
        times = [float(v) for v in df[df["platform"] == platform]["seconds"].tolist()]
        if not times:
            continue
        stats = {
            "command": platform,
            "times": times,
            "mean": statistics.mean(times),
            "median": statistics.median(times),
            "min": min(times),
            "max": max(times),
            "stddev": statistics.stdev(times) if len(times) > 1 else 0.0,
        }
        results.append(stats)

    payload = {
        "results": results,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("Wrote aggregate hyperfine JSON to %s", output_path)
    return output_path


def run_hyperfine_plot_scripts(
    scripts: dict[str, Path],
    aggregate_json: Path,
    plots_dir: Path,
) -> dict[str, Path]:
    plots_dir.mkdir(parents=True, exist_ok=True)
    whisker_png = plots_dir / "platform_whisker.png"
    histogram_png = plots_dir / "platform_histogram.png"

    whisker_cmd = [
        "python3",
        str(scripts["plot_whisker.py"]),
        str(aggregate_json),
        "--title",
        "Runtime Distribution by Platform",
        "-o",
        str(whisker_png),
    ]
    run_checked(whisker_cmd, "hyperfine plot_whisker.py")

    histogram_cmd = [
        "python3",
        str(scripts["plot_histogram.py"]),
        str(aggregate_json),
        "--title",
        "Runtime Histogram by Platform",
        "-o",
        str(histogram_png),
    ]
    run_checked(histogram_cmd, "hyperfine plot_histogram.py")

    return {
        "whisker": whisker_png,
        "histogram": histogram_png,
    }


def plot_results(df: pd.DataFrame, plot_path: Path, runs: int) -> None:
    plot_path.parent.mkdir(parents=True, exist_ok=True)

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
        "shacl-compiler2",
        "pyshacl",
        "topquadrant",
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
    plt.ylabel(f"Mean runtime over <= {runs} runs (s)")
    plt.title("SHACL validation runtime by platform (hyperfine)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_path)
    LOGGER.info("Saved plot to %s", plot_path)


def summary_table(df: pd.DataFrame) -> pd.DataFrame:
    return (
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


def print_summary(df: pd.DataFrame) -> None:
    summary = summary_table(df)
    LOGGER.info("Runtime summary (seconds):")
    for _, row in summary.iterrows():
        std = row["std"]
        std_val = 0.0 if pd.isna(std) else std
        LOGGER.info(
            "  %-16s mean=%.3f std=%.3f timeouts=%d/%d",
            row["platform"],
            row["mean"],
            std_val,
            int(row["timeouts"]),
            int(row["runs"]),
        )


def write_html_report(
    html_path: Path,
    df: pd.DataFrame,
    csv_path: Path,
    runtime_plot: Path,
    aggregate_json: Path,
    hyperfine_plot_paths: dict[str, Path],
    hyperfine_json_dir: Path,
) -> None:
    html_path.parent.mkdir(parents=True, exist_ok=True)
    summary = summary_table(df)

    def display_path(path: Path) -> str:
        try:
            return str(path.resolve().relative_to(REPO_ROOT.resolve()))
        except ValueError:
            return str(path.resolve())

    summary_html = summary.to_html(
        index=False,
        float_format=lambda v: f"{v:.4f}" if pd.notna(v) else "",
        classes="summary",
        border=0,
    )

    model_json_links = []
    for path in sorted(hyperfine_json_dir.glob("*.json")):
        item_path = display_path(path)
        model_json_links.append(
            f'<li><a href="{html.escape(str(item_path))}">{html.escape(path.name)}</a></li>'
        )
    model_json_list = "\n".join(model_json_links)

    generated_at = datetime.now(timezone.utc).isoformat()

    html_text = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Hyperfine Benchmark Report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif; margin: 2rem; color: #1f2937; }}
    h1, h2 {{ color: #111827; }}
    .meta {{ color: #4b5563; font-size: 0.95rem; }}
    img {{ max-width: 100%; border: 1px solid #d1d5db; border-radius: 6px; margin: 0.5rem 0 1.5rem 0; }}
    table.summary {{ border-collapse: collapse; width: 100%; max-width: 900px; }}
    table.summary th, table.summary td {{ border-bottom: 1px solid #e5e7eb; padding: 0.45rem 0.6rem; text-align: right; }}
    table.summary th:first-child, table.summary td:first-child {{ text-align: left; }}
    code {{ background: #f3f4f6; padding: 0.1rem 0.3rem; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>Hyperfine Benchmark Report</h1>
  <p class=\"meta\">Generated: {html.escape(generated_at)}</p>

  <h2>Artifacts</h2>
  <ul>
    <li>CSV: <a href=\"{html.escape(display_path(csv_path))}\">{html.escape(display_path(csv_path))}</a></li>
    <li>Aggregate Hyperfine JSON: <a href=\"{html.escape(display_path(aggregate_json))}\">{html.escape(display_path(aggregate_json))}</a></li>
  </ul>

  <h2>Runtime Summary</h2>
  {summary_html}

  <h2>Runtime vs Graph Size</h2>
  <img src=\"{html.escape(display_path(runtime_plot))}\" alt=\"Runtime vs graph size\" />

  <h2>Hyperfine Tool Plots</h2>
  <p>Generated via hyperfine scripts: <code>plot_whisker.py</code> and <code>plot_histogram.py</code>.</p>
  <img src=\"{html.escape(display_path(hyperfine_plot_paths["whisker"]))}\" alt=\"Whisker plot\" />
  <img src=\"{html.escape(display_path(hyperfine_plot_paths["histogram"]))}\" alt=\"Histogram plot\" />

  <h2>Raw Hyperfine JSON Files</h2>
  <ul>
    {model_json_list}
  </ul>
</body>
</html>
"""

    html_path.write_text(html_text, encoding="utf-8")
    LOGGER.info("Wrote HTML report to %s", html_path)


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    models = collect_models(args.models_glob)
    shapes_file = (REPO_ROOT / args.shapes_file).resolve()
    if not shapes_file.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes_file}")

    scripts = ensure_hyperfine_scripts(
        (REPO_ROOT / args.hyperfine_scripts_dir).resolve()
    )

    measurements = benchmark_platforms(
        models=models,
        shapes_file=shapes_file,
        runs=args.runs,
        warmup=args.warmup,
        uv_exe=args.uv,
        hyperfine_exe=args.hyperfine,
        skip_build=args.skip_build,
        compiled_out_dir=(REPO_ROOT / args.compiled_out_dir).resolve(),
        compiled_bin_name=args.compiled_bin_name,
        timeout_seconds=args.timeout_seconds,
        hyperfine_json_dir=(REPO_ROOT / args.hyperfine_json_dir).resolve(),
    )

    output_csv = (REPO_ROOT / args.output_csv).resolve()
    runtime_plot = (REPO_ROOT / args.plot_output).resolve()
    html_output = (REPO_ROOT / args.html_output).resolve()
    plots_dir = (REPO_ROOT / args.hyperfine_plots_dir).resolve()

    df = save_results(measurements, output_csv)
    plot_results(df, runtime_plot, args.runs)

    aggregate_json = build_platform_distribution_json(
        df,
        (REPO_ROOT / args.hyperfine_json_dir / "platform_distribution.json").resolve(),
    )
    helper_plots = run_hyperfine_plot_scripts(scripts, aggregate_json, plots_dir)

    write_html_report(
        html_path=html_output,
        df=df,
        csv_path=output_csv,
        runtime_plot=runtime_plot,
        aggregate_json=aggregate_json,
        hyperfine_plot_paths=helper_plots,
        hyperfine_json_dir=(REPO_ROOT / args.hyperfine_json_dir).resolve(),
    )
    print_summary(df)


if __name__ == "__main__":
    main()
