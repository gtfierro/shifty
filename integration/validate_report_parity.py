#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "brick-tq-shacl>=0.4.1",
#   "ontoenv>=0.5.1",
#   "pyshacl",
#   "rdflib",
# ]
# ///
"""Integration harness for cross-validator SHACL report parity.

This script runs:
- shifty
- shifty-pre (SHACL-IR)
- shifty-compile (compiled executable)
- pySHACL
- TopQuadrant

It writes each validation report to Turtle and performs pairwise graph
isomorphism checks across all generated reports.
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pyshacl
import rdflib
from brick_tq_shacl import infer as tq_infer
from brick_tq_shacl import validate as tq_validate
from ontoenv import OntoEnv
from rdflib.compare import graph_diff, isomorphic, to_isomorphic
from rdflib.util import guess_format


LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parent.parent
PLATFORMS = ("pyshacl", "topquadrant", "shifty", "shifty-pre", "shifty-compile")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate SHACL validation reports across multiple validators and "
            "check pairwise report graph isomorphism."
        )
    )
    parser.add_argument(
        "--shapes-file",
        required=True,
        help="Path to the SHACL shapes graph.",
    )
    parser.add_argument(
        "--data-file",
        required=True,
        help="Path to the data graph to validate.",
    )
    parser.add_argument(
        "--out-dir",
        default="integration/artifacts/validation-parity",
        help="Output directory for generated reports and pairwise diff artifacts.",
    )
    parser.add_argument(
        "--compiled-out-dir",
        default="integration/artifacts/validation-parity/compiled-shacl",
        help="Output directory for the generated shifty-compile executable.",
    )
    parser.add_argument(
        "--compiled-bin-name",
        default="shifty-compiled-parity",
        help="Binary name for the generated shifty-compile executable.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip build steps and reuse existing shifty artifacts.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=None,
        help="Timeout for each subprocess command.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    args = parser.parse_args()
    if args.timeout_seconds is not None and args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than 0")
    return args


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level), format="%(levelname)s %(message)s"
    )


def resolve_path(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate.resolve()


def run_checked(
    command: Iterable[str], label: str, timeout_seconds: float | None = None
) -> subprocess.CompletedProcess[str]:
    command_list = list(command)
    LOGGER.info("-> %s: %s", label, " ".join(command_list))
    try:
        result = subprocess.run(
            command_list,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{label} timed out after {timeout_seconds}s: {' '.join(command_list)}"
        ) from exc

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def parse_report_text(report_text: str, label: str) -> rdflib.Graph:
    for rdf_format in ("turtle", "ntriples", "xml"):
        graph = rdflib.Graph()
        try:
            graph.parse(data=report_text, format=rdf_format)
            return graph
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError(f"Could not parse {label} report in supported RDF formats")


def parse_graph_file(path: Path) -> rdflib.Graph:
    rdf_format = guess_format(str(path)) or "turtle"
    return rdflib.Graph().parse(path, format=rdf_format)


def write_graph(graph: rdflib.Graph, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    graph.serialize(destination=destination, format="turtle")


def resolve_compiled_binary(out_dir: Path, bin_name: str) -> Path:
    candidates = [
        out_dir / "target" / "release" / bin_name,
        out_dir / bin_name,
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Compiled executable not found; searched: {searched}")


def build_artifacts(
    *,
    shapes_file: Path,
    shacl_ir_file: Path,
    compiled_out_dir: Path,
    compiled_bin_name: str,
    skip_build: bool,
    timeout_seconds: float | None,
) -> Path:
    if not skip_build:
        run_checked(["cargo", "build", "-r"], "cargo build -r", timeout_seconds)
        run_checked(
            [
                "./target/release/shifty",
                "generate-ir",
                "--shapes-file",
                str(shapes_file),
                "--output-file",
                str(shacl_ir_file),
            ],
            "shifty generate-ir",
            timeout_seconds,
        )
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
            "shifty compile",
            timeout_seconds,
        )

    if not shacl_ir_file.exists():
        raise FileNotFoundError(
            f"Missing SHACL-IR file for shifty-pre: {shacl_ir_file} "
            "(run without --skip-build or pre-generate this file)"
        )
    return resolve_compiled_binary(compiled_out_dir, compiled_bin_name)


def run_shifty_report(
    *,
    shapes_file: Path,
    data_file: Path,
    timeout_seconds: float | None,
) -> rdflib.Graph:
    result = run_checked(
        [
            "./target/release/shifty",
            "validate",
            "--shapes-file",
            str(shapes_file),
            "--data-file",
            str(data_file),
            "--run-inference",
            "--format",
            "turtle",
        ],
        "shifty validate",
        timeout_seconds,
    )
    return parse_report_text(result.stdout, "shifty")


def run_shifty_pre_report(
    *,
    shacl_ir_file: Path,
    data_file: Path,
    timeout_seconds: float | None,
) -> rdflib.Graph:
    result = run_checked(
        [
            "./target/release/shifty",
            "validate",
            "--shacl-ir",
            str(shacl_ir_file),
            "--data-file",
            str(data_file),
            "--run-inference",
            "--format",
            "turtle",
        ],
        "shifty-pre validate",
        timeout_seconds,
    )
    return parse_report_text(result.stdout, "shifty-pre")


def run_shifty_compile_report(
    *,
    compiled_binary: Path,
    data_file: Path,
    timeout_seconds: float | None,
) -> rdflib.Graph:
    result = run_checked(
        [str(compiled_binary), str(data_file)],
        "shifty-compile validate",
        timeout_seconds,
    )
    return parse_report_text(result.stdout, "shifty-compile")


def run_pyshacl_report(*, shapes_file: Path, data_file: Path) -> rdflib.Graph:
    env = OntoEnv(temporary=True, no_search=True)
    model_graph = parse_graph_file(data_file)
    ontology_id = env.add(str(shapes_file))
    shapes_graph = env.get_graph(ontology_id)
    env.import_dependencies(shapes_graph)
    _, report_graph, _ = pyshacl.validate(
        data_graph=model_graph,
        shacl_graph=shapes_graph,
        ont_graph=shapes_graph,
        advanced=True,
        inplace=True,
        js=True,
        allow_warnings=True,
    )
    return report_graph


def run_topquadrant_report(*, shapes_file: Path, data_file: Path) -> rdflib.Graph:
    model_graph = parse_graph_file(data_file)
    shapes_graph = parse_graph_file(shapes_file)
    inferred_graph = tq_infer(model_graph, shapes_graph)
    _, report_graph, report_text = tq_validate(inferred_graph, shapes_graph)
    if isinstance(report_graph, rdflib.Graph):
        return report_graph
    return parse_report_text(report_text, "topquadrant")


def compare_pairwise(
    reports: Dict[str, rdflib.Graph], diff_root: Path
) -> Dict[str, Dict[str, bool]]:
    matrix: Dict[str, Dict[str, bool]] = {
        platform: {} for platform in sorted(reports.keys())
    }
    for left, right in itertools.combinations(sorted(reports.keys()), 2):
        left_graph = to_isomorphic(reports[left])
        right_graph = to_isomorphic(reports[right])
        equal = isomorphic(left_graph, right_graph)
        matrix[left][right] = equal
        matrix[right][left] = equal
        matrix[left][left] = True
        matrix[right][right] = True
        if not equal:
            pair_dir = diff_root / f"{left}__vs__{right}"
            pair_dir.mkdir(parents=True, exist_ok=True)
            both, only_left, only_right = graph_diff(left_graph, right_graph)
            both.serialize(destination=pair_dir / "both.ttl", format="turtle")
            only_left.serialize(
                destination=pair_dir / f"only_{left}.ttl", format="turtle"
            )
            only_right.serialize(
                destination=pair_dir / f"only_{right}.ttl", format="turtle"
            )
    for platform in matrix:
        matrix[platform][platform] = True
    return matrix


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    shapes_file = resolve_path(args.shapes_file)
    data_file = resolve_path(args.data_file)
    out_dir = resolve_path(args.out_dir)
    compiled_out_dir = resolve_path(args.compiled_out_dir)

    if not shapes_file.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes_file}")
    if not data_file.exists():
        raise FileNotFoundError(f"Data file not found: {data_file}")

    reports_dir = out_dir / "reports"
    diffs_dir = out_dir / "pairwise_diffs"
    shacl_ir_file = out_dir / "shapes.ir"
    reports_dir.mkdir(parents=True, exist_ok=True)
    diffs_dir.mkdir(parents=True, exist_ok=True)

    compiled_binary = build_artifacts(
        shapes_file=shapes_file,
        shacl_ir_file=shacl_ir_file,
        compiled_out_dir=compiled_out_dir,
        compiled_bin_name=args.compiled_bin_name,
        skip_build=args.skip_build,
        timeout_seconds=args.timeout_seconds,
    )
    LOGGER.info("Using compiled binary: %s", compiled_binary)

    reports: Dict[str, rdflib.Graph] = {}
    reports["pyshacl"] = run_pyshacl_report(
        shapes_file=shapes_file, data_file=data_file
    )
    reports["topquadrant"] = run_topquadrant_report(
        shapes_file=shapes_file, data_file=data_file
    )
    reports["shifty"] = run_shifty_report(
        shapes_file=shapes_file,
        data_file=data_file,
        timeout_seconds=args.timeout_seconds,
    )
    reports["shifty-pre"] = run_shifty_pre_report(
        shacl_ir_file=shacl_ir_file,
        data_file=data_file,
        timeout_seconds=args.timeout_seconds,
    )
    reports["shifty-compile"] = run_shifty_compile_report(
        compiled_binary=compiled_binary,
        data_file=data_file,
        timeout_seconds=args.timeout_seconds,
    )

    for platform in PLATFORMS:
        if platform not in reports:
            raise RuntimeError(f"Missing report for platform: {platform}")
        report_path = reports_dir / f"{platform}.ttl"
        write_graph(reports[platform], report_path)
        LOGGER.info("Wrote %s report: %s", platform, report_path)

    matrix = compare_pairwise(reports, diffs_dir)
    matrix_path = out_dir / "pairwise_isomorphism.json"
    matrix_path.write_text(
        json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8"
    )

    LOGGER.info("Pairwise isomorphism matrix:")
    for left in sorted(matrix.keys()):
        for right in sorted(matrix[left].keys()):
            LOGGER.info("  %s vs %s: %s", left, right, matrix[left][right])

    mismatches: list[Tuple[str, str]] = []
    for left, right in itertools.combinations(sorted(reports.keys()), 2):
        if not matrix[left][right]:
            mismatches.append((left, right))

    if mismatches:
        LOGGER.error("Found %d non-isomorphic pair(s):", len(mismatches))
        for left, right in mismatches:
            LOGGER.error("  %s vs %s", left, right)
        LOGGER.error("Diff artifacts written to: %s", diffs_dir)
        return 1

    LOGGER.info("All validator report pairs are isomorphic.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
