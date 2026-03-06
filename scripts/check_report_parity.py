#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "brick-tq-shacl>=0.4.1",
#   "ontoenv==0.5.1",
#   "pyshacl",
#   "rdflib",
# ]
# ///
"""Check SHACL report parity across validation platforms.

This script mirrors the platform setup in run_benchmarks.py and verifies whether:
- pyshacl
- topquadrant
- shifty
- shifty+pre
- shifty+compile

produce equivalent validation reports under canonical SHACL-report comparison.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import itertools
import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parent.parent
PLATFORMS = ("pyshacl", "topquadrant", "shifty", "shifty+pre", "shifty+compile")
ONTOENV_VERSION = "0.5.1"
RUNTIME_DEPS: List[Tuple[str, str]] = [
    ("rdflib", "rdflib"),
    ("pyshacl", "pyshacl"),
    ("brick_tq_shacl", "brick-tq-shacl>=0.4.1"),
    ("ontoenv", "ontoenv==0.5.1"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run pyshacl/topquadrant/shifty/shifty+pre/shifty+compile and "
            "check whether their validation reports are pairwise equivalent."
        )
    )
    parser.add_argument(
        "--shapes-file",
        required=True,
        help="SHACL shapes file used for all validators.",
    )
    parser.add_argument(
        "--data-file",
        required=True,
        help="Data graph file to validate.",
    )
    parser.add_argument(
        "--out-dir",
        default="integration/artifacts/report-parity-bench",
        help="Output directory for reports, matrices, and diffs.",
    )
    parser.add_argument(
        "--compiled-out-dir",
        default="target/compiled-shacl-benchmark",
        help="Output directory for the generated shifty+compile executable.",
    )
    parser.add_argument(
        "--compiled-bin-name",
        default="shacl-compiled-benchmark",
        help="Binary name for the generated shifty+compile executable.",
    )
    parser.add_argument(
        "--shacl-ir-file",
        default="shapes.ir",
        help="Path for generated SHACL-IR cache used by shifty+pre.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip cargo build / generate-ir / compile steps.",
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
    logging.basicConfig(level=getattr(logging, level), format="%(levelname)s %(message)s")


def resolve_path(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate.resolve()


def ensure_runtime_dependencies() -> None:
    missing_specs: List[str] = []
    missing_imports: List[str] = []
    for import_name, uv_spec in RUNTIME_DEPS:
        if importlib.util.find_spec(import_name) is None:
            missing_imports.append(import_name)
            missing_specs.append(uv_spec)
            continue

        if import_name == "ontoenv":
            try:
                installed = importlib.metadata.version("ontoenv")
            except importlib.metadata.PackageNotFoundError:
                installed = None
            if installed != ONTOENV_VERSION:
                missing_imports.append(
                    f"ontoenv=={ONTOENV_VERSION} (found {installed or 'missing'})"
                )
                missing_specs.append(f"ontoenv=={ONTOENV_VERSION}")

    if not missing_specs:
        return

    if os.environ.get("SHFTY_PARITY_DEPS_BOOTSTRAPPED") == "1":
        raise RuntimeError(
            "Required Python deps are still missing after uv bootstrap: "
            + ", ".join(sorted(missing_imports))
        )

    env = os.environ.copy()
    env["SHFTY_PARITY_DEPS_BOOTSTRAPPED"] = "1"
    env.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")

    command: List[str] = ["uv", "run"]
    for spec in missing_specs:
        command.extend(["--with", spec])
    command.extend(["python3", str(Path(__file__).resolve()), *sys.argv[1:]])

    LOGGER.info(
        "Missing Python deps (%s); re-running via uv bootstrap.",
        ", ".join(sorted(missing_imports)),
    )
    result = subprocess.run(command, cwd=REPO_ROOT, env=env)
    raise SystemExit(result.returncode)


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
    import rdflib

    for rdf_format in ("turtle", "ntriples", "xml"):
        graph = rdflib.Graph()
        try:
            graph.parse(data=report_text, format=rdf_format)
            return graph
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError(f"Could not parse {label} report in supported RDF formats")


def parse_graph_file(path: Path) -> rdflib.Graph:
    import rdflib
    from rdflib.util import guess_format

    rdf_format = guess_format(str(path)) or "turtle"
    return rdflib.Graph().parse(path, format=rdf_format)


def write_graph(graph: rdflib.Graph, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    graph.serialize(destination=destination, format="turtle")


def create_temporary_ontoenv():
    from ontoenv import OntoEnv

    try:
        return OntoEnv(temporary=True, search_directories=[])
    except TypeError:
        # Compatibility fallback for older pre-0.5 APIs.
        return OntoEnv(temporary=True, no_search=True)


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


def ensure_artifacts(
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
            "shifty compile",
            timeout_seconds,
        )

    if not shacl_ir_file.exists():
        raise FileNotFoundError(
            f"Missing SHACL-IR file for shifty+pre: {shacl_ir_file} "
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
        "shifty+pre validate",
        timeout_seconds,
    )
    return parse_report_text(result.stdout, "shifty+pre")


def run_shifty_compile_report(
    *,
    compiled_binary: Path,
    data_file: Path,
    timeout_seconds: float | None,
) -> rdflib.Graph:
    result = run_checked(
        [str(compiled_binary), str(data_file)],
        "shifty+compile validate",
        timeout_seconds,
    )
    return parse_report_text(result.stdout, "shifty+compile")


def run_pyshacl_report(*, shapes_file: Path, data_file: Path) -> rdflib.Graph:
    import pyshacl

    env = create_temporary_ontoenv()
    model_graph = parse_graph_file(data_file)
    ontology_id = env.add(str(shapes_file))
    shapes_graph = env.get_graph(ontology_id)
    env.import_dependencies(shapes_graph)
    model_graph += shapes_graph
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
    import rdflib
    from brick_tq_shacl import infer as tq_infer
    from brick_tq_shacl import validate as tq_validate

    env = create_temporary_ontoenv()
    model_graph = parse_graph_file(data_file)
    ontology_id = env.add(str(shapes_file))
    shapes_graph = env.get_graph(ontology_id)
    env.import_dependencies(shapes_graph)
    model_graph += shapes_graph
    inferred_graph = tq_infer(model_graph, shapes_graph)
    _, report_graph, report_text = tq_validate(inferred_graph, shapes_graph)
    if isinstance(report_graph, rdflib.Graph):
        return report_graph
    return parse_report_text(report_text, "topquadrant")


def compare_pairwise(
    reports: Dict[str, rdflib.Graph], diff_root: Path
) -> Dict[str, Dict[str, bool]]:
    import rdflib

    from compare_validation_reports import (
        canonical_report_signature,
        graph_report_signature_counter,
        summarize_signature,
        stable_json,
    )
    from rdflib.namespace import RDF, SH
    from rdflib.term import BNode, Identifier

    matrix: Dict[str, Dict[str, bool]] = {
        platform: {} for platform in sorted(reports.keys())
    }

    def report_nodes_by_signature(graph: rdflib.Graph) -> Dict[str, List[Identifier]]:
        grouped: Dict[str, List[Identifier]] = {}
        for report_node in set(graph.subjects(RDF.type, SH.ValidationReport)):
            signature = stable_json(canonical_report_signature(graph, report_node))
            grouped.setdefault(signature, []).append(report_node)
        return grouped

    def add_reachable_subgraph(
        source_graph: rdflib.Graph,
        source_node: Identifier,
        destination_graph: rdflib.Graph,
        visited: set[Identifier],
    ) -> None:
        if source_node in visited:
            return
        visited.add(source_node)
        for predicate, obj in source_graph.predicate_objects(source_node):
            destination_graph.add((source_node, predicate, obj))
            if isinstance(obj, BNode):
                add_reachable_subgraph(source_graph, obj, destination_graph, visited)

    def build_diff_graph(
        source_graph: rdflib.Graph,
        signature_counter: Dict[str, int],
    ) -> rdflib.Graph:
        diff_graph = rdflib.Graph()
        for prefix, namespace in source_graph.namespaces():
            diff_graph.bind(prefix, namespace)

        grouped_nodes = report_nodes_by_signature(source_graph)
        for signature, count in signature_counter.items():
            nodes = grouped_nodes.get(signature, [])
            for report_node in nodes[:count]:
                add_reachable_subgraph(
                    source_graph,
                    report_node,
                    diff_graph,
                    visited=set(),
                )
        return diff_graph

    for left, right in itertools.combinations(sorted(reports.keys()), 2):
        left_counter = graph_report_signature_counter(reports[left])
        right_counter = graph_report_signature_counter(reports[right])
        equal = left_counter == right_counter
        matrix[left][right] = equal
        matrix[right][left] = equal
        matrix[left][left] = True
        matrix[right][right] = True
        if not equal:
            pair_dir = diff_root / f"{left}__vs__{right}"
            pair_dir.mkdir(parents=True, exist_ok=True)
            left_name = sanitize_name(left)
            right_name = sanitize_name(right)

            missing_from_right = left_counter - right_counter
            missing_from_left = right_counter - left_counter

            def counter_entries(counter: Dict[str, int]) -> List[Dict[str, object]]:
                entries: List[Dict[str, object]] = []
                for signature, count in sorted(counter.items()):
                    entries.append(
                        {
                            "count": count,
                            "summary": summarize_signature(signature),
                            "signature": json.loads(signature),
                        }
                    )
                return entries

            only_left_path = pair_dir / f"only_{left_name}.ttl"
            only_right_path = pair_dir / f"only_{right_name}.ttl"
            build_diff_graph(reports[left], missing_from_right).serialize(
                destination=only_left_path,
                format="turtle",
            )
            build_diff_graph(reports[right], missing_from_left).serialize(
                destination=only_right_path,
                format="turtle",
            )

            comparison = {
                "algorithm": "canonical-validation-report-signatures",
                "left": left,
                "right": right,
                "equal": False,
                "left_total_signatures": sum(left_counter.values()),
                "right_total_signatures": sum(right_counter.values()),
                "only_left_signature_count": sum(missing_from_right.values()),
                "only_right_signature_count": sum(missing_from_left.values()),
                "only_left_file": str(only_left_path),
                "only_right_file": str(only_right_path),
            }
            comparison_path = pair_dir / "comparison.json"
            comparison_path.write_text(
                json.dumps(comparison, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            normalized_left_path = pair_dir / f"normalized_{left_name}.json"
            normalized_right_path = pair_dir / f"normalized_{right_name}.json"
            normalized_left_path.write_text(
                json.dumps(counter_entries(left_counter), indent=2, sort_keys=True),
                encoding="utf-8",
            )
            normalized_right_path.write_text(
                json.dumps(counter_entries(right_counter), indent=2, sort_keys=True),
                encoding="utf-8",
            )

            summary_lines = [
                f"Pair: {left} vs {right}",
                "Algorithm: canonical-validation-report-signatures",
                "",
                f"Total signatures: {left}={sum(left_counter.values())}, {right}={sum(right_counter.values())}",
                f"Only in {left}: {sum(missing_from_right.values())}",
                f"Only in {right}: {sum(missing_from_left.values())}",
                "",
                f"Structured diff files:",
                f"- {comparison_path}",
                f"- {only_left_path}",
                f"- {only_right_path}",
                f"- {normalized_left_path}",
                f"- {normalized_right_path}",
            ]
            (pair_dir / "diff_summary.txt").write_text(
                "\n".join(summary_lines) + "\n",
                encoding="utf-8",
            )
    for platform in matrix:
        matrix[platform][platform] = True
    return matrix


def sanitize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name)


def run_model(
    *,
    shapes_file: Path,
    data_file: Path,
    shacl_ir_file: Path,
    compiled_binary: Path,
    timeout_seconds: float | None,
    model_out_dir: Path,
) -> Tuple[bool, Dict[str, Dict[str, bool]]]:
    reports_dir = model_out_dir / "reports"
    diffs_dir = model_out_dir / "pairwise_diffs"
    reports_dir.mkdir(parents=True, exist_ok=True)
    diffs_dir.mkdir(parents=True, exist_ok=True)

    reports: Dict[str, rdflib.Graph] = {}
    reports["pyshacl"] = run_pyshacl_report(shapes_file=shapes_file, data_file=data_file)
    reports["topquadrant"] = run_topquadrant_report(
        shapes_file=shapes_file, data_file=data_file
    )
    reports["shifty"] = run_shifty_report(
        shapes_file=shapes_file,
        data_file=data_file,
        timeout_seconds=timeout_seconds,
    )
    reports["shifty+pre"] = run_shifty_pre_report(
        shacl_ir_file=shacl_ir_file,
        data_file=data_file,
        timeout_seconds=timeout_seconds,
    )
    reports["shifty+compile"] = run_shifty_compile_report(
        compiled_binary=compiled_binary,
        data_file=data_file,
        timeout_seconds=timeout_seconds,
    )

    for platform in PLATFORMS:
        report_path = reports_dir / f"{sanitize_name(platform)}.ttl"
        write_graph(reports[platform], report_path)

    matrix = compare_pairwise(reports, diffs_dir)
    all_equal = all(
        matrix[left][right]
        for left, right in itertools.combinations(sorted(reports.keys()), 2)
    )
    return all_equal, matrix


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    ensure_runtime_dependencies()

    shapes_file = resolve_path(args.shapes_file)
    data_file = resolve_path(args.data_file)
    out_dir = resolve_path(args.out_dir)
    compiled_out_dir = resolve_path(args.compiled_out_dir)
    shacl_ir_file = resolve_path(args.shacl_ir_file)

    if not shapes_file.exists():
        raise FileNotFoundError(f"Shapes file not found: {shapes_file}")
    if not data_file.exists():
        raise FileNotFoundError(f"Data file not found: {data_file}")

    out_dir.mkdir(parents=True, exist_ok=True)

    compiled_binary = ensure_artifacts(
        shapes_file=shapes_file,
        shacl_ir_file=shacl_ir_file,
        compiled_out_dir=compiled_out_dir,
        compiled_bin_name=args.compiled_bin_name,
        skip_build=args.skip_build,
        timeout_seconds=args.timeout_seconds,
    )
    LOGGER.info("Using compiled binary: %s", compiled_binary)

    summary: Dict[str, object] = {
        "shapes_file": str(shapes_file),
        "data_file": str(data_file),
        "compiled_binary": str(compiled_binary),
    }

    all_equal, matrix = run_model(
        shapes_file=shapes_file,
        data_file=data_file,
        shacl_ir_file=shacl_ir_file,
        compiled_binary=compiled_binary,
        timeout_seconds=args.timeout_seconds,
        model_out_dir=out_dir,
    )
    matrix_path = out_dir / "pairwise_isomorphism.json"
    matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True), encoding="utf-8")

    summary["all_equal"] = all_equal
    summary["matrix_path"] = str(matrix_path)
    summary["artifact_dir"] = str(out_dir)

    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    LOGGER.info("Wrote parity summary: %s", summary_path)

    if not all_equal:
        LOGGER.error("Report mismatches found for data file: %s", data_file)
        return 1

    LOGGER.info("All validator reports are pairwise equivalent for: %s", data_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
