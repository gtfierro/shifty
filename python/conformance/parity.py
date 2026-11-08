"""Compare native CLI and Python file inputs against the pinned W3C reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import warnings
from collections import Counter
from pathlib import Path

import rdflib
from rdflib import Graph
from rdflib.compare import isomorphic
from rdflib.namespace import RDF, SH

import shifty

from .runner import (
    DATA_IDENTITY,
    DEFAULT_SUITE,
    MF,
    OPTIONS,
    ROOT,
    SHT,
    Documents,
    discover,
    is_validation_failure,
    local_path,
    metadata,
    report_graph,
    verify_fixtures,
)


def normalize(graph, root, messages):
    graph = report_graph(graph, root, messages)
    # Separate processes assign different labels to data blank nodes. Preserve
    # their node kind and sharing, but compare those labels by isomorphism.
    graph.remove((None, DATA_IDENTITY, None))
    return graph


def compare_case(case, documents, cli: Path, timeout: float):
    row = {
        "test": case.identifier,
        "status": case.status,
        "outcome": "error",
        "info": "",
    }
    if case.status == "rejected":
        row.update(outcome="untested", info="Rejected upstream test")
        return row
    try:
        graph = case.graph
        if (case.entry, RDF.type, SHT.Validate) not in graph:
            raise ValueError("Unknown test type")
        action = graph.value(case.entry, MF.action, any=False)
        expected = graph.value(case.entry, MF.result, any=False)
        if action is None or expected is None:
            raise ValueError("Test has no unique mf:action or mf:result")
        data = local_path(graph.value(action, SHT.dataGraph, any=False), documents.root)
        shapes = local_path(
            graph.value(action, SHT.shapesGraph, any=False), documents.root
        )
        args = (data,) if data == shapes else (data, shapes)
        python_failure = None
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", shifty.ShaclDiagnosticWarning)
                conforms, python_report, _ = shifty.validate(*args, **OPTIONS)
        except ValueError as error:
            if not is_validation_failure(str(error)):
                raise
            python_failure = str(error)

        command = [
            str(cli),
            "validate",
            "--shapes",
            str(shapes),
            "--report",
            "--no-infer",
            "--graph-mode",
            "data",
            "--on-unsupported",
            "error",
        ]
        if data != shapes:
            command.extend(["--data", str(data)])
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout
        )
        cli_failure = completed.stderr.strip().removeprefix("error: ")
        if completed.returncode == 1 and is_validation_failure(cli_failure):
            if expected == SHT.Failure and python_failure is not None:
                row.update(
                    outcome="passed",
                    info="Both interfaces diagnosed the expected validation failure",
                )
            else:
                row.update(outcome="failed", info="Unexpected CLI validation failure")
            return row
        if completed.returncode != 0:
            raise RuntimeError(
                f"CLI exited {completed.returncode}: {completed.stderr.strip()}"
            )
        if completed.stderr.strip():
            raise RuntimeError(f"CLI diagnostic: {completed.stderr.strip()}")
        if python_failure is not None or expected == SHT.Failure:
            row.update(
                outcome="failed",
                info="Interfaces did not both reject the expected validation failure",
            )
            return row
        previous = rdflib.NORMALIZE_LITERALS
        try:
            rdflib.NORMALIZE_LITERALS = False
            cli_report = Graph().parse(data=completed.stdout, format="turtle")
        finally:
            rdflib.NORMALIZE_LITERALS = previous
        python_roots = list(python_report.subjects(RDF.type, SH.ValidationReport))
        cli_roots = list(cli_report.subjects(RDF.type, SH.ValidationReport))
        if len(python_roots) != 1 or len(cli_roots) != 1:
            raise ValueError("Each interface must emit exactly one validation report")
        messages = {
            message
            for subject in (expected, *graph.objects(expected, SH.result))
            for message in graph.objects(subject, SH.resultMessage)
        }
        wanted = normalize(graph, expected, messages)
        python_normalized = normalize(python_report, python_roots[0], messages)
        cli_normalized = normalize(cli_report, cli_roots[0], messages)
        expected_conforms = graph.value(expected, SH.conforms, any=False)
        if expected_conforms is None:
            raise ValueError("Expected report has no conformance boolean")
        if conforms != expected_conforms.toPython() or not isomorphic(
            wanted, python_normalized
        ):
            row.update(
                outcome="failed", info="Python file-input report differs from expected"
            )
        elif not isomorphic(wanted, cli_normalized):
            row.update(outcome="failed", info="CLI report differs from expected")
        else:
            row["outcome"] = "passed"
    except Exception as error:
        row["info"] = f"{type(error).__name__}: {error}"
    return row


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cli", required=True, type=Path, help="Locally built shifty binary"
    )
    parser.add_argument("--test", help="One exact urn:x-shacl-test:/... identifier")
    parser.add_argument("--output", type=Path, default=ROOT / "target/interface-parity")
    parser.add_argument(
        "--timeout", type=float, default=30, help="Seconds allowed per CLI invocation"
    )
    args = parser.parse_args(argv)
    try:
        if args.timeout <= 0:
            raise ValueError("Timeout must be positive")
        cli = args.cli.resolve(strict=True)
        pin = verify_fixtures(DEFAULT_SUITE)
        documents = Documents(DEFAULT_SUITE / "tests")
        cases = discover(
            documents, DEFAULT_SUITE / "tests/manifest.ttl", include_unlisted=True
        )
        if args.test:
            cases = [case for case in cases if case.identifier == args.test]
        if not cases:
            raise ValueError("No tests match the selection")
        run = metadata(pin)
        version = subprocess.run(
            [str(cli), "version"],
            capture_output=True,
            text=True,
            check=True,
            timeout=args.timeout,
        ).stdout.strip()
        if version != run["shifty_version"]:
            raise ValueError(
                f"CLI version {version} differs from Python {run['shifty_version']}"
            )
        run.update(
            cli_sha256=hashlib.sha256(cli.read_bytes()).hexdigest(),
            cli_version=version,
            comparison="RDF graph isomorphism across independently parsed file inputs",
        )
        results = [compare_case(case, documents, cli, args.timeout) for case in cases]
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "parity.json").write_text(
            json.dumps({"metadata": run, "results": results}, indent=2) + "\n"
        )
        for status in ("approved", "proposed", "rejected"):
            print(
                f"{status}: {dict(Counter(row['outcome'] for row in results if row['status'] == status))}"
            )
        for row in results:
            if row["outcome"] not in ("passed", "untested"):
                print(f"{row['outcome'].upper()} {row['test']}: {row['info']}")
        if any(row["outcome"] == "error" for row in results):
            return 2
        return int(any(row["outcome"] == "failed" for row in results))
    except Exception as error:
        print(f"Parity runner error: {type(error).__name__}: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
