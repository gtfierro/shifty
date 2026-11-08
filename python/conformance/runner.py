"""Manifest discovery, W3C report comparison, and reproducible EARL output.

Fixtures are parsed with document IRIs and without RDFLib literal normalization.
Only manifest actions provide engine inputs; scanning for shapes or expected
booleans in the fixture tree would accidentally execute helper files as tests.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import subprocess
import warnings
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

import rdflib
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.compare import graph_diff, isomorphic, to_isomorphic
from rdflib.namespace import RDF, RDFS, SH, XSD

import shifty

MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
SHT = Namespace("http://www.w3.org/ns/shacl-test#")
EARL = Namespace("http://www.w3.org/ns/earl#")
DOAP = Namespace("http://usefulinc.com/ns/doap#")
PROV = Namespace("http://www.w3.org/ns/prov#")
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SUITE = ROOT / "testdata/w3c-shacl"
TEST_BASE = "urn:x-shacl-test:/"
OPTIONS = dict(infer=False, graph_mode="data", on_unsupported="error")
DATA_IDENTITY = URIRef("urn:shifty-test-data:identity")
EXPECTED_FAILURES = (
    "SHACL-SPARQL queries must not contain MINUS",
    "SHACL-SPARQL queries must not contain VALUES",
    "SHACL-SPARQL queries must not contain SERVICE",
    "SHACL-SPARQL must not assign potentially pre-bound variable ",
    "SHACL-SPARQL subquery does not project potentially pre-bound variable(s): ",
)
FIELDS = {
    SH.conforms,
    SH.focusNode,
    SH.resultSeverity,
    SH.sourceConstraint,
    SH.sourceConstraintComponent,
    SH.sourceShape,
    SH.value,
}
PATH_FIELDS = {
    RDF.first,
    RDF.rest,
    SH.inversePath,
    SH.alternativePath,
    SH.zeroOrMorePath,
    SH.oneOrMorePath,
    SH.zeroOrOnePath,
}


def local_path(iri: URIRef, root: Path) -> Path:
    """Resolve local documents only; test execution never fetches graph URLs."""
    parts = urlsplit(str(iri))
    if parts.scheme != "file" or parts.netloc or parts.query or parts.fragment:
        raise ValueError(f"Expected a local document IRI, got {iri}")
    path = Path(unquote(parts.path)).resolve()
    path.relative_to(root.resolve())
    return path


class Documents:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.cache = {}

    def load(self, iri: URIRef) -> Graph:
        path = local_path(iri, self.root)
        if path not in self.cache:
            # Literal lexical forms are part of RDF term identity. Normalizing
            # "01"^^xsd:integer here could hide an engine bug or alter a test.
            previous = rdflib.NORMALIZE_LITERALS
            try:
                rdflib.NORMALIZE_LITERALS = False
                graph = Graph(base=path.as_uri()).parse(
                    path, format="turtle", publicID=path.as_uri()
                )
            finally:
                rdflib.NORMALIZE_LITERALS = previous
            self.cache[path] = graph
        return self.cache[path]


@dataclass
class Case:
    identifier: str
    entry: URIRef
    document: URIRef
    status: str
    graph: Graph


def manifest_entries(graph: Graph, head):
    seen = set()
    while head != RDF.nil:
        if head in seen:
            raise ValueError("Cyclic mf:entries list")
        seen.add(head)
        first = list(graph.objects(head, RDF.first))
        rest = list(graph.objects(head, RDF.rest))
        if len(first) != 1 or len(rest) != 1:
            raise ValueError(
                "mf:entries list requires one rdf:first and rdf:rest per cell"
            )
        yield first[0]
        head = rest[0]


def discover(
    documents: Documents, manifest: Path, include_unlisted=False
) -> list[Case]:
    seen_documents, seen_entries = set(), set()
    cases = []

    def visit(document):
        if document in seen_documents:
            return
        seen_documents.add(document)
        graph = documents.load(document)
        manifests = list(graph.subjects(RDF.type, MF.Manifest))
        if not manifests:
            raise ValueError(f"No mf:Manifest in {document}")
        for subject in manifests:
            for included in sorted(graph.objects(subject, MF.include), key=str):
                visit(included)
            for entries in graph.objects(subject, MF.entries):
                for entry in manifest_entries(graph, entries):
                    if not isinstance(entry, URIRef) or entry in seen_entries:
                        raise ValueError(
                            f"Invalid or duplicate test identifier: {entry}"
                        )
                    seen_entries.add(entry)
                    # The W3C reports use this base for document-relative IDs.
                    path = local_path(entry, documents.root)
                    identifier = TEST_BASE + path.relative_to(documents.root).as_posix()
                    statuses = list(graph.objects(entry, MF.status))
                    if len(statuses) != 1 or statuses[0] not in {
                        SHT.approved,
                        SHT.proposed,
                        SHT.rejected,
                    }:
                        raise ValueError(f"Missing or unknown mf:status for {entry}")
                    cases.append(
                        Case(
                            identifier,
                            entry,
                            document,
                            str(statuses[0])[len(SHT) :],
                            graph,
                        )
                    )

    visit(URIRef(manifest.resolve().as_uri()))
    if include_unlisted:
        for path in sorted(documents.root.rglob("*.ttl")):
            iri = URIRef(path.as_uri())
            graph = documents.load(iri)
            if any(graph.triples((None, MF.entries, None))):
                visit(iri)
    if not cases:
        raise ValueError("Manifest contains no test entries")
    return sorted(cases, key=lambda case: case.identifier)


def report_graph(graph: Graph, root, expected_messages: set) -> Graph:
    """Project W3C comparison fields and clone each resultPath occurrence.

    Report/result identities are replaceable. Data blank nodes in focus/value
    positions are caller-owned identities, so anchor them before graph
    isomorphism. Anonymous source shapes remain subject to one consistent
    isomorphism mapping, never a per-result wildcard.
    """
    output = Graph()

    def path(node, active):
        if not isinstance(node, BNode):
            return node
        if node in active:
            raise ValueError("Cyclic resultPath in validation report")
        cloned = BNode()
        for predicate, obj in graph.predicate_objects(node):
            if predicate in PATH_FIELDS:
                output.add((cloned, predicate, path(obj, active | {node})))
        return cloned

    def copy_fields(source, dest, kind):
        output.add((dest, RDF.type, kind))
        # Check rather than invent required types. Subclasses are permitted by
        # the W3C preprocessing, but unrelated/missing types are not.
        if not any(
            kind in set(graph.transitive_objects(value, RDFS.subClassOf))
            for value in graph.objects(source, RDF.type)
        ):
            raise ValueError(f"Missing {kind} type on {source}")
        for predicate, obj in graph.predicate_objects(source):
            if predicate in FIELDS:
                output.add((dest, predicate, obj))
                if predicate in (SH.focusNode, SH.value) and isinstance(obj, BNode):
                    # Preserve the caller's data identity while keeping its RDF
                    # node kind. Skolemizing to an IRI could match a real IRI.
                    output.add((obj, DATA_IDENTITY, Literal(str(obj))))
            elif predicate == SH.resultPath:
                output.add((dest, predicate, path(obj, set())))
            elif predicate == SH.resultMessage and obj in expected_messages:
                output.add((dest, predicate, obj))

    report = BNode()
    copy_fields(root, report, SH.ValidationReport)
    for result in graph.objects(root, SH.result):
        dest = BNode()
        output.add((report, SH.result, dest))
        copy_fields(result, dest, SH.ValidationResult)
    return output


@dataclass
class Result:
    test: str
    status: str
    outcome: str
    info: str = ""
    algebra: str = "not-run"


def run_case(case: Case, documents: Documents, output: Path) -> Result:
    result = Result(case.identifier, case.status, "error")
    if case.status == "rejected":
        result.outcome, result.info = "untested", "Rejected upstream test"
        return result
    try:
        graph = case.graph
        if (case.entry, RDF.type, SHT.Validate) not in graph:
            raise ValueError(
                f"Unknown test type: {list(graph.objects(case.entry, RDF.type))}"
            )
        action = graph.value(case.entry, MF.action, any=False)
        expected = graph.value(case.entry, MF.result, any=False)
        if action is None or expected is None:
            raise ValueError("Test has no unique mf:action or mf:result")
        data = documents.load(graph.value(action, SHT.dataGraph, any=False))
        shapes = documents.load(graph.value(action, SHT.shapesGraph, any=False))
        args = (data,) if data is shapes else (data, shapes)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", shifty.ShaclDiagnosticWarning)
                conforms, actual, _ = shifty.validate(*args, **OPTIONS)
        except ValueError as error:
            message = str(error)
            # The Python API represents admission errors as ValueError. Only
            # diagnosed SHACL-SPARQL restrictions qualify as expected failures;
            # Turtle errors, unsupported features, and arbitrary errors do not.
            prefix = "invalid shapes graph: [error] "
            diagnostic = message.removeprefix(prefix).removeprefix(
                "invalid SPARQL in validator: "
            )
            if message.startswith(prefix) and diagnostic.startswith(EXPECTED_FAILURES):
                result.outcome = "passed" if expected == SHT.Failure else "failed"
                result.info = message
                return result
            raise
        if expected == SHT.Failure:
            result.outcome, result.info = (
                "failed",
                "Expected validation failure, got a report",
            )
            return result
        expected_conforms = graph.value(expected, SH.conforms, any=False)
        if expected_conforms not in (Literal(True), Literal(False)):
            raise ValueError("Expected report has no unique boolean sh:conforms")
        roots = {
            subject
            for subject, kind in actual.subject_objects(RDF.type)
            if SH.ValidationReport in actual.transitive_objects(kind, RDFS.subClassOf)
        }
        if len(roots) != 1:
            raise ValueError(f"Expected one actual report, got {len(roots)}")
        messages = {
            message
            for subject in (expected, *graph.objects(expected, SH.result))
            for message in graph.objects(subject, SH.resultMessage)
        }
        wanted = report_graph(graph, expected, messages)
        got = report_graph(actual, next(iter(roots)), messages)
        if isomorphic(wanted, got) and conforms == expected_conforms.toPython():
            result.outcome = "passed"
        else:
            result.outcome = (
                "partial" if conforms == expected_conforms.toPython() else "failed"
            )
            _, missing, extra = graph_diff(to_isomorphic(wanted), to_isomorphic(got))
            result.info = (
                f"Expected conforms={expected_conforms}, actual={conforms}; "
                f"report difference: {len(missing)} missing, {len(extra)} extra triples"
            )
            directory = output / "cases" / case.identifier.removeprefix(TEST_BASE)
            directory.mkdir(parents=True, exist_ok=True)
            wanted.serialize(directory / "expected.ttl", format="turtle")
            got.serialize(directory / "actual.ttl", format="turtle")
            missing.serialize(directory / "missing.ttl", format="turtle")
            extra.serialize(directory / "extra.ttl", format="turtle")
        try:
            algebra = shifty.validate_algebra(*args, **OPTIONS)
            result.algebra = (
                "passed"
                if algebra.conforms == expected_conforms.toPython()
                and not algebra.diagnostics
                else "failed"
            )
        except Exception as error:
            result.algebra = f"error: {error}"
    except Exception as error:
        result.info = f"{type(error).__name__}: {error}"
    return result


def verify_fixtures(suite: Path) -> dict:
    pin = json.loads((suite / "upstream.json").read_text())
    expected = pin["sha256"]
    actual = {
        path.relative_to(suite).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted((suite / "tests").rglob("*"))
        if path.is_file()
    }
    if expected != actual:
        raise ValueError("Upstream fixtures differ from upstream.json checksums")
    return pin


def metadata(suite: dict) -> dict:
    source_hash = hashlib.sha256()
    for path in sorted(
        [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]
        + list((ROOT / "crates").rglob("*.rs"))
        + list((ROOT / "crates").rglob("Cargo.toml"))
        + list((ROOT / "python/src").rglob("*.rs"))
        + list((ROOT / "python/shifty").glob("*.py"))
        + list(Path(__file__).parent.glob("*.py"))
        + [
            ROOT / "python/Cargo.toml",
            ROOT / "python/pyproject.toml",
            ROOT / "python/uv.lock",
        ]
    ):
        source_hash.update(path.relative_to(ROOT).as_posix().encode())
        source_hash.update(b"\0")
        source_hash.update(path.read_bytes())
        source_hash.update(b"\0")
    extension = Path(importlib.import_module("shifty._shifty").__file__)
    return {
        "shifty_version": importlib.metadata.version("pyshifty"),
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "working_tree": subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=ROOT, text=True
        ).splitlines(),
        "tracked_diff_sha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)
        ).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "source_tree_sha256": source_hash.hexdigest(),
        "extension_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
        "suite_revision": suite["revision"],
        "date": datetime.now(timezone.utc).isoformat(),
        "options": OPTIONS,
    }


def earl_report(results: list[Result], run: dict, assertor: str) -> Graph:
    graph = Graph()
    for prefix, ns in (("earl", EARL), ("doap", DOAP), ("prov", PROV), ("sht", SHT)):
        graph.bind(prefix, ns)
    subject = URIRef("https://github.com/gtfierro/shifty")
    agent = URIRef(assertor)
    activity = BNode()
    graph.add((agent, RDF.type, EARL.Assertor))
    graph.add((subject, RDF.type, DOAP.Project))
    graph.add((subject, RDF.type, EARL.Software))
    graph.add((subject, RDF.type, EARL.TestSubject))
    graph.add((subject, DOAP.name, Literal("Shifty")))
    graph.add((subject, DOAP.developer, URIRef("https://github.com/gtfierro")))
    release = BNode()
    graph.add((subject, DOAP.release, release))
    graph.add((release, RDF.type, DOAP.Version))
    graph.add((release, DOAP.revision, Literal(run["shifty_version"])))
    graph.add((activity, RDF.type, PROV.Activity))
    graph.add(
        (activity, PROV.used, URIRef(f"{subject}/commit/{run['source_revision']}"))
    )
    graph.add(
        (
            activity,
            PROV.used,
            URIRef(
                f"https://github.com/w3c/data-shapes/commit/{run['suite_revision']}"
            ),
        )
    )
    graph.add((activity, PROV.endedAtTime, Literal(run["date"], datatype=XSD.dateTime)))
    graph.add((activity, RDFS.comment, Literal(json.dumps(run, sort_keys=True))))
    outcomes = {
        "passed": EARL.passed,
        "partial": SHT.partial,
        "failed": EARL.failed,
        "error": EARL.cantTell,
        "untested": EARL.untested,
    }
    for row in results:
        assertion, result = BNode(), BNode()
        for predicate, obj in (
            (RDF.type, EARL.Assertion),
            (EARL.assertedBy, agent),
            (EARL.mode, EARL.automatic),
            (EARL.subject, subject),
            (EARL.test, URIRef(row.test)),
            (EARL.result, result),
            (PROV.wasGeneratedBy, activity),
        ):
            graph.add((assertion, predicate, obj))
        graph.add((result, RDF.type, EARL.TestResult))
        graph.add((result, EARL.outcome, outcomes[row.outcome]))
        graph.add((result, EARL.date, Literal(run["date"], datatype=XSD.dateTime)))
        if row.info:
            graph.add((result, EARL.info, Literal(row.info)))
    return graph


def baseline_errors(results: list[Result], baseline: dict) -> list[str]:
    actual = {
        row.test: {"status": row.status, "outcome": row.outcome, "algebra": row.algebra}
        for row in results
    }
    expected = baseline["results"]
    errors = []
    if actual.keys() != expected.keys():
        errors.append(
            f"Changed test inventory: missing={sorted(expected.keys() - actual.keys())}, added={sorted(actual.keys() - expected.keys())}"
        )
    rank = {"error": 0, "untested": 0, "failed": 1, "partial": 2, "passed": 3}
    for test in actual.keys() & expected.keys():
        old, new = expected[test], actual[test]
        if (
            new["status"] != old["status"]
            or rank[new["outcome"]] < rank[old["outcome"]]
        ):
            errors.append(f"Regressed: {test}: {old} -> {new}")
        if old["algebra"] == "passed" and new["algebra"] != "passed":
            errors.append(f"Algebra regressed: {test}: {new['algebra']}")
    return errors


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--section", choices=["all", "core", "sparql"], default="all")
    parser.add_argument("--test", help="Exact urn:x-shacl-test:/... identifier")
    parser.add_argument(
        "--include-unlisted",
        action="store_true",
        help="Also execute test manifests not reachable from the root (reported by status)",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "target/conformance")
    parser.add_argument("--assertor", default="https://github.com/gtfierro")
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Require full inventory and no regressions against this JSON",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Allow test failures/partial results; runner errors still exit 2",
    )
    args = parser.parse_args(argv)
    try:
        suite = args.suite.resolve()
        pin = verify_fixtures(suite)
        documents = Documents(suite / "tests")
        cases = discover(documents, suite / "tests/manifest.ttl", args.include_unlisted)
        if args.section != "all":
            cases = [
                case
                for case in cases
                if case.identifier.startswith(TEST_BASE + args.section + "/")
            ]
        if args.test:
            cases = [case for case in cases if case.identifier == args.test]
        if not cases:
            raise ValueError("No tests match the selection")
        args.output.mkdir(parents=True, exist_ok=True)
        results = [run_case(case, documents, args.output) for case in cases]
        run = metadata(pin)
        run["selection"] = {
            "section": args.section,
            "test": args.test,
            "include_unlisted": args.include_unlisted,
        }
        payload = {"metadata": run, "results": [asdict(row) for row in results]}
        (args.output / "results.json").write_text(json.dumps(payload, indent=2) + "\n")
        earl_report(results, run, args.assertor).serialize(
            args.output / "shifty-earl.ttl", format="turtle"
        )
        for status in ("approved", "proposed", "rejected"):
            counts = Counter(row.outcome for row in results if row.status == status)
            print(f"{status}: {dict(sorted(counts.items()))}")
        for row in results:
            if (
                row.outcome not in ("passed", "untested")
                or row.algebra == "failed"
                or row.algebra.startswith("error")
            ):
                print(
                    f"{row.outcome.upper()} {row.test}: {row.info} [algebra: {row.algebra}]"
                )
        if any(
            row.outcome == "error" or row.algebra.startswith("error") for row in results
        ):
            return 2
        if args.baseline:
            baseline = json.loads(args.baseline.read_text())
            if baseline["suite_revision"] != pin["revision"]:
                raise ValueError("Baseline uses a different suite revision")
            errors = baseline_errors(results, baseline)
            for error in errors:
                print(error)
            return int(bool(errors))
        return int(
            not args.report_only
            and any(
                row.outcome not in ("passed", "untested") or row.algebra == "failed"
                for row in results
            )
        )
    except Exception as error:
        print(f"Runner error: {type(error).__name__}: {error}")
        return 2
