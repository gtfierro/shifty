"""Parity checks must detect bad CLI reports and distinguish crashes from rejections."""

import subprocess
from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import RDF, SH

from conformance import parity
from conformance.runner import DEFAULT_SUITE, MF, SHT, Documents, discover, report_graph


@pytest.fixture
def suite_cases():
    documents = Documents(DEFAULT_SUITE / "tests")
    cases = discover(documents, DEFAULT_SUITE / "tests/manifest.ttl")
    return documents, {case.identifier.rsplit("/", 1)[1]: case for case in cases}


def expected_report(case):
    root = case.graph.value(case.entry, MF.result)
    messages = set(case.graph.objects(None, SH.resultMessage))
    return report_graph(case.graph, root, messages)


def test_native_python_and_cli_reports_match(suite_cases, monkeypatch):
    documents, cases = suite_cases
    case = cases["message-001"]
    graph = expected_report(case)
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(
            command, 0, graph.serialize(format="turtle"), ""
        )

    monkeypatch.setattr(parity.subprocess, "run", run)
    assert (
        parity.compare_case(case, documents, Path("/local/shifty"), 1)["outcome"]
        == "passed"
    )
    assert "--no-infer" in commands[0]
    assert commands[0][-2:] == ["--on-unsupported", "error"]


def test_wrong_cli_severity_fails_parity(suite_cases, monkeypatch):
    documents, cases = suite_cases
    case = cases["message-001"]
    graph = expected_report(case)
    result = graph.value(None, RDF.type, SH.ValidationResult)
    graph.set((result, SH.resultSeverity, SH.Warning))
    monkeypatch.setattr(
        parity.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 0, graph.serialize(format="turtle"), ""
        ),
    )
    assert (
        parity.compare_case(case, documents, Path("/local/shifty"), 1)["outcome"]
        == "failed"
    )


@pytest.mark.parametrize("code", [-11, 2])
def test_cli_crash_or_argument_error_cannot_pass_expected_failure(
    suite_cases, monkeypatch, code
):
    documents, cases = suite_cases
    case = cases["unsupported-sparql-001"]
    diagnostic = "error: invalid shapes graph: [error] SHACL-SPARQL queries must not contain MINUS"
    monkeypatch.setattr(
        parity.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, code, "", diagnostic
        ),
    )
    assert (
        parity.compare_case(case, documents, Path("/local/shifty"), 1)["outcome"]
        == "error"
    )


def test_cli_expected_rejection_counts_as_pass(suite_cases, monkeypatch):
    documents, cases = suite_cases
    case = cases["unsupported-sparql-001"]
    assert case.graph.value(case.entry, MF.result) == SHT.Failure
    diagnostic = "error: invalid shapes graph: [error] SHACL-SPARQL queries must not contain MINUS"
    monkeypatch.setattr(
        parity.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 1, "", diagnostic
        ),
    )
    assert (
        parity.compare_case(case, documents, Path("/local/shifty"), 1)["outcome"]
        == "passed"
    )


def test_cli_timeout_is_an_error(suite_cases, monkeypatch):
    documents, cases = suite_cases

    def run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(parity.subprocess, "run", run)
    assert (
        parity.compare_case(cases["message-001"], documents, Path("/local/shifty"), 1)[
            "outcome"
        ]
        == "error"
    )


def test_process_local_blank_labels_are_compared_by_isomorphism():
    graphs = []
    for label in ("python", "cli"):
        graph = Graph()
        root, result = BNode(), BNode()
        graph.add((root, RDF.type, SH.ValidationReport))
        graph.add((root, SH.conforms, Literal(False)))
        graph.add((root, SH.result, result))
        graph.add((result, RDF.type, SH.ValidationResult))
        graph.add((result, SH.focusNode, BNode(label)))
        graphs.append(parity.normalize(graph, root, set()))
    assert isomorphic(*graphs)
    second = graphs[1]
    result = second.value(None, SH.focusNode, BNode("cli"))
    second.set((result, SH.focusNode, URIRef("urn:cli")))
    assert not isomorphic(*graphs)
