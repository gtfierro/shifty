"""The conformance oracle must detect report defects, not just boolean changes."""

import json

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import RDF, RDFS, SH, XSD

import shifty
from conformance.runner import (
    DEFAULT_SUITE,
    EARL,
    MF,
    SHT,
    Case,
    Documents,
    Result,
    baseline_errors,
    discover,
    earl_report,
    main,
    report_graph,
    run_case,
    verify_fixtures,
)
from shifty._rdf import parse_ntriples


def report(extra="", value='"bad"', path="<urn:p>"):
    graph = Graph().parse(
        data=f"""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:report> a sh:ValidationReport; sh:conforms false;
            sh:result [ a sh:ValidationResult; sh:focusNode <urn:focus>;
                sh:resultPath {path}; sh:value {value};
                sh:resultSeverity sh:Violation;
                sh:sourceShape <urn:shape>;
                sh:sourceConstraintComponent sh:DatatypeConstraintComponent;
                {extra} ] .
        """,
        format="turtle",
    )
    return graph, URIRef("urn:report")


@pytest.mark.parametrize(
    "predicate", [SH.resultSeverity, SH.sourceConstraint, SH.resultMessage]
)
def test_report_comparison_detects_missing_fields(predicate):
    expected, root = report('sh:sourceConstraint <urn:c>; sh:resultMessage "expected";')
    actual = Graph()
    for triple in expected:
        if triple[1] != predicate:
            actual.add(triple)
    messages = set(expected.objects(None, SH.resultMessage))
    assert not isomorphic(
        report_graph(expected, root, messages), report_graph(actual, root, messages)
    )


def test_report_comparison_ignores_extra_messages_and_clones_paths():
    expected, root = report(path="([sh:inversePath <urn:p>] <urn:q>)")
    actual = Graph()
    for triple in expected:
        actual.add(triple)
    result = actual.value(root, SH.result)
    actual.add(
        (result, SH.resultMessage, Literal("Implementation-specific explanation"))
    )
    actual.add((result, SH.detail, BNode()))
    # A second result can reuse the same path structure in actual output.
    second = BNode()
    actual.add((root, SH.result, second))
    expected_second = BNode()
    expected.add((root, SH.result, expected_second))
    for predicate, obj in list(actual.predicate_objects(result)):
        actual.add((second, predicate, obj))
        if predicate not in (SH.resultMessage, SH.detail):
            expected.add((expected_second, predicate, obj))
    assert isomorphic(
        report_graph(expected, root, set()), report_graph(actual, root, set())
    )


def test_anonymous_shapes_are_not_independent_wildcards():
    expected, root = report()
    first = expected.value(root, SH.result)
    second = BNode()
    expected.add((root, SH.result, second))
    shared_shape = BNode()
    for predicate, obj in list(expected.predicate_objects(first)):
        expected.add((second, predicate, obj))
    expected.set((first, SH.sourceShape, shared_shape))
    expected.set((second, SH.sourceShape, shared_shape))
    actual = Graph()
    for triple in expected:
        actual.add(triple)
    actual.set((second, SH.sourceShape, BNode()))
    assert not isomorphic(
        report_graph(expected, root, set()), report_graph(actual, root, set())
    )


def test_data_blank_node_identity_is_preserved():
    expected, root = report(value="_:original")
    actual, _ = report(value="_:different")
    assert not isomorphic(
        report_graph(expected, root, set()), report_graph(actual, root, set())
    )


def test_missing_report_type_is_not_synthesized_into_a_pass():
    graph, root = report()
    graph.remove((root, RDF.type, SH.ValidationReport))
    with pytest.raises(ValueError, match="Missing.*ValidationReport"):
        report_graph(graph, root, set())


def test_manifest_actions_load_separate_documents_and_ignore_helper_booleans(tmp_path):
    (tmp_path / "manifest.ttl").write_text("""
        @prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
        <> a mf:Manifest; mf:include <case.ttl> .
    """)
    (tmp_path / "case.ttl").write_text("""
        @prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
        @prefix sht: <http://www.w3.org/ns/shacl-test#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <> a mf:Manifest; mf:entries (<case>) .
        <case> a sht:Validate; mf:status sht:approved;
            mf:action [sht:dataGraph <data.ttl>; sht:shapesGraph <shapes.ttl>];
            mf:result [a sh:ValidationReport; sh:conforms true] .
    """)
    (tmp_path / "data.ttl").write_text('<urn:person> <urn:name> "A" .')
    (tmp_path / "shapes.ttl").write_text("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:s> a sh:NodeShape; sh:targetNode <urn:person>;
            sh:property [sh:path <urn:name>; sh:minCount 1] .
        <urn:trap> sh:conforms false .
    """)
    documents = Documents(tmp_path)
    cases = discover(documents, tmp_path / "manifest.ttl")
    assert len(cases) == 1
    assert cases[0].identifier == "urn:x-shacl-test:/case"
    assert run_case(cases[0], documents, tmp_path / "out").outcome == "passed"
    (tmp_path / "data.ttl").unlink()
    assert run_case(cases[0], Documents(tmp_path), tmp_path / "out").outcome == "error"


@pytest.mark.parametrize(
    "message",
    [
        "parse error: Turtle syntax error",
        "invalid shapes graph: [error] Cannot parse SHACL-SPARQL query",
        "invalid shapes graph: [error] SHACL-SPARQL query uses an unknown prefix",
        "invalid shapes graph: [error] invalid SPARQL in validator: SHACL-SPARQL query uses an unknown prefix",
    ],
)
def test_unrelated_value_error_does_not_pass_expected_failure(
    tmp_path, monkeypatch, message
):
    documents = Documents(DEFAULT_SUITE / "tests")
    cases = discover(documents, DEFAULT_SUITE / "tests/manifest.ttl")
    case = next(
        case
        for case in cases
        if str(case.graph.value(case.entry, MF.result)) == str(SHT.Failure)
    )

    def unrelated_error(*args, **kwargs):
        raise ValueError(message)

    monkeypatch.setattr(shifty, "validate", unrelated_error)
    assert run_case(case, documents, tmp_path).outcome == "error"


def test_data_blank_node_cannot_match_an_iri():
    expected, root = report()
    result = expected.value(root, SH.result)
    expected.set((result, SH.value, BNode("original")))
    actual = Graph()
    for triple in expected:
        actual.add(triple)
    actual.set((result, SH.value, URIRef("urn:shifty-test-data:original")))
    assert not isomorphic(
        report_graph(expected, root, set()), report_graph(actual, root, set())
    )


def test_report_comparison_preserves_duplicate_results():
    expected, root = report()
    actual = Graph()
    for triple in expected:
        actual.add(triple)
    first = expected.value(root, SH.result)
    duplicate = BNode()
    expected.add((root, SH.result, duplicate))
    for predicate, obj in list(expected.predicate_objects(first)):
        expected.add((duplicate, predicate, obj))
    assert not isomorphic(
        report_graph(expected, root, set()), report_graph(actual, root, set())
    )


@pytest.mark.parametrize(
    "tail", ["", "; rdf:rest _:list", "; rdf:rest rdf:nil; rdf:first <other>"]
)
def test_malformed_manifest_list_is_not_silently_truncated(tmp_path, tail):
    manifest = tmp_path / "manifest.ttl"
    manifest.write_text(f"""
        @prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
        @prefix sht: <http://www.w3.org/ns/shacl-test#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        <> a mf:Manifest; mf:entries _:list .
        _:list rdf:first <case> {tail} .
        <case> mf:status sht:approved .
        <other> mf:status sht:approved .
    """)
    with pytest.raises(ValueError):
        discover(Documents(tmp_path), manifest)


@pytest.fixture
def message_case():
    documents = Documents(DEFAULT_SUITE / "tests")
    cases = discover(documents, DEFAULT_SUITE / "tests/manifest.ttl")
    case = next(case for case in cases if case.identifier.endswith("/misc/message-001"))
    return documents, case


def test_messages_from_other_reports_are_ignored(message_case, tmp_path, monkeypatch):
    documents, case = message_case
    unrelated = Literal("message from another test")
    case.graph.add((BNode(), SH.resultMessage, unrelated))
    original = shifty.validate

    def validate(*args, **kwargs):
        conforms, graph, text = original(*args, **kwargs)
        for result in list(graph.objects(None, SH.result)):
            graph.add((result, SH.resultMessage, unrelated))
        return conforms, graph, text

    monkeypatch.setattr(shifty, "validate", validate)
    assert run_case(case, documents, tmp_path).outcome == "passed"


def test_report_subclasses_and_named_results_are_normalized(
    message_case, tmp_path, monkeypatch
):
    documents, case = message_case
    original = shifty.validate

    def validate(*args, **kwargs):
        conforms, graph, text = original(*args, **kwargs)
        report_class = URIRef("urn:custom-report")
        result_class = URIRef("urn:custom-result")
        report = graph.value(None, RDF.type, SH.ValidationReport)
        results = list(graph.objects(report, SH.result))
        mapping = {
            report: URIRef("urn:report"),
            **{r: URIRef(f"urn:result:{i}") for i, r in enumerate(results)},
        }
        converted = Graph()
        for subject, predicate, obj in graph:
            converted.add(
                (mapping.get(subject, subject), predicate, mapping.get(obj, obj))
            )
        converted.set((URIRef("urn:report"), RDF.type, report_class))
        converted.add((report_class, RDFS.subClassOf, SH.ValidationReport))
        for result in results:
            converted.set((mapping[result], RDF.type, result_class))
        converted.add((result_class, RDFS.subClassOf, SH.ValidationResult))
        return conforms, converted, text

    monkeypatch.setattr(shifty, "validate", validate)
    assert run_case(case, documents, tmp_path).outcome == "passed"


@pytest.mark.parametrize("mode", [[], ["--report-only"], ["--baseline"]])
def test_algebra_crash_makes_cli_fail(message_case, tmp_path, monkeypatch, mode):
    from conformance import runner

    _, case = message_case
    result = Result(case.identifier, "approved", "passed", algebra="error: crashed")
    monkeypatch.setattr(runner, "run_case", lambda *args: result)
    if mode == ["--baseline"]:
        baseline = tmp_path / "baseline.json"
        baseline.write_text(
            json.dumps(
                {
                    "suite_revision": json.loads(
                        (DEFAULT_SUITE / "upstream.json").read_text()
                    )["revision"],
                    "results": {
                        case.identifier: {
                            "status": "approved",
                            "outcome": "passed",
                            "algebra": "passed",
                        }
                    },
                }
            )
        )
        mode = ["--baseline", str(baseline)]
    assert (
        main(["--test", case.identifier, "--output", str(tmp_path / "out"), *mode]) == 2
    )
    saved = json.loads((tmp_path / "out/results.json").read_text())
    assert saved["results"][0]["algebra"] == "error: crashed"


def test_algebra_mismatch_fails_default_gate(message_case, tmp_path, monkeypatch):
    from conformance import runner

    _, case = message_case
    monkeypatch.setattr(
        runner,
        "run_case",
        lambda *args: Result(case.identifier, "approved", "passed", algebra="failed"),
    )
    assert main(["--test", case.identifier, "--output", str(tmp_path)]) == 1


def test_unknown_test_type_is_accounted_for(tmp_path):
    graph = Graph()
    entry = URIRef("urn:unknown")
    graph.add((entry, RDF.type, URIRef("urn:unknown-type")))
    case = Case(
        "urn:x-shacl-test:/unknown", entry, URIRef("urn:document"), "approved", graph
    )
    assert run_case(case, Documents(tmp_path), tmp_path).outcome == "error"


def test_pinned_inventory_and_baseline_cannot_lose_tests():
    verify_fixtures(DEFAULT_SUITE)
    documents = Documents(DEFAULT_SUITE / "tests")
    official = discover(documents, DEFAULT_SUITE / "tests/manifest.ttl")
    assert len(official) == 120
    assert all(case.status == "approved" for case in official)
    extended = discover(documents, DEFAULT_SUITE / "tests/manifest.ttl", True)
    assert len(extended) == 121
    assert [case.identifier for case in extended if case.status == "proposed"] == [
        "urn:x-shacl-test:/sparql/component/nodeValidator-001"
    ]
    baseline = {
        "results": {
            case.identifier: {
                "status": case.status,
                "outcome": "passed",
                "algebra": "passed",
            }
            for case in official
        }
    }
    assert baseline_errors([], baseline)
    results = [
        Result(case.identifier, case.status, "passed", algebra="passed")
        for case in official
    ]
    assert not baseline_errors(results, baseline)
    results[0].outcome = "partial"
    assert baseline_errors(results, baseline)


def test_fixture_tampering_is_an_error(tmp_path):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.ttl").write_text("changed")
    (tmp_path / "upstream.json").write_text(json.dumps({"sha256": {}}))
    with pytest.raises(ValueError, match="checksums"):
        verify_fixtures(tmp_path)


def test_earl_keeps_partial_results_and_runner_errors_distinct():
    rows = [
        Result(f"urn:x-shacl-test:/{outcome}", "approved", outcome)
        for outcome in ("passed", "partial", "failed", "error")
    ]
    run = {
        "shifty_version": "0.5.1",
        "source_revision": "abc",
        "suite_revision": "def",
        "date": "2026-11-08T00:00:00+00:00",
    }
    graph = earl_report(rows, run, "urn:assertor")
    graph = Graph().parse(data=graph.serialize(format="turtle"), format="turtle")
    assert len(list(graph.subjects(RDF.type, EARL.Assertion))) == 4
    for row, outcome in zip(
        rows, (EARL.passed, SHT.partial, EARL.failed, EARL.cantTell)
    ):
        assertion = graph.value(None, EARL.test, URIRef(row.test))
        result = graph.value(assertion, EARL.result)
        assert graph.value(result, EARL.outcome) == outcome


def test_ntriples_decoding_preserves_lexical_forms_and_caller_nodes():
    data = """_:data <urn:date> "2011-01-01"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    _:data <urn:number> "01"^^<http://www.w3.org/2001/XMLSchema#integer> ."""
    caller = BNode()
    graph = parse_ntriples(data, {"data": caller})
    assert set(graph.subjects()) == {caller}
    assert str(graph.value(caller, URIRef("urn:date"))) == "2011-01-01"
    assert str(graph.value(caller, URIRef("urn:number"))) == "01"


@pytest.mark.parametrize("lexical", ["01", "+01"])
def test_public_report_preserves_offending_literal(lexical):
    value = Literal(lexical, datatype=XSD.integer, normalize=False)
    graph = Graph()
    graph.add((URIRef("urn:person"), URIRef("urn:p"), value))
    shapes = """@prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <urn:s> a sh:NodeShape; sh:targetNode <urn:person>;
            sh:property [sh:path <urn:p>; sh:datatype xsd:string] ."""
    conforms, report, _ = shifty.validate(graph, shapes, infer=False)
    assert not conforms
    assert list(report.objects(None, SH.value)) == [value]
