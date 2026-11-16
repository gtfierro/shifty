"""AF expressions agree across reports, algebra, prepared validation and evidence."""

from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal, Namespace
from rdflib.namespace import RDF, SH

import shifty

EX = Namespace("http://example.org/af#")
PREFIX = f"@prefix sh: <{SH}> . @prefix ex: <{EX}> . "
FIXTURES = Path(__file__).resolve().parents[2] / "testdata/regressions/af"


def verdict(interface, data, shapes):
    if interface == "w3c":
        return shifty.validate(data, shapes, infer=False)[0]
    if interface == "prepared":
        return shifty.PreparedValidator(shapes).validate(data, infer=False)[0]
    if interface == "evidence":
        return shifty.EvidenceSession(shapes, data, infer=False).validate().conforms
    return shifty.validate_algebra(data, shapes, infer=False).conforms


@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
@pytest.mark.parametrize(
    "expression,expected",
    [
        ("true", True),
        ("false", False),
        ("[ sh:path ex:missing ]", False),
        ("[ sh:union (true false) ]", False),
        ("[ sh:union (true true) ]", True),
        ("[ ex:ask () ]", False),
        ("[ ex:isTrue (true) ]", True),
        ("[ ex:isTrue (false) ]", False),
        ("[ ex:optional ([sh:path ex:missing]) ]", True),
        ("[ ex:identity ([sh:path ex:missing]) ]", False),
        ("[ ex:identity (true) ]", True),
        ("[ ex:identity ([ex:identity (true)]) ]", True),
        ("[ sh:nodes ex:other ; sh:path ex:flag ]", True),
    ],
)
def test_expression_result_must_equal_singleton_true(interface, expression, expected):
    shapes = (
        PREFIX
        + f"""
ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:expression {expression} .
ex:ask a sh:SPARQLFunction ; sh:ask "ASK {{ FILTER(false) }}" .
ex:isTrue a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
    sh:ask "ASK {{ FILTER(?arg = true) }}" .
ex:optional a sh:SPARQLFunction ;
    sh:parameter [sh:path ex:arg ; sh:optional true] ;
    sh:select "SELECT (!BOUND(?arg) AS ?answer) WHERE {{}}" .
ex:identity a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
    sh:select "SELECT ?arg WHERE {{}}" .
"""
    )
    assert verdict(interface, PREFIX + "ex:other ex:flag true .", shapes) == expected


@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
def test_property_expression_checks_values_and_empty_path_is_vacuous(interface):
    shapes = (
        PREFIX
        + """
ex:S a sh:NodeShape ; sh:targetNode ex:container ; sh:property [
    sh:path ex:child ; sh:expression [sh:path ex:flag] ] .
"""
    )
    assert verdict(interface, "", shapes)
    assert verdict(
        interface,
        PREFIX + "ex:container ex:child ex:good . ex:good ex:flag true .",
        shapes,
    )
    assert not verdict(interface, PREFIX + "ex:container ex:child ex:empty .", shapes)


def test_expression_report_identifies_value_source_path_and_message():
    source = (FIXTURES / "expressions.test.ttl").read_text()
    conforms, report, _ = shifty.validate(source, infer=False)
    assert not conforms
    results = list(report.objects(None, SH.result))
    assert len(results) == 5
    for result in results:
        assert report.value(result, SH.sourceConstraint) is not None
        assert report.value(result, SH.resultMessage) == Literal("needs a true flag")
        if report.value(result, SH.focusNode) == EX.container:
            assert report.value(result, SH.resultPath) == EX.child
            assert report.value(result, SH.value) in (EX.bad, EX.empty)


@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
def test_graph_reading_ask_function_keeps_blank_node_arguments(interface):
    shapes = (
        PREFIX
        + """
ex:S a sh:NodeShape ; sh:targetClass ex:Thing ; sh:expression [ex:check (sh:this)] .
ex:check a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
    sh:ask "ASK { ?arg <http://example.org/af#flag> true }" .
"""
    )
    graph = Graph()
    good, bad = BNode(), BNode()
    for node in (good, bad):
        graph.add((node, RDF.type, EX.Thing))
    graph.add((good, EX.flag, Literal(True)))
    assert not verdict(interface, graph, shapes)
    graph.add((bad, EX.flag, Literal(True)))
    assert verdict(interface, graph, shapes)


@pytest.mark.parametrize("policy", ["error", "ignore"])
@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
def test_expression_execution_failures_propagate_through_negation(interface, policy):
    shapes = (
        PREFIX
        + """
ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:not [sh:expression [ex:missing ()]] .
"""
    )
    with pytest.raises(shifty.ValidationFailure, match="has no sh:select or sh:ask"):
        if interface == "w3c":
            shifty.validate("", shapes, infer=False, on_unsupported=policy)
        elif interface == "prepared":
            shifty.PreparedValidator(shapes).validate(
                "", infer=False, on_unsupported=policy
            )
        elif interface == "evidence":
            shifty.EvidenceSession(
                shapes, "", infer=False, on_unsupported=policy
            ).validate()
        else:
            shifty.validate_algebra("", shapes, infer=False, on_unsupported=policy)


def test_recursive_node_expression_is_rejected():
    shapes = (
        PREFIX
        + """
ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:expression _:cycle .
_:cycle sh:path ex:p ; sh:nodes _:cycle .
"""
    )
    with pytest.raises(ValueError, match="recursive SHACL-AF node expression"):
        shifty.validate("", shapes)
