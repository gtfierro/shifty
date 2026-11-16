"""Shared SHACL function environment, including nested graph access."""

import pytest
from rdflib import BNode, Graph, Literal, Namespace

import shifty

EX = Namespace("http://ex/")
PREFIX = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> . "
INTERFACES = ["w3c", "algebra", "prepared", "evidence"]


def verdict(interface, data, shapes, **options):
    if interface == "w3c":
        return shifty.validate(data, shapes, **options)[0]
    if interface == "prepared":
        return shifty.PreparedValidator(shapes).validate(data, **options)[0]
    if interface == "evidence":
        return shifty.EvidenceSession(shapes, data, **options).validate().conforms
    return shifty.validate_algebra(data, shapes, **options).conforms


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize("context", ["expression", "sparql"])
@pytest.mark.parametrize("graph_mode", ["data", "union", "union-all"])
def test_nested_functions_preserve_blank_arguments_and_graph_mode(
    interface, context, graph_mode
):
    constraint = (
        "sh:expression [ex:outer (sh:this)]"
        if context == "expression"
        else 'sh:sparql [sh:select "SELECT $this WHERE { FILTER (!ex:outer($this)) }"]'
    )
    shapes = (
        PREFIX
        + f"""
    ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:p ; {constraint} .
    ex:outer a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
        sh:select "SELECT (ex:inner(?arg) AS ?result) WHERE {{}}" .
    ex:inner a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
        sh:ask "ASK {{ ?arg ex:p true }}" ."""
    )
    data = Graph()
    data.add((BNode(), EX.p, Literal(True)))
    assert verdict(interface, data, shapes, infer=False, graph_mode=graph_mode)
    data.add((BNode(), EX.p, Literal(False)))
    assert not verdict(interface, data, shapes, infer=False, graph_mode=graph_mode)


@pytest.mark.parametrize("interface", INTERFACES)
def test_nested_pure_calls_work_inside_function_bodies(interface):
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:expression [ex:outer (2)] .
    ex:outer a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
        sh:select "SELECT (ex:inner(?arg) = 3 AS ?result) WHERE {}" .
    ex:inner a sh:SPARQLFunction ; sh:parameter [sh:path ex:arg] ;
        sh:select "SELECT (?arg + 1 AS ?result) WHERE {}" ."""
    )
    assert verdict(interface, "", shapes, infer=False)


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize(
    "graph_mode,expected", [("data", False), ("union", True), ("union-all", True)]
)
def test_functions_read_the_selected_default_and_named_shapes_views(
    interface, graph_mode, expected
):
    shapes = (
        PREFIX
        + """ex:shapeData ex:flag true .
    ex:read a sh:SPARQLFunction ; sh:ask "ASK { ex:shapeData ex:flag true }" .
    ex:named a sh:SPARQLFunction ; sh:ask "ASK { GRAPH <urn:x-shacl:shapes-graph> { ex:shapeData ex:flag true } }" .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:expression [ex:named ()] ;
        sh:sparql [sh:select "SELECT $this WHERE { FILTER (!ex:read()) }"] ."""
    )
    assert (
        verdict(interface, "", shapes, infer=False, graph_mode=graph_mode) == expected
    )


@pytest.mark.parametrize("interface", INTERFACES)
def test_nested_functions_observe_newly_inferred_triples(interface):
    shapes = (
        PREFIX
        + """ex:outer a sh:SPARQLFunction ;
        sh:select "SELECT (ex:inner() AS ?result) WHERE {}" .
    ex:inner a sh:SPARQLFunction ; sh:ask "ASK { ex:x ex:flag true }" .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:order 0 ;
        sh:rule [a sh:SPARQLRule ; sh:construct "CONSTRUCT { $this ex:derived true } WHERE { FILTER(ex:outer()) }"] .
    ex:T a sh:NodeShape ; sh:targetNode ex:x ; sh:order 1 ;
        sh:rule [a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:flag ; sh:object true] .
    ex:Check a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [sh:path ex:derived ; sh:hasValue true] ."""
    )
    assert verdict(interface, "", shapes)
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert (EX.x, EX.derived, Literal(True)) in data


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize("policy", ["error", "ignore"])
def test_mutual_recursion_is_bounded_and_propagates_through_negation(interface, policy):
    shapes = (
        PREFIX
        + """ex:a a sh:SPARQLFunction ; sh:select "SELECT (ex:b() AS ?v) WHERE {}" .
    ex:b a sh:SPARQLFunction ; sh:select "SELECT (ex:a() AS ?v) WHERE {}" .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:not [
        sh:sparql [sh:select "SELECT $this WHERE { FILTER(ex:a()) }"] ] ."""
    )
    with pytest.raises(shifty.ValidationFailure, match="recursive function invocation"):
        verdict(interface, "", shapes, infer=False, on_unsupported=policy)
