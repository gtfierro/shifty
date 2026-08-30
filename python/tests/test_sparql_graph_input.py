"""SHACL-SPARQL must retain namespaces from rdflib.Graph inputs."""

import pytest
import rdflib

import shifty


PREFIXES = """\
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
"""

DATA = PREFIXES + "ex:focus a ex:Thing ."

SPARQL_CONSTRAINT = PREFIXES + """\
ex:S a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:sparql [
        sh:select "SELECT $this WHERE { $this a ex:Thing }"
    ] .
"""

SPARQL_RULE = PREFIXES + """\
ex:S a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:construct "CONSTRUCT { $this ex:inferred ex:Value } WHERE {}"
    ] .
"""

SPARQL_TARGET = PREFIXES + """\
ex:S a sh:NodeShape ;
    sh:target [
        sh:select "SELECT ?this WHERE { ?this a ex:Thing }"
    ] ;
    sh:nodeKind sh:BlankNode .
"""


def graph(turtle: str) -> rdflib.Graph:
    result = rdflib.Graph()
    result.parse(data=turtle, format="turtle")
    return result


def test_sparql_constraint_with_prefixed_name_survives_graph_input():
    conforms, _, _ = shifty.validate(DATA, graph(SPARQL_CONSTRAINT), infer=False)
    assert not conforms


def test_sparql_constraint_prefixes_survive_coalesced_graph_inputs():
    conforms, _, _ = shifty.validate(
        DATA, [graph(SPARQL_CONSTRAINT), rdflib.Graph()], infer=False
    )
    assert not conforms


def test_sparql_rule_with_prefixed_name_survives_graph_input():
    result = shifty.infer(DATA, graph(SPARQL_RULE))
    assert result.inferred_count == 1
    assert (
        rdflib.URIRef("http://example.org/focus"),
        rdflib.URIRef("http://example.org/inferred"),
        rdflib.URIRef("http://example.org/Value"),
    ) in result.graph()


def test_sparql_target_with_prefixed_name_survives_graph_input():
    conforms, _, _ = shifty.validate(DATA, graph(SPARQL_TARGET), infer=False)
    assert not conforms


@pytest.mark.parametrize(
    ("shapes", "operation"),
    [
        (
            """\
            <http://example.org/S> a <http://www.w3.org/ns/shacl#NodeShape> ;
                <http://www.w3.org/ns/shacl#targetClass> <http://example.org/Thing> ;
                <http://www.w3.org/ns/shacl#sparql> [
                    <http://www.w3.org/ns/shacl#select>
                        "SELECT $this WHERE { $this missing:p ?value }"
                ] .
            """,
            lambda shapes: shifty.validate(DATA, shapes, infer=False),
        ),
        (
            """\
            <http://example.org/S> a <http://www.w3.org/ns/shacl#NodeShape> ;
                <http://www.w3.org/ns/shacl#targetClass> <http://example.org/Thing> ;
                <http://www.w3.org/ns/shacl#rule> [
                    a <http://www.w3.org/ns/shacl#SPARQLRule> ;
                    <http://www.w3.org/ns/shacl#construct>
                        "CONSTRUCT { $this missing:p <http://example.org/value> } WHERE {}"
                ] .
            """,
            lambda shapes: shifty.infer(DATA, shapes),
        ),
    ],
)
def test_unresolved_sparql_prefix_is_a_shapes_graph_error(shapes, operation):
    with pytest.raises(ValueError, match="invalid SPARQL query"):
        operation(graph(shapes))


@pytest.mark.parametrize(
    "factory",
    [
        lambda shapes: shifty.EvidenceSession(shapes, DATA, infer=False),
        lambda shapes: shifty.RepairSession(shapes, DATA, infer=False),
        lambda shapes: shifty.PreparedValidator(shapes),
    ],
)
def test_unresolved_sparql_prefix_prevents_session_construction(factory):
    shapes = """\
    <http://example.org/S> a <http://www.w3.org/ns/shacl#NodeShape> ;
        <http://www.w3.org/ns/shacl#targetClass> <http://example.org/Thing> ;
        <http://www.w3.org/ns/shacl#sparql> [
            <http://www.w3.org/ns/shacl#select>
                "SELECT $this WHERE { $this missing:p ?value }"
        ] .
    """
    with pytest.raises(ValueError, match="invalid SPARQL query"):
        factory(graph(shapes))
