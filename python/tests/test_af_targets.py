"""Reusable target types agree across compiled and source-oriented interfaces."""

import pytest
from rdflib import Graph, Literal, Namespace

import shifty

EX = Namespace("http://ex/")
PREFIX = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> . "
INTERFACES = ["w3c", "algebra", "prepared", "evidence"]
TARGET = """ex:ByFlag a sh:SPARQLTargetType ;
    sh:parameter [sh:path ex:desired], [sh:path ex:optional ; sh:optional true] ;
    sh:select "SELECT ?this WHERE { ?this ex:flag ?desired . FILTER(!BOUND(?optional) || ?optional = true) }" ."""


def verdict(interface, data, shapes, **options):
    if interface == "w3c":
        return shifty.validate(data, shapes, **options)[0]
    if interface == "prepared":
        return shifty.PreparedValidator(shapes).validate(data, **options)[0]
    if interface == "evidence":
        return shifty.EvidenceSession(shapes, data, **options).validate().conforms
    return shifty.validate_algebra(data, shapes, **options).conforms


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize(
    "flag,optional,expected",
    [
        ("", "", True),
        ("ex:desired true", "", False),
        ("ex:desired false", "", True),
        ("ex:desired true", "; ex:optional false", True),
    ],
)
def test_required_and_optional_target_parameters(interface, flag, optional, expected):
    shapes = (
        PREFIX
        + TARGET
        + f"""
    ex:S a sh:NodeShape ; sh:target [a ex:ByFlag ; {flag or "ex:unused true"} {optional}] ; sh:nodeKind sh:Literal ."""
    )
    assert (
        verdict(interface, PREFIX + "ex:x ex:flag true .", shapes, infer=False)
        == expected
    )


@pytest.mark.parametrize("interface", INTERFACES)
def test_multiple_instances_of_the_same_target_type(interface):
    shapes = (
        PREFIX
        + TARGET
        + """
    ex:S a sh:NodeShape ; sh:target [a ex:ByFlag ; ex:desired true],
        [a ex:ByFlag ; ex:desired false] ; sh:nodeKind sh:Literal ."""
    )
    assert not verdict(interface, PREFIX + "ex:x ex:flag false .", shapes, infer=False)
    conforms, report, _ = shifty.validate(
        PREFIX + "ex:x ex:flag true . ex:y ex:flag false .", shapes, infer=False
    )
    assert not conforms
    from rdflib.namespace import SH

    assert set(report.objects(None, SH.focusNode)) == {EX.x, EX.y}


@pytest.mark.parametrize("interface", INTERFACES)
def test_target_type_prefix_declarations(interface):
    shapes = (
        PREFIX
        + """ex:Prefixes sh:declare [sh:prefix "custom" ; sh:namespace "http://ex/"] .
    ex:T a sh:SPARQLTargetType ; sh:prefixes ex:Prefixes ;
        sh:parameter [sh:path ex:desired] ; sh:select "SELECT ?this WHERE { ?this custom:flag ?desired }" .
    ex:S a sh:NodeShape ; sh:target [a ex:T ; ex:desired true] ; sh:nodeKind sh:Literal ."""
    )
    assert not verdict(interface, PREFIX + "ex:x ex:flag true .", shapes, infer=False)


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize(
    "value,diagnostic", [("[]", "blank node"), ("true, false", "multiple values")]
)
def test_invalid_target_parameter_values_are_rejected(interface, value, diagnostic):
    shapes = (
        PREFIX
        + TARGET
        + f"""ex:S a sh:NodeShape ; sh:target [a ex:ByFlag ; ex:desired {value}] ; sh:nodeKind sh:Literal ."""
    )
    with pytest.raises(ValueError, match=diagnostic):
        verdict(interface, "", shapes, infer=False)


@pytest.mark.parametrize("interface", INTERFACES)
def test_values_cannot_rebind_a_target_parameter_even_when_omitted(interface):
    shapes = (
        PREFIX
        + """ex:T a sh:SPARQLTargetType ; sh:parameter [sh:path ex:desired ; sh:optional true] ;
        sh:select "SELECT ?this WHERE { VALUES ?desired { true } BIND(ex:x AS ?this) }" .
    ex:S a sh:NodeShape ; sh:target [a ex:T] ; sh:nodeKind sh:Literal ."""
    )
    with pytest.raises(ValueError, match="VALUES must not bind.*desired"):
        verdict(interface, "", shapes, infer=False)


@pytest.mark.parametrize("interface", INTERFACES)
def test_unrelated_values_remain_allowed_in_target_types(interface):
    shapes = (
        PREFIX
        + """ex:T a sh:SPARQLTargetType ; sh:parameter [sh:path ex:desired] ;
        sh:select "SELECT ?this WHERE { VALUES ?this { ex:x } FILTER(?desired) }" .
    ex:S a sh:NodeShape ; sh:target [a ex:T ; ex:desired true] ; sh:nodeKind sh:Literal ."""
    )
    assert not verdict(interface, "", shapes, infer=False)


@pytest.mark.parametrize("interface", INTERFACES)
def test_new_inferred_triples_create_targets_on_the_next_pass(interface):
    shapes = (
        PREFIX
        + TARGET
        + """ex:S a sh:NodeShape ; sh:order 0 ;
        sh:target [a ex:ByFlag ; ex:desired true] ;
        sh:rule [a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:derived ; sh:object true] .
    ex:T a sh:NodeShape ; sh:order 1 ; sh:targetNode ex:x ;
        sh:rule [a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:flag ; sh:object true] .
    ex:Check a sh:NodeShape ; sh:targetNode ex:x ; sh:property [sh:path ex:derived ; sh:hasValue true] ."""
    )
    assert verdict(interface, "", shapes)
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert (EX.x, EX.derived, Literal(True)) in data
