"""VALUES follows SHACL 1.2: only collisions with pre-bound variables fail."""

import pytest
from rdflib.namespace import SH

import shifty

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
"""
DATA = PREFIXES + "ex:x ex:p ex:o ."


@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
@pytest.mark.parametrize(
    "pattern",
    [
        "VALUES ?p { ex:p } $this ?p ?o",
        "VALUES (?p ?o) { (ex:p ex:o) } $this ?p ?o",
        "$this ex:p ?value . FILTER EXISTS { VALUES ?p { ex:p } $this ?p ?value }",
        "{ SELECT $this ?p WHERE { VALUES ?p { ex:p } $this ?p ?o } }",
        "$this ex:p ?o . OPTIONAL { VALUES ?p { ex:p } $this ?p ?o }",
        "VALUES ?value { ex:o } $this ex:p ?value",
    ],
)
def test_unrelated_values_execute_in_constraints(interface, pattern):
    shapes = (
        PREFIXES
        + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:sparql [ sh:select "SELECT $this WHERE {{ {pattern} }}" ] ."""
    )
    if interface == "w3c":
        conforms, report, _ = shifty.validate(DATA, shapes, infer=False)
        assert len(list(report.objects(None, SH.result))) == 1
    elif interface == "prepared":
        conforms = shifty.PreparedValidator(shapes).validate(DATA, infer=False)[0]
    elif interface == "evidence":
        conforms = shifty.EvidenceSession(shapes, DATA, infer=False).validate().conforms
    else:
        conforms = shifty.validate_algebra(DATA, shapes, infer=False).conforms
    assert not conforms


@pytest.mark.parametrize("policy", ["error", "ignore"])
@pytest.mark.parametrize(
    "pattern,collisions",
    [
        ("VALUES ?this { ex:x }", "?this"),
        ("VALUES $this { ex:x }", "?this"),
        ("VALUES ?this { UNDEF }", "?this"),
        ("VALUES (?p ?this) { (ex:p ex:x) }", "?this"),
        ("VALUES (?this ?currentShape) { (ex:x ex:S) }", "?currentShape, ?this"),
        ("VALUES ?shapesGraph {}", "?shapesGraph"),
        ("FILTER EXISTS { VALUES ?this { ex:x } }", "?this"),
        ("FILTER NOT EXISTS { VALUES ?this { ex:x } }", "?this"),
        ("BIND(EXISTS { VALUES ?this { ex:x } } AS ?ok)", "?this"),
        ("{ SELECT $this WHERE { VALUES ?this { ex:x } } }", "?this"),
        ("OPTIONAL { VALUES ?this { ex:x } }", "?this"),
    ],
)
def test_values_collisions_fail_before_execution(policy, pattern, collisions):
    shapes = (
        PREFIXES
        + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:sparql [ sh:select "SELECT $this WHERE {{ {pattern} }}" ] ."""
    )
    with pytest.raises(ValueError, match="VALUES must not bind") as error:
        shifty.infer(DATA, shapes, on_unsupported=policy)
    assert f"pre-bound variable(s): {collisions}" in str(error.value)


@pytest.mark.parametrize("kind", ["select", "ask"])
@pytest.mark.parametrize("variable", ["arg", "value", "local"])
def test_custom_validator_values_use_its_prebinding_scope(kind, variable):
    query = f"SELECT $this WHERE {{ VALUES ?{variable} {{ ex:o }} $this ex:p ?o }}"
    if kind == "ask":
        query = f"ASK {{ VALUES ?{variable} {{ ex:o }} $value ex:p ?o }}"
    shapes = (
        PREFIXES
        + f'''ex:C a sh:ConstraintComponent ;
        sh:parameter [ sh:path ex:arg ; sh:optional true ] ;
        sh:validator [ sh:{kind} "{query}" ] .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg ex:o .'''
    )
    if variable == "arg" or (variable == "value" and kind == "ask"):
        with pytest.raises(ValueError, match="VALUES must not bind") as error:
            shifty.infer(DATA, shapes)
        assert f"pre-bound variable(s): ?{variable}" in str(error.value)
    else:
        shifty.validate(DATA, shapes, infer=False)


@pytest.mark.parametrize(
    "pattern",
    [
        "VALUES ?this { ex:x }",
        "FILTER EXISTS { VALUES ?this { ex:x } }",
    ],
)
def test_construct_rules_also_reject_values_collisions(pattern):
    shapes = (
        PREFIXES
        + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule ex:Rule .
        ex:Rule sh:construct "CONSTRUCT {{ $this ex:q ex:o }} WHERE {{ {pattern} }}" ."""
    )
    with pytest.raises(ValueError, match="VALUES must not bind") as error:
        shifty.infer(DATA, shapes)
    assert "<http://ex/Rule>" in str(error.value)
    assert "on shape <http://ex/S>" in str(error.value)
