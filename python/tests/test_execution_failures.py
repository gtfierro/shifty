"""Unavailable validation verdicts must not masquerade as SHACL findings."""

import pytest

import shifty

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
"""
FUNCTION = """
ex:exists a sh:SPARQLFunction ; sh:ask "ASK { ?s <http://ex/p> ?o }" .
"""
SHAPES = (
    PREFIXES
    + FUNCTION
    + """
ex:S a sh:NodeShape ; sh:targetNode ex:x ;
    sh:sparql [ sh:select "SELECT $this WHERE { FILTER (<http://ex/exists>()) }" ] .
"""
)
DATA = PREFIXES + 'ex:x ex:p "present" .'


@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize("algebra", [False, True])
def test_default_rejects_unsupported_function_and_ignore_is_explicit(prepared, algebra):
    if prepared:
        validator = shifty.PreparedValidator(SHAPES)
        call = validator.validate_algebra if algebra else validator.validate
        args = (DATA,)
    else:
        call = shifty.validate_algebra if algebra else shifty.validate
        args = (DATA, SHAPES)
    with pytest.raises(shifty.ValidationFailure, match="graph-reading SPARQL function"):
        call(*args, infer=False)
    result = call(*args, infer=False, on_unsupported="ignore")
    assert result.conforms if algebra else result[0]


@pytest.mark.parametrize(
    "method", ["validate", "validate_conformance", "find_failures"]
)
def test_evidence_projections_raise_the_same_execution_exception(method):
    session = shifty.EvidenceSession(SHAPES, DATA, infer=False)
    with pytest.raises(shifty.ValidationFailure):
        getattr(session, method)()


@pytest.mark.parametrize("algebra", [False, True])
def test_ignore_does_not_hide_an_execution_failure(algebra):
    shapes = (
        PREFIXES
        + """
ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ;
    sh:validator [ sh:ask "SELECT $this WHERE {}" ] .
ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true .
"""
    )
    call = shifty.validate_algebra if algebra else shifty.validate
    with pytest.raises(shifty.ValidationFailure):
        call(DATA, shapes, infer=False, on_unsupported="ignore")


def test_constraint_violation_is_still_an_ordinary_report():
    conforms, report, _ = shifty.validate(
        DATA,
        PREFIXES
        + "ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:nodeKind sh:Literal .",
        infer=False,
    )
    assert not conforms
    assert len(report) > 0


@pytest.mark.parametrize(
    "call", [shifty.validate, shifty.validate_algebra, shifty.infer]
)
def test_strict_inference_is_the_default(call):
    shapes = (
        PREFIXES
        + """
ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:rule [ a sh:SPARQLRule ;
    sh:construct "CONSTRUCT { $this <http://ex/p> [] } WHERE {}" ] .
"""
    )
    with pytest.raises(ValueError, match="strict inference failed"):
        call(DATA, shapes)
