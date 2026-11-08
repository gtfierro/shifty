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
WRONG_QUERY_FORM = (
    PREFIXES
    + """
ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ;
    sh:validator [ sh:ask "SELECT $this WHERE {}" ] .
ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true .
"""
)


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
    call = shifty.validate_algebra if algebra else shifty.validate
    with pytest.raises(shifty.ValidationFailure):
        call(DATA, WRONG_QUERY_FORM, infer=False, on_unsupported="ignore")


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


@pytest.mark.parametrize("guard", ["target", "condition", "filter"])
@pytest.mark.parametrize(
    "interface",
    [
        "validate",
        "validate_algebra",
        "infer",
        "prepared_validate",
        "prepared_validate_algebra",
        "prepared_witnesses",
        "evidence",
        "repair",
    ],
)
def test_inference_guard_failure_rejects_snapshot_without_mutating_data(
    guard, interface
):
    import rdflib

    target = "sh:targetNode ex:x"
    condition = ""
    subject = "sh:this"
    if guard == "target":
        target = """sh:target [ a sh:SPARQLTarget ;
            sh:select "SELECT ?this WHERE { BIND(<http://ex/exists>() AS ?this) }" ]"""
    elif guard == "condition":
        condition = "sh:condition ex:Guard ;"
    else:
        subject = "[ sh:filterShape ex:Guard ; sh:nodes sh:this ]"
    shapes = (
        PREFIXES
        + FUNCTION
        + f"""
ex:Guard a sh:NodeShape ; sh:not [ sh:sparql [
    sh:select "SELECT $this WHERE {{ FILTER (<http://ex/exists>()) }}" ] ] .
ex:S a sh:NodeShape ; {target} ; sh:rule [ a sh:TripleRule ;
    {condition} sh:subject {subject} ; sh:predicate ex:derived ; sh:object true ] .
"""
    )
    data = rdflib.Graph().parse(data=DATA, format="turtle")
    before = set(data)
    with pytest.raises(shifty.ValidationFailure, match="graph-reading SPARQL function"):
        if interface.startswith("prepared_"):
            validator = shifty.PreparedValidator(shapes)
            method = getattr(validator, interface.removeprefix("prepared_"))
            method(
                data,
                **({} if interface == "prepared_witnesses" else {"in_place": True}),
            )
        elif interface == "evidence":
            shifty.EvidenceSession(shapes, data)
        elif interface == "repair":
            shifty.RepairSession(shapes, data)
        else:
            getattr(shifty, interface)(data, shapes, in_place=True)
    assert set(data) == before


@pytest.mark.parametrize(
    "method,args",
    [
        ("gate", (shifty.RepairDelta.from_ntriples("", ""),)),
        ("witnesses", ()),
        ("witnesses_for", ("http://ex/S",)),
        ("satisfactions_for", ("http://ex/S",)),
    ],
)
def test_repair_adapters_preserve_execution_failure_type(method, args):
    session = shifty.RepairSession(WRONG_QUERY_FORM, DATA, infer=False)
    with pytest.raises(shifty.ValidationFailure, match="query form does not match"):
        getattr(session, method)(*args)


def test_repair_node_against_preserves_execution_failure_type():
    shapes = (
        WRONG_QUERY_FORM
        + """
ex:Container a sh:NodeShape ; sh:targetNode ex:y ;
    sh:property [ sh:path ex:child ; sh:qualifiedValueShape ex:S ; sh:qualifiedMinCount 1 ] .
"""
    )
    session = shifty.RepairSession(shapes, DATA, infer=False)
    failure = session.witnesses_for("http://ex/Container")[0]
    shape_id = failure.repair_tree().holes()[0].conforms_to
    assert shape_id is not None
    with pytest.raises(shifty.ValidationFailure, match="query form does not match"):
        session.repair_node_against("<http://ex/fresh>", shape_id)


def test_evidence_revalidation_preserves_inference_failure_type():
    shapes = (
        SHAPES
        + """
ex:RuleShape a sh:NodeShape ; sh:targetNode ex:x ; sh:rule [ a sh:TripleRule ;
    sh:condition ex:S ; sh:subject sh:this ; sh:predicate ex:derived ; sh:object true ] .
"""
    )
    session = shifty.EvidenceSession(shapes, DATA, infer=False)
    with pytest.raises(shifty.ValidationFailure, match="graph-reading SPARQL function"):
        session.revalidate(shifty.RepairDelta.from_ntriples("", ""), infer=True)


@pytest.mark.parametrize("policy", ["error", "ignore"])
def test_inference_query_failure_propagates_under_both_policies(policy):
    shapes = (
        WRONG_QUERY_FORM
        + """
ex:RuleShape a sh:NodeShape ; sh:targetNode ex:x ; sh:rule [ a sh:TripleRule ;
    sh:condition [ sh:not ex:S ] ;
    sh:subject sh:this ; sh:predicate ex:derived ; sh:object true ] .
"""
    )
    with pytest.raises(shifty.ValidationFailure, match="query form does not match"):
        shifty.infer(DATA, shapes, on_unsupported=policy)
