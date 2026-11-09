"""Public explanations describe SHACL declarations and keep algebra opt-in."""

import pytest

import shifty

PREFIXES = """
@prefix ex: <http://ex/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""


def make_mapping(constraints, values, *, with_session=True, sibling=False):
    other = "; sh:property [ sh:path ex:q ; sh:minCount 1 ]" if sibling else ""
    shapes = (
        PREFIXES
        + f"""
    ex:S a sh:NodeShape ; sh:targetNode ex:a ;
        sh:property [ sh:path ex:p ; {constraints} ] {other} .
    """
    )
    session = shifty.EvidenceSession(
        shapes, PREFIXES + f"ex:a ex:p {values} .", infer=False
    )
    (mapping,) = shifty.ShapeMap.from_run(
        session.validate(), session if with_session else None
    )
    return mapping


@pytest.mark.parametrize("with_session", [False, True])
def test_rejected_pattern_is_explained_as_a_pattern_failure(with_session):
    binding = make_mapping('sh:pattern "x"', '"a-value"', with_session=with_session)[
        "p"
    ]
    text = binding.explain()
    assert text.startswith("p: pattern constraint failed\n")
    assert 'rejected values: "a-value"' in text
    assert "observed count: " + ("1" if with_session else "unavailable") in text
    assert "qualifying values: " + ("none" if with_session else "unavailable") in text
    assert "count_high" not in text
    assert "required count" not in text
    assert "count_high" in binding.explain(algebra=True)


def test_passing_pattern_is_explained_as_satisfied():
    binding = make_mapping('sh:pattern "x"', '"x-value"')["p"]
    text = binding.explain()
    assert text.startswith("p: pattern constraint satisfied\n")
    assert 'qualifying values: "x-value"' in text
    assert "observed count: 1" in text
    assert "rejected values" not in text


def test_compound_failure_lists_declarations_without_claiming_all_failed():
    binding = make_mapping(
        'sh:pattern "x" ; sh:datatype xsd:string ; sh:minCount 1', '"bad"'
    )["p→string"]
    text = binding.explain()
    assert text.startswith("p→string: constraints failed\n")
    assert "declared constraints: datatype, min count, pattern" in text
    assert "required count: at least 1" in text
    assert 'rejected values: "bad"' in text
    assert "datatype constraint failed" not in text
    assert "count_high" not in text


def test_cardinality_shortfall_describes_bounds_and_missing_values():
    binding = make_mapping("sh:minCount 2 ; sh:maxCount 3", '"one"')["p"]
    text = binding.explain()
    assert "declared constraints: max count, min count" in text
    assert "required count: at least 2, at most 3" in text
    assert "observed count: 1" in text
    assert "missing values: 1" in text
    assert 'qualifying values: "one"' in text
    assert "count_low" not in text
    assert "count_low" in binding.explain(algebra=True)


def test_passing_sibling_without_session_keeps_unknown_values_in_explanation():
    mapping = make_mapping("sh:minCount 1", '"one"', with_session=False, sibling=True)
    binding = mapping["p"]
    assert binding.ok
    assert binding.evidence is None
    text = binding.explain()
    assert "min count constraint satisfied" in text
    assert "observed count: unavailable" in text
    assert "qualifying values: unavailable" in text
    assert "evidence not materialized" in binding.explain(algebra=True)


def test_pathless_constraints_have_shacl_explanations():
    shapes = (
        PREFIXES + "ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:nodeKind sh:Literal ."
    )
    (mapping,) = shifty.shape_map(PREFIXES + "ex:a a ex:T .", shapes, infer=False)
    (binding,) = mapping.values()
    assert binding.explain() == "node_kind: node kind constraint failed"
    assert binding.explain(algebra=True) == "atom"
