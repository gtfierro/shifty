"""Statement-oriented evidence, provenance, progress, and projections."""

import json

import pytest

import shifty


PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
"""


def selected_foci(run):
    return [focus for statement in run.statements for focus in statement.selected_foci]


def test_statement_grouping_partition_and_empty_selection():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    ex:Empty a sh:NodeShape ; sh:targetClass ex:Missing ; sh:nodeKind sh:IRI .
    """
    data = PREFIXES + """
    ex:good a ex:T ; ex:p ex:value .
    ex:bad a ex:T .
    ex:unselected ex:p ex:value .
    """
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()

    assert not run.conforms
    assert len(run.statements) == 2
    assert sorted(len(statement.selected_foci) for statement in run.statements) == [0, 2]
    foci = selected_foci(run)
    assert {focus.status for focus in foci} == {"pass", "fail"}
    assert all(
        isinstance(focus.evidence, shifty.Satisfaction)
        if focus.status == "pass"
        else isinstance(focus.evidence, shifty.Failure)
        for focus in foci
    )
    assert all("unselected" not in focus.focus for focus in foci)


def test_duplicate_source_statements_keep_provenance_and_share_normalized_identity():
    shapes = PREFIXES + """
    ex:S1 a sh:NodeShape ; sh:targetNode ex:x ; sh:nodeKind sh:IRI .
    ex:S2 a sh:NodeShape ; sh:targetNode ex:x ; sh:nodeKind sh:IRI .
    """
    run = shifty.EvidenceSession(shapes, infer=False).validate()

    assert [statement.source_statement_id for statement in run.statements] == [0, 1]
    assert run.statements[0].normalized_statement_id == run.statements[1].normalized_statement_id
    assert run.statements[0].normalized_constraint_id == run.statements[1].normalized_constraint_id
    assert all(len(statement.selected_foci) == 1 for statement in run.statements)


def test_failed_conjunction_has_compact_failure_and_sibling_progress():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:nodeKind sh:IRI ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    """
    run = shifty.EvidenceSession(shapes, PREFIXES, infer=False).validate()
    focus = run.statements[0].selected_foci[0]

    assert focus.status == "fail"
    assert isinstance(focus.evidence, shifty.Failure)
    assert focus.progress is not None
    assert [child.status for child in focus.progress.evaluated_children] == ["pass", "fail"]
    assert [node.status for node in focus.evidence.walk()][0] == "fail"
    assert focus.evidence.missing_obligations()[0].missing == 1


def test_qualified_count_retains_matches_rejections_support_and_nested_evidence():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [
            sh:path ex:p ;
            sh:qualifiedValueShape [ sh:class ex:C ] ;
            sh:qualifiedMinCount 2
        ] .
    """
    data = PREFIXES + "ex:x ex:p ex:good, ex:near . ex:good a ex:C ."
    failure = shifty.EvidenceSession(shapes, data, infer=False).validate().statements[0].selected_foci[0].evidence

    assert isinstance(failure, shifty.Failure)
    assert failure.matched_values()[0] == "<http://ex/good>"
    assert "<http://ex/near>" in failure.offending_values()
    assert failure.missing_obligations()[0].missing == 1
    assert any("<http://ex/p>" in triple for triple in failure.supporting_triples())
    assert {node.status for node in failure.walk()} == {"pass", "fail"}
    assert all(isinstance(item, shifty.PathSupport) for item in failure.path_supports())
    assert all("status" in node.to_dict() for node in failure.walk())


def test_negation_crossing_and_tagged_json_are_structured():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ; sh:not [ sh:class ex:C ] .
    """
    data = PREFIXES + "ex:pass a ex:T . ex:fail a ex:T, ex:C ."
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()
    foci = selected_foci(run)

    passed = next(focus.evidence for focus in foci if focus.status == "pass")
    failed = next(focus.evidence for focus in foci if focus.status == "fail")
    assert {node.status for node in passed.walk()} == {"pass", "fail"}
    assert {node.status for node in failed.walk()} == {"pass", "fail"}

    encoded = run.to_json()
    decoded = run.to_dict()
    assert json.loads(encoded) == decoded
    focus_json = decoded["statements"][0]["selected_foci"]
    assert {item["evidence"]["status"] for item in focus_json} == {"pass", "fail"}


def test_repeated_validate_is_stable_and_matches_ordinary_validation():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [ sh:path (ex:p ex:q) ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:x ex:p ex:y . ex:y ex:q ex:z ."
    session = shifty.EvidenceSession(shapes, data, infer=False)
    first = session.validate()
    second = session.validate()
    ordinary = shifty.validate_algebra(data, shapes, infer=False)

    assert first.conforms == second.conforms == ordinary.conforms
    assert first.to_json() == second.to_json()
    satisfaction = first.statements[0].selected_foci[0].evidence
    assert satisfaction.matched_values() == ["<http://ex/z>"]
    assert len(satisfaction.supporting_triples()) == 2


def test_negative_recursive_cycle_is_rejected():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:not [ sh:path ex:p ; sh:qualifiedValueShape ex:S ;
                 sh:qualifiedMinCount 1 ] .
    ex:x ex:p ex:x .
    """
    with pytest.raises(ValueError, match="non-stratifiable"):
        shifty.EvidenceSession(shapes, infer=False)


def compaction_fixture():
    """A run with repeated subtrees: two shapes stating the same constraint."""
    shapes = PREFIXES + """
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] ;
        sh:property [ sh:path ex:n ; sh:datatype xsd:integer ] .
    ex:S2 a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] .
    """
    data = PREFIXES + """
    ex:good a ex:T ; ex:p ex:c1 ; ex:n 3 ; ex:label "hi"@en .
    ex:bad a ex:T ; ex:n "not a number" .
    ex:c1 a ex:C .
    """
    return shifty.EvidenceSession(shapes, data, infer=False).validate()


def test_compact_round_trips_through_expand():
    run = compaction_fixture()
    restored = shifty.expand_evidence(run.to_compact_json())
    assert restored == run.to_dict()


def test_compact_accepts_and_returns_either_text_or_dicts():
    run = compaction_fixture()
    from_text = shifty.expand_evidence(run.to_compact_json())
    from_dict = shifty.expand_evidence(run.to_compact_dict())
    assert from_text == from_dict == run.to_dict()

    as_text = shifty.expand_evidence(run.to_compact_json(), as_dict=False)
    assert isinstance(as_text, str)
    assert json.loads(as_text) == run.to_dict()


def test_catalog_can_travel_out_of_band():
    run = compaction_fixture()
    wire = run.to_compact_json(include_catalog=False)
    assert "constraints" not in json.loads(wire)

    catalog = run.to_dict()["constraints"]
    assert shifty.expand_evidence(wire, catalog) == run.to_dict()

    # Without the catalog the encoding cannot be completed, and says so.
    with pytest.raises(ValueError, match="catalog"):
        shifty.expand_evidence(wire)


def test_compact_is_smaller_and_elides_the_catalog():
    run = compaction_fixture()
    full = len(run.to_json())
    packed = len(run.to_compact_json())
    without_catalog = len(run.to_compact_json(include_catalog=False))
    assert without_catalog < packed < full
    # Repeated subtrees collapse: fewer stored nodes than emitted ones.
    stored = len(run.to_compact_dict()["nodes"])
    emitted = sum(
        len(focus.evidence.walk())
        for statement in run.statements
        for focus in statement.selected_foci
    )
    assert stored < emitted


def test_a_foreign_version_is_rejected():
    run = compaction_fixture()
    encoded = run.to_compact_dict()
    encoded["v"] = 999
    with pytest.raises(ValueError, match="version"):
        shifty.expand_evidence(encoded)
