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
    for node in [*passed.walk(), *failed.walk()]:
        assert isinstance(node.evidence_kind, shifty.EvidenceKind)
        assert node.kind == str(node.evidence_kind)
        assert node.status == node.evidence_kind.status
    assert shifty.EvidenceKind.NotHeld in {
        node.evidence_kind for node in passed.walk()
    }
    assert shifty.EvidenceKind.NotFailed in {
        node.evidence_kind for node in failed.walk()
    }

    encoded = run.to_json()
    decoded = run.to_dict()
    assert json.loads(encoded) == decoded
    focus_json = decoded["statements"][0]["selected_foci"]
    assert {item["evidence"]["status"] for item in focus_json} == {"pass", "fail"}


def test_evidence_kind_is_the_complete_typed_polarity_vocabulary():
    passing = [
        shifty.EvidenceKind.Irrefutable,
        shifty.EvidenceKind.AtomHeld,
        shifty.EvidenceKind.AllHeld,
        shifty.EvidenceKind.AnyHeld,
        shifty.EvidenceKind.CountHeld,
        shifty.EvidenceKind.AllValuesHeld,
        shifty.EvidenceKind.NotHeld,
        shifty.EvidenceKind.Blocked,
        shifty.EvidenceKind.Coinductive,
    ]
    failing = [
        shifty.EvidenceKind.AtomFailed,
        shifty.EvidenceKind.RelationalFailed,
        shifty.EvidenceKind.ClosedFailed,
        shifty.EvidenceKind.NotFailed,
        shifty.EvidenceKind.AllFailed,
        shifty.EvidenceKind.AnyFailed,
        shifty.EvidenceKind.CountLow,
        shifty.EvidenceKind.CountHigh,
        shifty.EvidenceKind.Opaque,
    ]

    assert len(set(passing + failing)) == 18
    assert {kind.status for kind in passing} == {"pass"}
    assert {kind.status for kind in failing} == {"fail"}
    assert str(shifty.EvidenceKind.AllValuesHeld) == "all_values_held"
    assert str(shifty.EvidenceKind.CountHigh) == "count_high"


def test_disjunction_evidence_obeys_the_boolean_duality_law():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x, ex:y ;
        sh:or ( [ sh:class ex:A ] [ sh:class ex:B ] ) .
    """
    data = PREFIXES + "ex:x a ex:A . ex:y a ex:C ."
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()
    results = {result.focus: result for result in selected_foci(run)}

    passing = results["<http://ex/x>"].evidence.walk()
    failing = results["<http://ex/y>"].evidence.walk()
    assert shifty.EvidenceKind.AnyHeld in {
        node.evidence_kind for node in passing
    }
    assert shifty.EvidenceKind.AnyFailed in {
        node.evidence_kind for node in failing
    }
    # A holding disjunction retains only holding branches, all on the positive
    # side. A failed disjunction retains every branch, all on the negative side.
    assert {node.status for node in passing} == {"pass"}
    assert {node.status for node in failing} == {"fail"}
    assert sum(
        node.evidence_kind == shifty.EvidenceKind.CountLow for node in failing
    ) >= 2


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


def projection_fixture():
    """One focus that passes one statement and fails another, plus a second focus."""
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 3 ] .
    ex:S2 a sh:NodeShape ; sh:targetClass ex:T ; sh:nodeKind sh:IRI .
    """
    data = PREFIXES + """
    ex:a a ex:T ; ex:p ex:v1, ex:v2 .
    ex:b a ex:T ; ex:p ex:v1, ex:v2, ex:v3 .
    """
    return shifty.EvidenceSession(shapes, data, infer=False).validate()


def test_focus_projections_split_by_polarity_and_ignore_angle_brackets():
    run = projection_fixture()

    results = run.results_for("http://ex/a")
    assert len(results) == 2
    assert {result.status for result in results} == {"pass", "fail"}
    assert [result.focus for result in results] == ["<http://ex/a>"] * 2
    # An IRI names the same focus with or without brackets.
    assert [r.status for r in run.results_for("<http://ex/a>")] == [
        r.status for r in results
    ]

    failures = run.failures_for("http://ex/a")
    satisfactions = run.satisfactions_for("http://ex/a")
    assert all(isinstance(failure, shifty.Failure) for failure in failures)
    assert all(isinstance(item, shifty.Satisfaction) for item in satisfactions)
    # The two polarities partition the results, and keep statement order.
    assert len(failures) + len(satisfactions) == len(results)
    assert [f.statement for f in failures] == sorted(f.statement for f in failures)

    # ex:b conforms, so it projects only on the satisfaction side.
    assert run.failures_for("http://ex/b") == []
    assert len(run.satisfactions_for("http://ex/b")) == 2

    # A focus no statement selected is empty, not an error.
    assert run.results_for("http://ex/unselected") == []
    assert run.failures_for("http://ex/unselected") == []
    assert run.satisfactions_for("http://ex/unselected") == []


def test_projections_agree_with_walking_the_statements():
    run = projection_fixture()
    walked = [
        (focus.focus, focus.status)
        for statement in run.statements
        for focus in statement.selected_foci
    ]
    projected = [
        (result.focus, result.status)
        for focus in ("http://ex/a", "http://ex/b")
        for result in run.results_for(focus)
    ]
    assert sorted(projected) == sorted(walked)

    # The projection hands back the very objects the statements hold, not copies.
    first = run.statements[0].selected_foci[0]
    assert any(result is first for result in run.results_for(first.focus))


def test_strict_lookups_resolve_one_or_refuse_to_guess():
    run = projection_fixture()

    # ex:a fails exactly one statement, so the strict lookup needs no hint.
    failure = run.failure_for("http://ex/a")
    assert isinstance(failure, shifty.Failure)
    assert failure.focus == "<http://ex/a>"
    assert run.failure_for("http://ex/a", statement=failure.statement) is not None

    # ex:b passes both statements, so the satisfaction side is ambiguous until a
    # statement picks one out.
    with pytest.raises(ValueError, match="pass statement="):
        run.satisfaction_for("http://ex/b")
    ids = [item.statement for item in run.satisfactions_for("http://ex/b")]
    assert run.satisfaction_for("http://ex/b", statement=ids[0]).statement == ids[0]

    # Misses name what was looked up, with and without a statement.
    with pytest.raises(ValueError, match="no failure for focus"):
        run.failure_for("http://ex/b")
    with pytest.raises(ValueError, match="under statement 99"):
        run.failure_for("http://ex/a", statement=99)
    with pytest.raises(ValueError, match="no satisfaction for focus"):
        run.satisfaction_for("http://ex/unselected")


def shape_fixture():
    """Named shapes that pass, fail, select nothing, and head no statement at
    all, plus one statement rooted at a blank shape."""
    shapes = PREFIXES + """
    ex:AHUShape a sh:NodeShape ; sh:targetClass ex:AHU ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    ex:VavShape a sh:NodeShape ; sh:targetClass ex:Vav ; sh:nodeKind sh:IRI .
    ex:UnusedShape a sh:NodeShape ; sh:nodeKind sh:IRI .
    ex:EmptyShape a sh:NodeShape ; sh:targetClass ex:Nobody ; sh:nodeKind sh:IRI .
    [] a sh:NodeShape ; sh:targetClass ex:Anon ; sh:nodeKind sh:IRI .
    """
    data = PREFIXES + """
    ex:a1 a ex:AHU ; ex:p ex:x .
    ex:a2 a ex:AHU .
    ex:v1 a ex:Vav .
    ex:n1 a ex:Anon .
    """
    return shapes, data, shifty.EvidenceSession(shapes, data, infer=False).validate()


def test_shape_iri_names_the_authored_shape_and_is_none_when_blank():
    _, _, run = shape_fixture()

    assert [statement.shape_iri for statement in run.statements] == [
        "http://ex/AHUShape",
        "http://ex/EmptyShape",
        "http://ex/VavShape",
        None,
    ]
    # The evidence objects agree with the statement that produced them.
    failure = run.failure_for("http://ex/a2")
    assert failure.shape_iri == "http://ex/AHUShape"
    assert run.satisfaction_for("http://ex/v1").shape_iri == "http://ex/VavShape"

    # A blank-rooted shape has no name, but its foci are still in the run.
    blank = [st for st in run.statements if st.shape_iri is None]
    assert [focus.focus for st in blank for focus in st.selected_foci] == [
        "<http://ex/n1>"
    ]


def test_covered_shapes_lists_named_statement_shapes_in_statement_order():
    _, _, run = shape_fixture()

    covered = run.covered_shapes()
    assert covered == ["http://ex/AHUShape", "http://ex/EmptyShape", "http://ex/VavShape"]
    # Exactly the named shapes the statements head — no blank-rooted shape, and
    # not ex:UnusedShape, which heads no statement.
    assert covered == [
        st.shape_iri for st in run.statements if st.shape_iri is not None
    ]
    # A statement that selected nothing still covers its shape.
    empty = next(st for st in run.statements if st.shape_iri == "http://ex/EmptyShape")
    assert empty.selected_foci == []
    assert "http://ex/EmptyShape" in covered


def test_shape_scoped_projections_split_by_polarity():
    _, _, run = shape_fixture()

    results = run.results_for_shape("http://ex/AHUShape")
    assert [(r.focus, r.status) for r in results] == [
        ("<http://ex/a1>", "pass"),
        ("<http://ex/a2>", "fail"),
    ]
    assert [f.focus for f in run.failures_for_shape("http://ex/AHUShape")] == [
        "<http://ex/a2>"
    ]
    assert [s.focus for s in run.satisfactions_for_shape("http://ex/AHUShape")] == [
        "<http://ex/a1>"
    ]
    # Angle brackets are optional, as they are for a focus.
    assert [f.focus for f in run.failures_for_shape("<http://ex/AHUShape>")] == [
        "<http://ex/a2>"
    ]
    # Every projected evaluation reports the shape it was projected by.
    assert {f.shape_iri for f in run.failures_for_shape("http://ex/AHUShape")} == {
        "http://ex/AHUShape"
    }


def test_shape_scoped_projection_agrees_with_revalidating_under_shape_names():
    shapes, data, run = shape_fixture()
    scoped = shifty.EvidenceSession(shapes, data, infer=False).validate(
        shape_names=["http://ex/AHUShape"]
    )

    projected = sorted(
        (r.focus, r.status) for r in run.results_for_shape("http://ex/AHUShape")
    )
    revalidated = sorted(
        (focus.focus, focus.status)
        for statement in scoped.statements
        for focus in statement.selected_foci
    )
    assert projected == revalidated


def test_an_unknown_shape_raises_while_an_uncovered_one_projects_empty():
    _, _, run = shape_fixture()

    # Named, has a statement, selected nothing.
    assert run.results_for_shape("http://ex/EmptyShape") == []
    # Named in the schema, but heads no statement — still not an error.
    assert run.results_for_shape("http://ex/UnusedShape") == []
    assert "http://ex/UnusedShape" not in run.covered_shapes()
    # Names no shape at all: a typo is reported rather than silently empty.
    with pytest.raises(ValueError, match="no shape named"):
        run.results_for_shape("http://ex/Typo")
    with pytest.raises(ValueError, match="no shape named"):
        run.failures_for_shape("http://ex/Typo")
    with pytest.raises(ValueError, match="no shape named"):
        run.satisfactions_for_shape("http://ex/Typo")


def test_values_for_path_projects_matched_values_without_parsing_text():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [ sh:path ex:p ; sh:minCount 3 ] ;
        sh:property [ sh:path ex:q ; sh:minCount 1 ] .
    ex:S2 a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [ sh:path ex:q ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:x ex:p ex:v1, ex:v2 ; ex:q ex:w ."
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()

    failure = run.failure_for("http://ex/x")
    assert failure.values_for_path("<http://ex/p>") == [
        "<http://ex/v1>",
        "<http://ex/v2>",
    ]
    # The bare IRI names the same single-predicate path as the bracketed form.
    assert failure.values_for_path("http://ex/p") == failure.values_for_path(
        "<http://ex/p>"
    )
    # Every value belongs to some path, and no path invents one.
    assert failure.values_for_path("<http://ex/absent>") == []
    assert set(failure.values_for_path("<http://ex/p>")) <= set(
        failure.matched_values()
    )

    satisfaction = run.satisfaction_for("http://ex/x")
    assert satisfaction.values_for_path("<http://ex/q>") == ["<http://ex/w>"]
    assert satisfaction.values_for_path("<http://ex/p>") == []


def obligation_fixture():
    """A qualified count that is short, with one reached-but-rejected candidate."""
    shapes = PREFIXES + """
    ex:AHUShape a sh:NodeShape ; sh:targetClass ex:AHU ;
        sh:property [ sh:path ex:hasPoint ;
                      sh:qualifiedValueShape [ sh:class ex:Temp ] ;
                      sh:qualifiedMinCount 2 ] .
    """
    data = PREFIXES + """
    ex:ahu1 a ex:AHU ; ex:hasPoint ex:t1, ex:other .
    ex:t1 a ex:Temp .
    ex:other a ex:Flow .
    """
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()
    return run.failure_for("http://ex/ahu1")


def test_a_missing_obligation_describes_the_edge_that_would_close_it():
    failure = obligation_fixture()
    on_focus = [o for o in failure.missing_obligations() if o.node == failure.focus]
    assert len(on_focus) == 1

    obligation = on_focus[0]
    assert obligation.node == "<http://ex/ahu1>"
    assert obligation.path == "<http://ex/hasPoint>"
    assert (obligation.observed_count, obligation.required_count) == (1, 2)
    assert obligation.missing == 1
    # What an added value must satisfy, structured — no explain() parsing.
    assert obligation.qualifier.kind == shifty.ConstraintKind.ClassMembership
    assert obligation.qualifier.definition == "instance of <http://ex/Temp>"
    assert isinstance(obligation.qualifier.id, int)


def test_a_nested_deficit_reports_its_own_node_not_the_focus():
    failure = obligation_fixture()
    obligations = failure.missing_obligations()

    # The rejected candidate's own class check is short too. Before the node was
    # reported, the two deficits were indistinguishable without reading text.
    nodes = {o.node for o in obligations}
    assert nodes == {"<http://ex/ahu1>", "<http://ex/other>"}
    nested = next(o for o in obligations if o.node == "<http://ex/other>")
    assert nested.node != failure.focus
    assert nested.path == "rdf:type/rdfs:subClassOf*"


def test_an_obligation_path_round_trips_into_values_for_path():
    failure = obligation_fixture()
    obligation = next(
        o for o in failure.missing_obligations() if o.node == failure.focus
    )

    # The rendered path is the same spelling values_for_path accepts, so a driver
    # can go from "what is missing" to "what is already there" without parsing.
    already = failure.values_for_path(obligation.path)
    assert already == ["<http://ex/t1>"]
    assert len(already) == obligation.observed_count


def test_values_for_path_addresses_a_sequence_path_by_its_rendered_form():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [ sh:path (ex:p ex:q) ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:x ex:p ex:y . ex:y ex:q ex:z ."
    run = shifty.EvidenceSession(shapes, data, infer=False).validate()
    satisfaction = run.satisfaction_for("http://ex/x")

    assert satisfaction.matched_values() == ["<http://ex/z>"]
    assert satisfaction.values_for_path("<http://ex/p>/<http://ex/q>") == [
        "<http://ex/z>"
    ]
    # A prefix of the sequence is not the path that was counted.
    assert satisfaction.values_for_path("<http://ex/p>") == []


RULE_SHAPES = PREFIXES + """
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
ex:SensorRule a sh:NodeShape ; sh:targetClass ex:Sensor ;
    sh:rule [ a sh:TripleRule ;
              sh:subject sh:this ;
              sh:predicate rdf:type ;
              sh:object ex:Point ] .
ex:AHUShape a sh:NodeShape ; sh:targetClass ex:AHU ;
    sh:property [ sh:path ex:hasPoint ;
                  sh:qualifiedValueShape [ sh:class ex:Point ] ;
                  sh:qualifiedMinCount 1 ] .
"""

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

# Adds a point that only a rule makes an ex:Point.
ADD_SENSOR = (
    "<http://ex/ahu1> <http://ex/hasPoint> <http://ex/s1> .\n"
    f"<http://ex/s1> {RDF_TYPE} <http://ex/Sensor> ."
)


def test_revalidate_returns_the_run_the_edit_would_produce():
    shapes = PREFIXES + """
    ex:AHUShape a sh:NodeShape ; sh:targetClass ex:AHU ;
        sh:property [ sh:path ex:hasPoint ; sh:minCount 2 ] .
    """
    data = PREFIXES + "ex:ahu1 a ex:AHU ; ex:hasPoint ex:t1 ."
    session = shifty.EvidenceSession(shapes, data, infer=False)
    before = session.validate()
    assert not before.conforms

    delta = shifty.RepairDelta.from_ntriples(
        add="<http://ex/ahu1> <http://ex/hasPoint> <http://ex/t2> ."
    )
    after = session.revalidate(delta)

    # A full run, so every projection works on it.
    assert isinstance(after, shifty.EvidenceRun)
    assert after.conforms
    assert after.failures_for("http://ex/ahu1") == []
    assert after.satisfaction_for("http://ex/ahu1").values_for_path(
        "<http://ex/hasPoint>"
    ) == ["<http://ex/t1>", "<http://ex/t2>"]
    assert after.covered_shapes() == before.covered_shapes()

    # Pure: the session keeps its own snapshot and `before` stays valid.
    assert not session.validate().conforms
    assert not before.conforms
    assert [f.focus for f in before.failures_for("http://ex/ahu1")] == [
        "<http://ex/ahu1>"
    ]


def test_revalidate_reruns_rules_by_default_and_can_skip_them():
    data = PREFIXES + "ex:ahu1 a ex:AHU ."
    session = shifty.EvidenceSession(RULE_SHAPES, data)
    assert not session.validate().conforms

    delta = shifty.RepairDelta.from_ntriples(add=ADD_SENSOR)
    # ex:s1 is only an ex:Point once the rule fires over the patched graph.
    assert session.revalidate(delta).conforms
    assert not session.revalidate(delta, infer=False).conforms


def test_revalidate_with_inference_drops_a_derivation_the_edit_invalidated():
    data = PREFIXES + "ex:ahu1 a ex:AHU ; ex:hasPoint ex:s1 . ex:s1 a ex:Sensor ."
    session = shifty.EvidenceSession(RULE_SHAPES, data)
    assert session.validate().conforms, "ex:s1 is derived to be an ex:Point"

    # Deleting the support must take the derivation with it. Inference only
    # adds, so this is only right because the rules re-run over the graph as it
    # was before they last ran.
    drop = shifty.RepairDelta.from_ntriples(
        delete=f"<http://ex/s1> {RDF_TYPE} <http://ex/Sensor> ."
    )
    assert not session.revalidate(drop).conforms

    # Skipping them patches the already-derived graph, where the stale triple
    # survives — cheaper, and wrong for an edit that feeds a rule.
    assert session.revalidate(drop, infer=False).conforms


def test_revalidate_defaults_to_the_sessions_own_inference_setting():
    data = PREFIXES + "ex:ahu1 a ex:AHU ."
    session = shifty.EvidenceSession(RULE_SHAPES, data, infer=False)
    delta = shifty.RepairDelta.from_ntriples(add=ADD_SENSOR)

    # Defaulting to the session keeps before and after on one baseline: a
    # session that never ran the rules does not start now.
    assert not session.revalidate(delta).conforms
    # But the override is there.
    assert session.revalidate(delta, infer=True).conforms


def test_revalidate_takes_the_same_options_as_validate():
    data = PREFIXES + "ex:ahu1 a ex:AHU . ex:v1 a ex:Vav ."
    shapes = RULE_SHAPES + """
    ex:VavShape a sh:NodeShape ; sh:targetClass ex:Vav ; sh:nodeKind sh:BlankNode .
    """
    session = shifty.EvidenceSession(shapes, data)
    delta = shifty.RepairDelta.from_ntriples(add=ADD_SENSOR)

    scoped = session.revalidate(delta, shape_names=["http://ex/AHUShape"])
    assert scoped.covered_shapes() == ["http://ex/AHUShape"]
    assert scoped.conforms, "the ex:Vav failure is out of scope"


def ondemand_fixture():
    """Two authored statements that normalize to one, over three foci."""
    shapes = PREFIXES + """
    ex:S1 a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    ex:S2 a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:good a ex:T ; ex:p ex:v . ex:bad a ex:T . ex:bad2 a ex:T ."
    return shifty.EvidenceSession(shapes, data, infer=False)


def test_conformance_counts_agree_with_a_full_run():
    session = ondemand_fixture()
    counts = session.validate_conformance()
    run = session.validate()

    assert isinstance(counts, shifty.ConformanceRun)
    assert counts.conforms is run.conforms is False
    assert not counts

    # The counts are over normalized statements — a merged statement is decided
    # once — while the run reports a focus row per *authored* statement. Here
    # ex:S1 and ex:S2 state the same constraint, so 6 rows are 3 decisions.
    rows = [(st.normalized_statement_id, f) for st in run.statements for f in st.selected_foci]
    assert len(rows) == 6
    decided = {(sid, f.focus) for sid, f in rows}
    assert counts.selected_pairs == len(decided) == 3
    assert counts.passed == len({(s, f.focus) for s, f in rows if f.status == "pass"})
    assert counts.failed == len({(s, f.focus) for s, f in rows if f.status == "fail"})


def test_find_failures_hands_back_exactly_the_failing_pairs():
    session = ondemand_fixture()
    counts, pairs = session.find_failures()

    assert counts.failed == counts.selected_pairs - counts.passed
    # One handle per failing *normalized* pair, deduplicated by construction:
    # two authored statements share one normalized statement here.
    assert {p.focus for p in pairs} == {"<http://ex/bad>", "<http://ex/bad2>"}
    assert all(isinstance(p, shifty.SelectedPair) for p in pairs)
    assert "<http://ex/good>" not in {p.focus for p in pairs}


def test_a_selected_pair_separates_normalized_from_authored_statements():
    session = ondemand_fixture()
    _, pairs = session.find_failures()
    pair = pairs[0]

    # The two authored statements collapsed to one normalized statement, so the
    # handle names one of the former and both of the latter. A single bare
    # `statement` field could not have said this.
    assert pair.normalized_statement == 0
    assert pair.source_statements == [0, 1]
    assert not hasattr(pair, "statement")


def test_explain_materializes_one_pair_as_a_usable_run():
    session = ondemand_fixture()
    _, pairs = session.find_failures()
    pair = next(p for p in pairs if p.focus == "<http://ex/bad>")

    one = session.explain(pair)
    assert isinstance(one, shifty.EvidenceRun)
    assert not one.conforms
    # One statement per authored statement that normalizes to the pair's.
    assert [st.source_statement_id for st in one.statements] == pair.source_statements
    assert [st.shape_iri for st in one.statements] == [
        "http://ex/S1",
        "http://ex/S2",
    ]

    # Every projection works on it, including the strict ones.
    assert [f.focus for f in one.failures_for(pair.focus)] == [pair.focus] * 2
    failure = one.failure_for(pair.focus, statement=0)
    assert [(o.node, o.path, o.missing) for o in failure.missing_obligations()] == [
        ("<http://ex/bad>", "<http://ex/p>", 1)
    ]

    # And the evidence is the same as the full run's for that pair.
    from_run = session.validate().failure_for(pair.focus, statement=0)
    assert failure.to_dict() == from_run.to_dict()


def test_explain_canonical_drops_the_progress_view():
    session = ondemand_fixture()
    _, pairs = session.find_failures()

    with_progress = session.explain(pairs[0])
    without = session.explain_canonical(pairs[0])
    assert [f.progress for st in without.statements for f in st.selected_foci] == [
        None,
        None,
    ]
    # The evidence itself is unchanged; only the progress view is dropped.
    assert [f.evidence.to_dict() for st in without.statements for f in st.selected_foci] == [
        f.evidence.to_dict() for st in with_progress.statements for f in st.selected_foci
    ]


def test_explain_omits_the_catalog_and_constraints_serves_it_once():
    session = ondemand_fixture()
    _, pairs = session.find_failures()

    # Fixed per snapshot, so a per-pair run does not carry it.
    assert session.explain(pairs[0]).to_dict()["constraints"] == {
        "source": [],
        "normalized": [],
    }

    catalog = session.constraints()
    assert set(catalog) == {"source", "normalized"}
    assert catalog["source"] and catalog["normalized"]

    # Which closes the out-of-band loop: a run can now travel without its
    # catalog and be expanded against the session's copy, with no need to
    # materialize a full run just to obtain one.
    run = session.validate()
    wire = run.to_compact_json(include_catalog=False)
    assert shifty.expand_evidence(wire, catalog) == run.to_dict()


def test_the_cheap_entry_points_take_shape_names():
    # Distinct constraints, so nothing merges and scoping is unambiguous.
    shapes = PREFIXES + """
    ex:AHUShape a sh:NodeShape ; sh:targetClass ex:AHU ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    ex:VavShape a sh:NodeShape ; sh:targetClass ex:Vav ;
        sh:property [ sh:path ex:q ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:a1 a ex:AHU . ex:v1 a ex:Vav ."
    session = shifty.EvidenceSession(shapes, data, infer=False)

    assert session.validate_conformance().selected_pairs == 2
    scoped = session.validate_conformance(shape_names=["http://ex/AHUShape"])
    assert (scoped.selected_pairs, scoped.failed) == (1, 1)

    _, pairs = session.find_failures(shape_names=["http://ex/AHUShape"])
    assert [p.focus for p in pairs] == ["<http://ex/a1>"]


COLLAPSING_SHAPES = PREFIXES + """
ex:S1 a sh:NodeShape ; sh:targetClass ex:T ;
    sh:property [ sh:path ex:p ; sh:minCount 1 ] .
ex:S2 a sh:NodeShape ; sh:targetClass ex:T ;
    sh:property [ sh:path ex:p ; sh:minCount 1 ] .
"""
COLLAPSING_DATA = PREFIXES + "ex:bad a ex:T ."


def test_shape_names_reaches_a_shape_that_cse_collapsed_onto_another():
    # ex:S1 and ex:S2 state the same constraint, so normalization merges them.
    # Each name must still select its own statement and only its own.
    session = shifty.EvidenceSession(COLLAPSING_SHAPES, COLLAPSING_DATA, infer=False)

    assert [st.shape_iri for st in session.validate().statements] == [
        "http://ex/S1",
        "http://ex/S2",
    ]
    for name in ("http://ex/S1", "http://ex/S2"):
        scoped = session.validate(shape_names=[name])
        assert [st.shape_iri for st in scoped.statements] == [name]
        assert scoped.covered_shapes() == [name]
        assert session.validate_conformance(shape_names=[name]).selected_pairs == 1


def test_scoped_failure_handle_explains_only_the_selected_authored_statement():
    session = shifty.EvidenceSession(COLLAPSING_SHAPES, COLLAPSING_DATA, infer=False)

    _, pairs = session.find_failures(shape_names=["http://ex/S1"])
    assert len(pairs) == 1
    assert pairs[0].source_statements == [0]
    assert [st.shape_iri for st in session.explain(pairs[0]).statements] == [
        "http://ex/S1"
    ]


def test_a_collapsed_shape_is_still_addressable_by_either_name():
    # The reverse lookup goes through the same name table, so a shape that lost
    # a name to the merge used to be unfindable by it.
    session = shifty.RepairSession(COLLAPSING_SHAPES, COLLAPSING_DATA, infer=False)
    for name in ("http://ex/S1", "http://ex/S2"):
        assert [w.focus for w in session.witnesses_for(name)] == ["<http://ex/bad>"]
    with pytest.raises(ValueError, match="no shape named"):
        session.witnesses_for("http://ex/Nope")


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


def test_a_dangling_compact_reference_is_rejected():
    encoded = {
        "v": 1,
        "conforms": False,
        "terms": [],
        "nodes": [],
        "statements": {"#": 999},
        "constraints": [],
    }
    with pytest.raises(ValueError, match="invalid node reference 999"):
        shifty.expand_evidence(encoded)
