"""Exact value and count projections, including canonical failure omissions."""

import pytest

import shifty
from shifty import Iri, Literal

PREFIXES = """
@prefix ex: <http://ex/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""


def make_binding(constraints, values="", *, session=True, extra="", sibling=False):
    other = "; sh:property [ sh:path ex:q ; sh:maxCount 1 ]" if sibling else ""
    shapes = (
        PREFIXES
        + f"""
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:name "slot" ; sh:path ex:p ; {constraints} ] {other} .
    """
    )
    data = PREFIXES + f"ex:a a ex:T {('; ex:p ' + values) if values else ''} . {extra}"
    origin = shifty.EvidenceSession(shapes, data, infer=False)
    (mapping,) = shifty.ShapeMap.from_run(
        origin.validate(), origin if session else None
    )
    return next(b for k, b in mapping.items() if str(k) != "q")


@pytest.mark.parametrize("sibling", [False, True])
@pytest.mark.parametrize("minimum", ["", "; sh:minCount 1"])
@pytest.mark.parametrize(
    "values, accepted, rejected, observed, ok",
    [
        ('"a-value"', [], [Literal("a-value")], 1, False),
        ('"x-value"', [Literal("x-value")], [], 1, True),
        ('"a-value", "x-value"', [Literal("x-value")], [Literal("a-value")], 2, False),
    ],
)
def test_pattern_keeps_accepted_and_rejected_separate(
    sibling, minimum, values, accepted, rejected, observed, ok
):
    binding = make_binding(f'sh:pattern "x" {minimum}', values, sibling=sibling)
    assert binding.ok is ok
    assert binding.values == accepted
    assert binding.partial_values == ([] if ok else accepted)
    assert binding.rejected_values == rejected
    assert binding.observed == observed
    assert binding.missing == 0


@pytest.mark.parametrize("with_session", [False, True])
@pytest.mark.parametrize("values, expected", [("", 0), ('"x"', 1)])
def test_successful_universal_observes_all_checked_values(
    with_session, values, expected
):
    binding = make_binding('sh:pattern "x"', values, session=with_session)
    assert binding.ok
    assert binding.observed == expected
    assert binding.values == ([] if expected == 0 else [Literal("x")])


def test_failed_universal_without_session_has_unknown_qualifying_set():
    binding = make_binding('sh:pattern "x"', '"a", "x"', session=False)
    assert not binding.ok
    assert binding.values is None
    assert binding.partial_values is None
    assert binding.rejected_values == [Literal("a")]
    assert binding.observed is None


@pytest.mark.parametrize("with_session", [False, True])
def test_overflow_count_is_exact(with_session):
    binding = make_binding("sh:maxCount 1", '"one", "two"', session=with_session)
    assert not binding.ok
    assert binding.observed == 2
    assert binding.values == [Literal("one"), Literal("two")]
    assert binding.partial_values == binding.values
    assert binding.rejected_values == []
    assert binding.missing == 0


def test_ordinary_cardinality_counts_values_rejected_by_datatype():
    binding = make_binding("sh:datatype xsd:string ; sh:minCount 3", '"good", 3')
    assert binding.observed == 2
    assert binding.values == [Literal("good")]
    assert binding.partial_values == binding.values
    assert binding.rejected_values == [
        Literal("3", "http://www.w3.org/2001/XMLSchema#integer")
    ]
    assert binding.missing == 1


@pytest.mark.parametrize("with_session", [False, True])
@pytest.mark.parametrize(
    "values, expected, rejected, observed, missing",
    [
        ("", [], [], 0, 1),
        ("ex:other", [], [Iri("http://ex/other")], 0, 1),
        ("ex:required, ex:other", [Iri("http://ex/required")], [], 1, 0),
    ],
)
def test_has_value_only_projects_qualifying_matches(
    with_session, values, expected, rejected, observed, missing
):
    binding = make_binding("sh:hasValue ex:required", values, session=with_session)
    assert binding.ok is (missing == 0)
    assert binding.values == expected
    assert binding.partial_values == ([] if binding.ok else expected)
    assert binding.rejected_values == rejected
    assert binding.observed == observed
    assert binding.missing == missing


def test_has_value_qualification_survives_an_unqualified_count():
    binding = make_binding("sh:hasValue ex:required ; sh:minCount 1", "ex:other")
    assert not binding.ok
    assert binding.values == binding.partial_values == []
    assert binding.rejected_values == [Iri("http://ex/other")]
    assert binding.observed is None
    assert binding.missing == 1


def test_multiple_has_values_keep_matches_from_each_count_domain():
    binding = make_binding(
        "sh:hasValue ex:first, ex:second, ex:missing", "ex:first, ex:second, ex:other"
    )
    assert not binding.ok
    assert (
        binding.values
        == binding.partial_values
        == [
            Iri("http://ex/first"),
            Iri("http://ex/second"),
        ]
    )
    assert binding.rejected_values == [Iri("http://ex/other")]
    assert binding.observed is None
    assert binding.missing == 1


@pytest.mark.parametrize("with_session", [False, True])
@pytest.mark.parametrize("bound", ["sh:qualifiedMinCount 2", "sh:qualifiedMaxCount 0"])
def test_qualified_counts_observe_qualifying_values(with_session, bound):
    binding = make_binding(
        f"sh:qualifiedValueShape [ sh:class ex:V ] ; {bound}",
        "ex:good, ex:other",
        session=with_session,
        extra="ex:good a ex:V . ex:other a ex:Other .",
    )
    assert not binding.ok
    assert binding.observed == 1
    assert binding.values == [Iri("http://ex/good")]


@pytest.mark.parametrize("with_session", [False, True])
def test_distinct_count_domains_do_not_report_a_single_count(with_session):
    binding = make_binding(
        "sh:minCount 2 ; sh:qualifiedValueShape [ sh:class ex:V ] ; sh:qualifiedMinCount 1",
        "ex:good, ex:other",
        session=with_session,
        extra="ex:good a ex:V . ex:other a ex:Other .",
    )
    assert binding.ok
    assert binding.observed is None
    if with_session:
        assert binding.values == [Iri("http://ex/good")]


def test_optional_qualified_value_shape_still_extracts_and_counts_matches():
    binding = make_binding(
        "sh:qualifiedValueShape [ sh:class ex:V ]",
        "ex:good, ex:other",
        extra="ex:good a ex:V . ex:other a ex:Other .",
    )
    assert binding.ok
    assert binding.values == [Iri("http://ex/good")]
    assert binding.observed == 1


@pytest.mark.parametrize("qualified_bound", ["sh:qualifiedMinCount 2 ;", ""])
def test_qualified_disjointness_is_applied_to_extraction_and_counts(qualified_bound):
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "first" ;
            sh:qualifiedValueShape [ sh:class ex:V ] ; QUALIFIED_BOUND
            sh:qualifiedValueShapesDisjoint true ] ;
        sh:property [ sh:path ex:p ; sh:name "second" ;
            sh:qualifiedValueShape [ sh:class ex:Other ] ; sh:qualifiedMinCount 1 ] .
    """
    ).replace("QUALIFIED_BOUND", qualified_bound)
    data = (
        PREFIXES
        + """
    ex:a a ex:T ; ex:p ex:good, ex:both .
    ex:good a ex:V . ex:both a ex:V, ex:Other .
    """
    )
    (mapping,) = shifty.shape_map(data, shapes, infer=False)
    binding = mapping.by_name("first")
    assert binding.ok is (not qualified_bound)
    assert binding.values == [Iri("http://ex/good")]
    assert binding.observed == 1
    assert binding.rejected_values == (
        [Iri("http://ex/both")] if qualified_bound else []
    )


def test_observation_is_lazy_and_shared_by_values_and_counts():
    class Proxy:
        calls = 0

        def __init__(self, raw):
            self.raw = raw

        def _binding_observation(self, focus, source):
            self.calls += 1
            return self.raw._binding_observation(focus, source)

        def __getattr__(self, name):
            return getattr(self.raw, name)

    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ] .
    """
    )
    data = PREFIXES + 'ex:a a ex:T ; ex:p "x", "bad" .'
    session = shifty.EvidenceSession(shapes, data, infer=False)
    proxy = Proxy(session._inner)
    (mapping,) = shifty.ShapeMap.from_run(session.validate(), proxy)
    binding = mapping["p"]
    assert proxy.calls == 0
    assert binding.observed == 2
    assert binding.values == binding.partial_values == [Literal("x")]
    assert binding.rejected_values == [Literal("bad")]
    assert proxy.calls == 1


def test_revalidated_run_never_recovers_values_from_the_original_snapshot():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ] .
    """
    )
    data = PREFIXES + 'ex:a a ex:T ; ex:p "x-before" .'
    session = shifty.EvidenceSession(shapes, data, infer=False)
    delta = shifty.RepairDelta.from_ntriples(
        add='<http://ex/a> <http://ex/p> "x-after" .',
        delete='<http://ex/a> <http://ex/p> "x-before" .',
    )
    after = session.revalidate(delta)
    (mapping,) = shifty.ShapeMap.from_run(after, session)
    assert mapping["p"].values == [Literal("x-after")]
    assert mapping["p"].observed == 1


@pytest.mark.parametrize("graph_mode", ["data", "union", "union-all"])
def test_value_recovery_respects_the_validation_graph_mode(graph_mode):
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ] .
    ex:a ex:p "x-from-shapes" .
    """
    )
    data = PREFIXES + 'ex:a a ex:T ; ex:p "bad" .'
    (mapping,) = shifty.shape_map(data, shapes, infer=False, graph_mode=graph_mode)
    binding = mapping["p"]
    assert binding.values == (
        [] if graph_mode == "data" else [Literal("x-from-shapes")]
    )
    assert binding.observed == (1 if graph_mode == "data" else 2)
    assert binding.rejected_values == [Literal("bad")]


@pytest.mark.parametrize("infer", [False, True])
def test_value_recovery_reads_the_inferred_snapshot(infer):
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ] ;
        sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
            sh:predicate ex:p ; sh:object "x-inferred" ] .
    """
    )
    data = PREFIXES + 'ex:a a ex:T ; ex:p "bad" .'
    (mapping,) = shifty.shape_map(data, shapes, infer=infer)
    binding = mapping["p"]
    assert binding.values == ([Literal("x-inferred")] if infer else [])
    assert binding.observed == (2 if infer else 1)
    assert binding.rejected_values == [Literal("bad")]
