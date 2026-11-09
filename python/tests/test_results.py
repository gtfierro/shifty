"""Public result contracts and focused reason/provenance regressions.

Input admission lives in test_errors.py, and diagnostic propagation across
interfaces lives in test_interface_agreement.py.
"""

import pytest
import rdflib
from rdflib.compare import isomorphic

import shifty

PREFIXES = """\
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:  <http://example.org/> .
"""

SHAPES = (
    PREFIXES
    + """\
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] ;
    sh:property [
        sh:path ex:age ;
        sh:maxCount 1 ;
        sh:datatype xsd:integer ;
    ] .
"""
)


VALID_DATA = (
    PREFIXES
    + """\
ex:Alice a ex:Person ; ex:name "Alice" ; ex:age 30 .
"""
)

VIOLATION_DATA = (
    PREFIXES
    + """\
ex:Bob a ex:Person .
"""
)


# ── InferResult tests ────────────────────────────────────────────────────────


@pytest.mark.parametrize("target, expected_count", [("Thing", 1), ("NonExistent", 0)])
def test_infer_result_contract(target, expected_count):
    shapes = (
        PREFIXES
        + f"""
        ex:S a sh:NodeShape ; sh:targetClass ex:{target} ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:derived ; sh:object ex:Thing ] .
    """
    )
    data = PREFIXES + "ex:a a ex:Thing ; ex:original ex:value ."
    result = shifty.infer(data.encode(), shapes.encode())

    assert isinstance(result, shifty.InferResult)
    assert isinstance(result.inferred_count, int)
    assert result.inferred_count == expected_count
    assert isinstance(result.diagnostics, list)
    assert result.diagnostics == []
    assert f"inferred={expected_count}" in repr(result)

    assert isinstance(result.graph_ntriples, str)
    serialized = rdflib.Graph().parse(data=result.graph_ntriples, format="nt")
    graph = result.graph()
    assert isinstance(graph, rdflib.Graph)
    expected = rdflib.Graph().parse(data=data, format="turtle")
    EX = rdflib.Namespace("http://example.org/")
    if expected_count:
        expected.add((EX.a, EX.derived, EX.Thing))
    assert isomorphic(serialized, expected)
    assert isomorphic(graph, expected)


# ── AlgebraResult tests ──────────────────────────────────────────────────────


class TestAlgebraResult:
    @pytest.mark.parametrize(
        "data, conforms",
        [(VALID_DATA, True), (VIOLATION_DATA, False)],
        ids=["passing", "failing"],
    )
    def test_result_contract(self, data, conforms):
        result = shifty.validate_algebra(data.encode(), SHAPES.encode())
        assert isinstance(result, shifty.AlgebraResult)
        assert result.conforms is conforms
        assert bool(result) is conforms
        assert f"conforms={conforms}" in repr(result)
        assert isinstance(result.violations, list)
        if conforms:
            assert result.violations == []
        else:
            (violation,) = result.violations
            assert isinstance(violation, shifty.Violation)
            assert violation.focus_node == "<http://example.org/Bob>"
            assert isinstance(violation.reasons, list)
            (reason,) = violation.reasons
            assert isinstance(reason.message, str) and reason.message
            assert reason.path == "ex:name"
            assert reason.value is None or isinstance(reason.value, str)
            assert reason.author_message is None

    def test_reason_exposes_algebraic_constraint_provenance(self):
        result = shifty.validate_algebra(
            VIOLATION_DATA.encode(), SHAPES.encode(), infer=False
        )
        reason = result.violations[0].reasons[0]

        assert isinstance(reason.constraint, shifty.Constraint)
        assert reason.constraint_id == reason.constraint.id
        assert reason.constraint_kind == reason.constraint.kind
        assert reason.statement_id == result.violations[0].statement_id
        assert reason.constraint_kind == shifty.ConstraintKind.Cardinality
        assert "min" in reason.constraint.json or "Count" in reason.constraint.json
        assert reason.constraint.render

    @pytest.mark.parametrize(
        "constraint",
        ["sh:datatype xsd:integer", "sh:class ex:Required", "sh:nodeKind sh:IRI"],
    )
    def test_shared_invalid_value_keeps_a_reason_for_each_property(self, constraint):
        shapes = (
            PREFIXES
            + f"""
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; {constraint} ] ;
                sh:property [ sh:path ex:q ; {constraint} ] .
            """
        )
        data = PREFIXES + 'ex:x ex:p "bad" ; ex:q "bad" .'

        result = shifty.validate_algebra(data, shapes, infer=False)

        assert not result.conforms
        assert len(result.violations) == 1
        reasons = result.violations[0].reasons
        assert len(reasons) == 2
        assert {r.path for r in reasons} == {"ex:p", "ex:q"}
        assert reasons[0].value == reasons[1].value
        assert reasons[0].message == reasons[1].message

    def test_distinct_constraints_keep_reasons_with_the_same_message(self):
        shapes = (
            PREFIXES
            + """
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:sparql [
                    sh:message "Constraint failed" ;
                    sh:select "SELECT $this WHERE { FILTER(true) }"
                ] ;
                sh:sparql [
                    sh:message "Constraint failed" ;
                    sh:select "SELECT $this WHERE { BIND(1 AS ?n) FILTER(?n = 1) }"
                ] .
            """
        )
        data = PREFIXES + "ex:x a ex:Thing ."

        result = shifty.validate_algebra(data, shapes, infer=False)

        assert not result.conforms
        assert len(result.violations) == 1
        reasons = result.violations[0].reasons
        assert len(reasons) == 2
        assert len({r.constraint_id for r in reasons}) == 2
        assert reasons[0].value == reasons[1].value
        assert reasons[0].path == reasons[1].path
        assert reasons[0].message == reasons[1].message

    def test_author_message_surfaces_sh_message(self):
        """The source shape's sh:message rides through onto the reason (with
        {$this} resolved), while a constraint without one leaves it None."""
        shapes = """
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetClass ex:T ;
            sh:property [ sh:path ex:kind ; sh:class ex:Kind ;
                sh:message "{$this} needs a known kind" ] ;
            sh:property [ sh:path ex:name ; sh:minCount 1 ] .
        ex:a a ex:T ; ex:kind ex:X .
        """
        result = shifty.validate_algebra(shapes.encode(), shapes.encode(), infer=False)
        reasons = [r for v in result.violations for r in v.reasons]

        authored = [r for r in reasons if r.author_message is not None]
        assert authored, "expected at least one authored message"
        assert authored[0].author_message == "<http://ex/a> needs a known kind"
        # The generated message is still available alongside the author's.
        assert authored[0].message and authored[0].message != authored[0].author_message

        # The message-less sh:minCount constraint keeps author_message None.
        assert any(r.author_message is None for r in reasons)


# ── Multiple violations tests ────────────────────────────────────────────────

MULTI_VIOLATION_DATA = (
    PREFIXES
    + """\
ex:Bob   a ex:Person .
ex:Carol a ex:Person ; ex:name "Carol" ; ex:age 1 ; ex:age 2 .
"""
)


def test_multiple_violations_identify_each_focus_and_constraint():
    result = shifty.validate_algebra(MULTI_VIOLATION_DATA.encode(), SHAPES.encode())
    assert not result.conforms
    assert len(result.violations) == 2
    by_focus = {v.focus_node: v for v in result.violations}
    assert set(by_focus) == {"<http://example.org/Bob>", "<http://example.org/Carol>"}
    for focus, path in [("Bob", "ex:name"), ("Carol", "ex:age")]:
        (reason,) = by_focus[f"<http://example.org/{focus}>"].reasons
        assert reason.path == path
        assert reason.constraint_kind == shifty.ConstraintKind.Cardinality
        assert reason.message


# ── Edge cases for result objects ────────────────────────────────────────────


class TestResultEdgeCases:
    def test_complex_path_violation(self):
        shapes = (
            PREFIXES
            + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path (ex:address ex:street) ;
                sh:minCount 1 ;
            ] .
        """
        )
        data = PREFIXES + "ex:Person a ex:Person ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        # Should have violation for missing street
        assert not result.conforms
        assert len(result.violations) >= 1
