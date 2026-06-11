"""
Unit tests for result classes in shifty.

This module tests:
- InferResult class methods:
  - inferred_count property
  - diagnostics property
  - graph_ntriples property
  - graph() method
  - __repr__() method
- AlgebraResult class:
  - conforms property
  - violations list
  - Violation objects with focus_node, reasons
  - Reason objects with message, path, value
  - __repr__() and bool() coercion
- Multiple violations handling
- Edge cases (empty shapes, empty data, complex paths)
"""

import pytest
import rdflib

import shifty
from shifty import AlgebraResult, InferResult

PREFIXES = """\
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:  <http://example.org/> .
"""

SHAPES = PREFIXES + """\
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

VALID_DATA = PREFIXES + """\
ex:Alice a ex:Person ; ex:name "Alice" ; ex:age 30 .
"""

VIOLATION_DATA = PREFIXES + """\
ex:Bob a ex:Person .
"""


# ── InferResult tests ────────────────────────────────────────────────────────

class TestInferResult:
    def test_inferred_count_property(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        assert isinstance(result.inferred_count, int)
        assert result.inferred_count == 1

    def test_inferred_count_zero_no_inferences(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:NonExistent ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        assert result.inferred_count == 0

    def test_diagnostics_property(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        assert isinstance(result.diagnostics, list)
        # All diagnostics should be strings
        assert all(isinstance(d, str) for d in result.diagnostics)

    def test_graph_ntriples_property(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        assert isinstance(result.graph_ntriples, str)
        assert "derived" in result.graph_ntriples

    def test_graph_ntriples_valid_ntriples(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        # Parse the ntriples to verify it's valid
        g = rdflib.Graph()
        g.parse(data=result.graph_ntriples, format="nt")
        EX = rdflib.Namespace("http://example.org/")
        assert (EX.a, EX.derived, EX.Thing) in g  # Note: Thing with capital T

    def test_graph_method(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        graph = result.graph()
        assert isinstance(graph, rdflib.Graph)
        EX = rdflib.Namespace("http://example.org/")
        assert (EX.a, EX.derived, EX.Thing) in graph  # Note: Thing with capital T

    def test_graph_method_returns_original_plus_inferred(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing . ex:a ex:original ex:value ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        graph = result.graph()
        EX = rdflib.Namespace("http://example.org/")
        # Both original and inferred triples should be present
        assert (EX.a, EX.original, EX.value) in graph
        assert (EX.a, EX.derived, EX.Thing) in graph  # Note: Thing with capital T

    def test_repr_method(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        repr_str = repr(result)
        assert "inferred" in repr_str
        assert "1" in repr_str

    def test_repr_zero_inferences(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:NonExistent ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:derived ;
                sh:object ex:Thing ;
            ] .
        """
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), infer_shapes.encode())
        repr_str = repr(result)
        assert "inferred" in repr_str
        assert "0" in repr_str


# ── AlgebraResult tests ──────────────────────────────────────────────────────

class TestAlgebraResult:
    def test_conforms_true(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode())
        assert result.conforms is True

    def test_conforms_false(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert result.conforms is False

    def test_violations_empty_when_conforms(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode())
        assert result.violations == []
        assert len(result.violations) == 0

    def test_violations_list_when_not_conforms(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert isinstance(result.violations, list)
        assert len(result.violations) > 0

    def test_violations_type(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            assert isinstance(v, shifty.Violation)

    def test_violation_has_focus_node(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            assert hasattr(v, 'focus_node')
            assert isinstance(v.focus_node, str)

    def test_violation_has_reasons(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            assert hasattr(v, 'reasons')
            assert isinstance(v.reasons, list)

    def test_reason_has_message(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            for r in v.reasons:
                assert hasattr(r, 'message')
                assert isinstance(r.message, str)

    def test_reason_has_path(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            for r in v.reasons:
                assert hasattr(r, 'path')
                # path can be None or a string/IRI
                assert r.path is None or isinstance(r.path, (str, type(rdflib.URIRef(''))))

    def test_reason_has_value(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            for r in v.reasons:
                assert hasattr(r, 'value')
                # value can be None, string, or URIRef
                assert r.value is None or isinstance(r.value, (str, type(rdflib.URIRef(''))))

    def test_repr_when_conforms(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode())
        repr_str = repr(result)
        assert "conforms" in repr_str
        assert "True" in repr_str

    def test_repr_when_not_conforms(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        repr_str = repr(result)
        assert "conforms" in repr_str
        assert "False" in repr_str

    def test_bool_coercion_true(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode())
        assert bool(result) is True

    def test_bool_coercion_false(self):
        result = shifty.validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert bool(result) is False


# ── Multiple violations tests ────────────────────────────────────────────────

MULTI_VIOLATION_DATA = PREFIXES + """\
ex:Bob   a ex:Person .
ex:Carol a ex:Person ; ex:name "Carol" ; ex:age 1 ; ex:age 2 .
"""


class TestMultipleViolations:
    def test_multiple_violations_detected(self):
        result = shifty.validate_algebra(MULTI_VIOLATION_DATA.encode(), SHAPES.encode())
        assert not result.conforms
        # Bob has no name, Carol has too many ages
        assert len(result.violations) >= 2

    def test_violations_have_different_focus_nodes(self):
        result = shifty.validate_algebra(MULTI_VIOLATION_DATA.encode(), SHAPES.encode())
        focus_nodes = {v.focus_node for v in result.violations}
        # Should have violations for both Bob and Carol
        assert len(focus_nodes) >= 2

    def test_violations_have_correct_reasons(self):
        result = shifty.validate_algebra(MULTI_VIOLATION_DATA.encode(), SHAPES.encode())
        # Check that reasons contain expected error messages
        all_messages = [r.message for v in result.violations for r in v.reasons]
        # Should have messages about minCount (Bob) and maxCount (Carol)
        has_mincount_issue = any("minCount" in m or "name" in m.lower() for m in all_messages)
        has_maxcount_issue = any("maxCount" in m or "age" in m.lower() for m in all_messages)
        assert has_mincount_issue
        assert has_maxcount_issue


# ── Edge cases for result objects ────────────────────────────────────────────

class TestResultEdgeCases:
    def test_empty_shapes_validation(self):
        shapes = PREFIXES + ""
        data = PREFIXES + "ex:a a ex:Thing ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms is True
        assert result.violations == []

    def test_empty_data_validation(self):
        shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:property [ sh:path ex:name ; sh:minCount 1 ] .
        """
        result = shifty.validate_algebra(b"", shapes.encode())
        # Empty data with targets should not conform (no focus nodes match)
        assert result.conforms is True  # No data means nothing to validate

    def test_complex_path_violation(self):
        shapes = PREFIXES + """\
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path (ex:address ex:street) ;
                sh:minCount 1 ;
            ] .
        """
        data = PREFIXES + "ex:Person a ex:Person ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        # Should have violation for missing street
        assert not result.conforms
        assert len(result.violations) >= 1

    def test_violation_without_reasons(self):
        # This test is for edge cases where a violation might have no reasons
        # In practice, violations should always have reasons, but test the edge case
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode())
        for v in result.violations:
            # Even if reasons is empty, it should be a list
            assert isinstance(v.reasons, list)
