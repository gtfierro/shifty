"""
Integration tests for the shifty Python bindings.

This module provides comprehensive integration tests covering:
- validate() function (pyshacl-compatible W3C report interface)
- validate_algebra() function (structured algebraic result interface)
- infer() function (SHACL-AF forward-chaining inference)
- graph_mode parameter (union, data, union-all modes)
- Various input types (bytes, str, pathlib.Path, rdflib.Graph)
- File-based operations
- Type coercion and boolean evaluation
"""

import pathlib
import textwrap

import pytest
import rdflib
from rdflib.compare import isomorphic

import shifty
from shifty import AlgebraResult, InferResult, validate, validate_algebra

PREFIXES = textwrap.dedent("""\
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix ex:  <http://example.org/> .
""")

SHAPES = PREFIXES + textwrap.dedent("""\
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
""")

CONFORMS_DATA = PREFIXES + textwrap.dedent("""\
    ex:Alice a ex:Person ;
        ex:name "Alice" ;
        ex:age 30 .
""")

VIOLATION_DATA = PREFIXES + textwrap.dedent("""\
    ex:Bob a ex:Person .
""")

MULTI_VIOLATION_DATA = PREFIXES + textwrap.dedent("""\
    ex:Bob   a ex:Person .
    ex:Carol a ex:Person ; ex:name "Carol" ; ex:age 1 ; ex:age 2 .
""")


def test_version_is_programmatically_available():
    assert isinstance(shifty.__version__, str)
    assert shifty.__version__
    assert shifty.version() == shifty.__version__


# ── validate() — pyshacl-compatible ──────────────────────────────────────────

class TestValidatePyshacl:
    def test_returns_tuple(self):
        result = validate(CONFORMS_DATA.encode(), SHAPES.encode())
        assert isinstance(result, tuple) and len(result) == 3

    def test_conforms_true(self):
        conforms, report_graph, text = validate(CONFORMS_DATA.encode(), SHAPES.encode())
        assert conforms is True

    def test_conforms_false(self):
        conforms, _, _ = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        assert conforms is False

    def test_report_graph_is_rdflib(self):
        _, report_graph, _ = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        assert isinstance(report_graph, rdflib.Graph)

    def test_report_graph_has_validation_report(self):
        SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        _, report_graph, _ = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        reports = list(report_graph.subjects(rdflib.RDF.type, SH.ValidationReport))
        assert len(reports) == 1

    def test_report_graph_conforms_false(self):
        SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        _, report_graph, _ = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        conforms_vals = list(
            report_graph.objects(None, SH.conforms)
        )
        assert any(str(v) == "false" for v in conforms_vals)

    def test_results_text_contains_summary(self):
        _, _, text = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        assert "Validation Report" in text
        assert "Conforms: False" in text

    def test_results_text_empty_when_conforms(self):
        _, _, text = validate(CONFORMS_DATA.encode(), SHAPES.encode())
        assert "Conforms: True" in text

    def test_shapes_none_uses_data(self):
        # When shapes=None the data graph should be used as the shapes graph too.
        combined = SHAPES + "\n" + CONFORMS_DATA
        conforms, _, _ = validate(combined.encode())
        assert conforms is True

    def test_explicit_none_uses_embedded_shapes(self):
        combined = SHAPES + "\n" + VIOLATION_DATA
        conforms, _, _ = validate(combined.encode(), None, infer=False)
        assert conforms is False

    def test_embedded_graph_matches_explicit_self_validation(self):
        combined = (SHAPES + "\n" + VIOLATION_DATA).encode()
        embedded = validate(combined, infer=False)
        explicit = validate(combined, combined, infer=False)

        assert embedded[0] == explicit[0]
        assert isomorphic(embedded[1], explicit[1])

    @pytest.mark.parametrize("graph_mode", ["union", "data", "union-all"])
    def test_embedded_graph_modes_are_equivalent(self, graph_mode):
        combined = (SHAPES + "\n" + VIOLATION_DATA).encode()
        result = validate(combined, graph_mode=graph_mode, infer=False)

        assert result[0] is False

    def test_embedded_file_path(self, tmp_path):
        combined_file = tmp_path / "combined.ttl"
        combined_file.write_text(SHAPES + "\n" + CONFORMS_DATA)

        conforms, _, _ = validate(combined_file, infer=False)

        assert conforms is True

    def test_accepts_file_path(self, tmp_path):
        data_file = tmp_path / "data.ttl"
        shapes_file = tmp_path / "shapes.ttl"
        data_file.write_text(CONFORMS_DATA)
        shapes_file.write_text(SHAPES)
        conforms, _, _ = validate(data_file, shapes_file)
        assert conforms is True

    def test_accepts_pathlib_path(self, tmp_path):
        data_file = tmp_path / "data.ttl"
        shapes_file = tmp_path / "shapes.ttl"
        data_file.write_text(CONFORMS_DATA)
        shapes_file.write_text(SHAPES)
        conforms, _, _ = validate(pathlib.Path(data_file), pathlib.Path(shapes_file))
        assert conforms is True

    def test_accepts_rdflib_graph(self):
        data_g = rdflib.Graph()
        data_g.parse(data=CONFORMS_DATA, format="turtle")
        shapes_g = rdflib.Graph()
        shapes_g.parse(data=SHAPES, format="turtle")
        conforms, _, _ = validate(data_g, shapes_g)
        assert conforms is True

    def test_bool_coercion(self):
        conforms, _, _ = validate(CONFORMS_DATA.encode(), SHAPES.encode())
        assert conforms
        conforms, _, _ = validate(VIOLATION_DATA.encode(), SHAPES.encode())
        assert not conforms


# ── validate_algebra() — structured violations ───────────────────────────────

class TestValidateAlgebra:
    def test_returns_algebra_result(self):
        result = validate_algebra(CONFORMS_DATA.encode(), SHAPES.encode())
        assert isinstance(result, AlgebraResult)

    def test_conforms_true(self):
        result = validate_algebra(CONFORMS_DATA.encode(), SHAPES.encode())
        assert result.conforms is True
        assert result.violations == []

    def test_conforms_false(self):
        result = validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert result.conforms is False
        assert len(result.violations) > 0

    def test_violation_has_focus_node(self):
        result = validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        focus_nodes = [v.focus_node for v in result.violations]
        assert any("Bob" in fn for fn in focus_nodes)

    def test_violation_has_reasons(self):
        result = validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert all(len(v.reasons) > 0 for v in result.violations)

    def test_reason_has_message(self):
        result = validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        all_messages = [r.message for v in result.violations for r in v.reasons]
        assert any("name" in m.lower() or "minCount" in m or "1" in m
                   for m in all_messages)

    def test_multi_violation(self):
        result = validate_algebra(MULTI_VIOLATION_DATA.encode(), SHAPES.encode())
        assert not result.conforms
        # Bob (no name) and Carol (age > maxCount 1) both violate
        focus_nodes = {v.focus_node for v in result.violations}
        assert any("Bob" in fn for fn in focus_nodes)
        assert any("Carol" in fn for fn in focus_nodes)

    def test_severity_threshold_and_ordering(self):
        shapes = PREFIXES + textwrap.dedent("""\
            ex:InfoShape a sh:NodeShape ;
                sh:targetNode ex:a ;
                sh:nodeKind sh:Literal ;
                sh:severity sh:Info .
            ex:WarningShape a sh:NodeShape ;
                sh:targetNode ex:z ;
                sh:nodeKind sh:Literal ;
                sh:severity sh:Warning .
            ex:ViolationShape a sh:NodeShape ;
                sh:targetNode ex:m ;
                sh:nodeKind sh:Literal .
        """)
        result = validate_algebra(
            b"",
            shapes.encode(),
            infer=False,
        )

        assert result.conforms is False
        assert [
            (finding.severity, finding.focus_node)
            for finding in result.violations
        ] == [
            ("Violation", "<http://example.org/m>"),
            ("Warning", "<http://example.org/z>"),
            ("Info", "<http://example.org/a>"),
        ]
        assert all(reason.severity in {"Violation", "Warning", "Info"}
                   for finding in result.violations
                   for reason in finding.reasons)

        advisories = validate_algebra(
            b"",
            shapes.split("ex:ViolationShape", 1)[0].encode(),
            minimum_severity="violation",
            infer=False,
        )
        assert advisories.conforms is True
        assert [finding.severity for finding in advisories.violations] == [
            "Warning",
            "Info",
        ]

    def test_bool_coercion(self):
        assert validate_algebra(CONFORMS_DATA.encode(), SHAPES.encode())
        assert not validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())

    def test_repr(self):
        r = validate_algebra(CONFORMS_DATA.encode(), SHAPES.encode())
        assert "conforms=True" in repr(r)
        r = validate_algebra(VIOLATION_DATA.encode(), SHAPES.encode())
        assert "conforms=False" in repr(r)

    def test_graph_mode_data(self):
        # "data" mode: shapes are embedded in data, class target found
        combined = SHAPES + "\n" + VIOLATION_DATA
        result = validate_algebra(combined.encode(), graph_mode="data")
        assert not result.conforms

    def test_embedded_graph_matches_explicit_self_validation(self):
        combined = (SHAPES + "\n" + VIOLATION_DATA).encode()
        embedded = validate_algebra(combined, infer=False)
        explicit = validate_algebra(combined, combined, infer=False)

        assert embedded.conforms == explicit.conforms
        assert [
            (violation.focus_node, violation.shape_name)
            for violation in embedded.violations
        ] == [
            (violation.focus_node, violation.shape_name)
            for violation in explicit.violations
        ]

    def test_explicit_none_uses_embedded_shapes(self):
        combined = (SHAPES + "\n" + VIOLATION_DATA).encode()
        result = validate_algebra(combined, None, infer=False)
        assert result.conforms is False

    @pytest.mark.parametrize("graph_mode", ["union", "data", "union-all"])
    def test_embedded_graph_modes_are_equivalent(self, graph_mode):
        combined = (SHAPES + "\n" + VIOLATION_DATA).encode()
        result = validate_algebra(combined, graph_mode=graph_mode, infer=False)

        assert result.conforms is False

    def test_embedded_rdflib_graph(self):
        combined = rdflib.Graph()
        combined.parse(data=SHAPES + "\n" + CONFORMS_DATA, format="turtle")

        result = validate_algebra(combined, infer=False)

        assert result.conforms is True


# ── infer() ───────────────────────────────────────────────────────────────────

INFER_SHAPES = PREFIXES + textwrap.dedent("""\
    ex:S a sh:NodeShape ;
        sh:targetClass ex:Thing ;
        sh:rule [
            a sh:TripleRule ;
            sh:subject sh:this ;
            sh:predicate ex:knows2 ;
            sh:object [ sh:path ex:knows ]
        ] .
""")

INFER_DATA = PREFIXES + textwrap.dedent("""\
    ex:a a ex:Thing ; ex:knows ex:b .
""")


class TestInfer:
    def test_returns_infer_result(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        assert isinstance(result, InferResult)

    def test_inferred_count(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        assert result.inferred_count == 1

    def test_graph_ntriples_is_string(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        assert isinstance(result.graph_ntriples, str)
        assert "knows2" in result.graph_ntriples

    def test_graph_returns_rdflib(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        g = result.graph()
        assert isinstance(g, rdflib.Graph)
        EX = rdflib.Namespace("http://example.org/")
        assert (EX.a, EX.knows2, EX.b) in g

    def test_embedded_graph_matches_explicit_self_inference(self):
        combined = (INFER_SHAPES + "\n" + INFER_DATA).encode()
        embedded = shifty.infer(combined)
        explicit = shifty.infer(combined, combined)

        assert embedded.inferred_count == explicit.inferred_count
        assert isomorphic(embedded.graph(), explicit.graph())

    def test_explicit_none_uses_embedded_rules(self):
        combined = (INFER_SHAPES + "\n" + INFER_DATA).encode()
        result = shifty.infer(combined, None)
        assert result.inferred_count == 1

    def test_repr(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        assert "inferred=1" in repr(result)


# ── graph_mode variants ───────────────────────────────────────────────────────

class TestGraphMode:
    def test_union_mode(self):
        conforms, _, _ = validate(CONFORMS_DATA.encode(), SHAPES.encode(),
                                  graph_mode="union")
        assert conforms

    def test_data_mode(self):
        conforms, _, _ = validate(CONFORMS_DATA.encode(), SHAPES.encode(),
                                  graph_mode="data")
        assert conforms

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="graph_mode"):
            validate(CONFORMS_DATA.encode(), SHAPES.encode(), graph_mode="bad")


# ── multiple shapes/data graphs are unioned ───────────────────────────────────

# A second shapes graph adding an extra constraint, and a second data graph
# adding an extra conforming instance, so we can prove both lists merge.
EXTRA_SHAPES = PREFIXES + textwrap.dedent("""\
    ex:NamedThingShape a sh:NodeShape ;
        sh:targetClass ex:NamedThing ;
        sh:property [
            sh:path ex:label ;
            sh:minCount 1 ;
            sh:datatype xsd:string ;
        ] .
""")

EXTRA_DATA = PREFIXES + textwrap.dedent("""\
    ex:Widget a ex:NamedThing ; ex:label "widget" .
""")


class TestMultipleGraphsUnion:
    """Lists/tuples of graphs are merged at the RDF triple level before being
    passed to the engine — the programmatic analogue of the CLI's repeatable
    --shapes / --data."""

    def test_multiple_shapes_union_enforced(self):
        # Bob (Person, no name) violates PersonShape; Widget satisfies the
        # extra shape only when EXTRA_SHAPES is merged in.
        conforms, report, text = validate(
            [VIOLATION_DATA.encode(), EXTRA_DATA.encode()],
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
        )
        assert not conforms
        # Bob's missing name must surface in the report text.
        assert "Bob" in text
        SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        assert list(report.subjects(rdflib.RDF.type, SH.ValidationReport))

    def test_unioned_shapes_match_single_concatenated(self):
        # Unioning two separate shapes graphs must behave like one graph
        # holding both, for conforming data.
        merged_shapes = rdflib.Graph()
        for src in (SHAPES, EXTRA_SHAPES):
            g = rdflib.Graph()
            g.parse(data=src, format="turtle")
            for t in g:
                merged_shapes.add(t)
        single, _, _ = validate(
            [CONFORMS_DATA.encode(), EXTRA_DATA.encode()],
            merged_shapes,
        )
        listed, _, _ = validate(
            [CONFORMS_DATA.encode(), EXTRA_DATA.encode()],
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
        )
        assert single == listed

    def test_list_data_union_enforced(self):
        # Alice conforms as Person; Widget only conforms once EXTRA_SHAPES is
        # included. With both shapes, the unioned data still conforms overall.
        conforms, _, _ = validate(
            [CONFORMS_DATA.encode(), EXTRA_DATA.encode()],
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
            graph_mode="data",
        )
        assert conforms

    def test_tuple_accepted_same_as_list(self):
        a, _, _ = validate(
            [CONFORMS_DATA.encode(), EXTRA_DATA.encode()],
            (SHAPES.encode(), EXTRA_SHAPES.encode()),
        )
        b, _, _ = validate(
            (CONFORMS_DATA.encode(), EXTRA_DATA.encode()),
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
        )
        assert a == b

    def test_single_element_list_preserves_fast_path(self):
        # A one-element list should behave exactly like passing the element
        # directly (both shapes and data).
        direct, _, _ = validate(CONFORMS_DATA.encode(), SHAPES.encode())
        listed, _, _ = validate([CONFORMS_DATA.encode()], [SHAPES.encode()])
        assert direct == listed

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="empty"):
            validate([], SHAPES.encode())
        with pytest.raises(ValueError, match="empty"):
            validate(CONFORMS_DATA.encode(), [])

    def test_prepared_validator_multiple_shapes(self):
        pv = shifty.PreparedValidator([SHAPES.encode(), EXTRA_SHAPES.encode()])
        conforms, _, _ = pv.validate(
            [CONFORMS_DATA.encode(), EXTRA_DATA.encode()]
        )
        assert conforms

    def test_validate_algebra_multiple_graphs(self):
        result = validate_algebra(
            [VIOLATION_DATA.encode(), EXTRA_DATA.encode()],
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
        )
        assert not result.conforms
        # Bob's missing name must be reported.
        focuses = {v.focus_node for v in result.violations}
        assert any("Bob" in f for f in focuses)

    def test_repair_session_multiple_shapes(self):
        session = shifty.RepairSession(
            [SHAPES.encode(), EXTRA_SHAPES.encode()],
            [VIOLATION_DATA.encode(), EXTRA_DATA.encode()],
        )
        ws = session.witnesses()
        assert ws  # Bob fails PersonShape

    def test_rdflib_graphs_in_list(self):
        g1 = rdflib.Graph()
        g1.parse(data=CONFORMS_DATA, format="turtle")
        g2 = rdflib.Graph()
        g2.parse(data=EXTRA_DATA, format="turtle")
        s1 = rdflib.Graph()
        s1.parse(data=SHAPES, format="turtle")
        s2 = rdflib.Graph()
        s2.parse(data=EXTRA_SHAPES, format="turtle")
        conforms, _, _ = validate([g1, g2], [s1, s2])
        assert conforms

    def test_caller_graph_not_mutated(self):
        g = rdflib.Graph()
        g.parse(data=CONFORMS_DATA, format="turtle")
        before = len(g)
        validate([g, EXTRA_DATA.encode()], [SHAPES.encode()])
        assert len(g) == before
