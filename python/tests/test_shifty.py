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


@pytest.mark.parametrize(
    ("legacy", "replacement"),
    [
        ("FocusWitness", "Failure"),
        ("FocusSatisfaction", "Satisfaction"),
    ],
)
def test_released_repair_type_aliases_warn(legacy, replacement):
    with pytest.warns(DeprecationWarning, match=f"use shifty.{replacement}"):
        value = getattr(shifty, legacy)
    assert value is getattr(shifty, replacement)


def test_write_back_does_not_render_delta_without_a_target():
    def unexpected_delta():
        raise AssertionError("ordinary validation must not render its delta")

    shifty._write_back_derived(None, unexpected_delta)


@pytest.mark.parametrize(
    ("wrapper", "native_name"),
    [
        ("infer", "_infer"),
        ("validate", "_validate_w3c"),
        ("validate_algebra", "_validate_algebra"),
    ],
)
def test_default_wrappers_do_not_read_delta(monkeypatch, wrapper, native_name):
    class NativeResult:
        conforms = True
        report_turtle = ""
        _report_ntriples = ""
        results_text = ""
        # Every real result type carries this; the W3C wrappers read it to
        # warn, which is not reading the inference delta.
        diagnostics = ()

        @property
        def inferred_ntriples(self):
            raise AssertionError("default call read the inference delta")

        @property
        def _inferred_ntriples(self):
            raise AssertionError("default call read the inference delta")

    monkeypatch.setattr(shifty, native_name, lambda *args: NativeResult())
    getattr(shifty, wrapper)(INFER_DATA.encode(), INFER_SHAPES.encode())


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
        conforms_vals = list(report_graph.objects(None, SH.conforms))
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

    def test_accepts_http_urls(self, monkeypatch):
        urls = {
            "https://example.test/data.ttl": CONFORMS_DATA.encode(),
            "https://example.test/shapes.ttl": SHAPES.encode(),
        }

        class Response:
            headers = {"Content-Type": "text/turtle; charset=utf-8"}

            def __init__(self, url):
                self.url = url

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return False

            def read(self):
                return urls[self.url]

            def geturl(self):
                return self.url

        calls = []

        def urlopen(url, *, timeout):
            calls.append((url, timeout))
            return Response(url)

        monkeypatch.setattr("urllib.request.urlopen", urlopen)
        conforms, _, _ = validate(
            "https://example.test/data.ttl",
            "https://example.test/shapes.ttl",
            infer=False,
        )

        assert conforms is True
        assert calls == [
            ("https://example.test/data.ttl", 30),
            ("https://example.test/shapes.ttl", 30),
        ]

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
        assert any(
            "name" in m.lower() or "minCount" in m or "1" in m for m in all_messages
        )

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
            (finding.severity, finding.focus_node) for finding in result.violations
        ] == [
            ("Violation", "<http://example.org/m>"),
            ("Warning", "<http://example.org/z>"),
            ("Info", "<http://example.org/a>"),
        ]
        assert all(
            reason.severity in {"Violation", "Warning", "Info"}
            for finding in result.violations
            for reason in finding.reasons
        )

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

# A rule whose subject is the focus node, fired on a focus node that is itself
# a blank node reached through ex:hasDim. Whatever it derives belongs on that
# blank node, so the derived triple is only useful if it stays reachable from
# ex:r1 -- which is the question these fixtures exist to ask.
BNODE_RULES = PREFIXES + textwrap.dedent("""\
    ex:DimShape a sh:NodeShape ;
        sh:targetClass ex:Dim ;
        sh:rule [
            a sh:TripleRule ;
            sh:subject sh:this ;
            sh:predicate ex:area ;
            sh:object [ sh:path ex:width ]
        ] .
""")

BNODE_SPARQL_RULES = PREFIXES + textwrap.dedent("""\
    ex:DimShape a sh:NodeShape ;
        sh:targetNode ex:r1 ;
        sh:rule [
            a sh:SPARQLRule ;
            sh:construct "CONSTRUCT { ?dim ex:area ?width } WHERE { $this ex:hasDim ?dim . ?dim ex:width ?width }"
        ] .
""")

BNODE_DATA = PREFIXES + textwrap.dedent("""\
    ex:r1 ex:hasDim [ a ex:Dim ; ex:width 4 ] .
""")


def _bnode_graph():
    graph = rdflib.Graph()
    graph.parse(data=BNODE_DATA, format="turtle")
    return graph


def _derived_area(graph):
    """The ex:area values reachable by walking ex:r1 -> ex:hasDim -> ex:area.

    Reads the derived triple the way an application would, through the node
    that points at it, rather than by scanning the graph for it. A triple
    attached to some other blank node is invisible here even though the graph
    contains it, which is exactly the failure worth catching."""
    EX = rdflib.Namespace("http://example.org/")
    dim = graph.value(EX.r1, EX.hasDim)
    return list(graph.objects(dim, EX.area))


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

    def test_inferred_ntriples_is_just_the_delta(self):
        result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())
        assert isinstance(result.inferred_ntriples, str)
        assert "knows2" in result.inferred_ntriples
        # The delta shouldn't carry the original, already-asserted triples.
        assert "ex:a a ex:Thing" not in result.inferred_ntriples
        EX = rdflib.Namespace("http://example.org/")
        delta = rdflib.Graph()
        delta.parse(data=result.inferred_ntriples, format="nt")
        assert (EX.a, EX.knows2, EX.b) in delta
        assert (EX.a, rdflib.RDF.type, EX.Thing) not in delta

    def test_inferred_ntriples_empty_when_nothing_inferred(self):
        result = shifty.infer(CONFORMS_DATA.encode(), INFER_SHAPES.encode())
        assert result.inferred_count == 0
        assert result.inferred_ntriples == ""


class TestInferInPlace:
    def test_requires_rdflib_graph(self):
        with pytest.raises(TypeError):
            shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode(), in_place=True)

    def test_adds_inferred_triples_to_input_graph(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")
        original_len = len(data)

        result = shifty.infer(data, INFER_SHAPES.encode(), in_place=True)

        EX = rdflib.Namespace("http://example.org/")
        assert (EX.a, EX.knows2, EX.b) in data
        assert len(data) == original_len + result.inferred_count

    def test_graph_returns_same_object_as_input(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        result = shifty.infer(data, INFER_SHAPES.encode(), in_place=True)

        assert result.graph() is data

    def test_matches_non_in_place_result(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        in_place_result = shifty.infer(data, INFER_SHAPES.encode(), in_place=True)
        copy_result = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode())

        assert isomorphic(data, copy_result.graph())
        assert in_place_result.inferred_count == copy_result.inferred_count

    def test_no_op_when_nothing_inferred(self):
        data = rdflib.Graph()
        data.parse(data=CONFORMS_DATA, format="turtle")
        before = set(data)

        result = shifty.infer(data, INFER_SHAPES.encode(), in_place=True)

        assert result.inferred_count == 0
        assert set(data) == before


class TestInferInPlaceAcceptsSingletonSequence:
    """A one-member sequence names the graph it holds, on every other path."""

    @pytest.mark.parametrize("wrap", [list, tuple])
    def test_singleton_sequence_is_extended(self, wrap):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        result = shifty.infer(wrap([data]), INFER_SHAPES.encode(), in_place=True)

        EX = rdflib.Namespace("http://example.org/")
        assert result.inferred_count == 1
        assert (EX.a, EX.knows2, EX.b) in data

    def test_longer_sequence_is_still_rejected(self):
        """A union of several inputs is a new graph the caller never sees."""
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        with pytest.raises(TypeError):
            shifty.infer([data, rdflib.Graph()], INFER_SHAPES.encode(), in_place=True)


class TestValidateInPlace:
    def test_requires_rdflib_graph(self):
        with pytest.raises(TypeError):
            validate(INFER_DATA.encode(), INFER_SHAPES.encode(), in_place=True)

    def test_requires_infer_true(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")
        with pytest.raises(ValueError):
            validate(data, INFER_SHAPES.encode(), in_place=True, infer=False)

    def test_adds_inferred_triples_to_input_graph(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")
        original_len = len(data)

        conforms, _, _ = validate(data, INFER_SHAPES.encode(), in_place=True)

        EX = rdflib.Namespace("http://example.org/")
        assert conforms
        assert (EX.a, EX.knows2, EX.b) in data
        assert len(data) == original_len + 1

    def test_report_graph_is_unaffected(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        _, report, _ = validate(data, INFER_SHAPES.encode(), in_place=True)

        assert isinstance(report, rdflib.Graph)
        assert report is not data

    def test_no_op_when_nothing_inferred(self):
        data = rdflib.Graph()
        data.parse(data=CONFORMS_DATA, format="turtle")
        before = set(data)

        validate(data, INFER_SHAPES.encode(), in_place=True)

        assert set(data) == before

    def test_matches_separately_computed_inference(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        validate(data, INFER_SHAPES.encode(), in_place=True)
        expected = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode()).graph()

        assert isomorphic(data, expected)


class TestValidateAlgebraInPlace:
    def test_requires_rdflib_graph(self):
        with pytest.raises(TypeError):
            validate_algebra(INFER_DATA.encode(), INFER_SHAPES.encode(), in_place=True)

    def test_requires_infer_true(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")
        with pytest.raises(ValueError):
            validate_algebra(data, INFER_SHAPES.encode(), in_place=True, infer=False)

    def test_adds_inferred_triples_to_input_graph(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")
        original_len = len(data)

        result = validate_algebra(data, INFER_SHAPES.encode(), in_place=True)

        EX = rdflib.Namespace("http://example.org/")
        assert result.conforms
        assert (EX.a, EX.knows2, EX.b) in data
        assert len(data) == original_len + 1

    def test_no_op_when_nothing_inferred(self):
        data = rdflib.Graph()
        data.parse(data=CONFORMS_DATA, format="turtle")
        before = set(data)

        validate_algebra(data, INFER_SHAPES.encode(), in_place=True)

        assert set(data) == before

    def test_matches_separately_computed_inference(self):
        data = rdflib.Graph()
        data.parse(data=INFER_DATA, format="turtle")

        validate_algebra(data, INFER_SHAPES.encode(), in_place=True)
        expected = shifty.infer(INFER_DATA.encode(), INFER_SHAPES.encode()).graph()

        assert isomorphic(data, expected)


@pytest.mark.parametrize("method", ["_validate_algebra", "_validate_w3c"])
def test_native_validation_only_keeps_delta_when_requested(method):
    native_validate = getattr(shifty, method)
    ordinary = native_validate(data=INFER_DATA.encode(), shapes=INFER_SHAPES.encode())
    retained = native_validate(
        data=INFER_DATA.encode(),
        shapes=INFER_SHAPES.encode(),
        keep_inferred=True,
    )

    assert ordinary.conforms == retained.conforms
    assert ordinary._inferred_ntriples == ""
    assert "knows2" in retained._inferred_ntriples
    assert not hasattr(ordinary, "inferred_ntriples")


class TestInPlaceBlankNodes:
    """Derived triples about a blank node have to land on the caller's node.

    A blank node is named only by the label its document gives it, so these
    exercise the one case where that name has to survive a full round trip:
    the data goes out to the engine, a rule fires on a blank node, and the
    triple that comes back has to rejoin the node it describes.
    """

    def test_infer_attaches_to_the_original_blank_node(self):
        graph = _bnode_graph()

        result = shifty.infer(graph, BNODE_RULES.encode(), in_place=True)

        assert result.inferred_count == 1
        assert _derived_area(graph) == [rdflib.Literal(4)]

    def test_non_in_place_infer_keeps_caller_blank_node(self):
        graph = _bnode_graph()
        original = next(
            node for node in graph.all_nodes() if isinstance(node, rdflib.BNode)
        )

        inferred = shifty.infer(graph, BNODE_RULES.encode()).graph()

        assert original in inferred.all_nodes()
        assert inferred.value(
            original, rdflib.URIRef("http://example.org/area")
        ) == rdflib.Literal(4)

    @pytest.mark.parametrize("prepared", [False, True])
    def test_report_rejoins_data_node_without_merging_shapes_node(self, prepared):
        ex = rdflib.Namespace("http://example.org/")
        sh = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        data = rdflib.Graph()
        node = rdflib.BNode("same")
        data.add((node, ex.p, ex.o))
        shapes = PREFIXES + textwrap.dedent("""\
            ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:p ;
                sh:property _:same .
            _:same sh:path ex:q ; sh:minCount 1 .
        """)

        run = (
            shifty.PreparedValidator(shapes.encode()).validate
            if prepared
            else shifty.validate
        )
        conforms, report, _ = run(data, shapes.encode()) if not prepared else run(data)

        assert not conforms
        results = list(report.subjects(rdflib.RDF.type, sh.ValidationResult))
        assert len(results) == 1
        assert report.value(results[0], sh.focusNode) == node
        assert report.value(results[0], sh.sourceShape) != node

    def test_sparql_rule_attaches_to_the_original_blank_node(self):
        graph = _bnode_graph()

        first = shifty.infer(graph, BNODE_SPARQL_RULES.encode(), in_place=True)
        second = shifty.infer(graph, BNODE_SPARQL_RULES.encode(), in_place=True)

        assert first.inferred_count == 1
        assert first.diagnostics == []
        assert second.inferred_count == 0
        assert _derived_area(graph) == [rdflib.Literal(4)]

    def test_sparql_rule_keeps_shapes_blank_node_distinct_from_data(self):
        EX = rdflib.Namespace("http://example.org/")
        graph = rdflib.Graph()
        data_node = rdflib.BNode("same")
        graph.add((EX.r1, EX.marker, data_node))
        shapes = PREFIXES + textwrap.dedent("""\
            ex:S a sh:NodeShape ; sh:targetNode ex:r1 ;
                sh:rule [ a sh:SPARQLRule ;
                    sh:construct "CONSTRUCT { $this ex:uses ?option } WHERE { GRAPH $shapesGraph { ex:Config ex:option ?option } }" ] .
            ex:Config ex:option _:same .
            _:same ex:kind ex:K .
        """)

        result = shifty.infer(graph, shapes.encode(), in_place=True)

        assert result.inferred_count == 1
        assert result.diagnostics == []
        assert graph.value(EX.r1, EX.uses) != data_node

    def test_sparql_rule_rejoins_data_blank_node_after_shapes_label_collision(self):
        EX = rdflib.Namespace("http://example.org/")
        graph = rdflib.Graph()
        data_node = rdflib.BNode("same")
        graph.add((EX.r1, EX.marker, data_node))
        shapes = PREFIXES + textwrap.dedent("""\
            ex:S a sh:NodeShape ; sh:targetNode ex:r1 ; sh:rule _:same .
            _:same a sh:SPARQLRule ;
                sh:construct "CONSTRUCT { ?node ex:flag ex:K } WHERE { $this ex:marker ?node }" .
        """)

        result = shifty.infer(graph, shapes.encode(), in_place=True)

        assert result.inferred_count == 1
        assert result.diagnostics == []
        assert (data_node, EX.flag, EX.K) in graph

    def test_validate_attaches_to_the_original_blank_node(self):
        graph = _bnode_graph()

        validate(graph, BNODE_RULES.encode(), in_place=True)

        assert _derived_area(graph) == [rdflib.Literal(4)]

    def test_validate_algebra_attaches_to_the_original_blank_node(self):
        graph = _bnode_graph()

        validate_algebra(graph, BNODE_RULES.encode(), in_place=True)

        assert _derived_area(graph) == [rdflib.Literal(4)]

    def test_no_orphan_blank_node_is_introduced(self):
        graph = _bnode_graph()

        shifty.infer(graph, BNODE_RULES.encode(), in_place=True)

        nodes = {
            term
            for triple in graph
            for term in triple
            if isinstance(term, rdflib.BNode)
        }
        assert len(nodes) == 1

    def test_repeated_runs_stay_stable(self):
        """Running twice derives the same triple onto the same node.

        A run that minted a new blank node each time would leave the graph
        growing on every call, so this pins the fixed point down."""
        graph = _bnode_graph()

        shifty.infer(graph, BNODE_RULES.encode(), in_place=True)
        after_first = set(graph)
        second = shifty.infer(graph, BNODE_RULES.encode(), in_place=True)

        assert second.inferred_count == 0
        assert set(graph) == after_first

    def test_matches_the_graph_built_without_in_place(self):
        graph = _bnode_graph()

        shifty.infer(graph, BNODE_RULES.encode(), in_place=True)
        separate = shifty.infer(BNODE_DATA.encode(), BNODE_RULES.encode()).graph()

        assert isomorphic(graph, separate)

    @pytest.mark.parametrize(
        "label",
        [
            "plain1",
            "in.terior.dots",
            "trailing.",
            "with-dash",
            "0leading-digit",
            "has space",
            "-leading-dash",
            "unicode-é",
            '<angle>"quote',
            # A name that already looks like an encoded label, which must not
            # be mistaken for one on the way back.
            "shiftyx20",
            "shiftyx" + "has space".encode("utf-8").hex(),
        ],
    )
    def test_any_blank_node_label_keeps_its_derived_triple(self, label):
        """The node's name must not decide whether the feature works.

        rdflib will name a BNode anything, and applications do — identifiers
        carried over from a JSON-LD ``@id`` or a database key land here
        unchanged. Whatever the label, the derived triple has to come back to
        the node it describes."""
        EX = rdflib.Namespace("http://example.org/")
        graph = rdflib.Graph()
        dim = rdflib.BNode(label)
        graph.add((EX.r1, EX.hasDim, dim))
        graph.add((dim, rdflib.RDF.type, EX.Dim))
        graph.add((dim, EX.width, rdflib.Literal(4)))

        result = shifty.infer(graph, BNODE_RULES.encode(), in_place=True)

        assert result.inferred_count == 1
        assert _derived_area(graph) == [rdflib.Literal(4)]
        nodes = {
            term
            for triple in graph
            for term in triple
            if isinstance(term, rdflib.BNode)
        }
        assert len(nodes) == 1


# ── graph_mode variants ───────────────────────────────────────────────────────


class TestGraphMode:
    def test_union_mode(self):
        conforms, _, _ = validate(
            CONFORMS_DATA.encode(), SHAPES.encode(), graph_mode="union"
        )
        assert conforms

    def test_data_mode(self):
        conforms, _, _ = validate(
            CONFORMS_DATA.encode(), SHAPES.encode(), graph_mode="data"
        )
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
        conforms, _, _ = pv.validate([CONFORMS_DATA.encode(), EXTRA_DATA.encode()])
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
