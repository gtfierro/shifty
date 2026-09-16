"""
Unit tests for type conversion functions in shifty.

This module tests the graph input conversion functions which handle:
- bytes input (passthrough)
- pathlib.Path input (file reading)
- str input (file path or raw Turtle text)
- rdflib.Graph input (a prefix block plus an N-Triples body, which is valid
  Turtle carrying both the namespace bindings and a label for every blank node)
- Error cases for unsupported types
"""

import pathlib
from unittest import mock

import pytest
import rdflib

import shifty
from shifty import _coalesce_graph_input, _to_rdf_input, _to_turtle_bytes

# ── _to_turtle_bytes() tests ─────────────────────────────────────────────────


class TestToTurtleBytes:
    def test_bytes_input(self):
        data = b"@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        result = _to_turtle_bytes(data)
        assert result == data
        assert isinstance(result, bytes)

    def test_bytes_input_preserves_content(self):
        original = b"@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        result = _to_turtle_bytes(original)
        assert result == original

    def test_pathlib_path_file_exists(self, tmp_path):
        test_file = tmp_path / "data.ttl"
        original_content = "@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        test_file.write_text(original_content)

        result = _to_turtle_bytes(pathlib.Path(test_file))
        assert result == original_content.encode("utf-8")
        assert isinstance(result, bytes)

    def test_missing_rdf_filename_raises_file_not_found(self, tmp_path):
        nonexistent = tmp_path / "nonexistent.ttl"
        with pytest.raises(FileNotFoundError):
            _to_turtle_bytes(str(nonexistent))

    def test_string_path_exists(self, tmp_path):
        test_file = tmp_path / "data.ttl"
        original_content = "@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        test_file.write_text(original_content)

        result = _to_turtle_bytes(str(test_file))
        assert result == original_content.encode("utf-8")

    def test_string_directory_raises_is_a_directory(self, tmp_path):
        with pytest.raises(IsADirectoryError):
            _to_turtle_bytes(str(tmp_path))

    def test_coalesced_string_directory_raises_is_a_directory(self, tmp_path):
        turtle = "<urn:a> <urn:b> <urn:c> ."
        with pytest.raises(IsADirectoryError):
            _coalesce_graph_input([turtle, str(tmp_path)])

    def test_coalesced_missing_rdf_filename_raises_file_not_found(self, tmp_path):
        turtle = "<urn:a> <urn:b> <urn:c> ."
        with pytest.raises(FileNotFoundError):
            _coalesce_graph_input([turtle, str(tmp_path / "missing.ttl")])

    def test_string_raw_ttl_text(self):
        ttl_text = "@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        result = _to_turtle_bytes(ttl_text)
        assert result == ttl_text.encode("utf-8")
        result = _to_turtle_bytes(ttl_text)
        assert result == ttl_text.encode("utf-8")

    def test_long_turtle_string_is_never_probed_as_a_path(self):
        turtle = "@prefix ex: <http://example.org/> .\n" + "# comment\n" * 500
        result = _to_rdf_input(turtle)
        assert result.data == turtle.encode("utf-8")
        assert result.format == "turtle"

    def test_overlong_single_line_turtle_is_never_probed_as_a_path(self):
        turtle = '<urn:a> <urn:b> "' + "x" * 4096 + '" .'
        assert _to_rdf_input(turtle).data == turtle.encode("utf-8")

    def test_short_inline_turtle_survives_an_oserror_from_path_probe(self):
        turtle = "<urn:a> <urn:b> <urn:c> ."
        with mock.patch.object(pathlib.Path, "is_file", side_effect=OSError):
            result = _to_rdf_input(turtle)
        assert result.data == turtle.encode("utf-8")

    def test_string_empty_graph(self):
        assert _to_turtle_bytes("") == b""

    def test_path_is_passed_to_native_layer(self, tmp_path):
        test_file = tmp_path / "data.ttl"
        test_file.write_text("@prefix ex: <http://example.org/> .")
        result = _to_rdf_input(test_file)
        assert result.data is None
        assert result.path == str(test_file)
        assert result.format == "turtle"

    def test_ntriples_path_format(self, tmp_path):
        test_file = tmp_path / "data.nt"
        test_file.write_text("<http://ex/s> <http://ex/p> <http://ex/o> .")
        assert _to_rdf_input(test_file).format == "nt"

    def test_rdflib_graph(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g)
        assert isinstance(result, bytes)
        # Should contain an N-Triples serialization of the graph
        assert b"example.org" in result

    def test_rdflib_graph_preserves_content(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g)
        # The serialization should be valid Turtle.
        g2 = rdflib.Graph()
        g2.parse(data=result, format="turtle")
        assert (EX.a, EX.b, EX.c) in g2

    def test_rdflib_graph_writes_one_statement_per_triple(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g).decode("utf-8")

        assert (
            "<http://example.org/a> <http://example.org/b> <http://example.org/c> ."
            in result
        )

    @pytest.mark.parametrize(
        "value",
        [
            rdflib.Literal("plain"),
            rdflib.Literal('quotes " and \\ backslash'),
            rdflib.Literal("line\nbreak\ttab"),
            rdflib.Literal("é — 🙂"),
            rdflib.Literal("tagged", lang="en"),
            rdflib.Literal(4),
            rdflib.Literal(1.5),
            rdflib.Literal(True),
            rdflib.Literal("2020-01-01", datatype=rdflib.XSD.date),
        ],
    )
    def test_rdflib_graph_round_trips_literals(self, value):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, value))

        reparsed = rdflib.Graph()
        reparsed.parse(data=_to_turtle_bytes(g), format="turtle")

        assert list(reparsed.objects(EX.a, EX.b)) == [value]

    def test_rdflib_graph_declares_namespace_bindings(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.bind("ex", EX)
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g).decode("utf-8")

        assert "@prefix ex: <http://example.org/> ." in result

    def test_rdflib_graph_labels_every_blank_node(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        node = rdflib.BNode()
        g.add((EX.a, EX.has, node))
        g.add((node, EX.width, rdflib.Literal(4)))

        result = _to_turtle_bytes(g).decode("utf-8")

        # The abbreviated `[ ... ]` form would leave this node unnamed.
        assert f"_:{node}" in result

    @pytest.mark.parametrize(
        "prefix",
        [
            "2020",  # opens on a digit
            "-lead",  # opens on a dash
            "trailing.",  # closes on a dot
            ".leading",  # opens on a dot
            "#hash",
            "sl/ash",
        ],
    )
    def test_rdflib_graph_skips_unspellable_prefix(self, prefix):
        """A binding whose name is not a Turtle prefix must not be declared.

        rdflib accepts these names, so a graph can carry one without its owner
        ever noticing; emitting it would make the document unparseable and
        break every call on that graph, not only the ones that use it."""
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.bind("ex", EX)
        g.bind(prefix, rdflib.Namespace("http://example.org/other/"))
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g)

        reparsed = rdflib.Graph()
        reparsed.parse(data=result, format="turtle")
        assert (EX.a, EX.b, EX.c) in reparsed

    @pytest.mark.parametrize(
        "prefix", ["ex", "", "a1", "with-dash", "with_underscore", "in.terior", "é"]
    )
    def test_rdflib_graph_declares_spellable_prefix(self, prefix):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.bind(prefix, EX, override=True, replace=True)
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g).decode("utf-8")

        assert f"@prefix {prefix}: <http://example.org/> ." in result

    @pytest.mark.parametrize(
        "label",
        [
            "plain1",
            "in.terior.dots",
            "with-dash",
            "with_underscore",
            "0leading-digit",
            "has space",
            "-leading-dash",
            "trailing.",
            "unicode-é",
            "<angle>",
            'quote"mark',
        ],
    )
    def test_rdflib_graph_round_trips_any_blank_node_label(self, label):
        """rdflib puts no constraints on a BNode's name, so any label can arrive.

        Whatever the name, the document has to parse and the node has to stay
        one node: the two triples below share a subject, and a label rendered
        two different ways would split them across two nodes."""
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        node = rdflib.BNode(label)
        g.add((EX.a, EX.has, node))
        g.add((node, EX.width, rdflib.Literal(4)))

        reparsed = rdflib.Graph()
        reparsed.parse(data=_to_turtle_bytes(g), format="turtle")

        subject = reparsed.value(EX.a, EX.has)
        assert isinstance(subject, rdflib.BNode)
        assert list(reparsed.objects(subject, EX.width)) == [rdflib.Literal(4)]

    def test_rdflib_graph_skips_undeclarable_namespace(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.bind("ex", EX)
        g.bind("broken", rdflib.Namespace("http://example.org/a b>c#"))
        g.add((EX.a, EX.b, EX.c))

        result = _to_turtle_bytes(g).decode("utf-8")

        assert "broken" not in result
        # The document still parses, and the sound bindings survive.
        reparsed = rdflib.Graph()
        reparsed.parse(data=result, format="turtle")
        assert (EX.a, EX.b, EX.c) in reparsed

    def test_unsupported_type_int(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes(123)

    def test_unsupported_type_dict(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes({"not": "valid"})

    def test_unsupported_type_list(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes([1, 2, 3])

    def test_unsupported_type_none(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes(None)

    def test_unsupported_type_float(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes(3.14)

    def test_unsupported_type_tuple(self):
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_turtle_bytes((1, 2))

    def test_error_message_includes_type(self):
        with pytest.raises(TypeError) as exc_info:
            _to_turtle_bytes(123)
        assert "int" in str(exc_info.value)

    def test_error_message_suggests_valid_types(self):
        with pytest.raises(TypeError) as exc_info:
            _to_turtle_bytes(123)
        assert "rdflib.Graph" in str(exc_info.value)
        assert "pathlib.Path" in str(exc_info.value)
        assert "bytes" in str(exc_info.value)


# ── Integration tests with _to_turtle_bytes ──────────────────────────────────


class TestToTurtleBytesIntegration:
    def test_validate_with_bytes(self):
        shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> . [] a sh:NodeShape ; sh:targetClass ex:Person ; sh:property [ sh:path ex:name ; sh:minCount 1 ] ."
        data = '@prefix ex: <http://example.org/> . ex:a a ex:Person ; ex:name "Test" .'
        conforms, _, _ = shifty.validate(data.encode(), shapes.encode())
        assert conforms is True

    def test_validate_algebra_with_bytes(self):
        shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> . [] a sh:NodeShape ; sh:targetClass ex:Person ; sh:property [ sh:path ex:name ; sh:minCount 1 ] ."
        data = '@prefix ex: <http://example.org/> . ex:a a ex:Person ; ex:name "Test" .'
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms is True

    def test_infer_with_bytes(self):
        shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> . [] a sh:NodeShape ; sh:targetClass ex:Thing ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:derived ; sh:object ex:Thing ] ."
        data = "@prefix ex: <http://example.org/> . ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), shapes.encode())
        assert result.inferred_count == 1
