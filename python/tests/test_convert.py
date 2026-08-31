"""
Unit tests for type conversion functions in shifty.

This module tests the graph input conversion functions which handle:
- bytes input (passthrough)
- pathlib.Path input (file reading)
- str input (file path or raw Turtle text)
- rdflib.Graph input (Turtle serialization with namespaces)
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

    def test_rdflib_graph_string_serialization(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        # Mock a graph that returns string from serialize
        with mock.patch.object(g, 'serialize', return_value="ex:a ex:b ex:c .") as serialize:
            result = _to_turtle_bytes(g)
            assert result == b"ex:a ex:b ex:c ."
            serialize.assert_called_once_with(format="turtle", encoding="utf-8")

    def test_rdflib_graph_bytes_serialization(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        # Mock a graph that returns bytes from serialize
        with mock.patch.object(g, 'serialize', return_value=b"ex:a ex:b ex:c .") as serialize:
            result = _to_turtle_bytes(g)
            assert result == b"ex:a ex:b ex:c ."
            serialize.assert_called_once_with(format="turtle", encoding="utf-8")

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
        data = "@prefix ex: <http://example.org/> . ex:a a ex:Person ; ex:name \"Test\" ."
        conforms, _, _ = shifty.validate(data.encode(), shapes.encode())
        assert conforms is True

    def test_validate_algebra_with_bytes(self):
        shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> . [] a sh:NodeShape ; sh:targetClass ex:Person ; sh:property [ sh:path ex:name ; sh:minCount 1 ] ."
        data = "@prefix ex: <http://example.org/> . ex:a a ex:Person ; ex:name \"Test\" ."
        result = shifty.validate_algebra(data.encode(), shapes.encode())
        assert result.conforms is True

    def test_infer_with_bytes(self):
        shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> . [] a sh:NodeShape ; sh:targetClass ex:Thing ; sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:derived ; sh:object ex:Thing ] ."
        data = "@prefix ex: <http://example.org/> . ex:a a ex:Thing ."
        result = shifty.infer(data.encode(), shapes.encode())
        assert result.inferred_count == 1
