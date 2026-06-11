"""
Unit tests for type conversion functions in shifty.

This module tests the `_to_turtle_bytes()` function which handles:
- bytes input (passthrough)
- pathlib.Path input (file reading)
- str input (file path or raw Turtle text)
- rdflib.Graph input (Turtle serialization)
- Error cases for unsupported types
"""

import pathlib
from unittest import mock

import pytest
import rdflib

import shifty
from shifty import _to_turtle_bytes


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

    def test_pathlib_path_file_not_exists(self, tmp_path):
        nonexistent = tmp_path / "nonexistent.ttl"
        # When path doesn't exist, str path is treated as raw Turtle text
        result = _to_turtle_bytes(str(nonexistent))
        assert result == str(nonexistent).encode("utf-8")

    def test_string_path_exists(self, tmp_path):
        test_file = tmp_path / "data.ttl"
        original_content = "@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        test_file.write_text(original_content)
        
        result = _to_turtle_bytes(str(test_file))
        assert result == original_content.encode("utf-8")

    def test_string_raw_ttl_text(self):
        ttl_text = "@prefix ex: <http://example.org/> . ex:a ex:b ex:c ."
        result = _to_turtle_bytes(ttl_text)
        assert result == ttl_text.encode("utf-8")
        result = _to_turtle_bytes(ttl_text)
        assert result == ttl_text.encode("utf-8")

    def test_string_empty_graph(self):
        ttl_text = ""
        # Empty string is treated as a path (current directory), which exists
        # This is a limitation of the current implementation
        # In practice, empty graph data should use b""
        with pytest.raises(IsADirectoryError):
            _to_turtle_bytes(ttl_text)

    def test_rdflib_graph(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        result = _to_turtle_bytes(g)
        assert isinstance(result, bytes)
        # Should contain some turtle serialization of the graph
        assert b"example.org" in result

    def test_rdflib_graph_preserves_content(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        result = _to_turtle_bytes(g)
        # The serialization should be valid Turtle
        g2 = rdflib.Graph()
        g2.parse(data=result, format="turtle")
        assert (EX.a, EX.b, EX.c) in g2

    def test_rdflib_graph_string_serialization(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        # Mock a graph that returns string from serialize
        with mock.patch.object(g, 'serialize', return_value="ex:a ex:b ex:c ."):
            result = _to_turtle_bytes(g)
            assert result == b"ex:a ex:b ex:c ."

    def test_rdflib_graph_bytes_serialization(self):
        g = rdflib.Graph()
        EX = rdflib.Namespace("http://example.org/")
        g.add((EX.a, EX.b, EX.c))
        
        # Mock a graph that returns bytes from serialize
        with mock.patch.object(g, 'serialize', return_value=b"ex:a ex:b ex:c ."):
            result = _to_turtle_bytes(g)
            assert result == b"ex:a ex:b ex:c ."

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
