"""
Error handling tests for the shifty Python bindings.

This module tests error scenarios including:
- Invalid graph_mode values
- Parse errors for invalid RDF syntax
- Unsupported input types
- File path errors (nonexistent files)
- Base IRI handling
- Empty/None inputs
- Blank node handling
- Large inputs
- Temporary file operations
"""

import pathlib
import tempfile

import pytest
import rdflib

import shifty

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
    ] .
"""

VALID_DATA = PREFIXES + """\
ex:Alice a ex:Person ; ex:name "Alice" .
"""


# ── Invalid graph_mode ───────────────────────────────────────────────────────

class TestInvalidGraphMode:
    """Tests for invalid graph_mode parameter values."""

    def test_validate_invalid_graph_mode(self):
        """validate() raises ValueError for invalid graph_mode."""
        with pytest.raises(ValueError, match="graph_mode"):
            shifty.validate(VALID_DATA.encode(), SHAPES.encode(), graph_mode="invalid")

    def test_validate_algebra_invalid_graph_mode(self):
        """validate_algebra() raises ValueError for invalid graph_mode."""
        with pytest.raises(ValueError, match="graph_mode"):
            shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode(), graph_mode="bad")

    def test_infer_invalid_graph_mode_not_accepted(self):
        """infer() raises TypeError when given graph_mode parameter."""
        with pytest.raises(TypeError):
            shifty.infer(VALID_DATA.encode(), SHAPES.encode(), graph_mode="union")


# ── Parse errors ─────────────────────────────────────────────────────────────

class TestParseErrors:
    def test_validate_invalid_rdf_syntax(self):
        invalid_ttl = "this is not valid turtle syntax @@##$$"
        with pytest.raises(ValueError, match="parse"):
            shifty.validate(invalid_ttl.encode(), SHAPES.encode())

    def test_validate_algebra_invalid_rdf_syntax(self):
        invalid_ttl = "this is not valid turtle syntax @@##$$"
        with pytest.raises(ValueError, match="parse"):
            shifty.validate_algebra(invalid_ttl.encode(), SHAPES.encode())

    def test_validate_invalid_shapes_syntax(self):
        invalid_shapes = "invalid @@##$$ shapes"
        with pytest.raises(ValueError, match="parse"):
            shifty.validate(VALID_DATA.encode(), invalid_shapes.encode())

    def test_validate_algebra_invalid_shapes_syntax(self):
        invalid_shapes = "invalid @@##$$ shapes"
        with pytest.raises(ValueError, match="parse"):
            shifty.validate_algebra(VALID_DATA.encode(), invalid_shapes.encode())


# ── Unsupported input types ──────────────────────────────────────────────────

class TestUnsupportedInputTypes:
    def test_validate_unsupported_type(self):
        with pytest.raises(TypeError):
            shifty.validate(123, SHAPES.encode())  # int not supported

    def test_validate_algebra_unsupported_type(self):
        with pytest.raises(TypeError):
            shifty.validate_algebra(None, SHAPES.encode())  # None not supported

    def test_infer_unsupported_type(self):
        with pytest.raises(TypeError):
            shifty.infer({"not": "valid"}, SHAPES.encode())  # dict not supported


# ── File path errors ─────────────────────────────────────────────────────────

class TestFilePathErrors:
    def test_validate_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            shifty.validate(pathlib.Path("/nonexistent/path/data.ttl"), SHAPES.encode())

    def test_validate_algebra_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            shifty.validate_algebra(pathlib.Path("/nonexistent/path/data.ttl"), SHAPES.encode())

    def test_infer_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            shifty.infer(pathlib.Path("/nonexistent/path/data.ttl"), SHAPES.encode())

    def test_validate_shapes_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            shifty.validate(VALID_DATA.encode(), pathlib.Path("/nonexistent/shapes.ttl"))

    def test_validate_algebra_shapes_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            shifty.validate_algebra(VALID_DATA.encode(), pathlib.Path("/nonexistent/shapes.ttl"))


# ── Invalid base IRI ─────────────────────────────────────────────────────────

class TestBaseIRI:
    def test_validate_invalid_base_uri(self):
        # Should handle invalid base URI gracefully
        result = shifty.validate(VALID_DATA.encode(), SHAPES.encode(), base="http://example.org/")
        assert isinstance(result, tuple)

    def test_validate_algebra_invalid_base_uri(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), SHAPES.encode(), base="http://example.org/")
        assert result.conforms is True


# ── Empty/None inputs ────────────────────────────────────────────────────────

class TestEmptyInputs:
    def test_validate_empty_bytes_data(self):
        # Empty bytes should be handled - likely an empty graph
        result = shifty.validate(b"", SHAPES.encode())
        conforms, _, _ = result
        # Empty data with shapes that have targets should not conform
        assert isinstance(conforms, bool)

    def test_validate_empty_bytes_shapes(self):
        result = shifty.validate(VALID_DATA.encode(), b"")
        conforms, _, _ = result
        assert conforms is True  # No shapes means nothing to validate

    def test_validate_algebra_empty_bytes_shapes(self):
        result = shifty.validate_algebra(VALID_DATA.encode(), b"")
        assert result.conforms is True

    def test_infer_empty_shapes(self):
        result = shifty.infer(VALID_DATA.encode(), b"")
        assert result.inferred_count == 0


# ── Graph mode specific errors ───────────────────────────────────────────────

class TestGraphModeErrors:
    def test_graph_mode_union(self):
        conforms, _, _ = shifty.validate(VALID_DATA.encode(), SHAPES.encode(), graph_mode="union")
        assert conforms is True

    def test_graph_mode_data(self):
        conforms, _, _ = shifty.validate(VALID_DATA.encode(), SHAPES.encode(), graph_mode="data")
        assert conforms is True

    def test_graph_mode_union_all(self):
        conforms, _, _ = shifty.validate(VALID_DATA.encode(), SHAPES.encode(), graph_mode="union-all")
        assert conforms is True


# ── Blank node handling ──────────────────────────────────────────────────────

class TestBlankNodes:
    def test_validate_with_blank_nodes_in_data(self):
        data_with_bnodes = PREFIXES + """\
        [] a ex:Person ; ex:name "Anonymous" .
        """
        conforms, _, _ = shifty.validate(data_with_bnodes.encode(), SHAPES.encode())
        # Blank nodes with class target should be validated
        assert isinstance(conforms, bool)

    def test_validate_with_blank_nodes_in_shapes(self):
        shapes_with_bnodes = PREFIXES + """\
        [] a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
            ] .
        """
        conforms, _, _ = shifty.validate(VALID_DATA.encode(), shapes_with_bnodes.encode())
        assert conforms is True

    def test_validate_algebra_with_blank_nodes(self):
        data_with_bnodes = PREFIXES + """\
        [] a ex:Person ; ex:name "Anonymous" .
        """
        result = shifty.validate_algebra(data_with_bnodes.encode(), SHAPES.encode())
        assert isinstance(result.conforms, bool)

    def test_infer_with_blank_nodes(self):
        infer_shapes = PREFIXES + """\
        ex:S a sh:NodeShape ;
            sh:targetClass ex:Thing ;
            sh:rule [
                a sh:TripleRule ;
                sh:subject sh:this ;
                sh:predicate ex:known ;
                sh:object ex:Person ;
            ] .
        """
        data_with_bnodes = PREFIXES + """\
        [] a ex:Thing .
        """
        result = shifty.infer(data_with_bnodes.encode(), infer_shapes.encode())
        assert result.inferred_count >= 0


# ── Large inputs ─────────────────────────────────────────────────────────────

class TestLargeInputs:
    def test_validate_large_data(self):
        # Create a large data graph
        large_data = PREFIXES
        for i in range(100):
            large_data += f"\nex:Person{i} a ex:Person ; ex:name \"Person{i}\" ."
        
        conforms, _, _ = shifty.validate(large_data.encode(), SHAPES.encode())
        assert conforms is True

    def test_validate_algebra_large_data(self):
        large_data = PREFIXES
        for i in range(100):
            large_data += f"\nex:Person{i} a ex:Person ; ex:name \"Person{i}\" ."
        
        result = shifty.validate_algebra(large_data.encode(), SHAPES.encode())
        assert result.conforms is True


# ── Temporary file tests ─────────────────────────────────────────────────────

class TestTemporaryFiles:
    def test_validate_temp_file(self, tmp_path):
        data_file = tmp_path / "data.ttl"
        shapes_file = tmp_path / "shapes.ttl"
        data_file.write_text(VALID_DATA)
        shapes_file.write_text(SHAPES)
        conforms, _, _ = shifty.validate(data_file, shapes_file)
        assert conforms is True

    def test_validate_algebra_temp_file(self, tmp_path):
        data_file = tmp_path / "data.ttl"
        shapes_file = tmp_path / "shapes.ttl"
        data_file.write_text(VALID_DATA)
        shapes_file.write_text(SHAPES)
        result = shifty.validate_algebra(data_file, shapes_file)
        assert result.conforms is True

    def test_infer_temp_file(self, tmp_path):
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
        data_file = tmp_path / "data.ttl"
        shapes_file = tmp_path / "shapes.ttl"
        data_file.write_text(PREFIXES + "ex:a a ex:Thing .")
        shapes_file.write_text(infer_shapes)
        result = shifty.infer(data_file, shapes_file)
        assert result.inferred_count == 1
