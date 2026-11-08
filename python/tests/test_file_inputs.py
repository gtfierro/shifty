"""File inputs resolve RDF references against each document's own location."""

from pathlib import Path
from urllib.parse import quote

import pytest
from rdflib import URIRef
from rdflib.namespace import SH

import shifty


@pytest.mark.parametrize("suffix", [".ttl", ".rdf-data"])
def test_separate_files_have_independent_document_bases(tmp_path, suffix):
    directory = tmp_path / "inputs #é"
    directory.mkdir()
    data = directory / f"data #é{suffix}"
    shapes = directory / f"shapes{suffix}"
    data.write_text('<> <urn:p> "value" .')
    shapes.write_text(f"""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:S> a sh:NodeShape; sh:targetNode <{quote(data.name)}>;
            sh:property [sh:path <urn:p>; sh:minCount 1] .
    """)
    conforms, _, _ = shifty.validate(data, shapes, infer=False, graph_mode="data")
    assert conforms
    assert shifty.validate_algebra(
        data, shapes, infer=False, graph_mode="data"
    ).conforms


def test_explicit_base_overrides_file_location(tmp_path):
    path = tmp_path / "combined.ttl"
    path.write_text("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <> <urn:p> "bad" .
        <urn:S> a sh:NodeShape; sh:targetNode <>;
            sh:property [sh:path <urn:p>; sh:class <urn:Expected>] .
    """)
    conforms, report, _ = shifty.validate(
        path, infer=False, base="http://example.org/override"
    )
    assert not conforms
    assert list(report.objects(None, SH.focusNode)) == [
        URIRef("http://example.org/override")
    ]


def test_relative_path_uses_current_directory(tmp_path, monkeypatch):
    path = tmp_path / "combined.ttl"
    path.write_text("""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <> <urn:p> "value" .
        <urn:S> a sh:NodeShape; sh:targetNode <>;
            sh:property [sh:path <urn:p>; sh:minCount 1] .
    """)
    monkeypatch.chdir(tmp_path)
    conforms, _, _ = shifty.validate(Path("combined.ttl"), infer=False)
    assert conforms
