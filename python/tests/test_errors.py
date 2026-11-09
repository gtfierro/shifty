"""Input admission and error contracts shared by the public entry points.

Detailed blank-node identity regressions live in test_shifty.py; conversion
edge cases live in test_convert.py.
"""

import pathlib

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

SHAPES = (
    PREFIXES
    + """\
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] .
"""
)

VALID_DATA = (
    PREFIXES
    + """\
ex:Alice a ex:Person ; ex:name "Alice" .
"""
)


VALIDATIONS = [shifty.validate, shifty.validate_algebra]
ENTRY_POINTS = [*VALIDATIONS, shifty.infer]


@pytest.mark.parametrize("validation", VALIDATIONS)
@pytest.mark.parametrize("embedded", [False, True])
def test_invalid_graph_mode(validation, embedded):
    args = ((SHAPES + VALID_DATA).encode(),) if embedded else (VALID_DATA, SHAPES)
    with pytest.raises(ValueError, match="graph_mode"):
        validation(*args, graph_mode="invalid")


def test_infer_does_not_accept_graph_mode():
    with pytest.raises(TypeError):
        shifty.infer(VALID_DATA, SHAPES, graph_mode="union")


@pytest.mark.parametrize("validation", VALIDATIONS)
@pytest.mark.parametrize("document", ["data", "shapes"])
def test_invalid_rdf_syntax(validation, document):
    args = [VALID_DATA.encode(), SHAPES.encode()]
    args[0 if document == "data" else 1] = b"invalid turtle @@##$$"
    with pytest.raises(ValueError, match="parse"):
        validation(*args)


@pytest.mark.parametrize("call", ENTRY_POINTS)
@pytest.mark.parametrize(
    "value", [123, None, {"not": "valid"}], ids=["int", "none", "dict"]
)
def test_unsupported_input_type(call, value):
    with pytest.raises(TypeError):
        call(value, SHAPES)


@pytest.mark.parametrize("call", ENTRY_POINTS)
@pytest.mark.parametrize("document", ["data", "shapes"])
def test_missing_file(call, document, tmp_path):
    args = [VALID_DATA, SHAPES]
    args[0 if document == "data" else 1] = tmp_path / "missing.ttl"
    with pytest.raises(FileNotFoundError):
        call(*args)


@pytest.mark.parametrize("validation", VALIDATIONS)
def test_explicit_base_resolves_relative_iris(validation):
    data = b'<Alice> <other> "Alice" .'
    shapes = b"""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        <S> a sh:NodeShape ; sh:targetNode <Alice> ;
            sh:property [ sh:path <name> ; sh:minCount 1 ] .
    """
    result = validation(data, shapes, base="http://example.org/", infer=False)
    if validation is shifty.validate:
        conforms, report, _ = result
        assert conforms is False
        SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")
        assert set(report.objects(None, SH.focusNode)) == {
            rdflib.URIRef("http://example.org/Alice")
        }
    else:
        assert result.conforms is False
        assert {v.focus_node for v in result.violations} == {
            "<http://example.org/Alice>"
        }


@pytest.mark.parametrize("validation", VALIDATIONS)
def test_empty_data_has_no_class_targets(validation):
    result = validation(b"", SHAPES)
    if validation is shifty.validate:
        assert result[0] is True
    else:
        assert result.conforms is True
        assert result.violations == []


@pytest.mark.parametrize("validation", VALIDATIONS)
@pytest.mark.parametrize("input_type", ["bytes", "prefixes", "graph", "file"])
def test_explicit_empty_shapes_raise(validation, input_type, tmp_path):
    if input_type == "file":
        shapes = tmp_path / "empty.ttl"
        shapes.write_text("")
    else:
        shapes = {"bytes": b"", "prefixes": PREFIXES, "graph": rdflib.Graph()}[
            input_type
        ]
    with pytest.raises(ValueError, match="explicit shapes graph is empty"):
        validation(VALID_DATA, shapes)


def test_embedded_empty_graph_remains_valid():
    conforms, _, _ = shifty.validate(b"")
    assert conforms is True


def test_infer_empty_shapes():
    result = shifty.infer(VALID_DATA, b"")
    assert result.inferred_count == 0


@pytest.mark.parametrize("validation", VALIDATIONS)
def test_many_focus_nodes(validation):
    data = PREFIXES + "\n".join(
        f'ex:Person{i} a ex:Person ; ex:name "Person{i}" .' for i in range(100)
    )
    result = validation(data, SHAPES)
    assert (
        result[0] is True if validation is shifty.validate else result.conforms is True
    )


@pytest.mark.parametrize("call", ENTRY_POINTS)
@pytest.mark.parametrize("as_path", [str, pathlib.Path], ids=["str", "Path"])
def test_file_inputs(call, as_path, tmp_path):
    # One rule makes inference observable for all three entry points.
    shapes = (
        SHAPES
        + """
        ex:PersonShape sh:property [ sh:path ex:known ; sh:minCount 1 ] .
        ex:S a sh:NodeShape ; sh:targetClass ex:Person ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:known ; sh:object ex:Person ] .
    """
    )
    data_file = tmp_path / "data.ttl"
    shapes_file = tmp_path / "shapes.ttl"
    data_file.write_text(VALID_DATA)
    shapes_file.write_text(shapes)
    result = call(as_path(data_file), as_path(shapes_file))
    if call is shifty.infer:
        assert result.inferred_count == 1
    else:
        assert result[0] is True if call is shifty.validate else result.conforms is True
