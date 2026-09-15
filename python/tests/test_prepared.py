import threading

import pytest
import rdflib

import shifty

SHAPES = b"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""

VALID = b"""
@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .
"""

INVALID = b"""
@prefix ex: <http://example.org/> .
ex:bob a ex:Person .
"""

RULE_SHAPES = b"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:S a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:knows2 ;
        sh:object [ sh:path ex:knows ]
    ] .
"""

RULE_DATA = """
@prefix ex: <http://example.org/> .
ex:a a ex:Thing ; ex:knows ex:b .
"""


def test_prepared_validator_matches_one_shot():
    prepared = shifty.PreparedValidator(SHAPES)

    expected = shifty.validate(INVALID, SHAPES, infer=False)
    actual = prepared.validate(INVALID, infer=False)

    assert actual[0] == expected[0]
    assert len(actual[1]) == len(expected[1])
    assert "Conforms: False" in actual[2]


def test_prepared_validator_reuses_shapes_for_multiple_graphs():
    prepared = shifty.PreparedValidator(SHAPES)

    assert prepared.validate_algebra(VALID, infer=False).conforms is True
    assert prepared.validate_algebra(INVALID, infer=False).conforms is False
    assert isinstance(prepared.diagnostics, list)


def test_prepared_validator_accepts_rdflib_graph():
    graph = rdflib.Graph()
    graph.parse(data=VALID, format="turtle")

    result = shifty.PreparedValidator(SHAPES).validate_algebra(graph, infer=False)

    assert result.conforms is True


def test_prepared_validator_rejects_empty_shapes():
    with pytest.raises(ValueError, match="explicit shapes graph is empty"):
        shifty.PreparedValidator(rdflib.Graph())


def test_prepared_validator_in_place_adds_inferred_triples():
    prepared = shifty.PreparedValidator(RULE_SHAPES)
    data = rdflib.Graph()
    data.parse(data=RULE_DATA, format="turtle")

    conforms, _, _ = prepared.validate(data, in_place=True)

    EX = rdflib.Namespace("http://example.org/")
    assert conforms
    assert (EX.a, EX.knows2, EX.b) in data


def test_prepared_validator_in_place_requires_rdflib_graph():
    prepared = shifty.PreparedValidator(RULE_SHAPES)
    with pytest.raises(TypeError):
        prepared.validate(RULE_DATA.encode(), in_place=True)


def test_prepared_validator_in_place_requires_infer_true():
    prepared = shifty.PreparedValidator(RULE_SHAPES)
    data = rdflib.Graph()
    data.parse(data=RULE_DATA, format="turtle")
    with pytest.raises(ValueError):
        prepared.validate(data, in_place=True, infer=False)


def test_validation_releases_gil():
    data = [
        "@prefix ex: <http://example.org/> .",
        *(f'ex:p{i} a ex:Person ; ex:name "Person {i}" .' for i in range(20_000)),
    ]
    started = threading.Event()
    finished = threading.Event()
    counter = 0

    def worker():
        nonlocal counter
        started.set()
        while not finished.is_set():
            counter += 1

    thread = threading.Thread(target=worker)
    thread.start()
    started.wait()
    before = counter
    shifty.validate_algebra("\n".join(data).encode(), SHAPES, infer=False)
    after = counter
    finished.set()
    thread.join()

    assert after > before
