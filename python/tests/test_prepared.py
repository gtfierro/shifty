import threading
import time

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


def test_validation_releases_gil():
    data = [
        "@prefix ex: <http://example.org/> .",
        *(
            f'ex:p{i} a ex:Person ; ex:name "Person {i}" .'
            for i in range(20_000)
        ),
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
