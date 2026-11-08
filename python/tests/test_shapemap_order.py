"""Shape-map ordering is a schema property, independent of graph iteration."""

import json
import subprocess
import sys
from decimal import Decimal

import pytest

import shifty

PREFIXES = """
@prefix ex: <http://ex/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
"""
DATA = PREFIXES + 'ex:a a ex:T ; ex:p "value" . ex:b a ex:T .'


@pytest.mark.parametrize("with_session", [False, True])
def test_decimal_order_positions_and_duplicate_ordinals(with_session):
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "later" ; sh:order 1.25 ; sh:maxCount 3 ] ;
        sh:property [ sh:path ex:p ; sh:name "first" ; sh:order -2.5 ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:q ; sh:name "unordered" ; sh:minCount 1 ] .
    """
    )
    session = shifty.EvidenceSession(shapes, DATA, infer=False)
    smap = shifty.ShapeMap.from_run(
        session.validate(), session if with_session else None
    )
    for mapping in smap:
        assert list(map(str, mapping)) == ["p", "p#2", "q"]
        assert [b.position for b in mapping.values()] == [1, 2, 3]
        assert [b.order for b in mapping.values()] == [
            Decimal("-2.5"),
            Decimal("1.25"),
            None,
        ]
        assert mapping["p"].min == 1
        assert mapping["p#2"].max == 3
        assert [b.position for _, b in mapping.unsuccessful] == (
            [3] if mapping.focus.value == "http://ex/a" else [1, 3]
        )
        if with_session:
            assert mapping["p"].name == "first"


def test_singleton_property_retains_its_order():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ; sh:order 99 ;
        sh:property [ sh:path ex:p ; sh:order 1.5 ; sh:minCount 1 ] .
    """
    )
    for mapping in shifty.shape_map(DATA, shapes, infer=False):
        assert mapping["p"].position == 1
        assert mapping["p"].order == Decimal("1.5")


@pytest.mark.parametrize("anonymous", [False, True])
def test_ties_and_unordered_properties_are_stable_across_processes(anonymous):
    properties = [
        'sh:path ex:p ; sh:name "required" ; sh:minCount 1 ; sh:order 2',
        'sh:path ex:p ; sh:name "optional" ; sh:maxCount 3 ; sh:order 2',
        'sh:path ex:q ; sh:name "missing" ; sh:minCount 1',
    ]
    documents = []
    for reverse in (False, True):
        indices = list(range(3))
        if reverse:
            indices.reverse()
        if anonymous:
            body = " ; ".join(f"sh:property [ {properties[i]} ]" for i in indices)
            shapes = PREFIXES + f"ex:S a sh:NodeShape ; sh:targetClass ex:T ; {body} ."
        else:
            shapes = PREFIXES + (
                "ex:S a sh:NodeShape ; sh:targetClass ex:T ; sh:property "
                + ", ".join(f"ex:prop{i}" for i in indices)
                + " . "
                + " ".join(f"ex:prop{i} {properties[i]} ." for i in indices)
            )
        documents.append(shapes)
    script = """
import json, sys, shifty
shapes, data = json.loads(sys.stdin.read())
result = []
for _ in range(3):
    mapping = next(iter(shifty.shape_map(data, shapes, infer=False)))
    result.append([(str(k), b.name, b.position, b.min, b.max) for k, b in mapping.items()])
print(json.dumps(result))
"""
    results = []
    for shapes in documents * 2:
        runs = json.loads(
            subprocess.check_output(
                [sys.executable, "-c", script],
                input=json.dumps([shapes, DATA]),
                text=True,
            )
        )
        results.extend(runs)
    assert all(result == results[0] for result in results)
    assert [entry[2] for entry in results[0]] == [1, 2, 3]


def test_by_name_uses_order_for_duplicate_names():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "same" ; sh:order 2 ; sh:maxCount 3 ] ;
        sh:property [ sh:path ex:p ; sh:name "same" ; sh:order 1 ; sh:minCount 1 ] .
    """
    )
    for mapping in shifty.shape_map(DATA, shapes, infer=False):
        assert mapping.by_name("same").min == 1
