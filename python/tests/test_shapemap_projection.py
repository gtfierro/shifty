"""Application projections distinguish empty, unknown, and failed slots."""

import pytest

import shifty
from shifty import Literal

PREFIXES = """
@prefix ex: <http://ex/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
"""


@pytest.mark.parametrize("python", [False, True])
@pytest.mark.parametrize("by", ["key", "name"])
def test_value_map_preserves_successful_empty_slots(python, by):
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:alpha ; sh:name "alpha" ; sh:pattern "x" ] ;
        sh:property [ sh:path ex:beta ; sh:name "beta" ; sh:minCount 1 ] .
    """
    )
    data = PREFIXES + 'ex:a a ex:T ; ex:beta "b" .'
    (mapping,) = shifty.shape_map(data, shapes, infer=False)
    projected = mapping.value_map(by=by, python=python)
    alpha, beta = (
        (mapping["alpha"].key, mapping["beta"].key)
        if by == "key"
        else ("alpha", "beta")
    )
    assert projected == {alpha: [], beta: ["b" if python else Literal("b")]}
    assert mapping.by_name("alpha").ok


def test_value_map_distinguishes_known_empty_from_unavailable_and_failed():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:empty ; sh:name "empty" ; sh:maxCount 1 ] ;
        sh:property [ sh:path ex:missing ; sh:name "missing" ; sh:minCount 1 ] .
    """
    )
    session = shifty.EvidenceSession(shapes, PREFIXES + "ex:a a ex:T .", infer=False)
    run = session.validate()
    (sighted,) = shifty.ShapeMap.from_run(run, session)
    (blind,) = shifty.ShapeMap.from_run(run)
    assert sighted["empty"].ok
    assert sighted["empty"].values == []
    assert sighted.value_map(by="name") == {"empty": []}
    assert blind["empty"].ok
    assert blind["empty"].values is None
    assert blind.value_map() == {}
    assert not sighted["missing"].ok


def test_name_projection_uses_rendered_key_for_unnamed_empty_binding():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:alpha ; sh:pattern "x" ] .
    """
    )
    (mapping,) = shifty.shape_map(PREFIXES + "ex:a a ex:T .", shapes, infer=False)
    assert mapping["alpha"].name is None
    assert mapping.value_map(by="name") == {"alpha": []}


def test_an_authored_empty_name_is_not_an_absent_name():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:alpha ; sh:name "" ; sh:pattern "x" ] .
    """
    )
    (mapping,) = shifty.shape_map(PREFIXES + "ex:a a ex:T .", shapes, infer=False)
    assert mapping["alpha"].name == ""
    assert mapping.value_map(by="name") == {"": []}
