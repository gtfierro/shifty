"""Tests for PreparedValidator.witnesses() — the inverse of validation."""

import shifty

SHAPES = b"""
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix zea: <http://example.org/zea#> .
@prefix ex:  <http://example.org/> .

ex:VavProfile a sh:NodeShape ;
    sh:targetClass ex:Vav ;
    sh:property [
        zea:role ex:OutsideAirTempRole ;
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
        sh:qualifiedMinCount 1 ;
        sh:qualifiedMaxCount 1 ;
    ] ;
    sh:property [
        zea:role ex:ReturnAirTempRole ;
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape [ sh:hasValue ex:rat ] ;
        sh:qualifiedMinCount 1 ;
        sh:qualifiedMaxCount 1 ;
    ] .
ex:OutsideAirTempRole zea:roleName "outsideAirTemp" .
ex:ReturnAirTempRole zea:roleName "returnAirTemp" .
"""

DATA = b"""
@prefix ex: <http://example.org/> .
ex:vav1 a ex:Vav ; ex:hasPoint ex:oat, ex:rat, ex:sat, ex:mat .
ex:vav2 a ex:Vav ; ex:hasPoint ex:sat .
"""


def test_multi_hop_key_path_disambiguates_same_typed_siblings():
    # The key isn't a direct annotation on the property shape — it's one hop
    # further, through an intermediate "role descriptor" node. A single
    # predicate lookup couldn't reach it; a property path can.
    validator = shifty.PreparedValidator(SHAPES)
    witnesses = validator.witnesses(DATA, key_path="zea:role/zea:roleName", infer=False)

    # Only ex:vav1 conforms (ex:vav2 is missing both qualified points).
    assert len(witnesses) == 2
    for w in witnesses:
        assert w.focus == "<http://example.org/vav1>"
        assert w.shape == "<http://example.org/VavProfile>"

    by_key = {w.key: w.values for w in witnesses}
    assert by_key["outsideAirTemp"] == ["<http://example.org/oat>"]
    assert by_key["returnAirTemp"] == ["<http://example.org/rat>"]


def test_inverse_key_path_resolves_through_a_reverse_hop():
    shapes = b"""
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix zea: <http://example.org/zea#> .
    @prefix ex:  <http://example.org/> .

    ex:Profile a sh:NodeShape ;
        sh:targetNode ex:vav1 ;
        sh:property ex:OatProp .
    ex:OatProp a sh:PropertyShape ;
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
        sh:qualifiedMinCount 1 ;
        sh:qualifiedMaxCount 1 .
    ex:RoleDescriptor zea:describes ex:OatProp ; zea:roleName "outsideAirTemp" .
    """
    data = b"""
    @prefix ex: <http://example.org/> .
    ex:vav1 ex:hasPoint ex:oat, ex:sat .
    """
    validator = shifty.PreparedValidator(shapes)
    witnesses = validator.witnesses(
        data, key_path="^zea:describes/zea:roleName", infer=False
    )

    assert len(witnesses) == 1
    assert witnesses[0].key == "outsideAirTemp"


def test_single_predicate_key_path_still_works():
    shapes = b"""
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix zea: <http://example.org/zea#> .
    @prefix ex:  <http://example.org/> .

    ex:Profile a sh:NodeShape ;
        sh:targetNode ex:vav1 ;
        sh:property [ zea:roleName "outsideAirTemp" ; sh:path ex:hasPoint ; sh:minCount 1 ] .
    """
    data = b"""
    @prefix ex: <http://example.org/> .
    ex:vav1 ex:hasPoint ex:oat .
    """
    validator = shifty.PreparedValidator(shapes)
    witnesses = validator.witnesses(data, key_path="zea:roleName", infer=False)

    assert len(witnesses) == 1
    assert witnesses[0].key == "outsideAirTemp"


def test_violating_focus_yields_no_witnesses():
    shapes = b"""
    @prefix sh:  <http://www.w3.org/ns/shacl#> .
    @prefix zea: <http://example.org/zea#> .
    @prefix ex:  <http://example.org/> .

    ex:Profile a sh:NodeShape ;
        sh:targetNode ex:good, ex:bad ;
        sh:property [
            zea:roleName "outsideAirTemp" ;
            sh:path ex:hasPoint ;
            sh:minCount 1 ;
        ] .
    """
    data = b"""
    @prefix ex: <http://example.org/> .
    ex:good ex:hasPoint ex:oat .
    ex:bad a ex:Thing .
    """
    validator = shifty.PreparedValidator(shapes)
    witnesses = validator.witnesses(data, key_path="zea:roleName", infer=False)

    assert len(witnesses) == 1
    assert witnesses[0].focus == "<http://example.org/good>"
    assert witnesses[0].values == ["<http://example.org/oat>"]


def test_missing_key_path_falls_back_to_property_shape_node():
    shapes = b"""
    @prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix ex: <http://example.org/> .

    ex:Profile a sh:NodeShape ;
        sh:targetNode ex:vav1 ;
        sh:property ex:PointRole .
    ex:PointRole a sh:PropertyShape ;
        sh:path ex:hasPoint ;
        sh:minCount 1 .
    """
    data = b"""
    @prefix ex: <http://example.org/> .
    ex:vav1 ex:hasPoint ex:a .
    """
    validator = shifty.PreparedValidator(shapes)
    witnesses = validator.witnesses(data, infer=False)

    assert len(witnesses) == 1
    assert witnesses[0].key == "<http://example.org/PointRole>"


def test_invalid_key_path_raises_value_error():
    validator = shifty.PreparedValidator(SHAPES)
    try:
        validator.witnesses(DATA, key_path="zea:role/", infer=False)
    except ValueError:
        pass
    else:
        raise AssertionError("expected a ValueError for a malformed key_path")


def test_repr_is_readable():
    validator = shifty.PreparedValidator(SHAPES)
    w = validator.witnesses(DATA, key_path="zea:role/zea:roleName", infer=False)[0]
    assert "PropertyWitness" in repr(w)
