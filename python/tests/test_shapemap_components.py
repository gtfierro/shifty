"""Authored component identities remain distinct from lowered algebra kinds."""

import json

import pytest

import shifty

SH = "http://www.w3.org/ns/shacl#"
PREFIXES = f"""
@prefix ex: <http://ex/> .
@prefix sh: <{SH}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""
DATA = PREFIXES + 'ex:a a ex:T ; ex:p "x" .'


@pytest.mark.parametrize("with_session", [False, True])
@pytest.mark.parametrize("sibling", [False, True])
def test_combined_property_components_survive_lowering_and_run_only_projection(
    with_session, sibling
):
    other = "; sh:property [ sh:path ex:q ; sh:maxCount 1 ]" if sibling else ""
    shapes = (
        PREFIXES
        + f"""
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "slot" ; sh:order 1.25 ;
            sh:pattern "x" ; sh:flags "i" ; sh:datatype xsd:string ;
            sh:minCount 1 ; sh:maxCount 1 ] {other} .
    """
    )
    session = shifty.EvidenceSession(shapes, DATA, infer=False)
    smap = shifty.ShapeMap.from_run(
        session.validate(), session if with_session else None
    )
    (mapping,) = smap
    binding = next(b for k, b in mapping.items() if str(k) != "q")
    assert binding.key.kind == "count"
    assert binding.constraint_components == tuple(
        SH + name + "ConstraintComponent"
        for name in [
            "Datatype",
            "MaxCount",
            "MinCount",
            "Pattern",
        ]
    )
    assert binding.min == binding.max == 1
    serialized = json.loads(json.dumps(smap.to_dict()))
    slot = serialized["shapes"]["http://ex/S"][0]["bindings"][str(binding.key)]
    assert slot["order"] == "1.25"
    assert slot["position"] == 1
    assert slot["constraint_components"] == list(binding.constraint_components)


def test_pattern_is_identifiable_without_changing_key_kind_or_bounds():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ] .
    """
    )
    (mapping,) = shifty.shape_map(DATA, shapes, infer=False)
    binding = mapping["p"]
    assert binding.constraint_components == (SH + "PatternConstraintComponent",)
    assert binding.key.kind == "count"
    assert binding.min is binding.max is None


def test_qualified_slot_components_exclude_its_nested_qualifier_components():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ;
            sh:qualifiedValueShape [ sh:class ex:V ] ;
            sh:qualifiedMinCount 1 ; sh:qualifiedMaxCount 2 ] .
    """
    )
    (mapping,) = shifty.shape_map(DATA, shapes, infer=False)
    (binding,) = mapping.values()
    assert binding.constraint_components == (
        SH + "QualifiedMaxCountConstraintComponent",
        SH + "QualifiedMinCountConstraintComponent",
    )


def test_pathless_authored_shape_retains_component_metadata():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ; sh:nodeKind sh:IRI .
    """
    )
    (mapping,) = shifty.shape_map(DATA, shapes, infer=False)
    (binding,) = mapping.values()
    assert binding.constraint_components == (SH + "NodeKindConstraintComponent",)


def test_custom_component_metadata_respects_parameters_and_shape_kind():
    shapes = (
        PREFIXES
        + """
    ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:limit ] ;
        sh:propertyValidator [ sh:select "SELECT $this WHERE { FILTER(false) }" ] .
    ex:NodeOnly a sh:ConstraintComponent ; sh:parameter [ sh:path ex:limit ] ;
        sh:nodeValidator [ sh:select "SELECT $this WHERE { FILTER(false) }" ] .
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "active" ; ex:limit 1 ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:q ; sh:name "inactive" ; sh:maxCount 1 ] .
    """
    )
    (mapping,) = shifty.shape_map(DATA, shapes, infer=False)
    assert mapping.by_name("active").constraint_components == (
        "http://ex/C",
        SH + "MinCountConstraintComponent",
    )
    assert mapping.by_name("inactive").constraint_components == (
        SH + "MaxCountConstraintComponent",
    )


@pytest.mark.parametrize("sibling", [False, True])
def test_optional_property_values_are_unknown_without_session(sibling):
    other = "; sh:property [ sh:path ex:q ; sh:maxCount 1 ]" if sibling else ""
    shapes = (
        PREFIXES
        + f"""
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:name "optional" ;
            sh:qualifiedValueShape [ sh:datatype xsd:string ] ] {other} .
    """
    )
    session = shifty.EvidenceSession(shapes, DATA, infer=False)
    (mapping,) = shifty.ShapeMap.from_run(session.validate())
    binding = next(b for k, b in mapping.items() if str(k) != "q")
    assert binding.ok
    assert binding.constraint_components == ()
    assert binding.values is None
    assert binding.observed is None
    assert binding.key not in mapping.value_map()
