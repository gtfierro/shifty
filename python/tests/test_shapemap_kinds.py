"""Shape-map kinds describe authored SHACL families, independently of status."""

import pytest

import shifty
from shifty import Key, Pred

PREFIXES = """
@prefix ex: <http://ex/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""


@pytest.mark.parametrize("with_session", [False, True])
@pytest.mark.parametrize(
    "constraints, kind, ok",
    [
        ('sh:pattern "x"', "pattern", True),
        ('sh:pattern "missing"', "pattern", False),
        ("sh:datatype xsd:string", "datatype", True),
        ("sh:nodeKind sh:IRI", "node_kind", False),
        ("sh:class ex:V", "class", False),
        ("sh:minCount 2", "count", False),
        ("sh:minCount 1 ; sh:maxCount 2", "count", True),
        (
            "sh:qualifiedValueShape [ sh:datatype xsd:string ] ; sh:qualifiedMinCount 2",
            "qualified_count",
            False,
        ),
        ('sh:hasValue "x"', "has_value", True),
        ('sh:in ( "x" "y" )', "in", True),
        ('sh:pattern "x" ; sh:minCount 1', "compound", True),
        ('sh:pattern "x" ; sh:datatype xsd:string', "compound", True),
    ],
)
def test_authored_family_is_available_without_a_session(
    with_session, constraints, kind, ok
):
    shapes = (
        PREFIXES
        + f"""
    ex:S a sh:NodeShape ; sh:targetNode ex:a ;
        sh:property [ sh:path ex:p ; {constraints} ] .
    """
    )
    session = shifty.EvidenceSession(shapes, PREFIXES + 'ex:a ex:p "x" .', infer=False)
    (mapping,) = shifty.ShapeMap.from_run(
        session.validate(), session if with_session else None
    )
    (binding,) = mapping.values()
    assert binding.ok is ok
    assert binding.key.kind == kind
    assert binding.key == Key(Pred("http://ex/p"), binding.qualifier, kind=kind)
    assert mapping[binding.key] is binding


def test_different_families_on_the_same_path_keep_distinct_display_keys():
    shapes = (
        PREFIXES
        + """
    ex:S a sh:NodeShape ; sh:targetNode ex:a ;
        sh:property [ sh:path ex:p ; sh:pattern "x" ; sh:order 1 ] ;
        sh:property [ sh:path ex:p ; sh:maxCount 2 ; sh:order 2 ] .
    """
    )
    (mapping,) = shifty.shape_map(PREFIXES + 'ex:a ex:p "x" .', shapes, infer=False)
    assert [(str(key), key.kind) for key in mapping] == [
        ("p", "pattern"),
        ("p#2", "count"),
    ]
    assert len(mapping.value_map(by="name")) == 2
    assert mapping["p"].key.kind == "pattern"
    assert mapping["p#2"].key.kind == "count"


@pytest.mark.parametrize("second", [False, True])
def test_custom_components_keep_their_iris_when_classifying_families(second):
    other = "ex:two 2 ;" if second else ""
    shapes = (
        PREFIXES
        + f"""
    ex:One a sh:ConstraintComponent ; sh:parameter [ sh:path ex:one ] ;
        sh:propertyValidator [ sh:select "SELECT $this WHERE {{ FILTER(false) }}" ] .
    ex:Two a sh:ConstraintComponent ; sh:parameter [ sh:path ex:two ] ;
        sh:propertyValidator [ sh:select "SELECT $this WHERE {{ FILTER(false) }}" ] .
    ex:S a sh:NodeShape ; sh:targetNode ex:a ;
        sh:property [ sh:path ex:p ; ex:one 1 ; {other} ] .
    """
    )
    (mapping,) = shifty.shape_map(PREFIXES + 'ex:a ex:p "x" .', shapes, infer=False)
    (binding,) = mapping.values()
    assert binding.ok
    assert binding.key.kind == ("compound" if second else "custom")
    assert binding.constraint_components == (
        ("http://ex/One", "http://ex/Two") if second else ("http://ex/One",)
    )
