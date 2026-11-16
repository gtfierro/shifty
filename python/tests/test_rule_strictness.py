"""Brick-style SHACL-AF rules execute under the default strict policy."""

import pytest
from rdflib import BNode, Graph, Literal, Namespace
from rdflib.namespace import RDF

import shifty

EX = Namespace("http://ex/")
BRICK = Namespace("https://brickschema.org/schema/Brick#")
PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""

GUARDED_RULE = (
    PREFIXES
    + """
ex:AddDefaultEVSEChargerDirection a sh:NodeShape ;
    sh:targetClass brick:Electric_Vehicle_Charging_Station ;
    sh:rule [ a sh:SPARQLRule ; sh:construct '''
        CONSTRUCT {
            $this brick:electricVehicleChargerDirectionality
                [ brick:value "unidirectional"^^xsd:string ]
        } WHERE {
            $this rdf:type brick:Electric_Vehicle_Charging_Station .
            FILTER NOT EXISTS {
                $this brick:electricVehicleChargerDirectionality ?direction .
            }
        }
    ''' ] .
"""
)


def test_construct_values_on_an_unbound_variable():
    shapes = (
        PREFIXES
        + """
ex:BuildingMeterRule a sh:NodeShape ; sh:targetNode ex:Meter ;
    sh:rule [ a sh:SPARQLRule ; sh:construct '''
        CONSTRUCT { $this ?prop ?value }
        WHERE {
            VALUES ?prop { brick:hasQuantity brick:hasSubstance }
            $this rdfs:subClassOf ?parent .
            ?parent ?prop ?value .
        }
    ''' ] .
ex:Parent brick:hasQuantity ex:Energy ; brick:hasSubstance ex:Electricity ;
    ex:unrelated ex:Other .
"""
    )
    data = Graph().parse(
        data=PREFIXES + "ex:Meter rdfs:subClassOf ex:Parent .", format="turtle"
    )
    result = shifty.infer(data, shapes, in_place=True)
    assert result.inferred_count == 2
    assert (EX.Meter, BRICK.hasQuantity, EX.Energy) in data
    assert (EX.Meter, BRICK.hasSubstance, EX.Electricity) in data
    assert (EX.Meter, EX.unrelated, EX.Other) not in data


@pytest.mark.parametrize("policy", ["error", "ignore"])
def test_values_collision_is_invalid_and_names_shape(policy):
    shapes = (
        PREFIXES
        + """
ex:BadConstraint a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
    sh:select "SELECT $this WHERE { VALUES ?this { ex:x } $this ex:p ?o }" ] .
"""
    )
    with pytest.raises(ValueError, match="VALUES must not bind.*\\?this") as error:
        shifty.infer(PREFIXES + "ex:x ex:p ex:o .", shapes, on_unsupported=policy)
    assert "on shape <http://ex/BadConstraint>" in str(error.value)


@pytest.mark.parametrize("policy", ["error", "ignore", "strict", "lenient"])
def test_guarded_blank_nodes_terminate_and_preserve_nested_triples(policy):
    data = Graph()
    foci = [EX.charger, BNode()]
    for focus in [*foci, EX.existing]:
        data.add((focus, RDF.type, BRICK.Electric_Vehicle_Charging_Station))
    data.add((EX.existing, BRICK.electricVehicleChargerDirectionality, EX.direction))
    result = shifty.infer(data, GUARDED_RULE, in_place=True, on_unsupported=policy)
    assert result.inferred_count == 4
    assert not result.diagnostics
    directions = []
    for focus in foci:
        (direction,) = data.objects(focus, BRICK.electricVehicleChargerDirectionality)
        assert isinstance(direction, BNode)
        assert (
            direction,
            BRICK.value,
            Literal("unidirectional"),
        ) in data
        directions.append(direction)
    assert directions[0] != directions[1]
    assert list(
        data.objects(EX.existing, BRICK.electricVehicleChargerDirectionality)
    ) == [EX.direction]
    assert shifty.infer(data, GUARDED_RULE, in_place=True).inferred_count == 0


@pytest.mark.parametrize("call", [shifty.validate, shifty.validate_algebra, "prepared"])
def test_validation_automatic_inference_accepts_guarded_blank_nodes(call):
    data = Graph().parse(
        data=PREFIXES + "ex:charger a brick:Electric_Vehicle_Charging_Station .",
        format="turtle",
    )
    if call == "prepared":
        shifty.PreparedValidator(GUARDED_RULE).validate(data, in_place=True)
    else:
        call(data, GUARDED_RULE, in_place=True)
    (direction,) = data.objects(EX.charger, BRICK.electricVehicleChargerDirectionality)
    assert (
        direction,
        BRICK.value,
        Literal("unidirectional"),
    ) in data


@pytest.mark.parametrize("policy", ["error", "ignore"])
@pytest.mark.parametrize(
    "construct",
    [
        "CONSTRUCT { $this ex:made [] } WHERE {}",
        "CONSTRUCT { $this ex:made ?node } WHERE { BIND(BNODE() AS ?node) }",
        # Reading a predicate that never changes must not suppress another firing.
        "CONSTRUCT { $this ex:made [] } WHERE { $this ex:p ex:o }",
    ],
)
def test_nonterminating_rules_fail_without_partial_writeback(policy, construct):
    shapes = (
        PREFIXES
        + f'''ex:GeneratorShape a sh:NodeShape ; sh:targetNode ex:x ;
            sh:rule ex:Generator .
        ex:Generator a sh:SPARQLRule ; sh:construct "{construct}" .'''
    )
    data = Graph().parse(data=PREFIXES + "ex:x ex:p ex:o .", format="turtle")
    before = set(data)
    with pytest.raises(shifty.ValidationFailure, match="within 3 iterations") as error:
        shifty.infer(
            data, shapes, in_place=True, on_unsupported=policy, max_iterations=3
        )
    assert "rule <http://ex/Generator> on shape <http://ex/GeneratorShape>" in str(
        error.value
    )
    assert set(data) == before


def test_iteration_limit_must_be_positive():
    with pytest.raises(ValueError, match="max_iterations must be greater than zero"):
        shifty.infer("", max_iterations=0)


def test_malformed_construct_names_rule_and_shape():
    shapes = (
        PREFIXES
        + """ex:RuleShape a sh:NodeShape ; sh:targetNode ex:x ; sh:rule ex:Rule .
        ex:Rule sh:construct "SELECT $this WHERE {}" ."""
    )
    with pytest.raises(ValueError, match="expected SPARQL CONSTRUCT") as error:
        shifty.infer("", shapes)
    assert "<http://ex/Rule>" in str(error.value)
    assert "<http://ex/RuleShape>" in str(error.value)
