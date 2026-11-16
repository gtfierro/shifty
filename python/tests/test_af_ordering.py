"""AF shape/rule ordering and isolation, exercised through every session entry point."""

import pytest
from rdflib import Graph, Namespace

import shifty

EX = Namespace("http://ex/")
PREFIX = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> . "


def rule(order, value):
    return f'''[ a sh:SPARQLRule ; sh:order {order} ; sh:construct """
        CONSTRUCT {{ ex:x ex:chosen ex:{value} }} WHERE {{
        FILTER NOT EXISTS {{ ex:x ex:chosen ?previous }} }}""" ]'''


@pytest.mark.parametrize(
    "shape_a,shape_b,rule_a,rule_b,expected",
    [
        (0, 1, 10, 0, {"A"}),
        ("0.11", "0.12", 10, 0, {"A"}),
        ("-0.12", "-0.11", 10, 0, {"A"}),
        (0, 0, 10, 0, {"A", "B"}),
        ("0.10", "0.1", "0.12", "0.11", {"A", "B"}),
    ],
)
def test_shapes_are_ordered_before_rules(shape_a, shape_b, rule_a, rule_b, expected):
    shapes = (
        PREFIX
        + f"""
    ex:A a sh:NodeShape ; sh:targetNode ex:x ; sh:order {shape_a} ;
        sh:rule {rule(rule_a, "A")} .
    ex:B a sh:NodeShape ; sh:targetNode ex:x ; sh:order {shape_b} ;
        sh:rule {rule(rule_b, "B")} .
    """
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.objects(EX.x, EX.chosen)) == {EX[value] for value in expected}


@pytest.mark.parametrize("second,expected", [("0.12", {"A"}), ("0.110", {"A", "B"})])
def test_rules_inside_one_shape_use_decimal_order_and_isolate_ties(second, expected):
    shapes = (
        PREFIX
        + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule {rule("0.11", "A")}, {rule(second, "B")} ."""
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.objects(EX.x, EX.chosen)) == {EX[value] for value in expected}


@pytest.mark.parametrize("interface", ["w3c", "algebra", "prepared", "evidence"])
def test_inferred_order_is_shared_by_validation_interfaces(interface):
    shapes = (
        PREFIX
        + f"""
    ex:A a sh:NodeShape ; sh:targetNode ex:x ; sh:order 0.1 ;
        sh:rule {rule(10, "A")} .
    ex:B a sh:NodeShape ; sh:targetNode ex:x ; sh:order 0.2 ;
        sh:rule {rule(0, "B")} .
    ex:Check a sh:NodeShape ; sh:targetNode ex:x ;
        sh:property [ sh:path ex:chosen ; sh:hasValue ex:A ; sh:maxCount 1 ] ."""
    )
    if interface == "w3c":
        conforms = shifty.validate("", shapes)[0]
    elif interface == "prepared":
        conforms = shifty.PreparedValidator(shapes).validate("")[0]
    elif interface == "evidence":
        conforms = shifty.EvidenceSession(shapes, "").validate().conforms
    else:
        conforms = shifty.validate_algebra("", shapes).conforms
    assert conforms


def test_focus_nodes_in_one_rule_observe_the_same_snapshot():
    shapes = (
        PREFIX
        + '''ex:S a sh:NodeShape ; sh:targetNode ex:a, ex:b ;
        sh:rule [ a sh:SPARQLRule ; sh:construct """
            CONSTRUCT { $this ex:chosen true } WHERE {
                FILTER NOT EXISTS { ?any ex:chosen true } }""" ] .'''
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.subjects(EX.chosen, None)) == {EX.a, EX.b}


def test_decimal_function_parameter_orders():
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:expression [ ex:subtract (5 2) ] .
    ex:subtract a sh:SPARQLFunction ;
        sh:parameter [ sh:path ex:z ; sh:order 0.11 ],
                     [ sh:path ex:a ; sh:order 0.12 ] ;
        sh:select "SELECT (?z - ?a = 3 AS ?result) WHERE {}" ."""
    )
    assert shifty.validate("", shapes, infer=False)[0]


def test_tied_shapes_keep_their_own_earlier_rule_output():
    shapes = (
        PREFIX
        + f"""
    ex:A a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule {rule("0.1", "A")}, {rule("0.2", "hidden")} .
    ex:B a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule {rule(100, "B")} ."""
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.objects(EX.x, EX.chosen)) == {EX.A, EX.B}


def test_orders_are_not_rounded_to_machine_precision():
    shapes = (
        PREFIX
        + f"""
    ex:A a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule {rule("0.10000000000000000001", "A")},
                {rule("0.10000000000000000002", "B")} ."""
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.objects(EX.x, EX.chosen)) == {EX.A}


def test_current_pass_additions_activate_later_rules_before_competing_guards():
    shapes = (
        PREFIX
        + """
    ex:A a sh:NodeShape ; sh:order 0 ; sh:targetNode ex:x ;
        sh:rule [a sh:SPARQLRule ; sh:construct "CONSTRUCT { $this ex:trigger true } WHERE { $this ex:seed true }"] .
    ex:B a sh:NodeShape ; sh:order 1 ; sh:targetNode ex:x ;
        sh:rule [a sh:SPARQLRule ; sh:construct "CONSTRUCT { $this ex:chosen ex:B } WHERE { $this ex:trigger true . FILTER NOT EXISTS { $this ex:chosen ?value } }"] .
    ex:C a sh:NodeShape ; sh:order 2 ; sh:targetNode ex:x ;
        sh:rule [a sh:SPARQLRule ; sh:construct "CONSTRUCT { $this ex:chosen ex:C } WHERE { $this ex:seed true . FILTER NOT EXISTS { $this ex:chosen ?value } }"] .
    ex:D a sh:NodeShape ; sh:order 3 ; sh:targetNode ex:x ;
        sh:rule [a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:seed ; sh:object true] ."""
    )
    data = Graph()
    shifty.infer(data, shapes, in_place=True)
    assert set(data.objects(EX.x, EX.chosen)) == {EX.B}
