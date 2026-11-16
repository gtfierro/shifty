"""AF result annotations preserve bindings, defaults and multiple values."""

import pytest
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import SH

import shifty

EX = Namespace("http://ex/")
PREFIX = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> . "
INTERFACES = ["w3c", "algebra", "prepared", "evidence"]
DECLARATIONS = """sh:resultAnnotation
    [sh:annotationProperty ex:score ; sh:annotationVarName "actual" ; sh:annotationValue 0],
    [sh:annotationProperty ex:label],
    [sh:annotationProperty ex:tag ; sh:annotationValue "first", "second"],
    [sh:annotationProperty ex:tag ; sh:annotationValue "second"],
    [sh:annotationProperty ex:absent]"""


def wire_term(term):
    if term.get("type") == "uri":
        return URIRef(term["value"]).n3()
    if term.get("type") == "bnode":
        return BNode(term["value"]).n3()
    return Literal(
        term["value"], datatype=term.get("datatype"), lang=term.get("language")
    ).n3()


def evidence_rows(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "result_annotations":
                for row in child:
                    yield {(p["value"], wire_term(v)) for p, v in row}
            elif key == "annotations" and child and isinstance(child[0], list):
                yield {(p["value"], wire_term(v)) for p, v in child}
            else:
                yield from evidence_rows(child)
    elif isinstance(value, list):
        for child in value:
            yield from evidence_rows(child)


def annotations(interface, shapes, data=""):
    if interface in ("w3c", "prepared"):
        if interface == "w3c":
            conforms, report, _ = shifty.validate(data, shapes, infer=False)
        else:
            conforms, report, _ = shifty.PreparedValidator(shapes).validate(
                data, infer=False
            )
        assert not conforms
        return [
            {
                (str(p), v.n3())
                for p, v in report.predicate_objects(result)
                if str(p).startswith(str(EX))
            }
            for result in report.objects(None, SH.result)
        ]
    if interface == "evidence":
        run = shifty.EvidenceSession(shapes, data, infer=False).validate()
        assert not run.conforms
        return list(evidence_rows(run.to_dict()))
    outcome = shifty.validate_algebra(data, shapes, infer=False)
    assert not outcome.conforms
    return [
        set(reason.annotations)
        for violation in outcome.violations
        for reason in violation.reasons
    ]


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize("custom", [False, True])
def test_solution_bindings_override_defaults_and_preserve_all_other_defaults(
    interface, custom
):
    query = 'SELECT $this ?actual ?label WHERE { BIND(42 AS ?actual) BIND("observed" AS ?label) }'
    if custom:
        shapes = (
            PREFIX
            + f"""ex:C a sh:ConstraintComponent ; sh:parameter [sh:path ex:activate] ;
            sh:nodeValidator [sh:select '{query}' ; {DECLARATIONS}] .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:activate true ."""
        )
    else:
        shapes = (
            PREFIX
            + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ;
            sh:sparql [sh:select '{query}' ; {DECLARATIONS}] ."""
        )
    assert annotations(interface, shapes) == [
        {
            (str(EX.score), Literal(42).n3()),
            (str(EX.label), Literal("observed").n3()),
            (str(EX.tag), Literal("first").n3()),
            (str(EX.tag), Literal("second").n3()),
        }
    ]


@pytest.mark.parametrize("interface", INTERFACES)
def test_each_solution_gets_its_own_annotations(interface):
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
        sh:select "SELECT $this ?score WHERE { VALUES ?score { 1 2 } }" ;
        sh:resultAnnotation [sh:annotationProperty ex:score] ] ."""
    )
    rows = annotations(interface, shapes)
    assert {frozenset(row) for row in rows} == {
        frozenset({(str(EX.score), Literal(1).n3())}),
        frozenset({(str(EX.score), Literal(2).n3())}),
    }


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize("validator", ["sh:nodeValidator", "sh:validator"])
def test_ask_validator_uses_all_constant_defaults(interface, validator):
    shapes = (
        PREFIX
        + f"""ex:C a sh:ConstraintComponent ; sh:parameter [sh:path ex:activate] ;
        {validator} [sh:ask "ASK {{ FILTER(false) }}" ;
            sh:resultAnnotation [sh:annotationProperty ex:tag ; sh:annotationValue "one", "two"]] .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:activate true ."""
    )
    assert annotations(interface, shapes) == [
        {(str(EX.tag), Literal("one").n3()), (str(EX.tag), Literal("two").n3())}
    ]


@pytest.mark.parametrize("interface", INTERFACES)
def test_projected_prebound_this_is_available_to_annotations(interface):
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
        sh:select "SELECT $this WHERE {}" ;
        sh:resultAnnotation [sh:annotationProperty ex:node ; sh:annotationVarName "this"] ] ."""
    )
    assert annotations(interface, shapes) == [{(str(EX.node), EX.x.n3())}]


@pytest.mark.parametrize("interface", INTERFACES)
@pytest.mark.parametrize(
    "declaration",
    [
        "sh:annotationValue 1",
        'sh:annotationProperty "literal"',
        "sh:annotationProperty ex:a, ex:b",
        "sh:annotationProperty ex:a ; sh:annotationVarName 42",
        'sh:annotationProperty ex:a ; sh:annotationVarName "a", "b"',
    ],
)
def test_malformed_annotations_are_rejected_on_load(interface, declaration):
    shapes = (
        PREFIX
        + f"""ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
        sh:select "SELECT $this WHERE {{}}" ; sh:resultAnnotation [{declaration}] ] ."""
    )
    with pytest.raises(ValueError, match="result annotation"):
        annotations(interface, shapes)


def test_annotation_blank_node_bindings_keep_caller_identity():
    data = Graph()
    node = BNode("same")
    data.add((node, EX.p, Literal(True)))
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:p ; sh:sparql [
        sh:select "SELECT $this ($this AS ?node) WHERE {}" ;
        sh:resultAnnotation [sh:annotationProperty ex:node] ] .
        _:same ex:unrelated true ."""
    )
    assert annotations("w3c", shapes, data) == [{(str(EX.node), node.n3())}]
    assert annotations("algebra", shapes, data) == [{(str(EX.node), node.n3())}]
    assert annotations("evidence", shapes, data) == [{(str(EX.node), node.n3())}]


@pytest.mark.parametrize("interface", INTERFACES)
def test_multiple_variables_can_annotate_the_same_property(interface):
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
        sh:select 'SELECT $this ?left ?right WHERE { BIND(ex:value AS ?left) BIND("bonjour"@fr AS ?right) }' ;
        sh:resultAnnotation [sh:annotationProperty ex:tag ; sh:annotationVarName "left"],
            [sh:annotationProperty ex:tag ; sh:annotationVarName "right"] ] ."""
    )
    assert annotations(interface, shapes) == [
        {
            (str(EX.tag), EX.value.n3()),
            (str(EX.tag), Literal("bonjour", lang="fr").n3()),
        }
    ]


@pytest.mark.parametrize("interface", INTERFACES)
def test_property_select_validator_preserves_path_and_annotations(interface):
    shapes = (
        PREFIX
        + """ex:C a sh:ConstraintComponent ; sh:parameter [sh:path ex:activate] ;
        sh:propertyValidator [sh:select "SELECT $this ?value ?detail WHERE { $this $PATH ?value . BIND(STR(?value) AS ?detail) }" ;
            sh:resultAnnotation [sh:annotationProperty ex:detail],
                [sh:annotationProperty ex:tag ; sh:annotationValue "one", "two"]] .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:property [sh:path ex:p ; ex:activate true] ."""
    )
    assert annotations(interface, shapes, PREFIX + 'ex:x ex:p "value" .') == [
        {
            (str(EX.detail), Literal("value").n3()),
            (str(EX.tag), Literal("one").n3()),
            (str(EX.tag), Literal("two").n3()),
        }
    ]


@pytest.mark.parametrize("interface", INTERFACES)
def test_explicit_unbound_variable_uses_multiple_defaults(interface):
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:sparql [
        sh:select "SELECT $this ?unbound WHERE {}" ;
        sh:resultAnnotation [sh:annotationProperty ex:tag ; sh:annotationVarName "unbound" ; sh:annotationValue "one", "two"] ] ."""
    )
    assert annotations(interface, shapes) == [
        {(str(EX.tag), Literal("one").n3()), (str(EX.tag), Literal("two").n3())}
    ]
