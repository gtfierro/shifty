"""Shape maps: typed key -> value bindings one level above the evidence trees."""

import pytest

import shifty
from shifty import (
    BNode,
    Cls,
    Const,
    Datatype,
    Id,
    Inv,
    Iri,
    Key,
    Literal,
    Pred,
    Seq,
    ShapeRef,
    Star,
    Term,
)
from shifty.shapemap import _local, _path_from_json
from shifty.terms import parse as term_parse


PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .
"""

ZONE_SHAPES = PREFIXES + """
ex:ZoneShape a sh:NodeShape ;
    sh:targetClass ex:Zone ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape [ sh:class ex:TempSensor ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [
        sh:path ex:hasPart ;
        sh:qualifiedValueShape [ sh:class ex:Space ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [ sh:path ex:label ; sh:minCount 1 ] .
"""

ZONE_DATA = PREFIXES + """
ex:z1 a ex:Zone ; ex:hasPoint ex:t1 ; ex:hasPart ex:sp1 ; ex:label "zone one" .
ex:t1 a ex:TempSensor .
ex:sp1 a ex:Space .

# Partial: has the sensor and the label, is missing the space, and reaches a
# near-miss (a part that is not a Space).
ex:z2 a ex:Zone ; ex:hasPoint ex:t2 ; ex:hasPart ex:notaspace ; ex:label "zone two" .
ex:t2 a ex:TempSensor .
ex:notaspace a ex:Thing .
"""


def mapping_for(smap, focus):
    if isinstance(focus, str):
        focus = Iri(focus[1:-1]) if focus.startswith("<") else Iri(focus)
    (found,) = [m for m in smap if m.focus == focus]
    return found


# ── v1 behavior, migrated to the typed API ──────────────────────────────────────


def test_shape_names_and_grouping():
    smap = shifty.shape_map(ZONE_DATA, ZONE_SHAPES, infer=False)
    assert smap.shape_names == ["http://ex/ZoneShape"]
    assert not smap.conforms
    assert len(smap["http://ex/ZoneShape"]) == 2
    assert {m.focus for m in smap} == {Iri("http://ex/z1"), Iri("http://ex/z2")}
    assert [m.focus for m in smap.conforming("http://ex/ZoneShape")] == [Iri("http://ex/z1")]
    assert [m.focus for m in smap.nonconforming("http://ex/ZoneShape")] == [Iri("http://ex/z2")]


def test_conforming_focus_binds_every_key():
    smap = shifty.shape_map(ZONE_DATA, ZONE_SHAPES, infer=False)
    m = mapping_for(smap, "<http://ex/z1>")
    assert m.conforms
    assert {str(k) for k in m.bindings} == {"hasPoint→TempSensor", "hasPart→Space", "label"}
    assert {str(k): b.values for k, b in m.successful} == {
        "hasPoint→TempSensor": [Iri("http://ex/t1")],
        "hasPart→Space": [Iri("http://ex/sp1")],
        "label": [Literal("zone one")],
    }
    assert m.unsuccessful == []
    assert m["hasPoint→TempSensor"].ok
    assert m["hasPoint→TempSensor"].path == Pred("http://ex/hasPoint")
    assert m["hasPoint→TempSensor"].qualifier == Cls("http://ex/TempSensor")


def test_partial_focus_keeps_passing_bindings_and_exposes_the_gap():
    smap = shifty.shape_map(ZONE_DATA, ZONE_SHAPES, infer=False)
    m = mapping_for(smap, "<http://ex/z2>")
    assert not m.conforms

    # The two satisfied keys still carry their exact values, even though the
    # failure witness elides passing siblings.
    assert {str(k): b.values for k, b in m.successful} == {
        "hasPoint→TempSensor": [Iri("http://ex/t2")],
        "label": [Literal("zone two")],
    }

    ((key, binding),) = m.unsuccessful
    assert str(key) == "hasPart→Space"
    assert not binding.ok
    assert binding.missing == 1
    assert binding.values == []
    assert binding.rejected_values == [Iri("http://ex/notaspace")]
    assert binding.evidence["type"] == "count_low"
    assert "count_low" in binding.explain()

    # Full drill-down: the mapping keeps the FocusEvaluation, whose failure
    # side is the ordinary evidence object.
    assert m.evaluation.status == "fail"
    assert m.evaluation.failure.explain()


def test_from_run_without_session_leaves_elided_values_unknown():
    session = shifty.EvidenceSession(ZONE_SHAPES, ZONE_DATA, infer=False)
    run = session.validate()

    blind = shifty.ShapeMap.from_run(run)
    m = mapping_for(blind, "<http://ex/z2>")
    assert {str(k): b.values for k, b in m.successful} == {
        "hasPoint→TempSensor": None,
        "label": None,
    }
    # No session: names and value annotations cannot resolve either.
    assert m["hasPoint→TempSensor"].name is None

    sighted = shifty.ShapeMap.from_run(run, session)
    m = mapping_for(sighted, "<http://ex/z2>")
    assert {str(k): b.values for k, b in m.successful} == {
        "hasPoint→TempSensor": [Iri("http://ex/t2")],
        "label": [Literal("zone two")],
    }


def test_duplicate_paths_disambiguate_by_qualifier_then_ordinal():
    shapes = PREFIXES + """
    ex:VavShape a sh:NodeShape ;
        sh:targetClass ex:Vav ;
        sh:property [
            sh:path ex:hasPoint ;
            sh:qualifiedValueShape [ sh:class ex:FlowSensor ] ;
            sh:qualifiedMinCount 1
        ] ;
        sh:property [
            sh:path ex:hasPoint ;
            sh:qualifiedValueShape [ sh:class ex:TempSensor ] ;
            sh:qualifiedMinCount 1
        ] ;
        sh:property [ sh:path ex:label ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:label ; sh:maxCount 5 ] .
    """
    data = PREFIXES + """
    ex:v1 a ex:Vav ; ex:hasPoint ex:f1 , ex:t1 ; ex:label "vav" .
    ex:f1 a ex:FlowSensor .
    ex:t1 a ex:TempSensor .
    """
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    assert m.conforms
    assert {str(k) for k in m.bindings} == {
        "hasPoint→FlowSensor",
        "hasPoint→TempSensor",
        "label",
        "label#2",
    }
    assert m["hasPoint→FlowSensor"].values == [Iri("http://ex/f1")]
    assert m["hasPoint→TempSensor"].values == [Iri("http://ex/t1")]
    # The two `label` keys share (path=Pred(label), qualifier=None); ordinal
    # disambiguates them in authored/lowering order.
    label_keys = sorted((k for k in m.bindings if k.path == Pred("http://ex/label")),
                        key=lambda k: k.ordinal)
    assert [k.ordinal for k in label_keys] == [1, 2]
    assert str(label_keys[0]) == "label"
    assert str(label_keys[1]) == "label#2"


def test_atomic_statement_yields_single_binding():
    shapes = PREFIXES + """
    ex:IriShape a sh:NodeShape ; sh:targetClass ex:T ; sh:nodeKind sh:IRI .
    """
    data = PREFIXES + """
    ex:good a ex:T .
    """
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    assert m.conforms
    assert len(m.bindings) == 1


def test_empty_selection_keeps_shape_with_no_mappings():
    shapes = PREFIXES + """
    ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    ex:Unused a sh:NodeShape ; sh:targetClass ex:NeverPresent ; sh:nodeKind sh:IRI .
    """
    data = PREFIXES + """
    ex:z a ex:Zone ; ex:p ex:v .
    """
    smap = shifty.shape_map(data, shapes, infer=False)
    assert set(smap.shape_names) == {"http://ex/ZoneShape", "http://ex/Unused"}
    assert smap["http://ex/Unused"] == []


def test_to_dict_round_trips_to_json():
    import json

    smap = shifty.shape_map(ZONE_DATA, ZONE_SHAPES, infer=False)
    summary = json.loads(json.dumps(smap.to_dict()))
    zone = summary["shapes"]["http://ex/ZoneShape"]
    by_focus = {entry["focus"]: entry for entry in zone}
    assert by_focus["<http://ex/z2>"]["bindings"]["hasPart→Space"]["missing"] == 1
    assert by_focus["<http://ex/z1>"]["conforms"]
    # `name` is present (possibly null) per binding.
    assert "name" in by_focus["<http://ex/z1>"]["bindings"]["label"]


def test_evidence_for_contract():
    session = shifty.EvidenceSession(ZONE_SHAPES, ZONE_DATA, infer=False)
    run = session.validate()
    (statement,) = [s for s in run.statements if s.selected_foci]
    assert statement.shape_name == "http://ex/ZoneShape"
    (failing,) = [f for f in statement.selected_foci if f.status == "fail"]
    assert failing.failure.shape_name == "http://ex/ZoneShape"

    for child in failing.progress.evaluated_children:
        evidence = session.evidence_for(failing.focus, child.normalized_constraint_ref)
        assert evidence.status == child.status
        assert evidence.evidence_kind.status == child.status
        assert "evidence" in evidence.to_dict()

    with pytest.raises(ValueError):
        session.evidence_for(failing.focus, 10_000_000)


def test_or_statement_stays_whole():
    shapes = PREFIXES + """
    ex:EitherShape a sh:NodeShape ; sh:targetClass ex:T ;
        sh:or ( [ sh:path ex:a ; sh:minCount 1 ] [ sh:path ex:b ; sh:minCount 1 ] ) .
    """
    data = PREFIXES + """
    ex:hasA a ex:T ; ex:a ex:v .
    ex:hasNone a ex:T .
    """
    smap = shifty.shape_map(data, shapes, infer=False)
    good = mapping_for(smap, "<http://ex/hasA>")
    bad = mapping_for(smap, "<http://ex/hasNone>")
    assert good.conforms and not bad.conforms
    # Every mapping gets at least one binding either way.
    assert good.bindings and bad.bindings


# ── 1. Key equality/hash/str/ordinal; match statements ──────────────────────────


def test_key_equality_hash_and_str():
    k1 = Key(Pred("http://ex/p"), Cls("http://ex/C"))
    k2 = Key(Pred("http://ex/p"), Cls("http://ex/C"))
    k3 = Key(Pred("http://ex/p"), Cls("http://ex/C"), ordinal=2)
    assert k1 == k2
    assert hash(k1) == hash(k2)
    assert k1 != k3
    assert str(k1) == "p→C"
    assert str(k3) == "p→C#2"

    pathless = Key(None, None, kind="nodekind")
    assert str(pathless) == "nodekind"

    # Usable as real dict keys.
    d = {k1: "a", k3: "b"}
    assert d[Key(Pred("http://ex/p"), Cls("http://ex/C"))] == "a"


def test_key_and_term_match_statements():
    def describe(key: Key) -> str:
        match key:
            case Key(path=Pred(iri), qualifier=Cls(cls_iri)):
                return f"class-qualified {iri} -> {cls_iri}"
            case Key(path=Pred(iri), qualifier=None):
                return f"plain {iri}"
            case Key(path=None, qualifier=None):
                return f"pathless ({key.kind})"
            case _:
                return "other"

    assert describe(Key(Pred("http://ex/p"), Cls("http://ex/C"))) == (
        "class-qualified http://ex/p -> http://ex/C"
    )
    assert describe(Key(Pred("http://ex/p"), None)) == "plain http://ex/p"
    assert describe(Key(None, None, kind="nodekind")) == "pathless (nodekind)"

    def describe_term(term: Term) -> str:
        match term:
            case Iri(value):
                return f"iri:{value}"
            case Literal(value, None, None):
                return f"plain-literal:{value}"
            case Literal(value, datatype, language):
                return f"literal:{value}:{datatype}:{language}"
            case BNode(id):
                return f"bnode:{id}"

    assert describe_term(Iri("http://ex/a")) == "iri:http://ex/a"
    assert describe_term(Literal("hi")) == "plain-literal:hi"
    assert describe_term(Literal("1", "http://www.w3.org/2001/XMLSchema#integer", None)) == (
        "literal:1:http://www.w3.org/2001/XMLSchema#integer:None"
    )
    assert describe_term(BNode("b0")) == "bnode:b0"


# ── 2. Term.parse <-> .n3() round-trips ──────────────────────────────────────────


@pytest.mark.parametrize(
    "term",
    [
        Iri("http://ex/thing"),
        Literal("plain string"),
        Literal("42", "http://www.w3.org/2001/XMLSchema#integer"),
        Literal("bonjour", None, "fr"),
        BNode("b0"),
        Literal('has "quotes" inside'),
        Literal("has\nnewlines\nhere"),
        Literal('mixed "quotes"\nand newlines'),
    ],
)
def test_term_parse_n3_round_trip(term):
    assert term_parse(term.n3()) == term


def test_literal_to_python_and_str():
    assert Literal("hi").to_python() == "hi"
    assert str(Literal("hi")) == "hi"
    assert Literal("42", "http://www.w3.org/2001/XMLSchema#integer").to_python() == 42
    assert Literal("3.5", "http://www.w3.org/2001/XMLSchema#double").to_python() == 3.5
    assert Literal("true", "http://www.w3.org/2001/XMLSchema#boolean").to_python() is True
    assert Literal("not-a-number", "http://www.w3.org/2001/XMLSchema#integer").to_python() == (
        "not-a-number"
    )


def test_term_from_json():
    from shifty.terms import from_json

    assert from_json({"type": "uri", "value": "http://ex/a"}) == Iri("http://ex/a")
    assert from_json({"type": "bnode", "value": "b0"}) == BNode("b0")
    assert from_json({"type": "literal", "value": "hi"}) == Literal("hi")
    assert from_json(
        {"type": "literal", "value": "hi", "xml:lang": "en"}
    ) == Literal("hi", None, "en")
    assert from_json(
        {
            "type": "literal",
            "value": "1",
            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
        }
    ) == Literal("1", "http://www.w3.org/2001/XMLSchema#integer", None)


# ── 3. name_path ─────────────────────────────────────────────────────────────────

NAME_SHAPES = PREFIXES + """
@prefix zea: <http://ex/zea#> .
ex:VavShape a sh:NodeShape ;
    sh:targetClass ex:Vav ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:name "the flow point" ;
        zea:role ex:FlowRole ;
        sh:qualifiedValueShape [ sh:class ex:FlowSensor ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape [ sh:class ex:TempSensor ] ;
        sh:qualifiedMinCount 1
    ] .
ex:FlowRole zea:roleName "flowRole" .
"""

NAME_DATA = PREFIXES + """
ex:v1 a ex:Vav ; ex:hasPoint ex:f1, ex:t1 .
ex:f1 a ex:FlowSensor .
ex:t1 a ex:TempSensor .
"""


def test_name_path_direct_sh_name():
    smap = shifty.shape_map(NAME_DATA, NAME_SHAPES, infer=False)
    (m,) = list(smap)
    assert m["hasPoint→FlowSensor"].name == "the flow point"
    assert m["hasPoint→FlowSensor"].names == ["the flow point"]


def test_name_path_multi_hop():
    smap = shifty.shape_map(
        NAME_DATA,
        NAME_SHAPES,
        infer=False,
        name_path="<http://ex/zea#role>/<http://ex/zea#roleName>",
    )
    (m,) = list(smap)
    assert m["hasPoint→FlowSensor"].name == "flowRole"


def test_name_path_absent_annotation_is_none():
    smap = shifty.shape_map(NAME_DATA, NAME_SHAPES, infer=False)
    (m,) = list(smap)
    binding = m["hasPoint→TempSensor"]
    assert binding.name is None
    assert binding.names is None


def test_by_name_lookup_and_value_map_fallback():
    smap = shifty.shape_map(NAME_DATA, NAME_SHAPES, infer=False)
    (m,) = list(smap)
    assert m.by_name("the flow point") is m["hasPoint→FlowSensor"]
    with pytest.raises(KeyError):
        m.by_name("does not exist")

    projected = m.value_map(by="name")
    assert projected["the flow point"] == [Iri("http://ex/f1")]
    # No name on the temp-sensor binding: falls back to str(key).
    assert projected["hasPoint→TempSensor"] == [Iri("http://ex/t1")]


def test_value_map_python_coercion():
    # Two properties, so the datatype+minCount pair on `ex:n` stays one
    # collapsed binding rather than the NodeShape itself collapsing away.
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:n ; sh:datatype xsd:integer ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:label ; sh:minCount 1 ] .
    """
    data = PREFIXES + 'ex:a a ex:T ; ex:n 42 ; ex:label "x" .'
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    projected = m.value_map(python=True)
    n_key = next(k for k in projected if str(k) == "n→integer")
    assert isinstance(n_key, Key)
    assert projected[n_key] == [42]


def test_name_path_none_skips_resolution():
    smap = shifty.shape_map(NAME_DATA, NAME_SHAPES, infer=False, name_path=None)
    (m,) = list(smap)
    assert m["hasPoint→FlowSensor"].name is None


def test_shape_map_uses_embedded_shapes_when_second_argument_is_omitted():
    smap = shifty.shape_map(NAME_SHAPES + NAME_DATA, infer=False)
    (mapping,) = list(smap)
    assert mapping.conforms


# ── 4. source provenance used by shape-map naming ────────────────────────────────


def test_binding_names_resolves_named_and_blank_property_shapes():
    session = shifty.EvidenceSession(NAME_SHAPES, NAME_DATA, infer=False)
    names = session._inner._binding_names()
    assert "the flow point" in {v for values in names.values() for v in values}

    # Deterministic across two parses of the same document.
    session2 = shifty.EvidenceSession(NAME_SHAPES, NAME_DATA, infer=False)
    names2 = session2._inner._binding_names()
    assert sorted(v for values in names.values() for v in values) == sorted(
        v for values in names2.values() for v in values
    )


def test_single_property_shape_keeps_its_sh_name_after_conjunction_elision():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [
            sh:path ex:p ; sh:name "only point" ;
            sh:qualifiedValueShape [ sh:class ex:V ] ;
            sh:qualifiedMinCount 1
        ] .
    """
    data = PREFIXES + """
    ex:a a ex:T ; ex:p ex:v .
    ex:v a ex:V .
    """
    (mapping,) = list(shifty.shape_map(data, shapes, infer=False))
    assert mapping["p→V"].name == "only point"


def test_optional_qualified_property_extracts_qualifying_values():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:required ; sh:minCount 1 ] ;
        sh:property [
            sh:path ex:optional ; sh:name "optional point" ;
            sh:qualifiedValueShape [ sh:class ex:V ]
        ] .
    """
    data = PREFIXES + """
    ex:a a ex:T ; ex:required ex:present ; ex:optional ex:good, ex:other .
    ex:good a ex:V .
    ex:other a ex:Other .
    """
    (mapping,) = list(shifty.shape_map(data, shapes, infer=False))
    optional = next(binding for binding in mapping.bindings.values() if binding.name == "optional point")
    assert optional.values == [Iri("http://ex/good")]


def test_default_name_path_works_for_rdflib_graph_input():
    import rdflib

    shapes = rdflib.Graph()
    shapes.parse(data=NAME_SHAPES, format="turtle")
    (mapping,) = list(shifty.shape_map(NAME_DATA, shapes, infer=False))
    assert mapping["hasPoint→FlowSensor"].name == "the flow point"


# ── 5. ShapeRef qualifier ────────────────────────────────────────────────────────

SHAPE_REF_SHAPES = PREFIXES + """
ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
    sh:property [
        sh:path ex:hasPart ;
        sh:qualifiedValueShape [ sh:node ex:HeatingCoilShape ] ;
        sh:qualifiedMinCount 1
    ] .
ex:HeatingCoilShape a sh:NodeShape ; sh:targetClass ex:HeatingCoil .
"""

SHAPE_REF_DATA = PREFIXES + """
ex:z a ex:Zone ; ex:hasPart ex:coil1 .
ex:coil1 a ex:HeatingCoil .
"""


def test_shape_ref_qualifier_from_qualified_value_shape():
    smap = shifty.shape_map(SHAPE_REF_DATA, SHAPE_REF_SHAPES, infer=False)
    (m,) = smap["http://ex/ZoneShape"]
    ((key, binding),) = m.successful
    assert key == Key(Pred("http://ex/hasPart"), ShapeRef("http://ex/HeatingCoilShape"))
    assert str(key) == "hasPart→HeatingCoilShape"
    assert binding.values == [Iri("http://ex/coil1")]


def test_shape_ref_qualifier_from_plain_sh_node():
    shapes = PREFIXES + """
    ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
        sh:property [ sh:path ex:hasPart ; sh:node ex:HeatingCoilShape ] .
    ex:HeatingCoilShape a sh:NodeShape ; sh:targetClass ex:HeatingCoil .
    """
    smap = shifty.shape_map(SHAPE_REF_DATA, shapes, infer=False)
    (m,) = smap["http://ex/ZoneShape"]
    ((key, binding),) = m.successful
    assert key.qualifier == ShapeRef("http://ex/HeatingCoilShape")


# ── 6. value_paths ────────────────────────────────────────────────────────────────

VALUE_PATH_SHAPES = PREFIXES + """
ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
    sh:property [ sh:path ex:hasPoint ; sh:minCount 1 ] .
"""

VALUE_PATH_DATA = PREFIXES + """
ex:z a ex:Zone ; ex:hasPoint ex:t1, ex:t2 .
ex:t1 ex:hasRef ex:r1 .
ex:r1 ex:hasId "TS-1" .
ex:t2 ex:hasRef ex:r2 .
ex:r2 ex:hasId "TS-2" .
"""


class _CountingSessionProxy:
    """Wraps an `EvidenceSession._inner` to count `resolve_path` calls,
    without exposing `_inner` itself (so `getattr(session, "_inner", session)`
    falls through to this proxy)."""

    def __init__(self, raw):
        self._raw = raw
        self.resolve_path_calls = 0

    def _resolve_path(self, nodes, path):
        self.resolve_path_calls += 1
        return self._raw._resolve_path(nodes, path)

    def __getattr__(self, name):
        return getattr(self._raw, name)


def test_value_paths_two_hop_and_batching():
    session = shifty.EvidenceSession(VALUE_PATH_SHAPES, VALUE_PATH_DATA, infer=False)
    run = session.validate()
    proxy = _CountingSessionProxy(session._inner)

    smap = shifty.ShapeMap.from_run(run, proxy, value_paths={"ts": "ex:hasRef/ex:hasId"})
    (m,) = list(smap)
    binding = m["hasPoint"]

    assert proxy.resolve_path_calls == 0  # lazy: nothing resolved yet

    annotations = binding.annotations
    assert proxy.resolve_path_calls == 1  # one batched call, not one per value

    by_value = {v: reached for v, reached in annotations["ts"].items()}
    assert by_value[Iri("http://ex/t1")] == [Literal("TS-1")]
    assert by_value[Iri("http://ex/t2")] == [Literal("TS-2")]

    # Reading annotations again (e.g. via another binding backed by the same
    # resolver) does not issue another call.
    _ = binding.annotated_values
    assert proxy.resolve_path_calls == 1


def test_value_paths_no_annotation_is_empty_list():
    shapes = PREFIXES + """
    ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
        sh:property [ sh:path ex:hasPoint ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:z a ex:Zone ; ex:hasPoint ex:t1 ."
    smap = shifty.shape_map(data, shapes, infer=False, value_paths={"ts": "ex:hasRef/ex:hasId"})
    (m,) = list(smap)
    binding = m["hasPoint"]
    (bound,) = binding.annotated_values
    assert bound.annotations["ts"] == []


def test_value_paths_absent_is_free():
    smap = shifty.shape_map(VALUE_PATH_DATA, VALUE_PATH_SHAPES, infer=False)
    (m,) = list(smap)
    binding = m["hasPoint"]
    assert binding.annotations == {}
    assert all(bv.annotations == {} for bv in binding.annotated_values)


# ── 7. Cardinality + severity ────────────────────────────────────────────────────


def test_cardinality_min_count():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 2 ] .
    """
    data = PREFIXES + "ex:a a ex:T ; ex:p ex:v1, ex:v2, ex:v3 ."
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    binding = m["p"]
    assert binding.min == 2
    assert binding.max is None
    assert binding.observed == 3
    assert not binding.expects_single


def test_cardinality_max_count_and_expects_single():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:maxCount 1 ] .
    """
    data = PREFIXES + "ex:a a ex:T ; ex:p ex:v1 ."
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    binding = m["p"]
    assert binding.min == 1
    assert binding.max == 1
    assert binding.expects_single
    assert binding.observed == 1


def test_cardinality_qualified_count():
    smap = shifty.shape_map(ZONE_DATA, ZONE_SHAPES, infer=False)
    m = mapping_for(smap, "<http://ex/z1>")
    binding = m["hasPoint→TempSensor"]
    assert binding.min == 1
    assert binding.max is None
    assert binding.observed == 1


def test_cardinality_collapsed_datatype_plus_min_count():
    # Two properties, so the datatype+minCount pair stays one collapsed
    # binding rather than the NodeShape itself collapsing away (which would
    # split them into two independent progress children).
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:label ; sh:datatype xsd:string ; sh:minCount 1 ] ;
        sh:property [ sh:path ex:other ; sh:minCount 1 ] .
    """
    data = PREFIXES + 'ex:a a ex:T ; ex:label "hi" ; ex:other ex:v .'
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    binding = m["label→string"]
    # The datatype forall's synthetic `max=0` (from the `∃≤0 π.¬φ` encoding)
    # must not leak into the real cardinality.
    assert binding.min == 1
    assert binding.max is None
    assert binding.observed == 1


def test_severity_from_sh_severity():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [
            sh:path ex:p ; sh:minCount 1 ; sh:severity sh:Warning
        ] .
    """
    data = PREFIXES + "ex:a a ex:T ."
    smap = shifty.shape_map(data, shapes, infer=False, minimum_severity="warning")
    (m,) = list(smap)
    binding = m["p"]
    assert binding.severity == "warning"
    assert not binding.ok


def test_severity_defaults_to_violation():
    shapes = PREFIXES + """
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
    """
    data = PREFIXES + "ex:a a ex:T ; ex:p ex:v ."
    smap = shifty.shape_map(data, shapes, infer=False)
    (m,) = list(smap)
    assert m["p"].severity == "violation"


# ── 8. for_focus ──────────────────────────────────────────────────────────────────


def test_for_focus_across_shapes():
    shapes = PREFIXES + """
    ex:ZoneShape a sh:NodeShape ; sh:targetClass ex:Zone ;
        sh:property [ sh:path ex:label ; sh:minCount 1 ] .
    ex:NamedThingShape a sh:NodeShape ; sh:targetClass ex:NamedThing ;
        sh:property [ sh:path ex:name ; sh:minCount 1 ] .
    """
    data = PREFIXES + """
    ex:z a ex:Zone, ex:NamedThing ; ex:label "zone" ; ex:name "shared" .
    """
    smap = shifty.shape_map(data, shapes, infer=False)
    mappings = smap.for_focus("<http://ex/z>")
    assert {m.shape_name for m in mappings} == {
        "http://ex/ZoneShape",
        "http://ex/NamedThingShape",
    }

    # Accepts a Term or a bare IRI too.
    assert smap.for_focus(Iri("http://ex/z")) == mappings
    assert smap.for_focus("http://ex/z") == mappings
    assert smap.for_focus("<http://ex/does-not-exist>") == []


# ── Path/qualifier plumbing ───────────────────────────────────────────────────────


def test_path_from_json_variants():
    assert _path_from_json("Id") == Id()
    assert _path_from_json({"Pred": {"value": "http://ex/p"}}) == Pred("http://ex/p")
    assert _path_from_json({"Inverse": {"Pred": {"value": "http://ex/p"}}}) == Inv(
        Pred("http://ex/p")
    )
    assert _path_from_json(
        {"Seq": [{"Pred": {"value": "http://ex/a"}}, {"Pred": {"value": "http://ex/b"}}]}
    ) == Seq((Pred("http://ex/a"), Pred("http://ex/b")))
    assert _path_from_json({"Star": {"Pred": {"value": "http://ex/p"}}}) == Star(
        Pred("http://ex/p")
    )


def test_local_name_compaction():
    assert _local("http://ex/hasPoint") == "hasPoint"
    assert _local("http://ex#hasPoint") == "hasPoint"
    assert _local("urn:zonepac-app/zonepac-zone") == "zonepac-zone"
