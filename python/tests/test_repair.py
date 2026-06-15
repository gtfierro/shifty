"""Tests for the symbolic-repair driver API (``shifty.RepairSession``)."""

from __future__ import annotations

import shifty

SHAPES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string ] .
"""

# bob has two names → sh:maxCount 1 violated.
DATA_MAXCOUNT = """
@prefix ex: <http://example.org/> .
ex:bob a ex:Person ; ex:name "Bob", "Bobby" .
"""

# alice has no name → sh:minCount 1 violated.
DATA_MINCOUNT = """
@prefix ex: <http://example.org/> .
ex:alice a ex:Person .
"""


def session(data: str) -> shifty.RepairSession:
    return shifty.RepairSession(SHAPES, data, infer=False)


def test_witnesses_enumerate_the_horizon():
    s = session(DATA_MAXCOUNT)
    ws = s.witnesses()
    assert len(ws) == 1
    fw = ws[0]
    assert fw.focus == "<http://example.org/bob>"
    assert fw.statement == 0
    assert "φ" in fw.target  # the rendered target selector


def test_witness_summary_and_explain():
    fw = session(DATA_MAXCOUNT).witnesses()[0]
    atoms = fw.summary()
    assert len(atoms) == 1
    a = atoms[0]
    assert a.kind == "count_high"
    assert a.path == "<http://example.org/name>"
    assert "max 1" in a.detail
    assert "CountHigh" in fw.explain()


def test_conforming_graph_has_no_witnesses():
    good = """
    @prefix ex: <http://example.org/> .
    ex:carol a ex:Person ; ex:name "Carol" .
    """
    assert session(good).witnesses() == []


def test_repair_tree_holes_and_candidates():
    fw = session(DATA_MAXCOUNT).witnesses()[0]
    tree = fw.repair_tree()
    assert not tree.is_blocked
    holes = tree.holes()
    assert len(holes) == 1
    # the over-count repair offers the two existing names as deletable options.
    cands = holes[0].candidates()
    assert set(cands) == {'"Bob"', '"Bobby"'}


def test_choices_expose_the_repeat():
    fw = session(DATA_MAXCOUNT).witnesses()[0]
    choices = fw.repair_tree().choices()
    kinds = {c.kind for c in choices}
    assert "repeat" in kinds


def test_discover_then_bind_and_gate_is_sound():
    s = session(DATA_MAXCOUNT)
    fw = s.witnesses()[0]
    tree = fw.repair_tree()

    plan = shifty.RepairPlan()
    for c in tree.choices():
        if c.kind == "repeat":
            plan.count(c.node_id, c.min)

    # discovery pass surfaces the per-instance hole; bind it.
    inst = tree.instantiate(plan)
    assert not inst.is_complete
    for hole in inst.open_holes:
        plan.bind(hole.id, hole.candidates()[0])

    inst = tree.instantiate(plan)
    assert inst.is_complete
    assert len(inst.delta.delete) == 1
    assert not inst.delta.add

    outcome = s.gate(inst.delta)
    assert outcome.is_sound
    assert outcome.is_progress
    assert len(outcome.fixed) == 1
    assert not outcome.introduced


def test_advance_reaches_conformance():
    s = session(DATA_MAXCOUNT)
    fw = s.witnesses()[0]
    tree = fw.repair_tree()
    plan = shifty.RepairPlan()
    for c in tree.choices():
        if c.kind == "repeat":
            plan.count(c.node_id, c.min)
    for hole in tree.instantiate(plan).open_holes:
        plan.bind(hole.id, hole.candidates()[0])
    delta = tree.instantiate(plan).delta

    s2 = s.advance(delta)
    assert s2.witnesses() == []  # the patched graph conforms


def test_apply_returns_rdflib_graph_minus_one_triple():
    s = session(DATA_MAXCOUNT)
    fw = s.witnesses()[0]
    tree = fw.repair_tree()
    plan = shifty.RepairPlan()
    for c in tree.choices():
        if c.kind == "repeat":
            plan.count(c.node_id, c.min)
    for hole in tree.instantiate(plan).open_holes:
        plan.bind(hole.id, hole.candidates()[0])
    delta = tree.instantiate(plan).delta

    g = s.apply(delta)
    import rdflib

    assert isinstance(g, rdflib.Graph)
    # original: type + two names = 3 triples; after deleting one name = 2.
    assert len(g) == 2


def test_mincount_repair_adds_a_hole_to_fill():
    s = session(DATA_MINCOUNT)
    fw = s.witnesses()[0]
    tree = fw.repair_tree()
    assert not tree.is_blocked
    # minCount is repaired by adding a name with an open hole (typed value).
    holes = tree.holes()
    assert holes, "expected at least one hole to fill"


def test_bind_round_trips_a_candidate_string():
    s = session(DATA_MAXCOUNT)
    tree = s.witnesses()[0].repair_tree()
    plan = shifty.RepairPlan()
    for c in tree.choices():
        if c.kind == "repeat":
            plan.count(c.node_id, c.min)
    hole = tree.instantiate(plan).open_holes[0]
    value = hole.candidates()[0]
    plan.bind(hole.id, value)  # the exact string candidates() returned
    delta = tree.instantiate(plan).delta
    s, p, o = delta.delete[0]
    assert o == value


def test_embedded_shapes_no_data_graph():
    embedded = SHAPES + DATA_MAXCOUNT
    s = shifty.RepairSession(embedded, infer=False)
    assert len(s.witnesses()) == 1


# ── subgraph patches (delta_from_graph) ──────────────────────────────────────

QUAL_SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:S a sh:NodeShape ; sh:targetNode ex:x ;
    sh:property [ sh:path ex:part ;
        sh:qualifiedValueShape ex:PartShape ; sh:qualifiedMinCount 1 ] .
ex:PartShape a sh:NodeShape ;
    sh:property [ sh:path ex:kind ; sh:minCount 1 ] .
"""

QUAL_DATA = """
@prefix ex: <http://example.org/> .
ex:x a ex:Thing .
"""


def test_subgraph_patch_with_conforming_node_is_accepted():
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    assert len(s.witnesses()) == 1
    # a subgraph that adds the part AND makes it conform (it has an ex:kind).
    delta = shifty.delta_from_graph(
        '@prefix ex: <http://example.org/> .\n'
        'ex:x ex:part ex:p1 . ex:p1 ex:kind "valve" .'
    )
    outcome = s.gate(delta)
    assert outcome.is_sound
    assert outcome.is_progress
    assert s.advance(delta).witnesses() == []


def test_subgraph_patch_without_conforming_structure_is_rejected():
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    # adds the part but not its required ex:kind → the qualified count stays unmet.
    delta = shifty.delta_from_graph(
        '@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 .'
    )
    outcome = s.gate(delta)
    assert outcome.is_sound  # introduces no new top-level violation
    assert not outcome.is_progress  # but fixes nothing


def test_delta_from_graph_accepts_rdflib_graph():
    import rdflib

    g = rdflib.Graph()
    g.parse(
        data='@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 . ex:p1 ex:kind "v" .',
        format="turtle",
    )
    delta = shifty.delta_from_graph(g)
    assert len(delta.add) == 2
    assert not delta.delete


def test_repair_delta_from_ntriples_add_and_delete():
    nt_add = '<http://example.org/x> <http://example.org/part> <http://example.org/p1> .\n'
    nt_del = '<http://example.org/x> <http://example.org/old> "gone" .\n'
    delta = shifty.RepairDelta.from_ntriples(nt_add, nt_del)
    assert len(delta.add) == 1
    assert len(delta.delete) == 1


# ── recursive build (conforms_to / repair_node_against) ──────────────────────


def test_conforms_to_hole_exposes_subshape_id():
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    hole = s.witnesses()[0].repair_tree().holes()[0]
    assert hole.conforms_to is not None  # a 'conforms to @N' hole


def test_value_hole_has_no_conforms_to():
    s = shifty.RepairSession(SHAPES, DATA_MAXCOUNT, infer=False)
    hole = s.witnesses()[0].repair_tree().holes()[0]
    assert hole.conforms_to is None  # the maxCount repair binds a value, not a node


def test_repair_node_against_builds_the_subshape():
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    sid = s.witnesses()[0].repair_tree().holes()[0].conforms_to
    sub = s.repair_node_against("<urn:f1>", sid)
    assert sub is not None
    assert sub.holes()  # the fresh node must gain at least one property
    # building it out + linking it makes sound progress:
    delta = shifty.delta_from_graph(
        '@prefix ex: <http://example.org/> .'
        ' ex:x ex:part <urn:f1> . <urn:f1> ex:kind "k" .'
    )
    assert s.gate(delta).is_progress


def test_repair_node_against_none_when_already_conforms():
    data = QUAL_DATA + '\n@prefix ex: <http://example.org/> .\n<urn:ok> ex:kind "x" .'
    s = shifty.RepairSession(QUAL_SHAPES, data, infer=False)
    sid = s.witnesses()[0].repair_tree().holes()[0].conforms_to
    assert s.repair_node_against("<urn:ok>", sid) is None
