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
    # the rendered target resolves the class instead of printing a bare φ
    assert fw.target == "class(<http://example.org/Person>)"
    # …and the same selector is available structured, for external processing
    assert fw.selector.kind == shifty.TargetKind.Class
    assert fw.selector.value == "<http://example.org/Person>"
    assert str(fw.selector) == fw.target


def test_witness_summary_and_explain():
    fw = session(DATA_MAXCOUNT).witnesses()[0]
    atoms = fw.summary()
    assert len(atoms) == 1
    a = atoms[0]
    assert a.kind == shifty.WitnessKind.CountHigh
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
    assert shifty.ChoiceKind.Repeat in kinds


def test_discover_then_bind_and_gate_is_sound():
    s = session(DATA_MAXCOUNT)
    fw = s.witnesses()[0]
    tree = fw.repair_tree()

    plan = shifty.RepairPlan()
    for c in tree.choices():
        if c.kind == shifty.ChoiceKind.Repeat:
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
        if c.kind == shifty.ChoiceKind.Repeat:
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
        if c.kind == shifty.ChoiceKind.Repeat:
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
        if c.kind == shifty.ChoiceKind.Repeat:
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


def test_delta_from_graph_unions_list_of_adds():
    add_a = '@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 .'
    add_b = '@prefix ex: <http://example.org/> .\nex:p1 ex:kind "valve" .'
    delta = shifty.delta_from_graph([add_a, add_b])
    assert len(delta.add) == 2
    assert not delta.delete
    # Same triples as a single concatenated document.
    single = shifty.delta_from_graph(add_a + " " + add_b)
    assert {tuple(t) for t in delta.add} == {tuple(t) for t in single.add}


def test_delta_from_graph_unions_list_of_deletes():
    del_a = '@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 .'
    del_b = '@prefix ex: <http://example.org/> .\nex:x ex:part ex:p2 .'
    delta = shifty.delta_from_graph(delete=[del_a, del_b])
    assert not delta.add
    assert len(delta.delete) == 2


def test_delta_from_graph_empty_and_none_serialize_to_nothing():
    assert not shifty.delta_from_graph(None, None).add
    assert not shifty.delta_from_graph(None, None).delete
    assert not shifty.delta_from_graph([], []).add
    assert not shifty.delta_from_graph([], []).delete
    assert not shifty.delta_from_graph().add
    assert not shifty.delta_from_graph().delete


def test_delta_from_graph_mixed_rdflib_and_turtle_in_list():
    import rdflib

    g = rdflib.Graph()
    g.parse(
        data='@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 .',
        format="turtle",
    )
    turtle = '@prefix ex: <http://example.org/> .\nex:p1 ex:kind "valve" .'
    delta = shifty.delta_from_graph([g, turtle])
    assert len(delta.add) == 2


def test_delta_from_graph_delete_then_add_is_net_add():
    # A triple present on both sides of the patch is a net-add: deletes are
    # applied first, then adds, so the re-add wins and the triple survives in
    # the materialized graph. Lists make it easy to hit this across sources.
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    triple = '@prefix ex: <http://example.org/> .\nex:x ex:part ex:p1 .'
    delta = shifty.delta_from_graph(add=[triple], delete=[triple])
    # The triple is in both sides...
    assert any(t[2] == "<http://example.org/p1>" for t in delta.add)
    assert any(t[2] == "<http://example.org/p1>" for t in delta.delete)
    # ...and survives in the patched graph (net-add).
    import rdflib
    patched = s.apply(delta)
    ex = rdflib.Namespace("http://example.org/")
    assert (ex.x, ex.part, ex.p1) in patched


def test_delta_from_graph_replaces_via_delete_then_add():
    # Standard "replace" pattern: delete the old value, add the new one. The
    # old value is gone and the new value is present after apply.
    s = shifty.RepairSession(QUAL_SHAPES, QUAL_DATA, infer=False)
    base = (
        '@prefix ex: <http://example.org/> .\n'
        'ex:x ex:part ex:old . ex:old ex:kind "v" .'
    )
    session = shifty.RepairSession(QUAL_SHAPES, base, infer=False)
    delta = shifty.delta_from_graph(
        add='@prefix ex: <http://example.org/> .\nex:x ex:part ex:new . ex:new ex:kind "v" .',
        delete='@prefix ex: <http://example.org/> .\nex:x ex:part ex:old . ex:old ex:kind "v" .',
    )
    patched = session.apply(delta)
    import rdflib
    ex = rdflib.Namespace("http://example.org/")
    assert (ex.x, ex.part, ex.new) in patched
    assert (ex.x, ex.part, ex.old) not in patched


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


CLASS_SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:S a sh:NodeShape ; sh:targetNode ex:x ;
    sh:property [ sh:path ex:part ;
        sh:qualifiedValueShape ex:PartShape ; sh:qualifiedMinCount 1 ] .
ex:PartShape a sh:NodeShape ; sh:class ex:Widget .
"""


def test_class_qualified_build_is_not_blocked_and_types_the_node():
    # sh:class lowers to rdf:type/rdfs:subClassOf* — building must materialize the
    # type assertion reflexively rather than block on "path materialization".
    s = shifty.RepairSession(CLASS_SHAPES, QUAL_DATA, infer=False)
    sid = s.witnesses()[0].repair_tree().holes()[0].conforms_to
    sub = s.repair_node_against("<urn:p1>", sid)
    assert sub is not None
    assert not sub.is_blocked
    # the build's value hole is the forced class constant:
    assert any(h.constraint.startswith("= ") for h in sub.holes())
    # typing the node + linking it fixes the violation:
    delta = shifty.delta_from_graph(
        '@prefix ex: <http://example.org/> .'
        ' ex:x ex:part <urn:p1> . <urn:p1> a ex:Widget .'
    )
    assert s.gate(delta).is_progress


# The class hierarchy lives only in the shapes graph (split data/shapes). A value
# typed with a *subclass* of the required class must still clear `sh:class`: the
# session evaluates against the data ∪ shapes context, so `rdf:type/subClassOf*`
# can follow `ex:Sub ⊑ ex:Super` even though the axiom is not in the data graph.
SUBCLASS_SHAPES = """
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <http://example.org/> .
ex:Sub rdfs:subClassOf ex:Super .
ex:VavShape a sh:NodeShape ; sh:targetClass ex:Vav ;
    sh:property [ sh:path ex:hasPoint ; sh:minCount 1 ; sh:class ex:Super ] .
"""

SUBCLASS_DATA = """
@prefix ex: <http://example.org/> .
ex:vav1 a ex:Vav .
"""


def test_subclass_typed_proposal_makes_progress_via_shapes_hierarchy():
    import rdflib

    s = shifty.RepairSession(SUBCLASS_SHAPES, SUBCLASS_DATA, infer=False)
    # propose a point typed with the *subclass* of the required class.
    delta = shifty.delta_from_graph(
        "@prefix ex: <http://example.org/> ."
        " ex:vav1 ex:hasPoint ex:p1 . ex:p1 a ex:Sub ."
    )
    assert s.gate(delta).is_progress
    # the evaluation context is internal: the shapes/ontology (subClassOf) never
    # leak into the emitted data graph.
    g = s.to_graph()
    assert not any(p == rdflib.RDFS.subClassOf for _, p, _ in g)


# ── full rendering & multi-shape obligations ─────────────────────────────────

# minCount + an sh:or of two classes on the same path: the added value must be
# an instance of one class or the other — a `ConformsTo` of an `Or` shape.
OR_OF_CLASSES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:S a sh:NodeShape ; sh:targetNode ex:vav1 ;
    sh:property [ sh:path ex:hasPoint ; sh:minCount 1 ;
        sh:or ( [ sh:class ex:TempSensor ] [ sh:class ex:FlowSensor ] ) ] .
ex:vav1 a ex:VAV .
"""

# A non-⊤ count qualifier (sh:qualifiedValueShape) *and* a sibling sh:class on the
# same path: every obligation is carried as a `ConformsToAll`.
CONFORMS_ALL = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:S a sh:NodeShape ; sh:targetNode ex:x ;
    sh:property [ sh:path ex:p ;
        sh:qualifiedValueShape ex:Q ; sh:qualifiedMinCount 1 ;
        sh:class ex:C ] .
ex:Q a sh:NodeShape ; sh:nodeKind sh:IRI .
ex:x a ex:Thing .
"""


def test_hole_constraint_fully_renders_an_or_of_classes():
    # The hole over an `sh:or` of classes used to print bare slot pointers
    # (`@2 ∨ @5`); now every sub-shape is inlined — no `@` leaks out.
    s = shifty.RepairSession(OR_OF_CLASSES, infer=False)
    hole = s.witnesses()[0].repair_tree().holes()[0]
    assert "@" not in hole.constraint
    assert "instance of" in hole.constraint and " or " in hole.constraint
    # the sub-shape id round-trips through the session's deep renderer.
    assert hole.conforms_to is not None
    assert s.describe_shape(hole.conforms_to) == hole.constraint


def test_conforms_to_all_exposes_every_subshape_and_definition():
    s = shifty.RepairSession(CONFORMS_ALL, infer=False)
    hole = s.witnesses()[0].repair_tree().holes()[0]
    # a multi-shape obligation: the single-id accessor is None, the set is complete.
    assert hole.conforms_to is None
    assert len(hole.conforms_to_shapes) == 2
    subs = hole.sub_shapes()
    assert len(subs) == 2
    for sid, definition in subs:
        assert isinstance(sid, int)
        assert "@" not in definition and definition
        # the session resolves the same id to the same definition.
        assert s.describe_shape(sid) == definition
    # the rendered constraint names both obligations, with no pointers.
    assert "@" not in hole.constraint
    assert "instance of" in hole.constraint and " and " in hole.constraint
    # each sub-shape id is a valid build target; the `instance of ex:C` obligation
    # a bare fresh node does not yet meet drives a recursive repair.
    trees = {
        sid: s.repair_node_against("<urn:fresh>", sid)
        for sid in hole.conforms_to_shapes
    }
    class_ids = [sid for sid, d in subs if "instance of" in d]
    assert class_ids and all(trees[sid] is not None for sid in class_ids)


def test_explain_has_no_unresolved_pointers():
    s = shifty.RepairSession(OR_OF_CLASSES, infer=False)
    text = s.witnesses()[0].repair_tree().explain()
    # the constraint is spelled out, not left as `@2 ∨ @5`.
    assert "instance of" in text and " or " in text
    assert "∨" not in text


# ── shape-scoped witnesses & satisfactions ───────────────────────────────────

TWO_SHAPE_SHAPES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
ex:WidgetShape a sh:NodeShape ;
    sh:targetClass ex:Widget ;
    sh:property [ sh:path ex:sku ; sh:minCount 1 ] .
"""

# carol passes ex:PersonShape, dan fails it; gizmo fails the unrelated WidgetShape.
TWO_SHAPE_DATA = """
@prefix ex: <http://example.org/> .
ex:carol a ex:Person ; ex:name "Carol" .
ex:dan   a ex:Person .
ex:gizmo a ex:Widget .
"""


def two_shape_session() -> shifty.RepairSession:
    return shifty.RepairSession(TWO_SHAPE_SHAPES, TWO_SHAPE_DATA, infer=False)


def test_witnesses_for_scopes_failures_to_one_shape():
    s = two_shape_session()
    # the whole horizon spans both shapes; witnesses_for narrows to ex:PersonShape.
    assert {w.focus for w in s.witnesses()} == {
        "<http://example.org/dan>",
        "<http://example.org/gizmo>",
    }
    ws = s.witnesses_for("http://example.org/PersonShape")
    assert [w.focus for w in ws] == ["<http://example.org/dan>"]
    # still a total FocusWitness: it synthesizes a repair tree.
    assert not ws[0].repair_tree().is_blocked


def test_witnesses_for_accepts_angle_brackets_and_rejects_unknown():
    s = two_shape_session()
    assert len(s.witnesses_for("<http://example.org/PersonShape>")) == 1
    try:
        s.witnesses_for("http://example.org/Nope")
    except ValueError as e:
        assert "Nope" in str(e)
    else:
        raise AssertionError("expected ValueError for an unknown shape")


def test_satisfactions_for_lists_passing_foci_with_matched_values():
    s = two_shape_session()
    sats = s.satisfactions_for("http://example.org/PersonShape")
    assert [fs.focus for fs in sats] == ["<http://example.org/carol>"]
    fs = sats[0]
    assert fs.statement == 0
    # the satisfaction side carries the same structured target as the witness side
    assert fs.target == "class(<http://example.org/Person>)"
    assert fs.selector.kind == shifty.TargetKind.Class
    assert fs.selector.value == "<http://example.org/Person>"
    # the matched value for the checked property surfaces in the flat summary.
    matched = [(a.path, a.value) for a in fs.summary() if a.kind == shifty.SatKind.Match]
    assert ("<http://example.org/name>", '"Carol"') in matched
    assert "Held" in fs.explain()


def test_satisfactions_for_is_scoped_and_dual_to_witnesses_for():
    s = two_shape_session()
    # dan fails, carol passes — the two views partition the targeted foci.
    fails = {w.focus for w in s.witnesses_for("http://example.org/PersonShape")}
    passes = {fs.focus for fs in s.satisfactions_for("http://example.org/PersonShape")}
    assert fails == {"<http://example.org/dan>"}
    assert passes == {"<http://example.org/carol>"}
    # the unrelated WidgetShape has no passing foci here.
    assert s.satisfactions_for("http://example.org/WidgetShape") == []


# ── structured targets ───────────────────────────────────────────────────────

TARGET_SHAPES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .

ex:SubjShape a sh:NodeShape ;
    sh:targetSubjectsOf ex:knows ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
ex:ObjShape a sh:NodeShape ;
    sh:targetObjectsOf ex:owner ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
ex:NodeShape_ a sh:NodeShape ;
    sh:targetNode ex:root ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""

TARGET_DATA = """
@prefix ex: <http://example.org/> .
ex:eve   ex:knows ex:carol .
ex:thing ex:owner ex:dan .
ex:root  ex:other 1 .
"""


def test_structured_target_kinds_and_values():
    s = shifty.RepairSession(TARGET_SHAPES, TARGET_DATA, infer=False)

    subj = s.witnesses_for("http://example.org/SubjShape")[0].selector
    assert subj.kind == shifty.TargetKind.SubjectsOf
    assert subj.value == "<http://example.org/knows>"

    obj = s.witnesses_for("http://example.org/ObjShape")[0].selector
    assert obj.kind == shifty.TargetKind.ObjectsOf
    assert obj.value == "<http://example.org/owner>"

    node = s.witnesses_for("http://example.org/NodeShape_")[0].selector
    assert node.kind == shifty.TargetKind.Node
    assert node.value == "<http://example.org/root>"

    # the structured value round-trips: a class IRI is a parseable N-Triples term
    assert all(t.value.startswith("<") for t in (subj, obj, node))
