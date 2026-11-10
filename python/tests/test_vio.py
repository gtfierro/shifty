"""Violation generation (``shifty.experimental.vio``) over the evidence engine."""

import json
import os
import subprocess
import sys
from collections import Counter

import pytest
import rdflib
from rdflib.compare import isomorphic

import shifty
from shifty.experimental.vio import ExpansionLimits, Universe, main

PREFIXES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .
"""

# The running example of arXiv 2611.07342, Fig. 6: an AHU must have a point
# that conforms to NamedPoint, a point with a description of length >= 3.
AHU_SHAPES = (
    PREFIXES
    + """
ex:AHUShape a sh:NodeShape ;
    sh:targetClass ex:AHU ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape ex:NamedPoint ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [ sh:path ex:label ; sh:datatype xsd:string ; sh:minCount 1 ; sh:maxCount 1 ] ;
    sh:property [ sh:path ex:feeds ; sh:class ex:Zone ; sh:maxCount 2 ] ;
    sh:property [ sh:path ex:status ; sh:in ( "on" "off" ) ] .

ex:NamedPoint a sh:NodeShape ;
    sh:property [ sh:path ex:description ; sh:minCount 1 ; sh:minLength 3 ] .

ex:ZoneShape a sh:NodeShape ;
    sh:targetClass ex:Zone ;
    sh:or ( [ sh:path ex:area ; sh:minCount 1 ] [ sh:path ex:volume ; sh:minCount 1 ] ) ;
    sh:not [ sh:path ex:deprecated ; sh:hasValue true ] .
"""
)

AHU_DATA = (
    PREFIXES
    + """
ex:ahu_1 a ex:AHU ;
    ex:hasPoint ex:RAT_Sensor ;
    ex:label "AHU-1" ;
    ex:feeds ex:z1 ;
    ex:status "on" .
ex:RAT_Sensor ex:description "RAT_01" .
ex:z1 a ex:Zone ; ex:area 30 .
ex:z2 a ex:Zone ; ex:volume 90 ; ex:area 12 ; ex:deprecated false .
"""
)


def t(s, p, o):
    return (s, p, o)


def ex(name):
    return f"<http://ex/{name}>"


@pytest.fixture(scope="module")
def ahu():
    return Universe(AHU_DATA, AHU_SHAPES)


def details(universe, focus):
    return {
        "; ".join(op.detail for op in e.operations) for e in universe.where(focus=focus)
    }


def lines(triples):
    return {f"{s} {p} {o} ." for s, p, o in triples}


def test_reference_graph_conforms_and_every_pair_is_broken(ahu):
    assert ahu.baseline_failures == frozenset()
    assert {(e.focus, e.shape) for e in ahu} == {
        (ex("ahu_1"), "http://ex/AHUShape"),
        (ex("z1"), "http://ex/ZoneShape"),
        (ex("z2"), "http://ex/ZoneShape"),
    }
    assert set(ahu.reference_ntriples.splitlines()) >= {
        f'{ex("ahu_1")} {ex("status")} "on" .'
    }


def test_expansion_reaches_the_three_leaves_of_the_running_example(ahu):
    """Fig. 7: VIO(ahu_1, φ) bottoms out in deleting hasPoint, deleting the
    description, or shortening "RAT_01" below three characters."""
    injections = ahu.where(focus="http://ex/ahu_1")
    has_point = t(ex("ahu_1"), ex("hasPoint"), ex("RAT_Sensor"))
    description = t(ex("RAT_Sensor"), ex("description"), '"RAT_01"')
    assert any(e.deletes == {has_point} and not e.adds for e in injections)
    assert any(e.deletes == {description} and not e.adds for e in injections)
    short = [
        e
        for e in injections
        if e.deletes == {description}
        and len(e.adds) == 1
        and next(iter(e.adds))[2] == '"RA"'
    ]
    assert short and short[0].kinds == ("substitute",)


def test_every_injection_breaks_its_target_alone(ahu):
    assert len(ahu) > 20
    assert len({e.key for e in ahu}) == len(ahu)
    for e in ahu:
        case = ahu.where(lambda x, key=e.key: x.key == key).draw(1)
        assert case.injections == (e,)
        assert any(f.focus == e.focus and f.injections == (e,) for f in case.failures)


def test_universals_and_maxcounts_add_values(ahu):
    ahu_details = details(ahu, ex("ahu_1"))
    # sh:in: a status outside the list, added or swapped in place.
    assert any(d.startswith("add ") and "via status" in d for d in ahu_details)
    assert any(d.startswith('replace "on" with') for d in ahu_details)
    # sh:datatype: a label that is not an xsd:string.
    assert any(d.startswith('replace "AHU-1" with') for d in ahu_details)
    # sh:maxCount 2 with one value needs two more. The sibling sh:class Zone
    # makes z2, an existing Zone, the preferred new value.
    feeds = [
        e
        for e in ahu.where(focus=ex("ahu_1"))
        if e.kinds == ("add_value", "add_value")
        and all(a[1] == ex("feeds") for a in e.adds)
    ]
    assert feeds
    assert t(ex("ahu_1"), ex("feeds"), ex("z2")) in feeds[0].adds


def test_class_constraint_breaks_by_removing_the_type(ahu):
    z1_type = t(
        ex("z1"), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", ex("Zone")
    )
    assert any(e.deletes == {z1_type} for e in ahu.where(focus=ex("ahu_1")))


def test_negation_flips_to_the_satisfying_direction(ahu):
    """VIO(z, ¬φ) = SAT(z, φ): make the deprecated-true check hold."""
    true = '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    z1 = ahu.where(focus=ex("z1"))
    assert any(e.adds == {t(ex("z1"), ex("deprecated"), true)} for e in z1)
    assert any(op.polarity == "satisfy" for e in z1 for op in e.operations)
    z2 = ahu.where(focus=ex("z2"))
    false = '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    assert any(
        e.deletes == {t(ex("z2"), ex("deprecated"), false)}
        and e.adds == {t(ex("z2"), ex("deprecated"), true)}
        for e in z2
    )


def test_disjunction_breaks_every_satisfied_branch(ahu):
    """z2 has both an area and a volume, so breaking the sh:or removes both."""
    z2 = ahu.where(focus=ex("z2"))
    both = {
        t(ex("z2"), ex("area"), '"12"^^<http://www.w3.org/2001/XMLSchema#integer>'),
        t(ex("z2"), ex("volume"), '"90"^^<http://www.w3.org/2001/XMLSchema#integer>'),
    }
    assert any(e.deletes == both for e in z2)
    assert not any(len(e.deletes & both) == 1 and not e.adds for e in z2)


def test_qualified_min_count_knocks_out_enough_matches():
    """Rule 2: with two qualifying points and qualifiedMinCount 1, both must
    stop qualifying."""
    data = (
        AHU_DATA
        + 'ex:ahu_1 ex:hasPoint ex:MAT_Sensor . ex:MAT_Sensor ex:description "MAT_01" .\n'
    )
    universe = Universe(data, AHU_SHAPES)
    point_edits = universe.where(
        lambda e: (
            e.predicates <= {ex("hasPoint"), ex("description")}
            and not any(a[1] == ex("hasPoint") for a in e.adds)
        ),
        focus=ex("ahu_1"),
    )
    assert point_edits
    for e in point_edits:
        footprint = e.deletes | e.adds
        touched = {tr[0] for tr in footprint} | {tr[2] for tr in footprint}
        assert {ex("RAT_Sensor"), ex("MAT_Sensor")} <= touched, e.key


def test_closed_shape_adds_a_forbidden_predicate():
    shapes = (
        PREFIXES
        + """
ex:S a sh:NodeShape ; sh:targetClass ex:A ; sh:closed true ;
    sh:ignoredProperties ( <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ) ;
    sh:property [ sh:path ex:p ; sh:minCount 1 ] .
"""
    )
    data = PREFIXES + "ex:a a ex:A ; ex:p 1 . ex:b ex:q 2 .\n"
    forbidden = Universe(data, shapes).where(lambda e: e.kinds == ("add_forbidden",))
    assert forbidden
    assert all(next(iter(e.adds))[1] == ex("q") for e in forbidden)
    assert forbidden.draw(1).injections[0] in forbidden


def test_cases_round_trip_through_the_ground_truth_repair(ahu):
    reference = set(ahu.reference_ntriples.splitlines())
    for seed in range(5):
        case = ahu.draw(4, seed=seed)
        assert (case.k, case.seed) == (4, seed)
        assert 1 <= len(case.injections) <= 4
        for i, a in enumerate(case.injections):
            for b in case.injections[i + 1 :]:
                assert not a.deletes & b.adds and not a.adds & b.deletes
        # Every injection is visible in the composite validation.
        realized = {e.key for f in case.failures for e in f.injections}
        assert realized == {e.key for e in case.injections}
        # The delta is the union of the injections, and the repair recovers
        # the reference graph exactly.
        adds = set().union(*(e.adds for e in case.injections))
        deletes = set().union(*(e.deletes for e in case.injections))
        assert set(case.delta.add) == adds and set(case.delta.delete) == deletes
        assert set(case.repair.add) == deletes and set(case.repair.delete) == adds
        graph = set(case.graph_ntriples.splitlines())
        assert graph == (reference - lines(deletes)) | lines(adds)
        repaired = (graph - lines(case.repair.delete)) | lines(case.repair.add)
        assert repaired == reference
        assert not shifty.validate_algebra(case.graph_ntriples, AHU_SHAPES).conforms


def test_case_graphs_apply_deltas(ahu):
    reference = rdflib.Graph().parse(data=ahu.reference_ntriples, format="nt")
    case = ahu.draw(3, seed=0)
    broken = case.graph()
    repaired = case.apply(case.repair)
    assert isomorphic(repaired, reference)
    assert not shifty.validate(broken, AHU_SHAPES)[0]
    assert shifty.validate(repaired, AHU_SHAPES)[0]
    # An empty delta leaves the broken graph as it is.
    assert set(case.apply(shifty.RepairDelta.from_ntriples())) == set(broken)


def test_case_apply_resolves_blank_nodes_by_label():
    shapes = PREFIXES + (
        "ex:S a sh:NodeShape ; sh:targetClass ex:A ; sh:property [ sh:path ex:p ;"
        " sh:node [ a sh:NodeShape ; sh:property [ sh:path ex:q ; sh:minCount 1 ] ] ] ."
    )
    data = PREFIXES + "ex:a a ex:A ; ex:p [ ex:q 1 ] , [ ex:q 2 ] .\n"
    universe = Universe(data, shapes)
    reference = rdflib.Graph().parse(data=universe.reference_ntriples, format="nt")
    cut = universe.where(lambda e: e.kinds == ("remove",) and ex("q") in e.predicates)
    case = cut.draw(1)
    (deleted,) = case.delta.delete
    assert deleted[0].startswith("_:")
    # The re-added triple lands on the node it came from, not a fresh one.
    assert isomorphic(case.apply(case.repair), reference)
    # A delete on a blank node removes that node's triple.
    broken = case.graph_ntriples.splitlines()
    (kept,) = [t for t in broken if t.startswith("_:") and ex("q") in t]
    removed = case.apply(shifty.RepairDelta.from_ntriples(delete=kept + "\n"))
    assert len(removed) == len(broken) - 1
    assert not list(removed.triples((None, rdflib.URIRef("http://ex/q"), None)))


def test_draws_vary_by_seed_and_k(ahu):
    keys = {tuple(e.key for e in ahu.draw(2, seed=s).injections) for s in range(6)}
    assert len(keys) > 1
    same = [tuple(e.key for e in ahu.draw(3, seed=1).injections) for _ in range(2)]
    assert same[0] == same[1]


def test_repair_sparql_inverts_the_delta(ahu):
    case = ahu.draw(3, seed=0)
    update = case.repair_sparql
    if case.delta.add:
        assert "DELETE DATA" in update
    if case.delta.delete:
        assert "INSERT DATA" in update
    for line in lines(case.delta.add) | lines(case.delta.delete):
        assert line in update


def test_several_injections_may_break_one_property(ahu):
    """As in the paper, a draw does not limit injections per property:
    compatible ones stacking on ahu_1's sh:in status share one failure."""
    case = ahu.where(predicate="http://ex/status").draw(2)
    assert len(case.injections) == 2
    (failure,) = [f for f in case.failures if f.focus == ex("ahu_1")]
    assert failure.injections == case.injections


def test_where_is_a_hard_filter_that_composes(ahu):
    zones = ahu.where(shape="http://ex/ZoneShape")
    assert zones and len(zones) < len(ahu)
    assert all(e.shape == "http://ex/ZoneShape" for e in zones)
    assert all(e in ahu for e in zones)
    for seed in range(3):
        case = zones.draw(2, seed=seed)
        assert case.injections
        assert all(e in zones for e in case.injections)
    z1 = zones.where(focus=[ex("z1"), "http://ex/nowhere"])
    assert z1 and all(e.focus == ex("z1") for e in z1)
    status = ahu.where(predicate="<http://ex/status>")
    assert status and all(ex("status") in e.predicates for e in status)
    assert not ahu.where(shape="http://ex/Nothing")
    with pytest.raises(ValueError):
        ahu.where(shape="http://ex/Nothing").draw(1)
    with pytest.raises(ValueError):
        ahu.draw(0)


def test_cli_filters_by_predicate(tmp_path):
    shapes = tmp_path / "shapes.ttl"
    data = tmp_path / "data.ttl"
    shapes.write_text(AHU_SHAPES)
    data.write_text(AHU_DATA)
    out = tmp_path / "suite"
    code = main(
        [
            "--data", str(data),
            "--shapes", str(shapes),
            "--out", str(out),
            "--predicate", "http://ex/status",
            "-k", "1", "-n", "2",
        ]
    )  # fmt: skip
    assert code == 0
    for case in (out / "k01").iterdir():
        manifest = json.loads((case / "case.json").read_text())
        for e in manifest["injections"]:
            assert "<http://ex/status>" in e["predicates"]


def test_weighted_steers_the_draw_and_keeps_the_universe(ahu):
    assert ahu.predicate_weights == {} and ahu.kind_weights == {}
    status = "http://ex/status"
    heavy = ahu.weighted(predicates={status: 1e6})
    assert len(heavy) == len(ahu)
    assert heavy.predicate_weights == {f"<{status}>": 1e6}
    first = heavy.draw(1, seed=0).injections[0]
    assert f"<{status}>" in first.predicates
    removes = ahu.weighted(kinds={"remove": 1e6})
    assert all(set(e.kinds) == {"remove"} for e in removes.draw(2).injections)
    # Weights carry through a filter, and weighted() replaces them.
    zones = removes.where(shape="http://ex/ZoneShape")
    assert zones.kind_weights == {"remove": 1e6}
    assert zones.weighted().kind_weights == {}


def test_weights_must_be_positive(ahu):
    with pytest.raises(ValueError):
        ahu.weighted(kinds={"remove": 0})
    with pytest.raises(ValueError):
        ahu.weighted(predicates={"http://ex/p": 0})
    with pytest.raises(ValueError):
        ahu.weighted(kinds={"rename": 2})


def test_limits_bound_the_universe():
    small = Universe(AHU_DATA, AHU_SHAPES, limits=ExpansionLimits(max_per_target=2))
    per_target = Counter((e.focus, e.statement) for e in small)
    assert per_target and max(per_target.values()) <= 2


def test_blank_node_labels_agree_through_injection():
    data = AHU_DATA + 'ex:ahu_1 ex:hasPoint [ ex:description "OAT_01" ] .\n'
    universe = Universe(data, AHU_SHAPES)
    reference = universe.reference_ntriples
    bnodes = {
        term
        for line in reference.splitlines()
        for term in line.split()
        if term.startswith("_:")
    }
    assert len(bnodes) == 1
    touching = universe.where(
        lambda e: any(term in bnodes for tr in e.deletes | e.adds for term in tr)
    )
    assert touching
    case = touching.draw(1)
    assert case.injections and case.failures


def test_universe_and_draws_are_reproducible_across_processes():
    """Normalized constraint ids, Python's string hashing, and the labels a
    parser gives anonymous blank nodes all differ between processes, so
    expansion and drawing must depend on none of them."""
    data = AHU_DATA + 'ex:ahu_1 ex:hasPoint [ ex:description "OAT_01" ] .\n'
    script = (
        "import json, sys\n"
        "from shifty.experimental.vio import Universe\n"
        "data, shapes = json.load(sys.stdin)\n"
        "u = Universe(data, shapes)\n"
        "case = u.draw(3, seed=5)\n"
        "print(json.dumps([[e.key for e in u], case.to_dict()]))\n"
    )
    runs = [
        subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps([data, AHU_SHAPES]),
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": str(hash_seed)},
        ).stdout
        for hash_seed in range(3)
    ]
    assert runs[0] == runs[1] == runs[2]
    assert any("_:" in key for key in json.loads(runs[0])[0])


def test_unconforming_pairs_are_never_broken():
    data = AHU_DATA + "ex:ahu_2 a ex:AHU .\n"
    universe = Universe(data, AHU_SHAPES)
    assert any(focus == ex("ahu_2") for focus, _ in universe.baseline_failures)
    assert not universe.where(focus=ex("ahu_2"))


def test_cli_writes_a_suite(tmp_path):
    shapes = tmp_path / "shapes.ttl"
    data = tmp_path / "data.ttl"
    shapes.write_text(AHU_SHAPES)
    data.write_text(AHU_DATA)
    out = tmp_path / "suite"
    code = main(
        [
            "--data", str(data),
            "--shapes", str(shapes),
            "--out", str(out),
            "-k", "1", "-k", "3",
            "-n", "2",
            "--seed", "10",
        ]
    )  # fmt: skip
    assert code == 0
    suite = json.loads((out / "suite.json").read_text())
    assert len(suite["cases"]) == 4
    assert [c["seed"] for c in suite["cases"]] == [10, 11, 10, 11]
    case = out / "k03" / "case-001"
    assert (case / "graph.nt").is_file()
    assert (case / "repair.ru").is_file()
    manifest = json.loads((case / "case.json").read_text())
    assert (manifest["k"], manifest["seed"]) == (3, 11)
    assert manifest["injections"]
    assert (out / "reference.nt").is_file()


def test_cli_jobs_write_the_same_suite(tmp_path):
    shapes = tmp_path / "shapes.ttl"
    data = tmp_path / "data.ttl"
    shapes.write_text(AHU_SHAPES)
    data.write_text(AHU_DATA)

    def run(jobs):
        out = tmp_path / f"jobs{jobs}"
        args = ["--data", str(data), "--shapes", str(shapes), "--out", str(out)]
        assert main([*args, "-k", "1", "-k", "3", "-n", "3", "-j", str(jobs)]) == 0
        return {
            str(p.relative_to(out)): p.read_bytes()
            for p in sorted(out.rglob("*"))
            if p.is_file()
        }

    serial = run(1)
    assert len(serial) == 2 + 6 * 3
    assert run(3) == serial


def test_case_manifest_matches_the_python_objects(ahu):
    case = ahu.draw(3, seed=2)
    manifest = case.to_dict()
    assert [e["key"] for e in manifest["injections"]] == [
        e.key for e in case.injections
    ]
    assert {tuple(t) for t in manifest["delta"]["add"]} == set(case.delta.add)
    assert {tuple(t) for t in manifest["delta"]["delete"]} == set(case.delta.delete)
    assert len(manifest["failures"]) == len(case.failures)
    # Keys from Rust agree with the Python spelling of the normal form.
    for e in ahu:
        recomputed = "\n".join(
            [f"- {s} {p} {o} ." for s, p, o in sorted(e.deletes)]
            + [f"+ {s} {p} {o} ." for s, p, o in sorted(e.adds)]
        )
        assert e.key == recomputed
        assert set(e.delta.add) == e.adds and set(e.delta.delete) == e.deletes


def test_universe_is_a_sequence(ahu):
    assert ahu[0] is next(iter(ahu))
    assert ahu[:2] == (ahu[0], ahu[1])
    assert ahu[-1] in ahu
    assert "not an injection" not in ahu
    assert repr(ahu) == "Universe(weights=0)"  # repr never expands


def test_streamed_and_cached_draws_agree():
    """A fresh universe streams its draws. Listing it caches the expansion,
    and later draws read the cache. Both give the same cases."""
    streamed = Universe(AHU_DATA, AHU_SHAPES)
    zones = streamed.where(lambda e: e.shape == "http://ex/ZoneShape")
    before = [
        [e.key for e in streamed.draw(3, seed=7).injections],
        [e.key for e in zones.draw(2, seed=1).injections],
    ]
    cached = Universe(AHU_DATA, AHU_SHAPES)
    assert len(cached) > 20
    zones = cached.where(lambda e: e.shape == "http://ex/ZoneShape")
    after = [
        [e.key for e in cached.draw(3, seed=7).injections],
        [e.key for e in zones.draw(2, seed=1).injections],
    ]
    assert before == after


def test_batched_draws_match_single_draws(ahu):
    requests = [(1, 0), (3, 7), (3, 8), (5, 2)]
    batch = ahu.draws(requests)
    assert [(c.k, c.seed) for c in batch] == requests
    for case, (k, seed) in zip(batch, requests):
        assert case.injections == ahu.draw(k, seed=seed).injections
    with pytest.raises(ValueError):
        ahu.draws([(2, 0), (0, 1)])


def test_case_injections_are_the_universe_objects(ahu):
    case = ahu.draw(3, seed=4)
    by_key = {e.key: e for e in ahu}
    assert all(by_key[e.key] is e for e in case.injections)


def test_filter_exceptions_propagate():
    universe = Universe(AHU_DATA, AHU_SHAPES)

    def boom(e):
        raise RuntimeError("boom")

    view = universe.where(boom)
    with pytest.raises(RuntimeError, match="boom"):
        view.draw(1)
    with pytest.raises(RuntimeError, match="boom"):
        len(view)
