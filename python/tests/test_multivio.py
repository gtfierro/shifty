"""Violation generation (``shifty.multivio``) over the evidence engine."""

import json
import os
import subprocess
import sys

import pytest

import shifty
from shifty.multivio import (
    ExpansionLimits,
    ViolationGenerator,
    _parse_ntriples,
    inverse_frequency_weights,
    main,
)

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
    return ViolationGenerator(AHU_SHAPES, AHU_DATA, seed=3)


def details(gen, focus):
    return {
        "; ".join(op.detail for op in e.operations)
        for e in gen.expressions()
        if e.focus == focus
    }


def test_reference_graph_conforms_and_every_pair_is_a_target(ahu):
    assert ahu.baseline_failures == set()
    assert {(tg.focus, tg.shape) for tg in ahu.targets()} == {
        (ex("ahu_1"), "http://ex/AHUShape"),
        (ex("z1"), "http://ex/ZoneShape"),
        (ex("z2"), "http://ex/ZoneShape"),
    }


def test_expansion_reaches_the_three_leaves_of_the_running_example(ahu):
    """Fig. 7: VIO(ahu_1, φ) bottoms out in deleting hasPoint, deleting the
    description, or shortening "RAT_01" below three characters."""
    exprs = [e for e in ahu.expressions() if e.focus == ex("ahu_1")]
    has_point = t(ex("ahu_1"), ex("hasPoint"), ex("RAT_Sensor"))
    description = t(ex("RAT_Sensor"), ex("description"), '"RAT_01"')
    assert any(e.deletes == {has_point} and not e.adds for e in exprs)
    assert any(e.deletes == {description} and not e.adds for e in exprs)
    short = [
        e
        for e in exprs
        if e.deletes == {description}
        and len(e.adds) == 1
        and next(iter(e.adds))[2] == '"RA"'
    ]
    assert short and short[0].kinds == ("substitute",)


def test_every_expression_makes_its_own_target_fail(ahu):
    exprs = ahu.expressions()
    assert len(exprs) > 20
    assert len({e.key for e in exprs}) == len(exprs)
    for e in exprs:
        assert ahu.verify(e), e.to_dict()


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
        for e in ahu.expressions()
        if e.focus == ex("ahu_1")
        and e.kinds == ("add_value", "add_value")
        and all(a[1] == ex("feeds") for a in e.adds)
    ]
    assert feeds
    assert t(ex("ahu_1"), ex("feeds"), ex("z2")) in feeds[0].adds


def test_class_constraint_breaks_by_removing_the_type(ahu):
    z1_type = t(
        ex("z1"), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", ex("Zone")
    )
    exprs = [e for e in ahu.expressions() if e.focus == ex("ahu_1")]
    assert any(e.deletes == {z1_type} for e in exprs)


def test_negation_flips_to_the_satisfying_direction(ahu):
    """VIO(z, ¬φ) = SAT(z, φ): make the deprecated-true check hold."""
    true = '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    z1 = [e for e in ahu.expressions() if e.focus == ex("z1")]
    assert any(e.adds == {t(ex("z1"), ex("deprecated"), true)} for e in z1)
    z2 = [e for e in ahu.expressions() if e.focus == ex("z2")]
    false = '"false"^^<http://www.w3.org/2001/XMLSchema#boolean>'
    assert any(
        e.deletes == {t(ex("z2"), ex("deprecated"), false)}
        and e.adds == {t(ex("z2"), ex("deprecated"), true)}
        for e in z2
    )


def test_disjunction_breaks_every_satisfied_branch(ahu):
    """z2 has both an area and a volume, so breaking the sh:or removes both."""
    z2 = [e for e in ahu.expressions() if e.focus == ex("z2")]
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
    gen = ViolationGenerator(AHU_SHAPES, data, seed=1)
    point_edits = [
        e
        for e in gen.expressions()
        if e.focus == ex("ahu_1")
        and e.predicates <= {ex("hasPoint"), ex("description")}
        and not any(a[1] == ex("hasPoint") for a in e.adds)
    ]
    assert point_edits
    for e in point_edits:
        touched = {tr[0] for tr in e.footprint} | {tr[2] for tr in e.footprint}
        assert {ex("RAT_Sensor"), ex("MAT_Sensor")} <= touched, e.to_dict()
        assert gen.verify(e)


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
    gen = ViolationGenerator(shapes, data)
    forbidden = [e for e in gen.expressions() if e.kinds == ("add_forbidden",)]
    assert forbidden
    assert all(next(iter(e.adds))[1] == ex("q") for e in forbidden)
    assert all(gen.verify(e) for e in forbidden)


def test_composed_cases_round_trip_through_the_ground_truth_repair(ahu):
    cases = ahu.generate(budget=4, cases=5)
    assert len(cases) == 5
    for case in cases:
        assert 1 <= len(case.expressions) <= 4
        slots = [e.slot for e in case.expressions]
        assert len(slots) == len(set(slots))
        for i, a in enumerate(case.expressions):
            for b in case.expressions[i + 1 :]:
                assert not a.conflicts_with(b)
        # Every injected expression is visible in the composite validation.
        realized = {i for v in case.violations for i in v["expressions"]}
        assert realized == set(range(len(case.expressions)))
        # G_test conforms to nothing new without the repair, and the repair
        # recovers the reference graph exactly.
        invalid = set(_parse_ntriples(case.invalid_ntriples()))
        assert invalid == (ahu.reference - case.deletes) | case.adds
        repaired = (
            invalid - {tuple(x) for x in case.to_dict()["repair"]["delete"]}
        ) | {tuple(x) for x in case.to_dict()["repair"]["add"]}
        assert repaired == ahu.reference
        assert not shifty.validate_algebra(case.invalid_ntriples(), AHU_SHAPES).conforms


def test_cases_from_one_traversal_differ_by_salt(ahu):
    keys = [
        tuple(e.key for e in c.expressions) for c in ahu.generate(budget=2, cases=6)
    ]
    assert len(set(keys)) > 1


def test_repair_sparql_inverts_the_delta(ahu):
    case = ahu.generate(budget=3, cases=1)[0]
    update = case.repair_sparql()
    if case.adds:
        assert "DELETE DATA" in update
    if case.deletes:
        assert "INSERT DATA" in update
    for triple in case.adds | case.deletes:
        assert f"{triple[0]} {triple[1]} {triple[2]} ." in update


def test_predicate_weights_steer_the_reservoir():
    status = "http://ex/status"
    heavy = ViolationGenerator(AHU_SHAPES, AHU_DATA, predicate_weights={status: 1e6})
    edits_status = [e for e in heavy.expressions() if f"<{status}>" in e.predicates]
    assert len(edits_status) >= 2
    sample = heavy.sample(len(edits_status), salt="s")
    assert all(f"<{status}>" in e.predicates for e in sample)


def test_kind_weights_steer_the_reservoir():
    gen = ViolationGenerator(AHU_SHAPES, AHU_DATA, kind_weights={"remove": 1e6})
    sample = gen.sample(4, salt="s")
    assert all(set(e.kinds) == {"remove"} for e in sample)


def test_weights_must_be_positive():
    with pytest.raises(ValueError):
        ViolationGenerator(AHU_SHAPES, AHU_DATA, predicate_weights={"http://ex/p": 0})


def test_inverse_frequency_weights_favour_rare_predicates(ahu):
    weights = inverse_frequency_weights(ahu.expressions())
    assert weights[ex("status")] > weights[ex("feeds")]


def test_limits_bound_the_universe():
    small = ViolationGenerator(
        AHU_SHAPES, AHU_DATA, limits=ExpansionLimits(max_per_target=2)
    )
    per_target = {}
    for e in small.expressions():
        per_target[e.target] = per_target.get(e.target, 0) + 1
    assert per_target and max(per_target.values()) <= 2


def test_blank_nodes_keep_their_labels_through_injection():
    data = AHU_DATA + 'ex:ahu_1 ex:hasPoint [ ex:description "OAT_01" ] .\n'
    gen = ViolationGenerator(AHU_SHAPES, data, seed=2)
    bnodes = {term for tr in gen.reference for term in tr if term.startswith("_:")}
    assert len(bnodes) == 1
    touching = [
        e
        for e in gen.expressions()
        if any(term in bnodes for tr in e.footprint for term in tr)
    ]
    assert touching
    assert all(gen.verify(e) for e in touching)


def test_seeded_universe_is_reproducible_across_processes():
    """Normalized constraint ids and Python's string hashing both differ
    between processes, so expansion order must depend on neither."""
    script = (
        "import json, sys\n"
        "from shifty.multivio import ViolationGenerator\n"
        "shapes, data = json.load(sys.stdin)\n"
        "gen = ViolationGenerator(shapes, data, seed=5)\n"
        "print(json.dumps([e.key for e in gen.expressions()]))\n"
    )
    runs = [
        subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps([AHU_SHAPES, AHU_DATA]),
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": str(hash_seed)},
        ).stdout
        for hash_seed in range(3)
    ]
    assert runs[0] == runs[1] == runs[2]


def test_unconforming_pairs_are_not_targets():
    data = AHU_DATA + "ex:ahu_2 a ex:AHU .\n"
    gen = ViolationGenerator(AHU_SHAPES, data)
    assert any(focus == ex("ahu_2") for focus, _ in gen.baseline_failures)
    assert all(e.focus != ex("ahu_2") for e in gen.expressions())


def test_cli_writes_a_suite(tmp_path):
    shapes = tmp_path / "shapes.ttl"
    data = tmp_path / "data.ttl"
    shapes.write_text(AHU_SHAPES)
    data.write_text(AHU_DATA)
    out = tmp_path / "suite"
    code = main(
        [
            "--shapes", str(shapes),
            "--data", str(data),
            "--out", str(out),
            "-k", "1", "-k", "3",
            "-n", "2",
        ]
    )  # fmt: skip
    assert code == 0
    suite = json.loads((out / "suite.json").read_text())
    assert len(suite["cases"]) == 4
    case = out / "k03" / "case-001"
    assert (case / "invalid.nt").is_file()
    assert (case / "repair.ru").is_file()
    manifest = json.loads((case / "case.json").read_text())
    assert manifest["budget"] == 3
    assert manifest["expressions"]
    assert (out / "reference.nt").is_file()
