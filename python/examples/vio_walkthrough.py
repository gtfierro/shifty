#!/usr/bin/env python3
"""A guided tour of ``shifty.experimental.vio``.

The walkthrough uses a small building model, the running example of arXiv
2611.07342 (Fig. 6), with a few more constraints. It prints each of the
module's three nouns in turn:

1. the **universe**: every way to break the reference graph,
2. an **injection**: one member, and how a few constraints break,
3. **views** of the universe: hard filters and sampling weights,
4. a **case**: a draw of compatible injections, its ground-truth repair,
   and a round trip back to the reference graph.

Usage:
    python vio_walkthrough.py [OUT]

With ``OUT``, the drawn case is also written there as ``graph.nt``,
``repair.ru``, and ``case.json``.
"""

import sys
from collections import Counter

import shifty
from shifty.experimental import vio

SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .

# An AHU needs a point that conforms to NamedPoint, one label string, at
# most two zones it feeds, and a status from a fixed list.
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

# A named point has a description at least three characters long.
ex:NamedPoint a sh:NodeShape ;
    sh:property [ sh:path ex:description ; sh:minCount 1 ; sh:minLength 3 ] .

# A zone has an area or a volume, and is not deprecated.
ex:ZoneShape a sh:NodeShape ;
    sh:targetClass ex:Zone ;
    sh:or ( [ sh:path ex:area ; sh:minCount 1 ] [ sh:path ex:volume ; sh:minCount 1 ] ) ;
    sh:not [ sh:path ex:deprecated ; sh:hasValue true ] .
"""

DATA = """
@prefix ex: <http://ex/> .

ex:ahu_1 a ex:AHU ;
    ex:hasPoint ex:RAT_Sensor ;
    ex:label "AHU-1" ;
    ex:feeds ex:z1 ;
    ex:status "on" .
ex:RAT_Sensor ex:description "RAT_01" .
ex:z1 a ex:Zone ; ex:area 30 .
ex:z2 a ex:Zone ; ex:volume 90 ; ex:area 12 ; ex:deprecated false .
"""

PREFIXES = {
    "http://ex/": "ex:",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf:",
    "http://www.w3.org/2001/XMLSchema#": "xsd:",
}


def short(text: str) -> str:
    """``<http://ex/ahu_1>`` → ``ex:ahu_1``, anywhere in ``text``."""
    for ns, prefix in PREFIXES.items():
        text = text.replace(f"<{ns}", prefix).replace(ns, prefix)
    return text.replace(">", "")


def triple(t: tuple[str, str, str]) -> str:
    return " ".join(short(x) for x in t)


def heading(title: str) -> None:
    print(f"\n{'═' * 72}\n{title}\n{'═' * 72}")


EFFECT = {"violate": "fail", "satisfy": "hold"}


def show(injection: vio.Injection, indent: str = "  ") -> None:
    """One injection: which pair it breaks, then each atomic edit.

    An edit can make a sub-shape *hold*: removing ``ex:z1 a ex:Zone`` breaks
    ``sh:class ex:Zone`` on ``ex:feeds``, which is the universal
    ``∃≤0 feeds.¬Zone``, by making ``¬Zone`` hold at ``ex:z1``.
    """
    print(
        f"{indent}breaks {short(injection.focus)} against {short(injection.shape or '?')}"
    )
    for op in injection.operations:
        print(
            f"{indent}  [{op.kind}] {short(op.detail)}  (makes its sub-shape {EFFECT[op.polarity]})"
        )
        for t in op.deletes:
            print(f"{indent}      - {triple(t)}")
        for t in op.adds:
            print(f"{indent}      + {triple(t)}")


def one_line(injection: vio.Injection) -> str:
    return (
        f"{'+'.join(injection.kinds):22} {short(injection.focus):9}"
        f" {short(injection.operations[0].detail)}"
    )


def main() -> None:
    # ── 1. The universe ─────────────────────────────────────────────────────
    heading("1. The universe")
    universe = vio.Universe(DATA, SHAPES)
    reference = universe.reference_ntriples.splitlines()
    print(f"The reference graph has {len(reference)} triples and conforms:")
    print(f"  pre-existing failures: {len(universe.baseline_failures)}")
    print(
        f"\nExpanding the evidence of every passing (focus, shape) pair gives"
        f"\n{len(universe)} distinct injections. Each is one way to make one pair"
        "\nfail, as a set of atomic edits. The universe is a sequence of them:"
    )
    for (focus, shape), n in sorted(
        Counter((e.focus, e.shape) for e in universe).items()
    ):
        print(f"  {short(focus):14} {short(shape or '?'):16} {n} injections")
    print("\nOperations by kind:")
    for kind, n in sorted(Counter(k for e in universe for k in e.kinds).items()):
        print(f"  {kind:14} {n}")

    # ── 2. Injections ───────────────────────────────────────────────────────
    heading("2. Injections")
    print(
        "The paper's Fig. 7: ex:ahu_1 needs a point conforming to NamedPoint."
        "\nVIO(ahu_1, φ) bottoms out in these leaves: cut the hasPoint edge, swap"
        "\nthe point, or break NamedPoint inside it, by cutting, shortening, or"
        "\nadding a too-short description:"
    )
    point = universe.where(
        lambda e: e.predicates <= {"<http://ex/hasPoint>", "<http://ex/description>"},
        focus="http://ex/ahu_1",
    )
    shown = set()
    for e in point:
        signature = (e.kinds, frozenset(t[1] for t in e.deletes))
        if signature in shown:
            continue
        shown.add(signature)
        show(e)
        print()

    print("Constraints under sh:not flip direction: breaking ¬φ makes φ hold.")
    show(universe.where(focus="http://ex/z1", predicate="http://ex/deprecated")[0])

    print("\nAn sh:or breaks only when every satisfied branch breaks at once:")
    show(universe.where(lambda e: len(e.operations) == 2, focus="http://ex/z2")[0])

    print(
        "\nA maxCount breaks by adding values. Values that keep the sibling"
        "\nsh:class Zone holding come first, so the case stays a cardinality"
        "\nerror where it can. Here z2 is the only other Zone, so the second"
        "\nvalue breaks sh:class too:"
    )
    show(
        universe.where(
            lambda e: (
                e.kinds == ("add_value", "add_value")
                and all(t[1] == "<http://ex/feeds>" for t in e.adds)
            )
        )[0]
    )
    print(
        "\nAn injection's key is its sorted net delta. It is its identity, the"
        "\nsame in every process:\n"
    )
    print("  " + short(point[0].key).replace("\n", "\n  "))

    # ── 3. Views ────────────────────────────────────────────────────────────
    heading("3. Views: filters and weights")
    print(
        "A universe is immutable. where() keeps the injections that match, and"
        "\nweighted() changes how a draw samples them. Both return views that"
        "\nshare the expansion, so they are cheap:"
    )
    zones = universe.where(shape="http://ex/ZoneShape")
    status = universe.where(predicate="http://ex/status")
    print(f"\n  universe.where(shape=ex:ZoneShape)      {len(zones):3} injections")
    print(f"  universe.where(predicate=ex:status)     {len(status):3} injections")
    print(
        "\nA draw takes a budget k and a seed. A reservoir keeps the injections"
        "\nwith the smallest keys -ln h(v) / w(v), where h hashes the injection"
        "\nwith the seed; without weights w = 1, so every injection is equally"
        "\nlikely. The first picks of two seeds:"
    )
    for seed in (0, 1):
        print(f"\n  universe.draw(3, seed={seed}):")
        for e in universe.draw(3, seed=seed).injections:
            print(f"    {one_line(e)}")
    print(
        "\nWeights skew the draw. They multiply into w as geometric means over"
        "\nthe predicates an injection edits and over its operation kinds;"
        "\nanything without an entry weighs 1. Unlike where(), they exclude"
        "\nnothing."
    )
    for label, view in [
        (
            "weighted(predicates={ex:status: 50})",
            universe.weighted(predicates={"http://ex/status": 50}),
        ),
        ("weighted(kinds={'remove': 50})", universe.weighted(kinds={"remove": 50})),
    ]:
        print(f"\n  universe.{label}.draw(3, seed=0):")
        for e in view.draw(3, seed=0).injections:
            print(f"    {one_line(e)}")

    # ── 4. A case ───────────────────────────────────────────────────────────
    heading("4. A case (k = 3)")
    case = universe.draw(3, seed=0)
    print(
        "draw() scans the reservoir and admits an injection only if it breaks"
        "\nits target when applied alone and its edits can be applied with the"
        "\nones already chosen. The combined edits are then revalidated."
    )
    print(f"\nInjected {len(case.injections)} (k={case.k}, seed={case.seed}):")
    for i, e in enumerate(case.injections):
        print(f"\n  #{i}")
        show(e, indent="  ")

    print("\nThe injected delta Δ (case.delta), with G_test = G_ref ⊕ Δ:")
    for t in sorted(case.delta.delete):
        print(f"  - {triple(t)}")
    for t in sorted(case.delta.add):
        print(f"  + {triple(t)}")

    print("\nFailures the engine reports for G_test (case.failures):")
    index = {e.key: i for i, e in enumerate(case.injections)}
    for f in case.failures:
        by = ", ".join(f"#{index[e.key]}" for e in f.injections) or "a side effect"
        print(f"  {short(f.focus):14} {short(f.shape or '?'):16} caused by {by}")

    print("\nThe ground-truth repair Δ⁻¹ (case.repair), as SPARQL Update:\n")
    print("  " + case.repair_sparql.replace("\n", "\n  ").rstrip())

    # ── Round trip ──
    # Apply the repair to G_test line by line, then revalidate both graphs.
    def lines(triples):
        return {" ".join(t) + " ." for t in triples}

    g_test = set(case.graph_ntriples.splitlines())
    repaired = (g_test - lines(case.repair.delete)) | lines(case.repair.add)
    repaired_nt = "\n".join(sorted(repaired)) + "\n"
    broken = shifty.validate_algebra(case.graph_ntriples, SHAPES).conforms
    fixed = shifty.validate_algebra(repaired_nt, SHAPES).conforms
    print("\nRound trip:")
    print(f"  G_test conforms:         {broken}")
    print(f"  G_test ⊕ Δ⁻¹ conforms:   {fixed}")
    print(f"  G_test ⊕ Δ⁻¹ == G_ref:   {repaired == set(reference)}")

    if len(sys.argv) > 1:
        path = case.save(sys.argv[1])
        print(f"\nWrote graph.nt, repair.ru, and case.json to {path}")


if __name__ == "__main__":
    main()
