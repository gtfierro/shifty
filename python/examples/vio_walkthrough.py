#!/usr/bin/env python3
"""A guided tour of ``shifty.experimental.vio``.

``vio`` breaks a valid graph on purpose. Each broken copy comes with the
exact edits that broke it, so the edits that undo them are a known correct
repair. That makes the broken copies test cases for repair tools.

The tour uses two sensors and three rules, and goes step by step:

1. build a universe: every way to break the graph,
2. look at a few injections, the members of the universe,
3. draw a case: a few injections applied together,
4. check that the case's repair restores the original graph,
5. steer which injections a draw picks,
6. draw a whole suite at once.

Usage:
    python vio_walkthrough.py [OUT]

With ``OUT``, the suite from step 6 is written there, one directory per
case.
"""

import sys
from collections import Counter

import rdflib
from rdflib.compare import isomorphic

import shifty
from shifty.experimental import vio

# Every sensor has one string label, a unit from a fixed list, and a room.
SHAPES = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .

ex:SensorShape a sh:NodeShape ;
    sh:targetClass ex:Sensor ;
    sh:property [ sh:path ex:label ; sh:datatype xsd:string ; sh:minCount 1 ; sh:maxCount 1 ] ;
    sh:property [ sh:path ex:unit ; sh:in ( "degC" "degF" ) ] ;
    sh:property [ sh:path ex:inRoom ; sh:class ex:Room ; sh:minCount 1 ] .
"""

# Both sensors follow the rules, so the graph is valid.
DATA = """
@prefix ex: <http://ex/> .

ex:s1 a ex:Sensor ; ex:label "Temp 1" ; ex:unit "degC" ; ex:inRoom ex:r1 .
ex:s2 a ex:Sensor ; ex:label "Temp 2" ; ex:unit "degF" ; ex:inRoom ex:r2 .
ex:r1 a ex:Room .
ex:r2 a ex:Room .
"""

PREFIXES = {
    "http://ex/": "ex:",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf:",
    "http://www.w3.org/2001/XMLSchema#": "xsd:",
}


def short(text: str) -> str:
    """``<http://ex/s1>`` → ``ex:s1``, anywhere in ``text``."""
    for ns, prefix in PREFIXES.items():
        text = text.replace(f"<{ns}", prefix).replace(ns, prefix)
    return text.replace(">", "")


def triple(t: tuple[str, str, str]) -> str:
    return " ".join(short(x) for x in t)


def step(title: str) -> None:
    print(f"\n── {title} " + "─" * (68 - len(title)))


def show(injection: vio.Injection, indent: str = "  ") -> None:
    """The node an injection breaks, then each of its edits."""
    print(f"{indent}breaks {short(injection.focus)}:")
    for op in injection.operations:
        print(f"{indent}  {op.kind}: {short(op.detail)}")
        for t in op.deletes:
            print(f"{indent}    - {triple(t)}")
        for t in op.adds:
            print(f"{indent}    + {triple(t)}")


def summary(case: vio.Case) -> str:
    return "; ".join(
        f"{short(e.focus)} {short(e.operations[0].detail)}" for e in case.injections
    )


def main() -> None:
    step("1. Build a universe")
    universe = vio.Universe(DATA, SHAPES)
    print(
        "vio.Universe(DATA, SHAPES) holds every way to break the graph that"
        "\nvio can find. Each way is an injection: a few edits that make one"
        "\nsensor fail one of its rules. The universe works like a list:"
    )
    print(f"\n  len(universe) == {len(universe)}")
    print("\nInjections per sensor and per predicate they edit:")
    counts = Counter(
        (short(e.focus), short(p)) for e in universe for p in sorted(e.predicates)
    )
    for (focus, predicate), n in sorted(counts.items()):
        print(f"  {focus:6} {predicate:10} {n}")

    step("2. Look at injections")

    def example(kind: str, predicate: str) -> vio.Injection:
        return next(
            e
            for e in universe
            if e.kinds == (kind,) and e.predicates == {f"<http://ex/{predicate}>"}
        )

    print(
        "An injection's operations are its edits. There are three kinds here."
        "\nA remove cuts a triple. Here the label goes missing:"
    )
    show(example("remove", "label"))
    print("\nA substitute swaps a value. Here the unit leaves the allowed list:")
    substitute = example("substitute", "unit")
    show(substitute)
    print("\nAn add_value adds a value. Here a second label is one too many:")
    show(example("add_value", "label"))
    print(
        "\nEach injection has a key: its edits as sorted N-Triples lines. The"
        "\nkey names the injection and is the same in every run. For the"
        "\nsubstitute above:\n"
    )
    print("  " + short(substitute.key).replace("\n", "\n  "))

    step("3. Draw a case")
    case = universe.draw(3, seed=0)
    print(
        "universe.draw(3, seed=0) picks up to 3 injections and applies them"
        "\ntogether. It only picks injections whose edits don't undo each"
        "\nother, and it checks with the validator that each one still breaks"
        "\nits rule in the combined graph."
    )
    print(f"\nThe case has {len(case.injections)} injections:")
    for i, e in enumerate(case.injections):
        print(f"\n  #{i}")
        show(e, indent="  ")
    print("\nTogether they make one change to the graph, case.delta:")
    for t in sorted(case.delta.delete):
        print(f"  - {triple(t)}")
    for t in sorted(case.delta.add):
        print(f"  + {triple(t)}")
    print("\nThe validator then reports these failures, case.failures:")
    index = {e.key: i for i, e in enumerate(case.injections)}
    for f in case.failures:
        by = ", ".join(f"#{index[e.key]}" for e in f.injections)
        print(f"  {short(f.focus)} fails, because of {by}")

    step("4. Check the repair")
    print(
        "case.graph() is the broken graph, as an rdflib graph. case.repair is"
        "\nthe change that undoes case.delta. case.apply(case.repair) applies"
        "\nit to the broken graph, and the original graph comes back:"
    )
    original = rdflib.Graph().parse(data=DATA, format="turtle")
    broken = case.graph()
    repaired = case.apply(case.repair)
    print(f"\n  broken graph is valid:      {shifty.validate(broken, SHAPES)[0]}")
    print(f"  repaired graph is valid:    {shifty.validate(repaired, SHAPES)[0]}")
    print(f"  repaired graph == original: {isomorphic(repaired, original)}")
    print("\nThe same repair, as a SPARQL update, is case.repair_sparql:\n")
    print("  " + case.repair_sparql.replace("\n", "\n  ").rstrip())

    step("5. Steer the draw")
    print(
        "The seed picks the case. The same seed always gives the same case;"
        "\na different seed gives another:"
    )
    for seed in (0, 0, 1):
        print(f"  seed={seed}: {summary(universe.draw(2, seed=seed))}")
    print("\nuniverse.where(...) keeps only matching injections, so a draw from")
    print("it can only pick those. Here, only injections that break ex:s1:")
    s1 = universe.where(focus="http://ex/s1")
    print(f"  {summary(s1.draw(2, seed=0))}")
    print(
        "\nuniverse.weighted(...) makes some injections more likely without"
        "\nexcluding the rest. Each weight applies to injections that edit that"
        "\npredicate; anything without a weight counts as 1. Here, ex:unit"
        "\nedits are 50 times as likely:"
    )
    units = universe.weighted(predicates={"http://ex/unit": 50})
    print(f"  {summary(units.draw(2, seed=0))}")

    step("6. Draw a suite")
    requests = [(k, seed) for k in (1, 2) for seed in range(3)]
    suite = universe.draws(requests)
    print(
        "universe.draws([(k, seed), ...]) draws many cases in one pass. Each"
        "\ncase is the same as the matching universe.draw(k, seed=seed):\n"
    )
    for c in suite:
        print(f"  k={c.k} seed={c.seed}: {summary(c)}")
    if len(sys.argv) > 1:
        for c in suite:
            c.save(f"{sys.argv[1]}/k{c.k}-seed{c.seed}")
        print(f"\nWrote graph.nt, repair.ru, and case.json per case to {sys.argv[1]}")


if __name__ == "__main__":
    main()
