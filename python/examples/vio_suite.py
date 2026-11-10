#!/usr/bin/env python3
"""Pick which violations ``shifty.experimental.vio`` injects, and draw a suite.

1. list the universe: every injection vio can make,
2. narrow it with ``where``,
3. weight it with ``weighted``,
4. draw a suite of cases in one pass with ``draws``, and save it.

Usage:
    python vio_suite.py [OUT]
"""

import sys

from shifty.experimental import vio

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

DATA = """
@prefix ex: <http://ex/> .

ex:s1 a ex:Sensor ; ex:label "Temp 1" ; ex:unit "degC" ; ex:inRoom ex:r1 .
ex:s2 a ex:Sensor ; ex:label "Temp 2" ; ex:unit "degF" ; ex:inRoom ex:r2 .
ex:r1 a ex:Room .
ex:r2 a ex:Room .
"""


def describe(case: vio.Case) -> str:
    return "; ".join(f"{e.focus}: {e.operations[0].detail}" for e in case.injections)


def main() -> None:
    universe = vio.Universe(DATA, SHAPES)

    print(f"1. The universe has {len(universe)} injections. The first five:")
    for e in universe[:5]:
        print(f"  {e.focus}: {e.operations[0].detail}")

    print("\n2. Only injections that break ex:s1:")
    s1 = universe.where(focus="http://ex/s1")
    print(f"  {len(s1)} injections; draw(2): {describe(s1.draw(2))}")

    print("\n3. Edits to ex:unit 50 times as likely as the rest:")
    units = universe.weighted(predicates={"http://ex/unit": 50})
    print(f"  draw(2): {describe(units.draw(2))}")

    print("\n4. A suite: three cases each for k = 1 and k = 2")
    suite = universe.draws([(k, seed) for k in (1, 2) for seed in range(3)])
    for case in suite:
        print(f"  k={case.k} seed={case.seed}: {describe(case)}")
    if len(sys.argv) > 1:
        for case in suite:
            case.save(f"{sys.argv[1]}/k{case.k}-seed{case.seed}")
        print(f"Saved graph.nt, repair.ru, and case.json per case under {sys.argv[1]}")


if __name__ == "__main__":
    main()
