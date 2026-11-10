#!/usr/bin/env python3
"""Break a valid graph with ``shifty.experimental.vio`` and validate it.

1. validate the data against the shapes: it conforms,
2. draw a case of injected violations,
3. build the broken graph, ``case.graph()``,
4. validate again: the report shows the injected violations,
5. apply the case's repair with ``case.apply(case.repair)``: the graph
   conforms again.

Usage:
    python vio_validate.py [K] [SEED]
"""

import sys

import rdflib

import shifty
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


def report(graph: rdflib.Graph) -> None:
    conforms, _, text = shifty.validate(graph, SHAPES)
    print(f"conforms: {conforms}")
    if not conforms:
        print(text, end="")


def main() -> None:
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    data = rdflib.Graph().parse(data=DATA, format="turtle")

    print("1. Validate the original data")
    report(data)

    print(f"\n2. Draw a case with up to {k} injections (seed {seed})")
    case = vio.Universe(DATA, SHAPES).draw(k, seed=seed)
    for e in case.injections:
        print(f"  {e.focus}: {'; '.join(op.detail for op in e.operations)}")

    print("\n3. Build the broken graph from the case's edits")
    for s, p, o in sorted(case.delta.delete):
        print(f"  - {s} {p} {o}")
    for s, p, o in sorted(case.delta.add):
        print(f"  + {s} {p} {o}")
    broken = case.graph()

    print("\n4. Validate the broken data")
    report(broken)
    print("The case expects these foci to fail:")
    for f in case.failures:
        print(f"  {f.focus}")

    print("\n5. Apply the case's repair and validate again")
    report(case.apply(case.repair))


if __name__ == "__main__":
    main()
