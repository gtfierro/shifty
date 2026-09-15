#!/usr/bin/env python3
"""Add SHACL-AF-derived triples directly into a caller-owned rdflib.Graph.

By default, infer(), validate(), and validate_algebra() always return a
fresh graph/report and never touch the rdflib.Graph you passed in. Pass
in_place=True when you'd rather keep working with your own graph object,
extended with whatever was derived — only the inferred delta crosses back
from Rust, not a full copy of the graph.
"""

import rdflib

import shifty

# Not a real area calculation -- just copies ex:width to ex:area -- but a
# small complete sh:rule.
RULES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .

ex:RectangleShape a sh:NodeShape ;
    sh:targetClass ex:Rectangle ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:area ;
        sh:object [ sh:path ex:width ] ;
    ] .
"""

SHAPES = (
    RULES
    + """
ex:RectangleShape sh:property [
    sh:path ex:area ;
    sh:minCount 1 ;
] .
"""
)

DATA = """
@prefix ex:  <http://example.org/> .

ex:r1 a ex:Rectangle ; ex:width 4 ; ex:height 5 .
"""


def main() -> None:
    # infer(..., in_place=True): the graph you passed in gets extended, and
    # .graph() just hands the same object back rather than re-parsing.
    data = rdflib.Graph()
    data.parse(data=DATA, format="turtle")
    print(f"before infer: {len(data)} triples")

    result = shifty.infer(data, RULES, in_place=True)
    print(f"inferred {result.inferred_count} triple(s)")
    print(f"after infer:  {len(data)} triples")
    print(f"result.graph() is data: {result.graph() is data}")

    # validate(..., in_place=True): same idea, but the derived triples come
    # from the inference validate() already runs internally (infer=True is
    # the default) rather than a separate infer() call. The report graph is
    # unaffected either way -- it's always a fresh rdflib.Graph.
    data = rdflib.Graph()
    data.parse(data=DATA, format="turtle")

    conforms, report, _ = shifty.validate(data, SHAPES, in_place=True)
    print(f"\nconforms: {conforms}")
    print(f"data now has {len(data)} triples (area was missing before validation)")
    print(f"report is a separate graph: {report is not data}")

    # validate_algebra(..., in_place=True): same option, structured result
    # path -- no report graph at all, so this is just the write-back.
    data = rdflib.Graph()
    data.parse(data=DATA, format="turtle")

    result = shifty.validate_algebra(data, SHAPES, in_place=True)
    print(f"\nconforms: {result.conforms}")
    print(f"data now has {len(data)} triples")


if __name__ == "__main__":
    main()
