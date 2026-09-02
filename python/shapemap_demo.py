"""Standalone demo of shifty's shape-map v2 API.

Run with:

    cd python && uv run maturin develop   # once, to build the extension
    uv run python /path/to/shapemap_demo.py

A small building-metadata SHACL profile: a Zone needs a temperature-sensor
point and a heating coil (referenced by a named shape). One zone is complete,
one is missing its coil.
"""

import shifty
from shifty import Cls, Iri, Literal, ShapeRef

SHAPES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:  <http://example.org/> .

ex:ZoneShape a sh:NodeShape ;
    sh:targetClass ex:Zone ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:name "zone temperature point" ;
        sh:qualifiedValueShape [ sh:class ex:TempSensor ] ;
        sh:qualifiedMinCount 1 ;
        sh:qualifiedMaxCount 1 ;
    ] ;
    sh:property [
        sh:path ex:hasPart ;
        sh:name "coil" ;
        sh:qualifiedValueShape [ sh:node ex:HeatingCoilShape ] ;
        sh:qualifiedMinCount 1 ;
    ] ;
    sh:property [
        sh:name "name" ;
        sh:path ex:label ;
        sh:datatype xsd:string ;
        sh:minCount 1 ;
        sh:severity sh:Warning ;
    ] .

ex:HeatingCoilShape a sh:NodeShape ; sh:targetClass ex:HeatingCoil .
"""

DATA = """
@prefix ex: <http://example.org/> .

ex:zone1 a ex:Zone ;
    ex:hasPoint ex:temp1 ;
    ex:hasPart ex:coil1 ;
    ex:label "Zone 1" .
ex:temp1 a ex:TempSensor ;
    ex:hasTimeseriesId "TS-ZONE1-TEMP" .
ex:coil1 a ex:HeatingCoil .

# Missing its heating coil, and its label — will fail two obligations.
ex:zone2 a ex:Zone ;
    ex:hasPoint ex:temp2 .
ex:temp2 a ex:TempSensor ;
    ex:hasTimeseriesId "TS-ZONE2-TEMP" .
"""


smap = shifty.shape_map(
    SHAPES,
    DATA,
    infer=True,
    minimum_severity="warning",
    name_path="sh:name",  # carry the author's name for each slot
    value_paths={"ts": "ex:hasTimeseriesId"},  # annotate bound values
)

print(smap)
for mapping in smap["http://example.org/ZoneShape"]:
    print(f"\n{mapping.focus.n3()}  conforms={mapping.conforms}")

    for key, binding in mapping.items():
        label = f"{key} ({binding.name})" if binding.name else str(key)
        print(
            f"  {label:35s} severity={binding.severity:9s} "
            f"min={binding.min} max={binding.max} observed={binding.observed}"
        )

        if binding.ok:
            for bound in binding.annotated_values:
                ts = bound.annotations.get("ts")
                extra = f"  ts={ts[0].value}" if ts else ""
                print(f"      + {bound.term.n3()}{extra}")
        else:
            print(f"      - missing {binding.missing}")

    # Typed Key/Term are pattern-matchable, not display strings to parse.
    for key, binding in mapping.successful:
        print(key.kind, key.ordinal, key.path, key.qualifier)
        print(f"{binding.name} => {binding.values}")
        match key.qualifier, (binding.values[0] if binding.values else None):
            case Cls(class_iri), Iri(value):
                print(f"  match: <{value}> is a <{class_iri}> (class-qualified)")
            case ShapeRef(shape_iri), Iri(value):
                print(f"  match: <{value}> conforms to <{shape_iri}> (shape-ref)")
            case None, Literal(value, datatype, _):
                print(f"  match: plain literal {value!r} (datatype={datatype})")

# value_map(): the application-configuration projection.
zone1 = smap.for_focus("<http://example.org/zone1>")[0]
print("\nvalue_map(by='name', python=True) for zone1:")
print(" ", zone1.value_map(by="name", python=True))
