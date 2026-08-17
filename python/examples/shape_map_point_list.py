"""The shape-map view of the ZonePAC point-list demo: typed key -> value bindings.

Run from the repository root after ``cd python && uv run maturin develop``:

    uv run python examples/shape_map_point_list.py

Where ``evidence_point_list.py`` walks the raw evidence trees, this shows the
level above them: one mapping per (shape, focus) pair, each a typed ``Key ->
Binding`` record of which property obligations bound to which values.
Partially-conforming equipment keeps the bindings it *does* satisfy — the raw
failure witness elides passing siblings, but the shape map materializes them
on demand — which is what a repair driver or an application-configuration
script wants to start from.

This variant of the point-list shapes also names the zone's point slot with
``sh:name`` (``name_path``) and gives the matched zone-temperature point a
timeseries id in the data graph (``value_paths``), to demonstrate both v2
features alongside the typed ``Key``/``Term`` vocabulary.
"""

from __future__ import annotations

from evidence_point_list import (
    BRICK_MODEL,
    BRICK_ONTOLOGY,
    DEMO_OVERLAY,
    compact,
)

import shifty
from shifty import Cls, Iri, Key, ShapeRef

# `POINT_LIST_SHAPES` plus `sh:name` on the zone's point slot (property
# shapes are blank nodes, so the name is authored on the same block rather
# than added from outside).
POINT_LIST_SHAPES_NAMED = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix demo: <urn:shifty-evidence-demo/> .
@prefix : <urn:zonepac-app/> .

<urn:zonepac-app> a owl:Ontology ;
    owl:imports <https://brickschema.org/schema/1.4/Brick> .

:zonepac-zone a sh:NodeShape ;
    sh:targetClass brick:HVAC_Zone ;
    sh:property [
        sh:path brick:hasPoint ;
        sh:name "zone temperature point" ;
        sh:qualifiedValueShape [ sh:class brick:Zone_Air_Temperature_Sensor ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [
        sh:path brick:hasPart ;
        sh:qualifiedValueShape [ sh:class brick:Space ] ;
        sh:qualifiedMinCount 1
    ] .

:zonepac-vav a sh:NodeShape ;
    sh:targetClass brick:Terminal_Unit ;
    sh:property [
        sh:path brick:hasPart ;
        sh:qualifiedValueShape [ sh:node :heating-coil ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [
        sh:path brick:hasPoint ;
        sh:qualifiedValueShape [ sh:class brick:Supply_Air_Flow_Sensor ] ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [
        sh:path brick:hasPoint ;
        sh:qualifiedValueShape [ sh:class brick:Supply_Air_Temperature_Sensor ] ;
        sh:qualifiedMinCount 1
    ] .

:heating-coil a sh:NodeShape ;
    sh:targetClass brick:Heating_Coil ;
    sh:property [
        sh:path brick:hasPoint ;
        sh:qualifiedValueShape [ sh:class brick:Position_Command ] ;
        sh:qualifiedMinCount 1
    ] .

:unused-equipment a sh:NodeShape ;
    sh:targetClass :NeverPresentEquipment ;
    sh:nodeKind sh:IRI .
"""

# A timeseries id on the matched zone-temperature point, for `value_paths` to
# reach from the bound value node (over the data graph, not the shapes graph).
TIMESERIES_OVERLAY = """
@prefix bldg: <http://buildsys.org/ontologies/bldg1#> .
@prefix demo: <urn:shifty-evidence-demo/> .

bldg:bldg1.ZONE.AHU02.RM100.Zone_Air_Temp demo:hasTimeseriesId "TS-RM100-ZoneTemp" .
"""


def main() -> None:
    smap = shifty.shape_map(
        POINT_LIST_SHAPES_NAMED.encode(),
        [BRICK_ONTOLOGY, BRICK_MODEL, DEMO_OVERLAY.encode(), TIMESERIES_OVERLAY.encode()],
        infer=False,
        graph_mode="union",
        name_path="sh:name",
        value_paths={"ts": "demo:hasTimeseriesId"},
    )

    print(smap)
    for name in smap.shape_names:
        group = smap.mappings[name]
        conforming = sum(mapping.conforms for mapping in group)
        print(f"\n{name}: {conforming}/{len(group)} conform")
        for mapping in group:
            marker = "ok  " if mapping.conforms else "PART"
            print(f"  [{marker}] {compact(mapping.focus.n3())}")
            for key, binding in mapping.successful:
                rendered = ", ".join(compact(v.n3()) for v in binding.values)
                label = f"{key} ({binding.name})" if binding.name else str(key)
                print(f"      + {label}: {rendered}")
                for bound in binding.annotated_values:
                    if bound.annotations.get("ts"):
                        ts = ", ".join(v.n3() for v in bound.annotations["ts"])
                        print(f"          timeseries: {ts}")
            for key, binding in mapping.unsuccessful:
                print(f"      - {key}: missing {binding.missing}", end="")
                if binding.rejected_values:
                    near = ", ".join(compact(v.n3()) for v in binding.rejected_values)
                    print(f" (near-misses: {near})", end="")
                print()

    # A `Key`/`Term` match statement — the typed vocabulary is
    # pattern-matchable rather than a display string to parse back apart.
    for mapping in smap["urn:zonepac-app/zonepac-vav"]:
        for key, binding in mapping.items():
            first = binding.values[0] if binding.ok and binding.values else None
            match key.qualifier, first:
                case Cls(iri), Iri(value):
                    print(f"\n{key}: <{value}> matched via class {compact(f'<{iri}>')}")
                case ShapeRef(iri), Iri(value):
                    print(f"\n{key}: <{value}> matched via shape-ref {compact(f'<{iri}>')}")

    # Anything unbound drills back down into the ordinary evidence objects:
    incomplete = smap.nonconforming("urn:zonepac-app/zonepac-zone")[0]
    key, binding = incomplete.unsuccessful[0]
    print(f"\nwitness subtree for {compact(incomplete.focus.n3())} / {key}:")
    print("  " + binding.explain().replace("\n", "\n  "))
    print("full failure evidence via mapping.evaluation.failure.explain() —",
          len(incomplete.evaluation.failure.explain().splitlines()), "lines")


if __name__ == "__main__":
    main()
