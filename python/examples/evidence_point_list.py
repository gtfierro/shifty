"""Demonstrate evidence validation with a ZonePAC-style Brick point list.

Run from the repository root after ``cd python && uv run maturin develop``:

    uv run python examples/evidence_point_list.py

The point-list shapes are adapted from BuildingMOTIF's ZonePAC library:
https://github.com/NatLabRockies/BuildingMOTIF/blob/develop/libraries/ZonePAC/shapes.ttl

The data is the repository's Brick ``bldg1.ttl`` benchmark model plus Brick's
ontology and a small demonstration overlay. The overlay makes exactly one zone
and one VAV complete, leaving their peers incomplete, so the evidence horizon
contains both selected passes and selected failures. A chiller in the benchmark
is not targeted by any point-list statement and is therefore absent.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import shifty


REPO = Path(__file__).resolve().parents[2]
BRICK_ONTOLOGY = REPO / "benchmark/brick/Brick.ttl"
BRICK_MODEL = REPO / "benchmark/brick/models/bldg1.ttl"


# The three application shapes from ZonePAC: zones need a zone-temperature
# sensor and a space; terminal units need a heating coil plus supply-air flow
# and temperature sensors; heating coils need a position command.
POINT_LIST_SHAPES = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix : <urn:zonepac-app/> .

<urn:zonepac-app> a owl:Ontology ;
    owl:imports <https://brickschema.org/schema/1.4/Brick> .

:zonepac-zone a sh:NodeShape ;
    sh:targetClass brick:HVAC_Zone ;
    sh:property [
        sh:path brick:hasPoint ;
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

# Kept in the run with selected_foci=[] to distinguish target coverage from a
# selected node that passed.
:unused-equipment a sh:NodeShape ;
    sh:targetClass :NeverPresentEquipment ;
    sh:nodeKind sh:IRI .
"""


# bldg1 has all the real point instances used below. The overlay adds only the
# application relationships needed to make RM100 and VAVRM100 complete, plus a
# representative heating coil and command. The other seven zones and VAVs are
# intentionally left unchanged and will fail their respective point lists.
DEMO_OVERLAY = """
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix bldg: <http://buildsys.org/ontologies/bldg1#> .
@prefix demo: <urn:shifty-evidence-demo/> .

bldg:RM100 brick:hasPoint
    bldg:bldg1.ZONE.AHU02.RM100.Zone_Air_Temp .

bldg:VAVRM100 brick:hasPart demo:heating-coil .

demo:heating-coil a brick:Heating_Coil ;
    brick:hasPoint demo:valve-position-command .

demo:valve-position-command a brick:Position_Command .
"""


def compact(term: str) -> str:
    """Make benchmark IRIs easier to scan in terminal output."""
    return (
        term.replace("<http://buildsys.org/ontologies/bldg1#", "bldg:")
        .replace("<urn:shifty-evidence-demo/", "demo:")
        .removesuffix(">")
    )


def main() -> None:
    session = shifty.EvidenceSession(
        POINT_LIST_SHAPES.encode(),
        [BRICK_ONTOLOGY, BRICK_MODEL, DEMO_OVERLAY.encode()],
        infer=False,
        graph_mode="union",
    )
    outcome = session.validate()

    foci = [focus for statement in outcome.statements for focus in statement.selected_foci]
    counts = Counter(focus.status for focus in foci)
    print(f"conforms: {outcome.conforms}")
    print(
        "coverage horizon: "
        f"{len(outcome.statements)} source statements, {len(foci)} selected pairs "
        f"({counts['pass']} pass, {counts['fail']} fail)"
    )

    for statement in outcome.statements:
        print(
            f"\nStatement {statement.source_statement_id} "
            f"(normalized {statement.normalized_statement_id})\n"
            f"  target: {statement.selector}\n"
            f"  constraint: {statement.constraint_kind} "
            f"(@{statement.normalized_constraint_id})"
        )
        if not statement.selected_foci:
            print("  [NO SELECTION] selected_foci=[]")
            continue

        for result in statement.selected_foci:
            print(f"\n  [{result.status.upper()}] {compact(result.focus)}")
            print("    canonical evidence:")
            for node in result.evidence.walk():
                print(
                    f"      {node.status:4} {node.kind} "
                    f"(@{node.constraint_id})"
                )
            if result.progress is not None:
                print("    immediate progress:")
                for child in result.progress.evaluated_children:
                    print(
                        f"      source @{child.source_constraint_ref}: "
                        f"{child.status}"
                    )
            if values := result.evidence.matched_values():
                print("    matched values:", ", ".join(compact(v) for v in values))
            if missing := result.evidence.missing_obligations():
                print("    missing obligations:", ", ".join(str(v.missing) for v in missing))
            if offenders := result.evidence.offending_values():
                print("    offending values:", ", ".join(compact(v) for v in offenders))
            if triples := result.evidence.supporting_triples():
                print(f"    positive certificate: {len(triples)} triple(s)")

    # The benchmark contains this chiller, but none of the three point-list
    # statements target brick:Chiller. Its absence is observably different from
    # a selected PASS result.
    chiller = "<http://buildsys.org/ontologies/bldg1#chiller>"
    selected_foci = {result.focus for result in foci}
    assert chiller not in selected_foci
    print(f"\n[UNSELECTED] {compact(chiller)} — no coverage row")

    # The complete object is JSON-compatible and uses explicit status/type tags.
    assert outcome.to_dict() == __import__("json").loads(outcome.to_json())


if __name__ == "__main__":
    main()
