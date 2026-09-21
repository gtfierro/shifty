"""Generate deterministic RDF workloads for shared-dataset benchmark coverage.

The manifest records the storage and query dimensions exercised by each case.
Run ``compare_shared_datasets.py`` with each case's shapes and data paths; keep
its alternating, warmed five-sample protocol for measurements.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

PREFIXES = """@prefix ex: <http://example.org/shared-bench/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
"""


@dataclass(frozen=True)
class Case:
    name: str
    source_rows: int
    nodes: int
    predicates: int
    demanded_predicates: int
    structure: str
    workload: str
    overlap_rows: int = 0


CASES = (
    Case("tiny-source-large-data", 0, 2000, 8, 1, "chain", "forward"),
    Case("large-source-small-data", 5000, 60, 2, 1, "star", "reverse"),
    Case("both-large-dense-demand", 5000, 2000, 8, 8, "cycle", "forward"),
    Case("source-data-overlap", 1000, 500, 4, 1, "chain", "forward", 500),
    Case("mixed-native-sparql", 500, 500, 4, 1, "cycle", "mixed-native"),
    Case("many-predicates-wildcard", 500, 200, 12, 0, "star", "wildcard"),
    Case("fallback-named-shapes", 2000, 500, 4, 1, "chain", "fallback"),
    Case("no-active-rules", 100, 500, 4, 1, "chain", "inactive-rule"),
    Case("triple-rule-rounds", 500, 1000, 4, 1, "star", "triple-rule"),
    Case("sparql-rule-rounds", 500, 1000, 4, 1, "cycle", "sparql-rule"),
)


def shapes(case: Case) -> str:
    lines = [PREFIXES, "ex:marker ex:active true ."]
    for index in range(case.source_rows):
        lines.append(f"ex:source{index} ex:metadata ex:value{index} .")

    property_shapes = []
    for index in range(case.demanded_predicates):
        property_shapes.append(
            f"    sh:property [ sh:path ex:p{index} ; sh:minCount 1 ]"
        )
    if case.workload == "reverse":
        property_shapes.append(
            "    sh:property [ sh:path [ sh:inversePath ex:link ] ; sh:minCount 1 ]"
        )
    if case.workload == "mixed-native":
        property_shapes.extend(
            [
                "    sh:property [ sh:path [ sh:inversePath ex:link ] ; sh:minCount 1 ]",
                (
                    '    sh:sparql [ sh:select "SELECT $this WHERE { '
                    "$this <http://example.org/shared-bench/p0> ?next . "
                    '?next <http://example.org/shared-bench/p1> ?value }" ]'
                ),
            ]
        )
    if case.workload == "wildcard":
        property_shapes.append(
            '    sh:sparql [ sh:select "SELECT $this WHERE { $this ?p ?o . '
            'FILTER(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) }" ]'
        )
    if case.workload == "fallback":
        property_shapes.append(
            '    sh:sparql [ sh:select "SELECT $this WHERE { '
            "GRAPH $shapesGraph { <http://example.org/shared-bench/marker> "
            "<http://example.org/shared-bench/active> true } "
            "$this <http://example.org/shared-bench/p0> ?v . "
            "FILTER(REGEX(STR(?v), 'n')) }\" ]"
        )
    if case.workload == "triple-rule":
        property_shapes.extend(
            [
                (
                    "    sh:rule [ a sh:TripleRule ; sh:order 0 ; sh:subject sh:this ;"
                    " sh:predicate ex:derived ; sh:object ex:constant ]"
                ),
                (
                    "    sh:rule [ a sh:TripleRule ; sh:order 1 ; sh:subject sh:this ;"
                    " sh:predicate ex:derivedAgain ; sh:object ex:constant ]"
                ),
            ]
        )
    if case.workload == "sparql-rule":
        for step in range(5):
            previous = "p0" if step == 0 else f"derived{step - 1}"
            property_shapes.append(
                f'    sh:rule [ a sh:SPARQLRule ; sh:order {step} ; sh:construct "'
                f"CONSTRUCT {{ $this <http://example.org/shared-bench/derived{step}> "
                "?value } WHERE { "
                f'$this <http://example.org/shared-bench/{previous}> ?value }}" ]'
            )
    if case.workload == "inactive-rule":
        lines.append(
            "ex:inactive a sh:NodeShape ; sh:targetClass ex:Never ; "
            "sh:rule [ a sh:TripleRule ; sh:subject sh:this ; "
            "sh:predicate ex:unused ; sh:object ex:constant ] ."
        )
    if case.overlap_rows:
        lines.append(
            "ex:overlapCheck a sh:NodeShape ; sh:targetNode ex:source0 ; "
            "sh:property [ sh:path ex:metadata ; sh:minCount 1 ] ."
        )
    lines.append(
        "ex:shape a sh:NodeShape ;\n"
        "    sh:targetClass ex:Entity ;\n" + " ;\n".join(property_shapes) + " ."
    )
    return "\n".join(lines) + "\n"


def data(case: Case) -> str:
    lines = [PREFIXES]
    for index in range(case.overlap_rows):
        lines.append(f"ex:source{index} ex:metadata ex:value{index} .")
    for node in range(case.nodes):
        lines.append(f"ex:n{node} a ex:Entity .")
        for predicate in range(case.predicates):
            target = (node + predicate + 1) % case.nodes
            lines.append(f"ex:n{node} ex:p{predicate} ex:n{target} .")
        if case.structure == "chain" and node + 1 < case.nodes:
            lines.append(f"ex:n{node} ex:link ex:n{node + 1} .")
        elif case.structure == "cycle":
            lines.append(f"ex:n{node} ex:link ex:n{(node + 1) % case.nodes} .")
        elif case.structure == "star" and node > 0:
            lines.append(f"ex:n0 ex:link ex:n{node} .")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for case in CASES:
        case_dir = args.output_dir / case.name
        case_dir.mkdir(exist_ok=True)
        shape_path = case_dir / "shapes.ttl"
        data_path = case_dir / "data.ttl"
        shape_path.write_text(shapes(case))
        data_path.write_text(data(case))
        manifest.append(
            {
                **asdict(case),
                "shapes": str(shape_path.resolve()),
                "data": str(data_path.resolve()),
            }
        )
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
