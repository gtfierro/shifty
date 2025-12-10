"""Demonstrate reusing a cached ShapeIR inside Python."""

from pathlib import Path

import rdflib
import shacl_rs

ROOT = Path(__file__).parent


def load_graph(filename: str) -> rdflib.Graph:
    graph = rdflib.Graph()
    graph.parse(ROOT / filename, format="turtle")
    return graph


def main() -> None:
    shapes_graph = load_graph("shapes.ttl")
    data_graph = load_graph("data.ttl")

    cache = shacl_rs.generate_ir(
        shapes_graph,
        skip_invalid_rules=True,
        warnings_are_errors=False,
        do_imports=True,
    )

    conforms, report_graph, report_text, diagnostics = cache.validate(
        data_graph,
        run_inference=True,
        inference={"min_iterations": 1, "max_iterations": 4},
        graphviz=True,
        heatmap=True,
        trace_events=True,
        return_inference_outcome=True,
    )

    print(f"Validation conforms? {conforms}")
    print(report_text)
    if diagnostics:
        print("Graphviz DOT snippet:")
        print(diagnostics.get("graphviz"))
        print("Heatmap DOT snippet:")
        print(diagnostics.get("heatmap"))
        print("Inference stats:", diagnostics.get("inference_outcome"))

    inferred_graph, inference_diag = cache.infer(
        data_graph,
        run_until_converged=True,
        graphviz=True,
        return_inference_outcome=True,
    )
    print(f"Inferred {len(inferred_graph)} triples from cached IR")
    if inference_diag:
        print("Inference diagnostics:", inference_diag.get("inference_outcome"))


if __name__ == "__main__":
    main()
