"""Demonstrate reusing a cached ShapeIR inside Python."""

from __future__ import annotations

from pathlib import Path

import rdflib
import shifty

ROOT = Path(__file__).parent


def load_graph(filename: str) -> rdflib.Graph:
    """Load a Turtle file from ``python/`` into an RDFLib graph."""

    graph = rdflib.Graph()
    graph.parse(ROOT / filename, format="turtle")
    return graph


def validate_with_cache(
    cache: shifty.CompiledShapeGraph, data_graph: rdflib.Graph
) -> None:
    """Run validation from a cached IR and print diagnostics."""

    conforms, _report_graph, report_text, diagnostics = cache.validate(
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


def infer_with_cache(
    cache: shifty.CompiledShapeGraph, data_graph: rdflib.Graph
) -> None:
    """Run inference from cached IR and show summary metrics."""

    inferred_graph, inference_diag = cache.infer(
        data_graph,
        run_until_converged=True,
        graphviz=True,
        return_inference_outcome=True,
    )
    print(f"Inferred {len(inferred_graph)} triples from cached IR")
    if inference_diag:
        print("Inference diagnostics:", inference_diag.get("inference_outcome"))


def main() -> None:
    """Load fixtures, cache the shapes IR, and run both validate+infer."""

    shapes_graph = load_graph("shapes.ttl")
    data_graph = load_graph("data.ttl")

    cache = shifty.generate_ir(
        shapes_graph,
        skip_invalid_rules=True,
        warnings_are_errors=False,
        do_imports=True,
    )

    validate_with_cache(cache, data_graph)
    infer_with_cache(cache, data_graph)


if __name__ == "__main__":
    main()
