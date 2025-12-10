"""End-to-end usage examples for the :mod:`shifty` Python bindings."""

from __future__ import annotations

from rdflib import Graph, Literal, Namespace, RDF, RDFS
import shifty  # built via `uvx maturin develop` inside python/

EX = Namespace("http://example.com/ns#")


def build_data_graph() -> Graph:
    """Create a simple example dataset."""

    graph = Graph()
    graph.bind("ex", EX)
    graph.add((EX.Person1, RDF.type, EX.Person))
    graph.add((EX.Person1, RDFS.label, Literal("Alice")))
    graph.add((EX.Person2, RDF.type, EX.Person))
    return graph


def build_shapes_graph() -> Graph:
    """Create a SHACL shape that requires ``ex:Person`` to have one ``rdfs:label``."""

    graph = Graph()
    graph.parse(
        data="""
            PREFIX sh: <http://www.w3.org/ns/shacl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX ex: <http://example.com/ns#>

            ex:PersonShape
                a sh:NodeShape ;
                sh:targetClass ex:Person ;
                sh:property [
                    sh:path rdfs:label ;
                    sh:minCount 1 ;
                    sh:maxCount 1 ;
                ] .
        """,
        format="turtle",
    )
    return graph


def demonstrate_validate(data: Graph, shapes: Graph) -> None:
    """Run vanilla validation and log the results."""

    conforms, results_graph, results_text = shifty.validate(data, shapes)
    print("Conforms?", conforms)
    print("Results triples:", len(results_graph))
    print(results_text)


def demonstrate_infer(data: Graph, shapes: Graph) -> None:
    """Run SHACL rule inference before validation."""

    inferred = shifty.infer(data, shapes)
    print("Inferred triples:", len(inferred))


def demonstrate_advanced_options(data: Graph, shapes: Graph) -> None:
    """Showcase inference options and diagnostics returned by ``validate``."""

    conforms, _, _ = shifty.validate(
        data,
        shapes,
        inference={"min_iterations": 1, "max_iterations": 4, "debug": True},
    )
    print("Conforms with inference options?", conforms)

    conforms2, _, _, diag = shifty.validate(
        data,
        shapes,
        run_inference=True,
        graphviz=True,
        trace_events=True,
        return_inference_outcome=True,
    )
    print("Conforms with diagnostics?", conforms2)
    print("Shapes DOT (first 80 chars):", diag.get("graphviz", "")[:80])
    print("Inference stats:", diag.get("inference_outcome"))
    print("Trace events count:", len(diag.get("trace_events", [])))


def main() -> None:
    """Run all demonstrations using pre-built graphs."""

    data = build_data_graph()
    shapes = build_shapes_graph()
    demonstrate_validate(data, shapes)
    demonstrate_infer(data, shapes)
    demonstrate_advanced_options(data, shapes)


if __name__ == "__main__":
    main()
