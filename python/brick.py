"""Utility script for fetching ontologies and running SHACL validation."""

from __future__ import annotations

from typing import Sequence

from ontoenv import OntoEnv
from rdflib import Graph
import shifty
import sys

ENV = OntoEnv()


def run_shacl_pipeline(model_path: str, env: OntoEnv = ENV) -> bool:
    """Resolve imports, run inference, and validate ``model_path``.

    Args:
        model_path: File system path or OntoEnv identifier for the model.
        env: Shared ``OntoEnv`` instance so repeated invocations reuse caches.

    Returns:
        ``True`` if the model conforms to its SHACL shapes, ``False`` otherwise.
    """

    model_name = env.add(model_path)
    print(f"Fetching dependencies for model: {model_name}")
    model_graph: Graph = env.get_graph(model_name)
    shape_graph, imported = env.get_closure(model_name)
    print(f"Imported ontologies for SHACL shape graph: {imported}")

    print("Running SHACL inference...")
    inferred = shifty.infer(model_graph, shape_graph)
    print(inferred.serialize(format="turtle"))

    valid, _results_graph, report_string = shifty.validate(
        model_graph, shape_graph, inference={"debug": True}
    )
    print("Validation Report:")
    print(report_string)
    print(f"Model is valid: {valid}")
    return bool(valid)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse command-line arguments and trigger the SHACL pipeline."""

    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        print("Usage: python brick.py <model.ttl>")
        return 1
    model_path = args[0]
    conforms = run_shacl_pipeline(model_path)
    return 0 if conforms else 2


if __name__ == "__main__":
    raise SystemExit(main())
