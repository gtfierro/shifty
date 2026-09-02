"""Apply a shape graph to a data graph and inspect the resulting shape map.

Run from ``python/`` after ``uv run maturin develop``:

    uv run python examples/shape_map_point_list.py
"""

from evidence_point_list import (
    BRICK_MODEL,
    BRICK_ONTOLOGY,
    DEMO_OVERLAY,
    POINT_LIST_SHAPES,
    compact,
)

import shifty

shape_graph = POINT_LIST_SHAPES.encode()
data_graph = [BRICK_ONTOLOGY, BRICK_MODEL, DEMO_OVERLAY.encode()]

shape_map = shifty.shape_map(data_graph, shape_graph, graph_mode="union")

for mapping in shape_map:
    status = "ok" if mapping.conforms else "incomplete"
    print(f"\n{compact(mapping.focus.n3())} [{status}]")

    for key, binding in mapping.successful:
        values = ", ".join(compact(value.n3()) for value in binding.values)
        print(f"  {key}: {values}")

    for key, binding in mapping.unsuccessful:
        print(f"  {key}: missing {binding.missing}")


# A missing binding still provides the detailed validation evidence on demand.
incomplete = next(mapping for mapping in shape_map if not mapping.conforms)
_, missing = incomplete.unsuccessful[0]
print("\nWhy is the first binding missing?")
print(missing.explain())
