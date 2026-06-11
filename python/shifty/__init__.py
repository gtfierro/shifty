"""
shifty — Python bindings for the shifty SHACL engine.

Two validation interfaces:

``validate(data_graph, shacl_graph=None, ...)``
    pyshacl-compatible.  Returns ``(conforms, report_graph, results_text)``
    where *report_graph* is a :class:`rdflib.Graph` containing the full W3C
    ``sh:ValidationReport``.

``validate_algebra(data_graph, shacl_graph=None, ...)``
    Returns an :class:`AlgebraResult` with a structured list of
    :class:`Violation` / :class:`Reason` objects representing the algebraic
    failure tree — useful for programmatic inspection.

``infer(data_graph, shapes_graph=None, ...)``
    Run SHACL-AF forward-chaining rules to a fixed point.
    Returns an :class:`InferResult`; call ``.graph()`` to get the
    result as an :class:`rdflib.Graph`.

Graph inputs
~~~~~~~~~~~~
All three functions accept any of:

* :class:`rdflib.Graph`       — serialized to Turtle automatically
* :class:`pathlib.Path`       — read as a Turtle/N-Triples file
* ``str``                     — treated as a file path if the path exists,
                                otherwise as raw Turtle text
* ``bytes``                   — raw Turtle bytes passed directly to the parser

``graph_mode`` values
~~~~~~~~~~~~~~~~~~~~~
* ``"union"``      (default) — focus nodes from data; evaluation uses
                               data ∪ shapes (standard SHACL default)
* ``"data"``       — focus nodes and evaluation use the data graph only
* ``"union-all"``  — focus nodes and evaluation both use data ∪ shapes
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Optional, Union

from ._shifty import (
    AlgebraResult,
    InferResult as _RustInferResult,
    Reason,
    Violation,
    W3cResult,
    _infer,
    _validate_algebra,
    _validate_w3c,
)

if TYPE_CHECKING:
    import rdflib

__all__ = [
    "validate",
    "validate_algebra",
    "infer",
    "AlgebraResult",
    "Violation",
    "Reason",
    "InferResult",
]

GraphInput = Union[str, bytes, pathlib.Path, "rdflib.Graph"]


def _to_turtle_bytes(graph: GraphInput) -> bytes:
    """Convert any supported graph input type to Turtle bytes."""
    if isinstance(graph, bytes):
        return graph
    if isinstance(graph, pathlib.Path):
        return graph.read_bytes()
    if isinstance(graph, str):
        p = pathlib.Path(graph)
        if p.exists():
            return p.read_bytes()
        return graph.encode("utf-8")
    # Duck-type for rdflib.Graph (and any object with a .serialize method)
    serialize = getattr(graph, "serialize", None)
    if serialize is not None:
        result = serialize(format="turtle")
        if isinstance(result, str):
            return result.encode("utf-8")
        if isinstance(result, bytes):
            return result
    raise TypeError(
        f"Cannot convert {type(graph).__name__!r} to RDF data. "
        "Expected rdflib.Graph, pathlib.Path, str (path or Turtle), or bytes."
    )


class InferResult:
    """Result of a SHACL-AF inference run."""

    def __init__(self, inner: _RustInferResult) -> None:
        self._inner = inner

    @property
    def inferred_count(self) -> int:
        """Number of newly derived triples."""
        return self._inner.inferred_count

    @property
    def diagnostics(self) -> list[str]:
        """Warnings about unsupported rule features."""
        return self._inner.diagnostics

    @property
    def graph_ntriples(self) -> str:
        """Full graph (original data + inferred triples) as N-Triples string."""
        return self._inner.graph_ntriples

    def graph(self) -> "rdflib.Graph":
        """Return the full graph as an :class:`rdflib.Graph`."""
        import rdflib

        g = rdflib.Graph()
        g.parse(data=self._inner.graph_ntriples, format="nt")
        return g

    def __repr__(self) -> str:
        return f"InferResult(inferred={self.inferred_count})"


def validate(
    data_graph: GraphInput,
    shacl_graph: Optional[GraphInput] = None,
    *,
    graph_mode: str = "union",
    infer: bool = True,
    base: Optional[str] = None,
) -> "tuple[bool, rdflib.Graph, str]":
    """Validate *data_graph* against *shacl_graph* (pyshacl-compatible).

    Parameters
    ----------
    data_graph:
        The RDF data to validate.
    shacl_graph:
        The SHACL shapes graph.  If ``None``, shapes are expected to be
        embedded in *data_graph* (standard SHACL pattern).
    graph_mode:
        ``"union"`` (default), ``"data"``, or ``"union-all"``.
    infer:
        Run SHACL-AF rules before validation (default ``True``).
    base:
        Base IRI for resolving relative IRIs in the inputs.

    Returns
    -------
    (conforms, report_graph, results_text)
        * *conforms* — ``True`` if the data graph satisfies all shapes.
        * *report_graph* — :class:`rdflib.Graph` containing the full W3C
          ``sh:ValidationReport``.
        * *results_text* — human-readable summary string.
    """
    import rdflib

    data_bytes = _to_turtle_bytes(data_graph)
    shapes_bytes = _to_turtle_bytes(shacl_graph) if shacl_graph is not None else None
    result: W3cResult = _validate_w3c(data_bytes, shapes_bytes, graph_mode, infer, base)

    g = rdflib.Graph()
    g.parse(data=result.report_turtle, format="turtle")

    return (result.conforms, g, result.results_text)


def validate_algebra(
    data_graph: GraphInput,
    shacl_graph: Optional[GraphInput] = None,
    *,
    graph_mode: str = "union",
    infer: bool = True,
    base: Optional[str] = None,
) -> AlgebraResult:
    """Validate and return a structured algebraic result.

    Unlike :func:`validate`, this uses the algebra execution path and returns
    an :class:`AlgebraResult` whose :attr:`~AlgebraResult.violations` are
    structured :class:`Violation` objects — each with a list of
    :class:`Reason` objects describing which constraint failed and on which
    value node.

    Parameters
    ----------
    data_graph, shacl_graph, graph_mode, infer, base:
        Same as :func:`validate`.

    Returns
    -------
    AlgebraResult
        ``.conforms`` is ``True`` when no violations were found.
        ``.violations`` lists each failing focus node with reasons.
    """
    data_bytes = _to_turtle_bytes(data_graph)
    shapes_bytes = _to_turtle_bytes(shacl_graph) if shacl_graph is not None else None
    return _validate_algebra(data_bytes, shapes_bytes, graph_mode, infer, base)


def infer(
    data_graph: GraphInput,
    shapes_graph: Optional[GraphInput] = None,
    *,
    base: Optional[str] = None,
) -> InferResult:
    """Run SHACL-AF forward-chaining rules to a fixed point.

    Parameters
    ----------
    data_graph:
        Input data graph.
    shapes_graph:
        Shapes graph containing ``sh:rule`` definitions.  If ``None``,
        rules are expected inside *data_graph*.
    base:
        Base IRI for resolving relative IRIs.

    Returns
    -------
    InferResult
        Call ``.graph()`` to get the result as an :class:`rdflib.Graph`,
        or read ``.graph_ntriples`` for the raw N-Triples string.
    """
    data_bytes = _to_turtle_bytes(data_graph)
    shapes_bytes = _to_turtle_bytes(shapes_graph) if shapes_graph is not None else None
    inner = _infer(data_bytes, shapes_bytes, base)
    return InferResult(inner)
