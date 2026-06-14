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

* :class:`rdflib.Graph`       — serialized to N-Triples automatically
* :class:`pathlib.Path`       — parsed directly as Turtle or N-Triples
* ``str``                     — treated as a file path if the path exists,
                                otherwise as raw Turtle text
* ``bytes``                   — raw Turtle bytes passed directly to the parser

``graph_mode`` values
~~~~~~~~~~~~~~~~~~~~~
* ``"union"``      (default) — focus nodes from data; evaluation uses
                               data ∪ shapes (standard SHACL default)
* ``"data"``       — focus nodes and evaluation use the data graph only
* ``"union-all"``  — focus nodes and evaluation both use data ∪ shapes

``graph_mode`` applies to ``validate`` and ``validate_algebra``. When the
shapes graph is omitted, all modes are equivalent because data and shapes are
the same embedded graph. ``infer`` does not accept ``graph_mode``.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, NamedTuple, Optional, Union

from ._shifty import (
    AlgebraResult,
    InferResult as _RustInferResult,
    PreparedValidator as _RustPreparedValidator,
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
    "PreparedValidator",
]

GraphInput = Union[str, bytes, pathlib.Path, "rdflib.Graph"]


class _RdfInput(NamedTuple):
    data: Optional[bytes]
    path: Optional[str]
    format: str


def _path_format(path: pathlib.Path) -> str:
    return "nt" if path.suffix.lower() in {".nt", ".ntriples"} else "turtle"


def _to_rdf_input(graph: GraphInput) -> _RdfInput:
    """Convert a public graph input into the native binding's input descriptor."""
    if isinstance(graph, bytes):
        return _RdfInput(graph, None, "turtle")
    if isinstance(graph, pathlib.Path):
        if not graph.exists():
            raise FileNotFoundError(graph)
        if not graph.is_file():
            raise IsADirectoryError(graph)
        return _RdfInput(None, str(graph), _path_format(graph))
    if isinstance(graph, str):
        path = pathlib.Path(graph)
        if path.is_file():
            return _RdfInput(None, str(path), _path_format(path))
        return _RdfInput(graph.encode("utf-8"), None, "turtle")
    serialize = getattr(graph, "serialize", None)
    if serialize is not None:
        result = serialize(format="nt", encoding="utf-8")
        if isinstance(result, str):
            result = result.encode("utf-8")
        if isinstance(result, bytes):
            return _RdfInput(result, None, "nt")
    raise TypeError(
        f"Cannot convert {type(graph).__name__!r} to RDF data. "
        "Expected rdflib.Graph, pathlib.Path, str (path or Turtle), or bytes."
    )


def _to_turtle_bytes(graph: GraphInput) -> bytes:
    """Compatibility helper that materializes the input as RDF bytes."""
    source = _to_rdf_input(graph)
    if source.data is not None:
        return source.data
    assert source.path is not None
    return pathlib.Path(source.path).read_bytes()


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


class PreparedValidator:
    """Parsed and planned SHACL shapes reusable across data graphs."""

    def __init__(self, shacl_graph: GraphInput, *, base: Optional[str] = None) -> None:
        shapes = _to_rdf_input(shacl_graph)
        self._inner = _RustPreparedValidator(
            shapes.data,
            shapes.path,
            shapes.format,
            base,
        )

    @property
    def diagnostics(self) -> list[str]:
        """Warnings produced while lowering the shapes graph."""
        return self._inner.diagnostics

    def validate(
        self,
        data_graph: GraphInput,
        *,
        graph_mode: str = "union",
        infer: bool = True,
        minimum_severity: str = "info",
        sort_results: bool = True,
    ) -> "tuple[bool, rdflib.Graph, str]":
        import rdflib

        data = _to_rdf_input(data_graph)
        result: W3cResult = self._inner.validate_w3c(
            data.data,
            data.path,
            data.format,
            graph_mode,
            infer,
            minimum_severity,
            sort_results,
        )
        graph = rdflib.Graph()
        graph.parse(data=result.report_turtle, format="turtle")
        return (result.conforms, graph, result.results_text)

    def validate_algebra(
        self,
        data_graph: GraphInput,
        *,
        graph_mode: str = "union",
        infer: bool = True,
        minimum_severity: str = "info",
        sort_results: bool = True,
    ) -> AlgebraResult:
        data = _to_rdf_input(data_graph)
        return self._inner.validate_algebra(
            data.data,
            data.path,
            data.format,
            graph_mode,
            infer,
            minimum_severity,
            sort_results,
        )

    def __repr__(self) -> str:
        return repr(self._inner)


def validate(
    data_graph: GraphInput,
    shacl_graph: Optional[GraphInput] = None,
    *,
    graph_mode: str = "union",
    infer: bool = True,
    minimum_severity: str = "info",
    sort_results: bool = True,
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
    minimum_severity:
        Lowest level that makes ``conforms`` false: ``"info"`` (default),
        ``"warning"``, or ``"violation"``. Lower-level results remain in the
        report graph.
    sort_results:
        Whether to sort validation results by severity and focus node
        (default ``True``).
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

    data = _to_rdf_input(data_graph)
    shapes = _to_rdf_input(shacl_graph) if shacl_graph is not None else _RdfInput(None, None, "turtle")
    result: W3cResult = _validate_w3c(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        graph_mode,
        infer,
        minimum_severity,
        sort_results,
        base,
    )

    g = rdflib.Graph()
    g.parse(data=result.report_turtle, format="turtle")

    return (result.conforms, g, result.results_text)


def validate_algebra(
    data_graph: GraphInput,
    shacl_graph: Optional[GraphInput] = None,
    *,
    graph_mode: str = "union",
    infer: bool = True,
    minimum_severity: str = "info",
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
    minimum_severity:
        Lowest level that makes ``conforms`` false: ``"info"`` (default),
        ``"warning"``, or ``"violation"``. All findings remain available in
        ``.violations`` regardless of this threshold.

    Returns
    -------
    AlgebraResult
        ``.conforms`` is ``True`` when no violations were found.
        ``.violations`` lists each failing focus node with reasons.
    """
    data = _to_rdf_input(data_graph)
    shapes = _to_rdf_input(shacl_graph) if shacl_graph is not None else _RdfInput(None, None, "turtle")
    return _validate_algebra(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        graph_mode,
        infer,
        minimum_severity,
        sort_results,
        base,
    )


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
    data = _to_rdf_input(data_graph)
    shapes = _to_rdf_input(shapes_graph) if shapes_graph is not None else _RdfInput(None, None, "turtle")
    inner = _infer(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        base,
    )
    return InferResult(inner)
