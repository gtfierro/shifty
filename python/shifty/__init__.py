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

``PreparedValidator(shacl_graph).witnesses(data_graph, ...)``
    The inverse of validation: for every focus node that *conforms* to a
    target/profile node shape, returns the values each ``sh:property``
    shape's ``sh:path`` resolved to. Returns a list of
    :class:`PropertyWitness`.

Graph inputs
~~~~~~~~~~~~
All three functions accept any of:

* :class:`rdflib.Graph`       — serialized to N-Triples automatically
* :class:`pathlib.Path`       — parsed directly as Turtle or N-Triples
* ``str``                     — treated as a file path if the path exists,
                                otherwise as raw Turtle text
* ``bytes``                   — raw Turtle bytes passed directly to the parser

A ``list`` or ``tuple`` of any of the above is also accepted for every
data/shapes argument; the members are unioned (merged at the RDF triple level,
the same way the CLI's repeatable ``--shapes`` / ``--data`` merge) before being
passed to the engine. A single input keeps its native fast path.

``graph_mode`` values
~~~~~~~~~~~~~~~~~~~~~
* ``"union"``      (default) — focus nodes from data; evaluation uses
                               data ∪ shapes (standard SHACL default)
* ``"data"``       — focus nodes and evaluation use the data graph only
* ``"union-all"``  — focus nodes and evaluation both use data ∪ shapes

``graph_mode`` applies to ``validate`` and ``validate_algebra``. When the
shapes graph is omitted or passed as ``None``, all modes are equivalent because
data and shapes are the same embedded graph. Passing an empty ``rdflib.Graph()``
is an explicit empty shapes graph, not embedded-shapes mode. ``infer`` does not
accept ``graph_mode``.

``shape_names`` applies to ``validate`` and ``validate_algebra``. It limits
validation to selected named shapes as top-level entry points while still
evaluating referenced helper shapes normally.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, NamedTuple, Optional, Sequence, Union

from ._shifty import (
    AlgebraResult,
    Choice,
    ChoiceKind,
    FocusSatisfaction,
    FocusWitness,
    Hole,
    SatAtom,
    SatKind,
    InferResult as _RustInferResult,
    Instantiated,
    PreparedValidator as _RustPreparedValidator,
    PropertyWitness,
    Reason,
    RepairDelta,
    RepairOutcome,
    RepairPlan,
    RepairSession as _RustRepairSession,
    RepairTree,
    Target,
    TargetKind,
    Violation,
    W3cResult,
    WitnessAtom,
    WitnessKind,
    __version__,
    _infer,
    _validate_algebra,
    _validate_w3c,
    version,
)

if TYPE_CHECKING:
    import rdflib

__all__ = [
    "validate",
    "validate_algebra",
    "infer",
    "version",
    "__version__",
    "AlgebraResult",
    "Violation",
    "Reason",
    "InferResult",
    "PreparedValidator",
    "PropertyWitness",
    # ── symbolic repair ──
    "RepairSession",
    "RepairPlan",
    "FocusWitness",
    "FocusSatisfaction",
    "Target",
    "TargetKind",
    "WitnessAtom",
    "WitnessKind",
    "SatAtom",
    "SatKind",
    "RepairTree",
    "Hole",
    "Choice",
    "ChoiceKind",
    "Instantiated",
    "RepairDelta",
    "RepairOutcome",
    "delta_from_graph",
]

GraphInput = Union[str, bytes, pathlib.Path, "rdflib.Graph"]
# Any single `GraphInput`, or a list/tuple of them to be unioned (merged at
# the RDF triple level) before being passed to the engine.
GraphInputs = Union[GraphInput, list[GraphInput], tuple[GraphInput, ...]]


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


def _as_rdflib_graph(graph: GraphInput) -> "rdflib.Graph":
    """Materialize a single graph input as a fresh :class:`rdflib.Graph`.

    Used by :func:`_coalesce_graph_input` to union several inputs. The caller's
    :class:`rdflib.Graph` is copied rather than mutated."""
    import rdflib

    if isinstance(graph, rdflib.Graph):
        merged = rdflib.Graph()
        for triple in graph:
            merged.add(triple)
        return merged
    if isinstance(graph, bytes):
        g = rdflib.Graph()
        g.parse(data=graph, format="turtle")
        return g
    if isinstance(graph, pathlib.Path):
        g = rdflib.Graph()
        g.parse(source=str(graph), format=_path_format(graph))
        return g
    if isinstance(graph, str):
        path = pathlib.Path(graph)
        if path.is_file():
            g = rdflib.Graph()
            g.parse(source=graph, format=_path_format(path))
            return g
        g = rdflib.Graph()
        g.parse(data=graph, format="turtle")
        return g
    raise TypeError(
        f"Cannot convert {type(graph).__name__!r} to RDF data. "
        "Expected rdflib.Graph, pathlib.Path, str (path or Turtle), or bytes."
    )


def _coalesce_graph_input(graph: "GraphInputs") -> GraphInput:
    """Normalize a graph input, unioning lists/tuples into one graph.

    A single input is returned unchanged so the native Rust parser keeps its
    direct-file / direct-bytes fast path. A list or tuple of inputs is merged
    at the RDF triple level (mirroring the CLI's repeatable ``--shapes`` /
    ``--data``) and returned as a single :class:`rdflib.Graph`. An empty
    sequence raises :class:`ValueError`."""
    if isinstance(graph, (list, tuple)):
        if len(graph) == 0:
            raise ValueError("graph input list must not be empty")
        if len(graph) == 1:
            return graph[0]
        import rdflib

        merged = rdflib.Graph()
        for item in graph:
            for triple in _as_rdflib_graph(item):
                merged.add(triple)
        return merged
    return graph


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

    def __init__(self, shacl_graph: GraphInputs, *, base: Optional[str] = None) -> None:
        shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph))
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
        data_graph: GraphInputs,
        *,
        graph_mode: str = "union",
        shape_names: Optional[Sequence[str]] = None,
        infer: bool = True,
        minimum_severity: str = "info",
        sort_results: bool = True,
        on_unsupported: str = "ignore",
    ) -> "tuple[bool, rdflib.Graph, str]":
        """Validate *data_graph* against the prepared shapes.

        ``shape_names`` optionally limits validation to the named shapes in
        that list as top-level entry points. Referenced helper shapes are still
        evaluated normally.
        """
        import rdflib

        data = _to_rdf_input(_coalesce_graph_input(data_graph))
        result: W3cResult = self._inner.validate_w3c(
            data.data,
            data.path,
            data.format,
            graph_mode,
            list(shape_names) if shape_names is not None else None,
            infer,
            minimum_severity,
            sort_results,
            on_unsupported,
        )
        graph = rdflib.Graph()
        graph.parse(data=result.report_turtle, format="turtle")
        return (result.conforms, graph, result.results_text)

    def validate_algebra(
        self,
        data_graph: GraphInputs,
        *,
        graph_mode: str = "union",
        shape_names: Optional[Sequence[str]] = None,
        infer: bool = True,
        minimum_severity: str = "info",
        sort_results: bool = True,
        on_unsupported: str = "ignore",
    ) -> AlgebraResult:
        """Validate using the algebra result path.

        ``shape_names`` optionally limits validation to the named shapes in
        that list as top-level entry points. Referenced helper shapes are still
        evaluated normally.
        """
        data = _to_rdf_input(_coalesce_graph_input(data_graph))
        return self._inner.validate_algebra(
            data.data,
            data.path,
            data.format,
            graph_mode,
            list(shape_names) if shape_names is not None else None,
            infer,
            minimum_severity,
            sort_results,
            on_unsupported,
        )

    def witnesses(
        self,
        data_graph: GraphInputs,
        *,
        key_path: Optional[str] = None,
        graph_mode: str = "union",
        infer: bool = True,
        on_unsupported: str = "ignore",
    ) -> list[PropertyWitness]:
        """Return the observed ``sh:property`` bindings for every focus node
        that *conforms* to a target/profile node shape — the inverse of
        :meth:`validate`/:meth:`validate_algebra`: successful bindings rather
        than violations.

        Parameters
        ----------
        data_graph:
            The RDF data to check.
        key_path:
            A SPARQL 1.1 property path expression (sequence ``/``,
            alternation ``|``, inverse ``^``, and the Kleene forms
            ``*``/``+``/``?`` are all supported), evaluated from each
            ``sh:property`` shape's own node, over the shapes graph, to
            produce a stable key. ``"zea:roleName"`` reaches a direct
            ``zea:roleName "outsideAirTemp"``-style annotation;
            ``"zea:role/zea:roleName"`` reaches one through an intermediate
            role-descriptor node; ``"^zea:describes/zea:roleName"`` reaches
            one where the descriptor points *at* the property shape instead.
            Prefixes resolve against the shapes document's declared
            ``@prefix``es. Property shapes where the path resolves to no
            value fall back to their own IRI/blank-node id as
            :attr:`PropertyWitness.key`.
        graph_mode, infer, on_unsupported:
            Same as :meth:`validate_algebra`.

        Returns
        -------
        list[PropertyWitness]
            One entry per ``sh:property`` shape reached from a target shape,
            per conforming focus node. Each has ``.focus``, ``.shape``,
            ``.key``, and ``.values`` (the deduped, rendered ``sh:path``
            bindings — narrowed to the ``sh:qualifiedValueShape`` matches
            when the property shape declares one).
        """
        data = _to_rdf_input(_coalesce_graph_input(data_graph))
        return self._inner.witnesses(
            data.data,
            data.path,
            data.format,
            key_path,
            graph_mode,
            infer,
            on_unsupported,
        )

    def __repr__(self) -> str:
        return repr(self._inner)


def _to_ntriples(graph: "Optional[GraphInputs]") -> str:
    """Serialize a subgraph (or a list/tuple of them, unioned) to N-Triples.

    Accepts an :class:`rdflib.Graph` or Turtle text/bytes, or a list/tuple of
    such inputs to be unioned at the RDF triple level. ``None`` and an empty
    sequence both serialize to the empty document (no triples).
    """
    if graph is None:
        return ""
    import rdflib

    def _parse_one(item: "GraphInput") -> rdflib.Graph:
        if isinstance(item, rdflib.Graph):
            return item
        if isinstance(item, (str, bytes)):
            g = rdflib.Graph()
            g.parse(data=item, format="turtle")
            return g
        raise TypeError(
            f"expected rdflib.Graph or Turtle text, got {type(item).__name__!r}"
        )

    if isinstance(graph, (list, tuple)):
        if not graph:
            return ""
        merged = rdflib.Graph()
        for item in graph:
            for triple in _parse_one(item):
                merged.add(triple)
        return merged.serialize(format="nt")

    if isinstance(graph, rdflib.Graph):
        return graph.serialize(format="nt")
    if isinstance(graph, (str, bytes)):
        g = rdflib.Graph()
        g.parse(data=graph, format="turtle")
        return g.serialize(format="nt")
    raise TypeError(
        f"expected rdflib.Graph or Turtle text, got {type(graph).__name__!r}"
    )


def delta_from_graph(
    add: "Optional[GraphInputs]" = None,
    delete: "Optional[GraphInputs]" = None,
) -> RepairDelta:
    """Build a :class:`RepairDelta` from hand-authored subgraph(s).

    Lets a driver propose a *subgraph* patch — e.g. a new node together with its
    type assertion and properties — instead of binding a single hole. Pass an
    :class:`rdflib.Graph` or Turtle text for the triples to ``add`` and/or
    ``delete``. The result gates and applies exactly like a synthesized delta, so
    :meth:`RepairSession.gate` still rejects a patch that doesn't make sound
    progress.

    ``add`` and ``delete`` each also accept a list (or tuple) of such inputs;
    the members are unioned at the RDF triple level before the delta is built.
    ``None`` and an empty sequence both mean "no triples" for that side.

    Application order is **deletes first, then adds** (``G ⊕ ΔG``), so a triple
    that appears in *both* sides is a net-add — the re-add wins. This holds
    whether the triple reaches both sides from a single input each or from the
    union of several, so when unioning multiple sources keep that resolution in
    mind: a triple you intend to *remove* must not also be re-asserted by one of
    the ``add`` sources (and vice versa, a triple you intend to *keep changed*
    should appear in ``delete`` then ``add`` — the standard replace pattern).
    """
    return RepairDelta.from_ntriples(_to_ntriples(add), _to_ntriples(delete))


class RepairSession:
    """Inspect and drive symbolic repair of a data graph.

    A session binds a shapes graph and a data graph (running SHACL-AF inference
    first, like :func:`validate`). It exposes the repair *primitives* so you can
    build your own driver: enumerate the violation horizon by focus node, inspect
    each violation's repair tree (its holes and decision points), enumerate
    candidate bindings, fold your own choices into a concrete delta, gate it, and
    apply it. **The library decides nothing** — every choice is yours.

    Typical loop::

        session = shifty.RepairSession(shapes, data)
        while True:
            ws = session.witnesses()
            if not ws:
                break                      # conforms
            fw = ws[0]                     # your focus-ordering policy
            tree = fw.repair_tree()
            plan = shifty.RepairPlan()
            for hole in tree.holes():
                plan.bind(hole.id, hole.candidates(limit=8)[0])   # your choice
            inst = tree.instantiate(plan)
            outcome = session.gate(inst.delta)
            if outcome.is_progress:
                session = session.advance(inst.delta)   # accept, re-witness
            else:
                break                       # reject; pick differently

    Parameters
    ----------
    shacl_graph:
        SHACL shapes graph (Turtle/N-Triples path, ``rdflib.Graph``, ``str``, or
        ``bytes``).
    data_graph:
        Data graph to repair. If ``None``, shapes are taken to embed the data
        (standard SHACL pattern), matching the CLI's ``repair`` with no
        ``--data``.
    infer:
        Run SHACL-AF rules before witnessing (default ``True``).
    base:
        Base IRI for resolving relative IRIs.
    """

    def __init__(
        self,
        shacl_graph: GraphInputs,
        data_graph: Optional[GraphInputs] = None,
        *,
        infer: bool = True,
        base: Optional[str] = None,
    ) -> None:
        shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph))
        data = (
            _to_rdf_input(_coalesce_graph_input(data_graph))
            if data_graph is not None
            else _RdfInput(None, None, "turtle")
        )
        self._inner = _RustRepairSession(
            shapes.data,
            shapes.path,
            shapes.format,
            data.data,
            data.path,
            data.format,
            infer,
            base,
        )

    @classmethod
    def _wrap(cls, inner: _RustRepairSession) -> "RepairSession":
        self = cls.__new__(cls)
        self._inner = inner
        return self

    @property
    def diagnostics(self) -> list[str]:
        """Warnings produced while lowering the shapes graph."""
        return self._inner.diagnostics

    def witnesses(self) -> list[FocusWitness]:
        """The violation horizon: one :class:`FocusWitness` per failing
        ``(focus node, statement)``. Empty ⟺ the graph conforms."""
        return self._inner.witnesses()

    def witnesses_for(self, shape_iri: str) -> list[FocusWitness]:
        """The violation horizon for a single shape: one :class:`FocusWitness`
        per failing ``(focus node, statement)`` whose statement targets
        ``shape_iri`` (matched against the schema's shape IRIs; angle brackets
        optional). The shape-scoped counterpart of :meth:`witnesses`; its
        satisfaction-side dual is :meth:`satisfactions_for`. Raises
        :class:`ValueError` if no shape is named ``shape_iri``."""
        return self._inner.witnesses_for(shape_iri)

    def satisfactions_for(self, shape_iri: str) -> list["FocusSatisfaction"]:
        """The satisfaction horizon for a single shape: one
        :class:`FocusSatisfaction` per *passing* ``(focus node, statement)``
        whose statement targets ``shape_iri`` — the dual of
        :meth:`witnesses_for`. Each entry records why the focus conforms,
        including the values matched along every checked path. Raises
        :class:`ValueError` if no shape is named ``shape_iri``."""
        return self._inner.satisfactions_for(shape_iri)

    def gate(self, delta: RepairDelta) -> RepairOutcome:
        """Re-validate ``G ⊕ ΔG`` and diff the violations against ``G`` — sound
        iff it introduces nothing. Decides and applies nothing.

        ``G ⊕ ΔG`` applies deletes first, then adds, so a triple in both sides
        of the delta is a net-add (the re-add wins)."""
        return self._inner.gate(delta)

    def apply(self, delta: RepairDelta) -> "rdflib.Graph":
        """Materialize ``G ⊕ ΔG`` as a fresh :class:`rdflib.Graph`.

        Deletes are applied first, then adds, so a triple present in both sides
        of the delta ends up in the result (net-add)."""
        import rdflib

        g = rdflib.Graph()
        g.parse(data=self._inner.apply_ntriples(delta), format="nt")
        return g

    def to_graph(self) -> "rdflib.Graph":
        """The session's current graph as an :class:`rdflib.Graph` — ``G`` with
        every accepted ``ΔG`` (via :meth:`advance`) already applied."""
        import rdflib

        g = rdflib.Graph()
        g.parse(data=self._inner.current_ntriples(), format="nt")
        return g

    def advance(self, delta: RepairDelta) -> "RepairSession":
        """A *new* session over ``G ⊕ ΔG`` (same schema, no re-inference) so you
        can accept a repair and re-witness from the patched graph."""
        return RepairSession._wrap(self._inner.advance(delta))

    def repair_node_against(self, node: str, shape_id: int) -> "Optional[RepairTree]":
        """Synthesize a tree that makes ``node`` conform to sub-shape ``shape_id``
        — the building block for repairing a ``conforms to`` hole (see
        :attr:`Hole.conforms_to` / :attr:`Hole.conforms_to_shapes`). Returns
        ``None`` if the node already conforms."""
        return self._inner.repair_node_against(node, shape_id)

    def describe_shape(self, shape_id: int) -> str:
        """A fully-expanded, human-readable definition of shape ``shape_id`` (the
        integer from :attr:`Hole.conforms_to` / :attr:`Hole.conforms_to_shapes`):
        every child shape inlined, no ``@id`` pointers. The lookup for
        understanding exactly what a ``conforms to`` hole demands."""
        return self._inner.describe_shape(shape_id)

    def __repr__(self) -> str:
        return repr(self._inner)


def validate(
    data_graph: GraphInputs,
    shacl_graph: Optional[GraphInputs] = None,
    *,
    graph_mode: str = "union",
    shape_names: Optional[Sequence[str]] = None,
    infer: bool = True,
    minimum_severity: str = "info",
    sort_results: bool = True,
    on_unsupported: str = "ignore",
    base: Optional[str] = None,
) -> "tuple[bool, rdflib.Graph, str]":
    """Validate *data_graph* against *shacl_graph* (pyshacl-compatible).

    Parameters
    ----------
    data_graph:
        The RDF data to validate. A list/tuple of inputs is unioned first.
    shacl_graph:
        The SHACL shapes graph.  If ``None``, shapes are expected to be
        embedded in *data_graph* (standard SHACL pattern). Passing an empty
        ``rdflib.Graph()`` means an explicit empty shapes graph. A list/tuple
        of inputs is unioned first.
    graph_mode:
        ``"union"`` (default), ``"data"``, or ``"union-all"``.
    shape_names:
        Optional list of named shape IRIs to use as top-level validation entry
        points. Referenced helper shapes are still evaluated normally. Bare
        IRIs and ``<iri>`` forms are both accepted.
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

    data = _to_rdf_input(_coalesce_graph_input(data_graph))
    shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph)) if shacl_graph is not None else _RdfInput(None, None, "turtle")
    result: W3cResult = _validate_w3c(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        graph_mode,
        list(shape_names) if shape_names is not None else None,
        infer,
        minimum_severity,
        sort_results,
        on_unsupported,
        base,
    )

    g = rdflib.Graph()
    g.parse(data=result.report_turtle, format="turtle")

    return (result.conforms, g, result.results_text)


def validate_algebra(
    data_graph: GraphInputs,
    shacl_graph: Optional[GraphInputs] = None,
    *,
    graph_mode: str = "union",
    shape_names: Optional[Sequence[str]] = None,
    infer: bool = True,
    minimum_severity: str = "info",
    sort_results: bool = True,
    on_unsupported: str = "ignore",
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
    data_graph, shacl_graph, graph_mode, shape_names, infer, base:
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
    data = _to_rdf_input(_coalesce_graph_input(data_graph))
    shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph)) if shacl_graph is not None else _RdfInput(None, None, "turtle")
    return _validate_algebra(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        graph_mode,
        list(shape_names) if shape_names is not None else None,
        infer,
        minimum_severity,
        sort_results,
        on_unsupported,
        base,
    )


def infer(
    data_graph: GraphInputs,
    shapes_graph: Optional[GraphInputs] = None,
    *,
    on_unsupported: str = "ignore",
    base: Optional[str] = None,
) -> InferResult:
    """Run SHACL-AF forward-chaining rules to a fixed point.

    Parameters
    ----------
    data_graph:
        Input data graph. A list/tuple of inputs is unioned first.
    shapes_graph:
        Shapes graph containing ``sh:rule`` definitions.  If ``None``,
        rules are expected inside *data_graph*. Passing an empty
        ``rdflib.Graph()`` means an explicit empty rules graph.
    base:
        Base IRI for resolving relative IRIs.

    Returns
    -------
    InferResult
        Call ``.graph()`` to get the result as an :class:`rdflib.Graph`,
        or read ``.graph_ntriples`` for the raw N-Triples string.
    """
    data = _to_rdf_input(_coalesce_graph_input(data_graph))
    shapes = _to_rdf_input(_coalesce_graph_input(shapes_graph)) if shapes_graph is not None else _RdfInput(None, None, "turtle")
    inner = _infer(
        data.data,
        data.path,
        data.format,
        shapes.data,
        shapes.path,
        shapes.format,
        on_unsupported,
        base,
    )
    return InferResult(inner)
