"""Symbolic repair.

**Experimental**: this module can change in any release, without a
deprecation period.

A :class:`RepairSession` computes the space of edits that would make a failing
node conform, and applies the ones you choose. It decides nothing itself::

    from shifty.experimental import repair

    session = repair.RepairSession(shapes, data)
    failure = session.witnesses()[0]
    tree = failure.repair_tree()

Failures and edits stay in ``shifty``, because the stable evidence API uses
them too: ``shifty.Failure``, ``shifty.Satisfaction`` and ``shifty.RepairDelta``.
This module re-exports them so a repair driver needs one import.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .. import (
    Failure,
    GraphInputs,
    RepairDelta,
    SatAtom,
    Satisfaction,
    SatKind,
    Target,
    TargetKind,
    WitnessAtom,
    WitnessKind,
    _coalesce_graph_input,
    _RdfInput,
    _to_rdf_input,
    delta_from_graph,
)
from .._shifty import (
    Choice,
    ChoiceKind,
    Hole,
    Instantiated,
    RepairOrigin,
    RepairOutcome,
    RepairPlan,
    RepairTree,
)
from .._shifty import (
    RepairSession as _RustRepairSession,
)

if TYPE_CHECKING:
    import rdflib

__all__ = [
    "RepairSession",
    "RepairPlan",
    "RepairTree",
    "RepairOrigin",
    "RepairOutcome",
    "Failure",
    "Satisfaction",
    "Target",
    "TargetKind",
    "WitnessAtom",
    "WitnessKind",
    "SatAtom",
    "SatKind",
    "Hole",
    "Choice",
    "ChoiceKind",
    "Instantiated",
    "RepairDelta",
    "delta_from_graph",
]


class RepairSession:
    """Inspect and drive symbolic repair of a data graph.

    A session binds a shapes graph and a data graph (running SHACL-AF inference
    first, like :func:`validate`). It exposes the repair *primitives* so you can
    build your own driver: enumerate the violation horizon by focus node, inspect
    each violation's repair tree (its holes and decision points), enumerate
    candidate bindings, fold your own choices into a concrete delta, gate it, and
    apply it. **The library decides nothing** — every choice is yours.

    Typical loop::

        session = repair.RepairSession(shapes, data)
        while True:
            ws = session.witnesses()
            if not ws:
                break                      # conforms
            fw = ws[0]                     # your focus-ordering policy
            tree = fw.repair_tree()
            plan = repair.RepairPlan()
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
    def _wrap(cls, inner: _RustRepairSession) -> RepairSession:
        self = cls.__new__(cls)
        self._inner = inner
        return self

    @property
    def diagnostics(self) -> list[str]:
        """Non-fatal lowering warnings and unsupported features.

        Invalid shapes diagnostics raise while constructing the session.
        """
        return self._inner.diagnostics

    def witnesses(self) -> list[Failure]:
        """The violation horizon: one :class:`Failure` per failing
        ``(focus node, statement)``. Empty ⟺ the graph conforms."""
        return self._inner.witnesses()

    def witnesses_for(self, shape_iri: str) -> list[Failure]:
        """The violation horizon for a single shape: one :class:`Failure`
        per failing ``(focus node, statement)`` whose statement targets
        ``shape_iri`` (matched against the schema's shape IRIs; angle brackets
        optional). The shape-scoped counterpart of :meth:`witnesses`; its
        satisfaction-side dual is :meth:`satisfactions_for`. Raises
        :class:`ValueError` if no shape is named ``shape_iri``."""
        return self._inner.witnesses_for(shape_iri)

    def satisfactions_for(self, shape_iri: str) -> list[Satisfaction]:
        """The satisfaction horizon for a single shape: one
        :class:`Satisfaction` per *passing* ``(focus node, statement)``
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

    def apply(self, delta: RepairDelta) -> rdflib.Graph:
        """Materialize ``G ⊕ ΔG`` as a fresh :class:`rdflib.Graph`.

        Deletes are applied first, then adds, so a triple present in both sides
        of the delta ends up in the result (net-add)."""
        import rdflib

        g = rdflib.Graph()
        g.parse(data=self._inner.apply_ntriples(delta), format="nt")
        return g

    def to_graph(self) -> rdflib.Graph:
        """The session's current graph as an :class:`rdflib.Graph` — ``G`` with
        every accepted ``ΔG`` (via :meth:`advance`) already applied."""
        import rdflib

        g = rdflib.Graph()
        g.parse(data=self._inner.current_ntriples(), format="nt")
        return g

    def advance(self, delta: RepairDelta) -> RepairSession:
        """A *new* session over ``G ⊕ ΔG`` (same schema, no re-inference) so you
        can accept a repair and re-witness from the patched graph."""
        return RepairSession._wrap(self._inner.advance(delta))

    def repair_node_against(self, node: str, shape_id: int) -> Optional[RepairTree]:
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

    def describe_shape_pretty(self, shape_id: int, width: int = 100) -> str:
        """:meth:`describe_shape`, laid out over several lines and indented by
        nesting depth, breaking only where a subtree does not fit in ``width``.
        A description that already fits comes back unchanged, so this is safe to
        call unconditionally."""
        return self._inner.describe_shape_pretty(shape_id, width)

    def __repr__(self) -> str:
        return repr(self._inner)
