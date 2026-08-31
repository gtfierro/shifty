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

``EvidenceSession(shacl_graph, data_graph).validate()``
    Returns complete selected-pair coverage: exactly one structured satisfaction
    trace or failure witness for each selected ``(statement, focus)`` pair.
    Three cheaper entry points share the same prepared snapshot:
    ``validate_conformance()`` for counts only, ``find_failures()`` for counts
    plus the pairs that failed, and ``explain(pair)`` for evidence about one of
    them. ``revalidate(delta)`` answers ``validate()`` for a proposed edit.

``shape_map(data_graph, shacl_graph=None, ...)``
    One level above the evidence trees: a ShEx-shapemap-style view with one
    :class:`Mapping` per selected ``(shape, focus)`` pair, each a typed
    :class:`Key` -> :class:`Binding` record of the shape's property
    obligations — bound keys carry the matched values as typed :class:`Term`\\
    s (including on partially-conforming foci), unbound keys carry the
    witness subtree, shortfall count, and near-misses. Pass ``name_path`` to
    carry the author's name for each slot, and ``value_paths`` to annotate
    each bound value from the data graph. See :mod:`shifty.shapemap` and
    :mod:`shifty.terms`.

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

* :class:`rdflib.Graph`       — serialized to Turtle, preserving namespace
                                bindings used by SHACL-SPARQL queries and rules
* :class:`pathlib.Path`       — parsed directly as Turtle or N-Triples
* ``str``                     — treated as an existing file path, an HTTP(S)
                                URL, or raw Turtle text. A missing recognized
                                RDF filename raises :class:`FileNotFoundError`;
                                a directory raises :class:`IsADirectoryError`.
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
data and shapes are the same embedded graph. An explicitly empty shapes graph
raises :class:`ValueError`; it is not embedded-shapes mode. ``infer`` does not
accept ``graph_mode``.

``shape_names`` applies to ``validate`` and ``validate_algebra``. It limits
validation to selected named shapes as top-level entry points while still
evaluating referenced helper shapes normally.
"""

from __future__ import annotations

import pathlib
import urllib.error
import urllib.parse
import urllib.request
import warnings
from typing import TYPE_CHECKING, NamedTuple, Optional, Sequence, Union

from ._shifty import (
    AlgebraResult,
    Choice,
    ChoiceKind,
    Constraint,
    ConstraintKind,
    EvidenceSession as _RustEvidenceSession,
    ChildEvaluation,
    EvaluationProgress,
    EvidenceNode,
    EvidenceKind,
    EvidenceRun,
    Failure,
    FocusEvaluation,
    Satisfaction,
    StatementEvaluation,
    Hole,
    SatAtom,
    SatKind,
    InferResult as _RustInferResult,
    Instantiated,
    ConformanceRun,
    MissingObligation,
    SelectedPair,
    PathSupport,
    PreparedValidator as _RustPreparedValidator,
    PropertyWitness,
    Reason,
    RepairDelta,
    RepairOutcome,
    RepairOrigin,
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
    expand_evidence_json as _expand_evidence_json,
    version,
)

from .shapemap import (
    Alt,
    Binding,
    BoundValue,
    Cls,
    Const,
    Datatype,
    Id,
    Inv,
    Key,
    Mapping,
    Pred,
    Seq,
    ShapeMap,
    ShapeRef,
    Star,
    shape_map,
)
from .terms import BNode, Iri, Literal, Term

if TYPE_CHECKING:
    import rdflib

    FocusWitness = Failure
    FocusSatisfaction = Satisfaction

__all__ = [
    "validate",
    "validate_algebra",
    "infer",
    "expand_evidence",
    "shape_map",
    "ShapeMap",
    "Mapping",
    "Binding",
    "BoundValue",
    "Key",
    "Id",
    "Pred",
    "Inv",
    "Seq",
    "Alt",
    "Star",
    "Cls",
    "Const",
    "Datatype",
    "ShapeRef",
    "Term",
    "Iri",
    "Literal",
    "BNode",
    "version",
    "__version__",
    "AlgebraResult",
    "Violation",
    "Reason",
    "Constraint",
    "ConstraintKind",
    "InferResult",
    "PreparedValidator",
    "EvidenceSession",
    "EvidenceRun",
    "StatementEvaluation",
    "FocusEvaluation",
    "EvaluationProgress",
    "ChildEvaluation",
    "EvidenceNode",
    "EvidenceKind",
    "ConformanceRun",
    "MissingObligation",
    "SelectedPair",
    "PathSupport",
    "PropertyWitness",
    # ── symbolic repair ──
    "RepairSession",
    "RepairPlan",
    "Failure",
    "Satisfaction",
    "FocusWitness",
    "FocusSatisfaction",
    "Target",
    "TargetKind",
    "WitnessAtom",
    "WitnessKind",
    "SatAtom",
    "SatKind",
    "RepairTree",
    "RepairOrigin",
    "Hole",
    "Choice",
    "ChoiceKind",
    "Instantiated",
    "RepairDelta",
    "RepairOutcome",
    "delta_from_graph",
]

_DEPRECATED_TYPE_ALIASES = {
    "FocusWitness": (Failure, "Failure"),
    "FocusSatisfaction": (Satisfaction, "Satisfaction"),
}
_WARNED_DEPRECATED_TYPE_ALIASES: set[str] = set()


def __getattr__(name: str):
    alias = _DEPRECATED_TYPE_ALIASES.get(name)
    if alias is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value, replacement = alias
    if name not in _WARNED_DEPRECATED_TYPE_ALIASES:
        warnings.warn(
            f"shifty.{name} is deprecated; use shifty.{replacement}",
            DeprecationWarning,
            stacklevel=2,
        )
        _WARNED_DEPRECATED_TYPE_ALIASES.add(name)
    return value


def __dir__() -> list[str]:
    return sorted([*globals(), *_DEPRECATED_TYPE_ALIASES])

GraphInput = Union[str, bytes, pathlib.Path, "rdflib.Graph"]
# Any single `GraphInput`, or a list/tuple of them to be unioned (merged at
# the RDF triple level) before being passed to the engine.
GraphInputs = Union[GraphInput, list[GraphInput], tuple[GraphInput, ...]]


class _RdfInput(NamedTuple):
    data: Optional[bytes]
    path: Optional[str]
    format: str


# Strings are intentionally dual-purpose: an existing path is read from disk;
# everything else is Turtle text. Only these suffixes make a missing string
# unambiguously look like an RDF filename rather than inline RDF.
_RDF_FILE_SUFFIXES = frozenset(
    {".ttl", ".nt", ".ntriples", ".n3", ".rdf", ".xml", ".jsonld", ".trig"}
)
_MAX_PATH_LENGTH = 4096


def _path_format(path: pathlib.Path) -> str:
    return "nt" if path.suffix.lower() in {".nt", ".ntriples"} else "turtle"


def _url_format(url: str, content_type: str = "") -> str:
    suffix = pathlib.PurePosixPath(urllib.parse.urlparse(url).path).suffix.lower()
    if suffix in {".nt", ".ntriples"}:
        return "nt"
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type == "application/n-triples":
        return "nt"
    return "turtle"


def _is_http_url(value: str) -> bool:
    parsed = urllib.parse.urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _might_be_path(value: str) -> bool:
    """Whether *value* is nonempty, short, single-line text worth probing as a path."""
    return bool(value) and "\n" not in value and len(value) < _MAX_PATH_LENGTH


def _fetch_url(url: str) -> _RdfInput:
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read()
            content_type = response.headers.get("Content-Type", "")
            final_url = response.geturl()
    except urllib.error.URLError as error:
        raise OSError(f"could not fetch RDF URL {url!r}: {error}") from error
    return _RdfInput(data, None, _url_format(final_url or url, content_type))


def _to_rdf_input(graph: GraphInput) -> _RdfInput:
    """Convert one public graph input into the native binding's descriptor.

    Strings have one deliberate policy: HTTP(S) URLs are fetched, short
    single-line strings may name local files, and all other strings are Turtle.
    Missing RDF-looking filenames and directories report filesystem errors;
    long or multiline Turtle is never probed as a path.
    """
    if isinstance(graph, bytes):
        return _RdfInput(graph, None, "turtle")
    if isinstance(graph, pathlib.Path):
        if not graph.exists():
            raise FileNotFoundError(graph)
        if not graph.is_file():
            raise IsADirectoryError(graph)
        return _RdfInput(None, str(graph), _path_format(graph))
    if isinstance(graph, str):
        if _is_http_url(graph):
            return _fetch_url(graph)
        if _might_be_path(graph):
            path = pathlib.Path(graph)
            try:
                is_file = path.is_file()
            except OSError:
                is_file = False
            if is_file:
                return _RdfInput(None, str(path), _path_format(path))
            if path.is_dir():
                raise IsADirectoryError(graph)
            if path.suffix.lower() in _RDF_FILE_SUFFIXES:
                raise FileNotFoundError(graph)
        return _RdfInput(graph.encode("utf-8"), None, "turtle")
    serialize = getattr(graph, "serialize", None)
    if serialize is not None:
        # N-Triples has no prefix declarations. Those declarations are part
        # of a shapes graph's meaning when its SHACL-SPARQL queries or rules
        # use prefixed names, so preserve rdflib's namespace manager in Turtle.
        result = serialize(format="turtle", encoding="utf-8")
        if isinstance(result, str):
            result = result.encode("utf-8")
        if isinstance(result, bytes):
            return _RdfInput(result, None, "turtle")
    raise TypeError(
        f"Cannot convert {type(graph).__name__!r} to RDF data. "
        "Expected rdflib.Graph, pathlib.Path, str (path, HTTP(S) URL, or Turtle), "
        "or bytes."
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
    :class:`rdflib.Graph` is copied rather than mutated; every other input is
    first classified by :func:`_to_rdf_input`, keeping list members consistent
    with a single graph argument."""
    import rdflib

    if isinstance(graph, rdflib.Graph):
        merged = rdflib.Graph()
        for prefix, namespace in graph.namespaces():
            merged.bind(prefix, namespace)
        for triple in graph:
            merged.add(triple)
        return merged
    source = _to_rdf_input(graph)
    g = rdflib.Graph()
    if source.path is not None:
        g.parse(source=source.path, format=source.format)
    else:
        assert source.data is not None
        g.parse(data=source.data, format=source.format)
    return g


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
            item_graph = _as_rdflib_graph(item)
            for prefix, namespace in item_graph.namespaces():
                merged.bind(prefix, namespace)
            for triple in item_graph:
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
        """Non-fatal lowering warnings and unsupported rule features.

        Invalid shapes diagnostics raise during construction instead of
        producing an inference result.
        """
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
        """Non-fatal diagnostics produced while lowering the shapes graph.

        Invalid shapes diagnostics raise while preparing the validator.
        """
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


def expand_evidence(
    compact: Union[str, dict],
    catalog: Optional[Union[str, dict, list]] = None,
    *,
    as_dict: bool = True,
) -> Union[str, dict]:
    """Restore a run compacted by :meth:`EvidenceRun.to_compact_json`.

    The compact encoding stores each distinct evidence node and RDF term once
    and refers to them by index; this puts the tree back exactly as
    :meth:`EvidenceRun.to_dict` would have produced it.

    Parameters
    ----------
    compact:
        The compact encoding, as JSON text or an already-parsed ``dict``.
    catalog:
        The constraint catalog, required only when the encoding was written
        with ``include_catalog=False``. This is the ``"constraints"`` value of
        the original run.
    as_dict:
        Return a ``dict`` (the default) or JSON text.

    Examples
    --------
    Send evidence across a process boundary without the catalog, which the
    receiver already has from the schema::

        run = shifty.EvidenceSession(shapes, data).validate()
        wire = run.to_compact_json(include_catalog=False)
        catalog = run.to_dict()["constraints"]
        restored = shifty.expand_evidence(wire, catalog)
        assert restored == run.to_dict()
    """
    import json as _json

    compact_text = compact if isinstance(compact, str) else _json.dumps(compact)
    catalog_text = (
        catalog
        if catalog is None or isinstance(catalog, str)
        else _json.dumps(catalog)
    )
    expanded = _expand_evidence_json(compact_text, catalog_text)
    return _json.loads(expanded) if as_dict else expanded


class EvidenceSession:
    """Prepared evidence validation over one immutable shapes/data snapshot.

    Parsing, lowering, optional inference, and dataset indexing happen in the
    constructor. :meth:`validate` returns every authored statement, including
    empty selections, with one tagged pass/fail object per selected focus, and
    is cheap to repeat because the snapshot is fixed.

    :meth:`revalidate` answers the same question for ``G ⊕ ΔG`` — the graph with
    a proposed edit applied — without disturbing this session's snapshot. It
    re-prepares, so it is not as cheap as a repeated :meth:`validate`.
    """

    def __init__(
        self,
        shacl_graph: GraphInputs,
        data_graph: Optional[GraphInputs] = None,
        *,
        infer: bool = True,
        graph_mode: str = "union",
        base: Optional[str] = None,
    ) -> None:
        shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph))
        data = (
            _to_rdf_input(_coalesce_graph_input(data_graph))
            if data_graph is not None
            else _RdfInput(None, None, "turtle")
        )
        self._inner = _RustEvidenceSession(
            shapes.data,
            shapes.path,
            shapes.format,
            data.data,
            data.path,
            data.format,
            infer,
            graph_mode,
            base,
        )

    @property
    def diagnostics(self) -> list[str]:
        """Non-fatal lowering warnings and unsupported features.

        Invalid shapes diagnostics raise while constructing the session.
        """
        return self._inner.diagnostics

    def validate(
        self,
        *,
        shape_names: Optional[Sequence[str]] = None,
        minimum_severity: str = "info",
        sort_results: bool = True,
    ) -> EvidenceRun:
        """Return the complete evidence coverage horizon for this snapshot."""
        return self._inner.validate(
            list(shape_names) if shape_names is not None else None,
            minimum_severity,
            sort_results,
        )

    def revalidate(
        self,
        delta: RepairDelta,
        *,
        infer: Optional[bool] = None,
        shape_names: Optional[Sequence[str]] = None,
        minimum_severity: str = "info",
        sort_results: bool = True,
    ) -> EvidenceRun:
        """Return the run :meth:`validate` would produce over ``G ⊕ ΔG`` — this
        session's graph with ``delta`` applied.

        Pure: the session keeps its own snapshot, so a run taken before the edit
        stays valid and comparable. Unlike :meth:`validate` this cannot reuse the
        prepared snapshot — a patched graph needs its own normalization,
        indexing, and SPARQL preparation — though it still skips file I/O,
        parsing, and schema lowering.

        ``infer`` re-runs SHACL-AF rules over the patched graph, so an added
        triple can fire a rule and a deleted one stops supporting what it
        derived. It defaults to whatever the session was built with, keeping the
        before and after runs on the same baseline. Pass ``False`` to patch the
        already-inferred graph and leave the rules alone — cheaper, and sound
        only if the edit fires none of them.
        """
        return self._inner.revalidate(
            delta,
            infer,
            list(shape_names) if shape_names is not None else None,
            minimum_severity,
            sort_results,
        )

    def validate_conformance(
        self, *, shape_names: Optional[Sequence[str]] = None
    ) -> ConformanceRun:
        """Decide every selected pair without materializing evidence.

        The cheapest of the four entry points. ``minimum_severity`` does not
        apply: with no failure evidence there is no per-constraint severity to
        weigh, so any failing pair makes ``conforms`` false.
        """
        return self._inner.validate_conformance(
            list(shape_names) if shape_names is not None else None
        )

    def find_failures(
        self, *, shape_names: Optional[Sequence[str]] = None
    ) -> tuple[ConformanceRun, list[SelectedPair]]:
        """The same pass as :meth:`validate_conformance`, plus a
        :class:`SelectedPair` handle for each pair that failed.

        This followed by :meth:`explain` on the pairs you care about is far
        cheaper than :meth:`validate` when failures are a small share of
        selected pairs, which is the usual case.
        """
        return self._inner.find_failures(
            list(shape_names) if shape_names is not None else None
        )

    def explain(self, pair: SelectedPair) -> EvidenceRun:
        """Materialize evidence for one ``pair``, as a run holding just that
        pair — every projection works on the result.

        Target selection is *not* re-run; ``pair`` is taken as already selected.
        Pairs should come from :meth:`find_failures` or an earlier run over this
        snapshot.

        The returned run carries **no constraint catalog** — it is fixed per
        snapshot, so take it once from :meth:`constraints`. That affects only
        serialization; the ``constraint`` objects on statements and evidence are
        present either way.
        """
        return self._inner.explain(pair)

    def explain_canonical(self, pair: SelectedPair) -> EvidenceRun:
        """:meth:`explain` without the authored-statement progress view."""
        return self._inner.explain_canonical(pair)

    def constraints(self) -> dict:
        """The source and normalized constraint catalogs for this snapshot.

        Fixed for the snapshot, so a caller explaining pairs one at a time takes
        this once rather than paying for it per pair. It is also the ``catalog``
        argument of :func:`expand_evidence`, which is what makes
        ``to_compact_json(include_catalog=False)`` usable — the catalog travels
        once, out of band.
        """
        return self._inner.constraints()

    def evidence_for(self, focus: str, constraint_id: int) -> EvidenceNode:
        """Evidence for *focus* against one *normalized* constraint id — any
        constraint in the run's catalog, not just a statement's top shape.

        A failing conjunction's failure evidence carries only the failing
        children; the run's ``EvaluationProgress`` says which children passed
        without materializing why. This is the drill-down for those elided
        passes: pass the focus (N-Triples syntax, as
        ``FocusEvaluation.focus`` renders it) and a child's
        ``normalized_constraint_ref``, and get back the corresponding typed
        :class:`EvidenceNode`. Its ``status`` and ``evidence_kind`` identify
        the result; ``to_dict()`` and ``to_json()`` provide serialized forms.
        No target selection is involved: the pair is taken as given, and a
        focus no statement selects still yields well-defined evidence.
        """
        return self._inner._evidence_for(focus, constraint_id)


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

    def satisfactions_for(self, shape_iri: str) -> list["Satisfaction"]:
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
        embedded in *data_graph* (standard SHACL pattern). An explicitly empty
        graph raises :class:`ValueError`. A list/tuple of inputs is unioned
        first.
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
