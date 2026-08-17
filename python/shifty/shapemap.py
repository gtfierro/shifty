"""Shape maps: typed key -> value bindings, one level above the evidence trees.

An :class:`~shifty.EvidenceSession` run answers "why did this (statement,
focus) pair pass or fail" with a full evidence tree. Most callers building
repair drivers or application configuration want a flatter view first: for
each shape, for each selected focus node, *which property obligations bound to
which values, and which are still unbound* — the same move ShEx shape maps
make from proof trees to (node, shape, status) rows, extended with the
key -> value bindings and with a path back down into the evidence for anything
unbound.

::

    smap = shifty.shape_map(shapes, data, name_path="sh:name",
                             value_paths={"ts": "ref:hasTimeseriesId"})
    smap.shape_names
    for mapping in smap["urn:zonepac-app/zonepac-zone"]:
        mapping.focus, mapping.conforms
        for key, binding in mapping.successful:
            binding.values             # the values the data supplied
            binding.name                # the author's name for the slot, if any
            binding.annotated_values    # each value plus its value_paths annotations
        for key, binding in mapping.unsuccessful:
            binding.missing             # how many values are still owed
            binding.partial_values      # near-misses that did qualify
            binding.evidence            # the witness subtree for this key alone
            mapping.evaluation.failure.explain()   # full drill-down

Partial coverage is exact: for a focus that fails its shape, the bindings for
the property shapes it *does* satisfy are materialized on demand through
``EvidenceSession.evidence_for`` (a failing conjunction's witness only carries
the failing children), so a partially-conforming node still yields every value
it can already supply.

Keys (:class:`Key`) are typed and hashable: a compact rendering of the
property shape's path plus its qualifier class when one is declared
(``str(key)`` reads ``hasPoint->Supply_Air_Flow_Sensor``), disambiguated by
``ordinal`` when several bindings share a ``(path, qualifier)`` pair. Keys are
constant per shape (shapes-graph side); pass ``name_path`` to also carry the
*author's* name for the slot (evaluated from the property shape's own node
over the shapes graph) and ``value_paths`` to annotate each bound *value*
(evaluated from the value node over the data graph) — the two are independent
features and are not conflated.
"""

from __future__ import annotations

import collections.abc
import dataclasses
from typing import TYPE_CHECKING, Callable, Iterator, Optional, Sequence, Union

from .terms import BNode, Iri, Literal, Term
from .terms import from_json as _term_from_json
from .terms import parse as _term_parse

if TYPE_CHECKING:  # pragma: no cover
    from . import EvidenceRun, EvidenceSession

__all__ = [
    "Id",
    "Pred",
    "Inv",
    "Seq",
    "Alt",
    "Star",
    "Path",
    "Cls",
    "Const",
    "Datatype",
    "ShapeRef",
    "Qualifier",
    "Key",
    "BoundValue",
    "Binding",
    "Mapping",
    "ShapeMap",
    "shape_map",
]

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_RDFS_SUBCLASS = "http://www.w3.org/2000/01/rdf-schema#subClassOf"


def _local(iri: str) -> str:
    """The local name of an IRI: the segment after the last '#', '/', or ':'."""
    for sep in ("#", "/", ":"):
        head, found, tail = iri.rpartition(sep)
        if found and tail:
            return tail
    return iri


# ── paths (mirror shifty_algebra::Path) ─────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class Id:
    pass


@dataclasses.dataclass(frozen=True)
class Pred:
    __match_args__ = ("iri",)
    iri: str


@dataclasses.dataclass(frozen=True)
class Inv:
    __match_args__ = ("path",)
    path: "Path"


@dataclasses.dataclass(frozen=True)
class Seq:
    __match_args__ = ("parts",)
    parts: "tuple[Path, ...]"


@dataclasses.dataclass(frozen=True)
class Alt:
    __match_args__ = ("parts",)
    parts: "tuple[Path, ...]"


@dataclasses.dataclass(frozen=True)
class Star:
    __match_args__ = ("path",)
    path: "Path"


Path = Union[Id, Pred, Inv, Seq, Alt, Star]


def _path_from_json(path) -> Optional[Path]:
    """The externally-tagged serde encoding of `shifty_algebra::Path` -> `Path`."""
    if path is None:
        return None
    if path == "Id":
        return Id()
    if not isinstance(path, dict):
        raise ValueError(f"cannot parse path: {path!r}")
    ((tag, body),) = path.items()
    if tag == "Pred":
        return Pred(body["value"])
    if tag == "Inverse":
        return Inv(_path_from_json(body))
    if tag == "Seq":
        return Seq(tuple(_path_from_json(p) for p in body))
    if tag == "Alt":
        return Alt(tuple(_path_from_json(p) for p in body))
    if tag == "Star":
        return Star(_path_from_json(body))
    raise ValueError(f"unrecognized path tag: {tag!r}")


def _is_class_path(path: Optional[Path]) -> bool:
    """True for the `rdf:type/rdfs:subClassOf*` shape of a class-membership path."""
    return (
        isinstance(path, Seq)
        and len(path.parts) == 2
        and path.parts[0] == Pred(_RDF_TYPE)
        and isinstance(path.parts[1], Star)
        and path.parts[1].path == Pred(_RDFS_SUBCLASS)
    )


def _path_str(path: Path, compact: bool = True) -> str:
    if isinstance(path, Id):
        return "id"
    if isinstance(path, Pred):
        return _local(path.iri) if compact else f"<{path.iri}>"
    if isinstance(path, Inv):
        return f"^{_path_str(path.path, compact)}"
    if isinstance(path, Star):
        return f"{_path_str(path.path, compact)}*"
    if isinstance(path, Seq):
        # `rdf:type/rdfs:subClassOf*` is class membership; render it like Turtle.
        if _is_class_path(path):
            return "a"
        return "/".join(_path_str(p, compact) for p in path.parts)
    if isinstance(path, Alt):
        return "|".join(_path_str(p, compact) for p in path.parts)
    raise TypeError(f"not a Path: {path!r}")


# ── qualifiers ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class Cls:
    __match_args__ = ("iri",)
    iri: str  # sh:class C / class-membership


@dataclasses.dataclass(frozen=True)
class Const:
    __match_args__ = ("term",)
    term: Term  # sh:hasValue / TestConst


@dataclasses.dataclass(frozen=True)
class Datatype:
    __match_args__ = ("iri",)
    iri: str  # sh:datatype (TestType)


@dataclasses.dataclass(frozen=True)
class ShapeRef:
    __match_args__ = ("iri",)
    iri: str  # sh:node <named shape>


Qualifier = Union[Cls, Const, Datatype, ShapeRef]


def _qualifier_local(q: Qualifier) -> str:
    if isinstance(q, (Cls, Datatype, ShapeRef)):
        return _local(q.iri)
    if isinstance(q, Const):
        return _local(q.term.value) if isinstance(q.term, Iri) else str(q.term)
    return "?"


# ── typed keys ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class Key:
    __match_args__ = ("path", "qualifier")
    path: Optional[Path]  # None for pathless constraints (nodeKind…)
    qualifier: Optional[Qualifier]
    ordinal: int = 1  # disambiguates identical (path, qualifier); n-th in lowering order
    kind: str = "count"  # constraint tag fallback for pathless keys

    def __str__(self) -> str:
        return _key_str(self)


def _key_str(key: Key) -> str:
    if key.path is not None:
        base = _path_str(key.path, compact=True)
        if key.qualifier is not None:
            base = f"{base}→{_qualifier_local(key.qualifier)}"
    else:
        base = key.kind
    if key.ordinal > 1:
        base = f"{base}#{key.ordinal}"
    return base


def _to_python_value(term: Term):
    if isinstance(term, Literal):
        return term.to_python()
    if isinstance(term, Iri):
        return term.value
    if isinstance(term, BNode):
        return term.id
    return term


# ── source constraint catalog ───────────────────────────────────────────────────


class _Catalog:
    """One side (source or normalized) of a run's constraint catalog."""

    def __init__(self, records: list[dict]) -> None:
        self._by_id = {record["id"]: record["constraint"] for record in records}

    def get(self, constraint_id: Optional[int]):
        return self._by_id.get(constraint_id)

    def unwrap(self, constraint_id: Optional[int]) -> Optional[int]:
        """Follow `Annotated` wrappers down to the logical constraint id."""
        seen = set()
        while constraint_id is not None and constraint_id not in seen:
            seen.add(constraint_id)
            constraint = self._by_id.get(constraint_id)
            if isinstance(constraint, dict) and "Annotated" in constraint:
                constraint_id = constraint["Annotated"]["shape"]
            else:
                break
        return constraint_id

    def logical(self, constraint_id: Optional[int]):
        return self.get(self.unwrap(constraint_id))

    def unwrap_checking_names(
        self,
        shape_name_of: "Optional[Callable[[int], Optional[str]]]",
        constraint_id: Optional[int],
    ):
        """Like :meth:`unwrap`, but checks `shape_name_of` at *every* wrapper
        along the way, not just the outermost. A blank `sh:qualifiedValueShape`
        whose sole content is `sh:node <named>` (the common ZonePAC-style
        ``heating-coil`` pattern) doubly-wraps: the blank node's own
        `Annotated` wraps the *named* shape's `Annotated` directly, so a plain
        outermost-only check would unwrap straight past the name. Returns
        ``(name_or_None, final_unwrapped_id)``.
        """
        seen = set()
        cid = constraint_id
        while cid is not None and cid not in seen:
            seen.add(cid)
            if shape_name_of is not None:
                name = shape_name_of(cid)
                if name is not None:
                    return name, cid
            constraint = self._by_id.get(cid)
            if isinstance(constraint, dict) and "Annotated" in constraint:
                cid = constraint["Annotated"]["shape"]
            else:
                break
        return None, cid


def _kind_tag(constraint) -> str:
    if isinstance(constraint, dict):
        return next(iter(constraint)).lower()
    return str(constraint).lower()


def _qualifier_from_json(
    catalog: _Catalog,
    shape_name_of: "Optional[Callable[[int], Optional[str]]]",
    qualifier_id: Optional[int],
) -> Optional[Qualifier]:
    """The `Qualifier` a count qualifier demands, when one is evident.

    Handles the common encodings: `sh:class` (class-membership count over a
    `TestConst`), `sh:hasValue`/`TestConst`, `sh:datatype`/`TestType`, a `Not`
    from the ∀-encoding, and a conjunction whose first labeled child wins.
    When `shape_name_of` resolves the qualifier's own (possibly `Not`-wrapped)
    id to a named shape, that name wins as a `ShapeRef` — a `sh:node
    <named-shape>` reference names itself, regardless of what it expands to.
    """
    if qualifier_id is None:
        return None
    body = catalog.get(qualifier_id)
    lookup_id = qualifier_id
    # `∀π.φ ≡ ∃≤0 π.¬φ`: the qualifier label may sit under a `Not`.
    if isinstance(body, dict) and "Not" in body:
        lookup_id = body["Not"]
    name, unwrapped = catalog.unwrap_checking_names(shape_name_of, lookup_id)
    if name is not None:
        return ShapeRef(name)
    constraint = catalog.get(unwrapped)
    if not isinstance(constraint, dict):
        return None
    if "TestConst" in constraint:
        return Const(_term_from_json(constraint["TestConst"]))
    if "TestType" in constraint:
        value_type = constraint["TestType"]
        if isinstance(value_type, dict) and "Datatype" in value_type:
            return Datatype(value_type["Datatype"]["value"])
        return None
    if "Count" in constraint:
        count = constraint["Count"]
        if _is_class_path(_path_from_json(count.get("path"))):
            inner = _qualifier_from_json(catalog, shape_name_of, count.get("qualifier"))
            if isinstance(inner, Const) and isinstance(inner.term, Iri):
                return Cls(inner.term.value)
            return inner
        return None
    if "And" in constraint or "Or" in constraint:
        for child in constraint.get("And", constraint.get("Or", [])):
            found = _qualifier_from_json(catalog, shape_name_of, child)
            if found is not None:
                return found
    return None


@dataclasses.dataclass(frozen=True)
class _KeyInfo:
    path: Optional[Path]
    qualifier: Optional[Qualifier]
    kind: str


def _derive_key_info(
    catalog: _Catalog,
    shape_name_of: "Optional[Callable[[int], Optional[str]]]",
    source_id: int,
) -> _KeyInfo:
    constraint = catalog.logical(source_id)
    if isinstance(constraint, dict) and "Count" in constraint:
        count = constraint["Count"]
        path = _path_from_json(count.get("path"))
        qualifier = _qualifier_from_json(catalog, shape_name_of, count.get("qualifier"))
        return _KeyInfo(path, qualifier, "count")
    if isinstance(constraint, dict) and ("And" in constraint or "Or" in constraint):
        is_and = "And" in constraint
        children = constraint.get("And", constraint.get("Or", []))
        infos = [_derive_key_info(catalog, shape_name_of, child) for child in children]
        paths = {info.path for info in infos if info.path is not None}
        if len(paths) == 1:
            (path,) = paths
            quals = [info.qualifier for info in infos if info.qualifier is not None]
            return _KeyInfo(path, quals[0] if quals else None, "count")
        return _KeyInfo(None, None, "and" if is_and else "or")
    return _KeyInfo(None, None, _kind_tag(constraint))


def _collect_bounds(catalog: _Catalog, constraint_id: Optional[int]):
    """`(min, max)` from the *source* constraint tree, through And containers
    only — the collapsed-datatype-plus-minCount case takes the tightest
    bounds across the conjuncts."""
    constraint = catalog.logical(constraint_id)
    if isinstance(constraint, dict) and "Count" in constraint:
        count = constraint["Count"]
        qualifier_body = catalog.get(count.get("qualifier"))
        if isinstance(qualifier_body, dict) and "Not" in qualifier_body:
            # The `∀π.φ ≡ ∃≤0 π.¬φ` encoding: its `max=0` describes
            # counterexamples to `φ`, not the property's real cardinality.
            return None, None
        return count.get("min"), count.get("max")
    if isinstance(constraint, dict) and "And" in constraint:
        mins, maxs = [], []
        for child_id in constraint["And"]:
            cmin, cmax = _collect_bounds(catalog, child_id)
            if cmin is not None:
                mins.append(cmin)
            if cmax is not None:
                maxs.append(cmax)
        return (max(mins) if mins else None), (min(maxs) if maxs else None)
    return None, None


def _severity_of(
    normalized_catalog: _Catalog,
    normalized_ref: Optional[int],
    statement_normalized_ref: Optional[int],
) -> str:
    def deepest_severity(ref: Optional[int]) -> Optional[str]:
        # A collapsed single-property NodeShape normalizes to nested
        # `Annotated{Annotated{...}}` (the NodeShape's own default wrapping
        # the property shape's own wrapper); the innermost explicit severity
        # is the one closest to the actual failing constraint and wins.
        found = None
        seen = set()
        while ref is not None and ref not in seen:
            seen.add(ref)
            constraint = normalized_catalog.get(ref)
            if not (isinstance(constraint, dict) and "Annotated" in constraint):
                break
            severity = constraint["Annotated"].get("severity")
            if severity is not None:
                found = severity
            ref = constraint["Annotated"]["shape"]
        return found

    severity = deepest_severity(normalized_ref) or deepest_severity(statement_normalized_ref)
    return (severity or "Violation").lower()


# ── evidence-tree readers ───────────────────────────────────────────────────────

_TRANSPARENT = {"all_held", "any_held", "all", "any"}


def _details(node: dict) -> dict:
    return node.get("details", {})


def _direct_children(node: dict) -> list[dict]:
    d = _details(node)
    for field in ("children", "failed", "branches", "satisfied"):
        if field in d:
            return d[field]
    return []


def _top_values(node: Optional[dict]) -> "list[Term]":
    """The values bound at the *top level* of an evidence subtree: what the
    property's own path matched, without descending into nested qualifier
    checks (whose matches are class/type terms, not bindings)."""
    if node is None:
        return []
    kind = node.get("type")
    d = _details(node)
    if kind == "count_held":
        found = [_term_from_json(m[0]) for m in d.get("matches", [])]
    elif kind == "for_all_held":
        found = [_term_from_json(v[0]) for v in d.get("values", [])]
    elif kind == "count_low":
        found = [_term_from_json(m["value"]) for m in d.get("qualifying_matches", [])]
    elif kind == "count_high":
        found = [_term_from_json(m[0]) for m in d.get("matched", [])]
    elif kind in _TRANSPARENT:
        found = [v for child in _direct_children(node) for v in _top_values(child)]
    else:
        found = []
    out: "list[Term]" = []
    for value in found:
        if value not in out:
            out.append(value)
    return out


def _top_counts(node: Optional[dict]) -> Iterator[dict]:
    """The subtree's own count nodes: reached through AND/OR containers only,
    never through a nested qualifier trace (whose counts describe a *value*,
    not this binding)."""
    if node is None:
        return
    if node.get("type") in _TRANSPARENT:
        for child in _direct_children(node):
            yield from _top_counts(child)
    elif node.get("type") in ("count_low", "count_high", "count_held", "for_all_held"):
        yield node


def _missing_count(node: Optional[dict]) -> int:
    return sum(
        _details(n).get("min", 0) - _details(n).get("have", 0)
        for n in _top_counts(node)
        if n.get("type") == "count_low"
    )


def _observed_count(node: Optional[dict]) -> Optional[int]:
    for n in _top_counts(node):
        d = _details(n)
        if n.get("type") == "count_held" and d.get("observed_count") is not None:
            return d["observed_count"]
        if n.get("type") == "count_low" and d.get("have") is not None:
            return d["have"]
    return None


def _rejected_values(node: Optional[dict]) -> "list[Term]":
    out: "list[Term]" = []
    for n in _top_counts(node):
        for rc in _details(n).get("rejected_candidates", []):
            term = _term_from_json(rc["value"])
            if term not in out:
                out.append(term)
    return out


def _explain(node: dict, depth: int = 0) -> list[str]:
    kind = node.get("type", "?")
    d = _details(node)
    line = "  " * depth + kind
    if "path" in d:
        line += f" {_path_str(_path_from_json(d['path']), compact=True)}"
    if kind == "count_low":
        line += f" (have {d.get('have')}, need ≥{d.get('min')})"
    if kind == "count_held" and d.get("observed_count") is not None:
        line += f" (observed {d.get('observed_count')})"
    values = _top_values(node) if kind not in _TRANSPARENT else []
    if values:
        line += ": " + ", ".join(v.n3() for v in values)
    lines = [line]
    for child in _direct_children(node):
        lines.extend(_explain(child, depth + 1))
    return lines


# ── public objects ──────────────────────────────────────────────────────────────


@dataclasses.dataclass
class BoundValue:
    """One bound value plus its :func:`shape_map`/``value_paths`` annotations."""

    term: Term
    annotations: "dict[str, list[Term]]" = dataclasses.field(default_factory=dict)


class _ValueAnnotationResolver:
    """Batches ``EvidenceSession.resolve_path`` calls across a whole
    :class:`ShapeMap`: on first use, collects every bound value term across
    every mapping and issues one ``resolve_path`` call per label, caching the
    result. ``value_paths`` is entirely optional; nothing here runs until a
    binding's ``annotations``/``annotated_values`` is actually read."""

    def __init__(self, inner, value_paths: "dict[str, str]", mappings: "list[Mapping]") -> None:
        self._inner = inner
        self._value_paths = value_paths
        self._mappings = mappings
        self._cache: "Optional[dict[str, dict[str, list[Term]]]]" = None

    def _ensure(self) -> None:
        if self._cache is not None:
            return
        nodes: "set[str]" = set()
        for mapping in self._mappings:
            for binding in mapping.bindings.values():
                for value in binding.values or []:
                    nodes.add(value.n3())
        node_list = sorted(nodes)
        cache: "dict[str, dict[str, list[Term]]]" = {}
        for label, path in self._value_paths.items():
            reached = self._inner.resolve_path(node_list, path) if node_list else {}
            cache[label] = {
                node: [_term_parse(t) for t in values] for node, values in reached.items()
            }
        self._cache = cache

    def annotate(self, values: "list[Term]") -> "list[BoundValue]":
        self._ensure()
        out = []
        for value in values:
            n3 = value.n3()
            annotations = {label: table.get(n3, []) for label, table in self._cache.items()}
            out.append(BoundValue(value, annotations))
        return out


class Binding:
    """One key of a mapping: a property obligation and what it bound to.

    ``status`` is ``"pass"`` (bound: ``values`` holds what the path matched)
    or ``"fail"`` (unbound: ``missing``/``partial_values``/``rejected_values``
    describe the shortfall and ``evidence`` is this key's witness subtree).
    """

    def __init__(
        self,
        key: Key,
        status: str,
        source_constraint_id: int,
        normalized_constraint_id: Optional[int],
        evidence: Optional[dict],
        resolve: Optional[Callable[[], dict]],
        bounds: "tuple[Optional[int], Optional[int]]",
        severity: str,
        names: "Optional[list[str]]",
    ) -> None:
        self.key = key
        self.status = status
        self.source_constraint_id = source_constraint_id
        self.constraint_id = normalized_constraint_id
        self.path = key.path
        self.qualifier = key.qualifier
        self.severity = severity
        self.names = names
        self._evidence = evidence
        self._resolve = resolve
        self._min, self._max = bounds
        self._value_resolver: "Optional[_ValueAnnotationResolver]" = None
        self._values: "Optional[list[Term]]" = None
        self._annotated_values: "Optional[list[BoundValue]]" = None

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    @property
    def name(self) -> Optional[str]:
        """The author's name for this slot (the first value of ``names``)."""
        return self.names[0] if self.names else None

    @property
    def evidence(self) -> Optional[dict]:
        """This key's evidence subtree (tagged plain-JSON node), materializing
        it on demand for a passing key inside a failing focus."""
        if self._evidence is None and self._resolve is not None:
            self._evidence = self._resolve()
            self._resolve = None
        return self._evidence

    @property
    def values(self) -> "Optional[list[Term]]":
        """The values the key's path bound. For a failing key these are the
        qualifying near-matches (same as ``partial_values``). ``None`` only
        when the evidence is unavailable — a passing key of a failing focus
        with no session to consult."""
        if self._values is None:
            evidence = self.evidence
            self._values = _top_values(evidence) if evidence is not None else None
        return self._values

    @property
    def partial_values(self) -> "list[Term]":
        """Values that did qualify under a failing count (never enough)."""
        return _top_values(self._evidence) if not self.ok else []

    @property
    def rejected_values(self) -> "list[Term]":
        """Near-miss candidates the path reached but the qualifier rejected."""
        return _rejected_values(self._evidence) if not self.ok else []

    @property
    def missing(self) -> int:
        """How many qualifying values are still owed (0 for a bound key)."""
        return _missing_count(self._evidence) if not self.ok else 0

    @property
    def min(self) -> Optional[int]:
        """The source constraint's declared lower bound, present even when
        evidence was never materialized."""
        return self._min

    @property
    def max(self) -> Optional[int]:
        """The source constraint's declared upper bound."""
        return self._max

    @property
    def observed(self) -> Optional[int]:
        """The count evidence actually observed; ``None`` without evidence."""
        return _observed_count(self.evidence)

    @property
    def expects_single(self) -> bool:
        return self._min == 1 and self._max == 1

    @property
    def annotated_values(self) -> "list[BoundValue]":
        """Every bound value paired with its ``value_paths`` annotations
        (empty per-value when ``value_paths`` was not configured)."""
        if self._annotated_values is None:
            values = self.values or []
            if self._value_resolver is None:
                self._annotated_values = [BoundValue(v, {}) for v in values]
            else:
                self._annotated_values = self._value_resolver.annotate(values)
        return self._annotated_values

    @property
    def annotations(self) -> "dict[str, dict[Term, list[Term]]]":
        """``label -> value -> reached``, pivoted from :attr:`annotated_values`."""
        out: "dict[str, dict[Term, list[Term]]]" = {}
        for bound in self.annotated_values:
            for label, reached in bound.annotations.items():
                out.setdefault(label, {})[bound.term] = reached
        return out

    def explain(self) -> str:
        """This key's evidence subtree as indented text."""
        evidence = self.evidence
        if evidence is None:
            return f"{self.key}: {self.status} (evidence not materialized)"
        return "\n".join(_explain(evidence))

    def __repr__(self) -> str:
        if self.ok:
            shown = self._values if self._values is not None else self._evidence
            detail = f"values={self.values!r}" if shown is not None else "values=?"
        else:
            detail = f"missing={self.missing}"
        return f"Binding(key={self.key!r}, status={self.status!r}, {detail})"


class Mapping(collections.abc.Mapping):
    """One (focus node, shape statement) association with its key bindings.

    Implements :class:`collections.abc.Mapping` over ``Key -> Binding``
    (``keys()``/``values()``/``items()``/``get()``/``in`` all follow);
    ``__getitem__`` also accepts a key's ``str()`` display form.
    """

    __match_args__ = ("focus", "shape_name", "conforms")

    def __init__(
        self,
        focus: Term,
        shape_name: Optional[str],
        target: str,
        conforms: bool,
        bindings: "dict[Key, Binding]",
        evaluation,
    ) -> None:
        self.focus = focus
        self.shape_name = shape_name
        self.target = target
        self.conforms = conforms
        self.bindings = bindings
        #: The underlying :class:`FocusEvaluation` — ``.failure`` /
        #: ``.satisfaction`` for full evidence objects (``explain()``,
        #: ``repair_tree()``, …).
        self.evaluation = evaluation

    @property
    def successful(self) -> "list[tuple[Key, Binding]]":
        """``(key, binding)`` for every bound key, in authored order."""
        return [(k, b) for k, b in self.bindings.items() if b.ok]

    @property
    def unsuccessful(self) -> "list[tuple[Key, Binding]]":
        """``(key, binding)`` for every unbound key; the binding carries the
        witness subtree, shortfall counts, and near-misses."""
        return [(k, b) for k, b in self.bindings.items() if not b.ok]

    def value_map(
        self, *, by: str = "key", python: bool = False
    ) -> "dict":
        """Bound keys only, projected for application configuration.

        ``by="key"`` (default) keys the result by :class:`Key`; ``by="name"``
        keys it by ``binding.name``, falling back to ``str(key)`` when a
        binding has no name. ``python=True`` coerces values with
        :meth:`Literal.to_python`, and renders `Iri`/`BNode` as their bare
        string.
        """
        if by not in ("key", "name"):
            raise ValueError(f"by must be 'key' or 'name', got {by!r}")
        out: "dict" = {}
        for key, binding in self.bindings.items():
            if not binding.ok or not binding.values:
                continue
            out_key = key if by == "key" else (binding.name or str(key))
            values = binding.values
            out[out_key] = [_to_python_value(v) for v in values] if python else list(values)
        return out

    def by_name(self, name: str) -> Binding:
        """The first binding (in binding order) whose ``name`` matches.
        Names are not guaranteed unique."""
        for binding in self.bindings.values():
            if binding.name == name:
                return binding
        raise KeyError(name)

    def __getitem__(self, key) -> Binding:
        if isinstance(key, Key):
            return self.bindings[key]
        if isinstance(key, str):
            for k, b in self.bindings.items():
                if str(k) == key:
                    return b
            raise KeyError(key)
        raise TypeError(f"Mapping keys are Key or str, got {type(key).__name__}")

    def __len__(self) -> int:
        return len(self.bindings)

    def __iter__(self) -> Iterator[Key]:
        return iter(self.bindings)

    def __repr__(self) -> str:
        bound = sum(1 for b in self.bindings.values() if b.ok)
        return (
            f"Mapping(focus={self.focus!r}, shape={self.shape_name!r}, "
            f"conforms={self.conforms}, bound={bound}/{len(self.bindings)})"
        )


def _normalize_focus(focus) -> Term:
    if not isinstance(focus, str):
        return focus
    try:
        return _term_parse(focus)
    except ValueError:
        return Iri(focus)


class ShapeMap:
    """Key -> value bindings for every selected (shape, focus) pair of a run."""

    def __init__(self, conforms: bool, mappings: "dict[str, list[Mapping]]") -> None:
        self.conforms = conforms
        self.mappings = mappings
        self._focus_index: "Optional[dict[Term, list[Mapping]]]" = None

    @property
    def shape_names(self) -> list[str]:
        """Every shape identity with at least one authored statement — shape
        IRIs, or ``_:statement-N`` placeholders for anonymous shapes."""
        return list(self.mappings)

    def __getitem__(self, shape_name: str) -> "list[Mapping]":
        return self.mappings[shape_name]

    def __iter__(self) -> Iterator[Mapping]:
        for group in self.mappings.values():
            yield from group

    def conforming(self, shape_name: str) -> "list[Mapping]":
        return [m for m in self.mappings[shape_name] if m.conforms]

    def nonconforming(self, shape_name: str) -> "list[Mapping]":
        return [m for m in self.mappings[shape_name] if not m.conforms]

    def for_focus(self, focus: "Union[Term, str]") -> "list[Mapping]":
        """Every mapping whose focus is *focus*, across shapes. ``focus`` may
        be a :class:`Term`, an N-Triples string, or a bare IRI."""
        term = _normalize_focus(focus)
        if self._focus_index is None:
            index: "dict[Term, list[Mapping]]" = {}
            for mapping in self:
                index.setdefault(mapping.focus, []).append(mapping)
            self._focus_index = index
        return self._focus_index.get(term, [])

    def to_dict(self) -> dict:
        """A plain-JSON summary: shape -> focus -> key -> values/missing/name."""
        return {
            "conforms": self.conforms,
            "shapes": {
                name: [
                    {
                        "focus": m.focus.n3(),
                        "target": m.target,
                        "conforms": m.conforms,
                        "bindings": {
                            str(key): {
                                "status": b.status,
                                "values": (
                                    [v.n3() for v in b.values] if b.values is not None else None
                                ),
                                "missing": b.missing,
                                "name": b.name,
                            }
                            for key, b in m.bindings.items()
                        },
                    }
                    for m in group
                ]
                for name, group in self.mappings.items()
            },
        }

    def __repr__(self) -> str:
        pairs = sum(len(group) for group in self.mappings.values())
        return (
            f"ShapeMap(conforms={self.conforms}, shapes={len(self.mappings)}, "
            f"mappings={pairs})"
        )

    @classmethod
    def from_run(
        cls,
        run: "EvidenceRun",
        session: "Optional[EvidenceSession]" = None,
        *,
        name_path: Optional[str] = "sh:name",
        value_paths: "Optional[dict[str, str]]" = None,
    ) -> "ShapeMap":
        """Build the shape map from an evidence run.

        Pass the ``session`` the run came from to materialize exact values for
        the passing keys of failing foci (a failing conjunction's witness
        carries only its failing children); without it those bindings report
        ``values = None``, and neither ``name_path`` nor ``value_paths`` can
        resolve (both need the shapes/data graphs the session holds).

        ``name_path`` (a SPARQL 1.1 property path, default ``sh:name``)
        carries the author's name for each slot; pass ``None`` to skip name
        resolution. ``value_paths`` (``{label: path}``) annotates each bound
        *value*, evaluated from the value node over the data graph; resolved
        lazily and in one batched call per label on first access.
        """
        data = run.to_dict()
        source_catalog = _Catalog(data["constraints"]["source"])
        normalized_catalog = _Catalog(data["constraints"]["normalized"])
        inner = getattr(session, "_inner", session)

        names_table: "dict[int, list[str]]" = {}
        shape_name_of = None
        if inner is not None:
            shape_name_of = inner.shape_name_of
            if name_path is not None:
                names_table = inner.binding_names(name_path)

        mappings: "dict[str, list[Mapping]]" = {}
        all_mappings: "list[Mapping]" = []
        for statement_py, statement in zip(run.statements, data["statements"]):
            name = statement_py.shape_name
            group_key = name or f"_:statement-{statement_py.source_statement_id}"
            group = mappings.setdefault(group_key, [])
            for focus_py, focus in zip(
                statement_py.selected_foci, statement["selected_foci"]
            ):
                mapping = _build_mapping(
                    statement_py,
                    name,
                    focus_py,
                    focus,
                    source_catalog,
                    normalized_catalog,
                    inner,
                    names_table,
                    shape_name_of,
                )
                group.append(mapping)
                all_mappings.append(mapping)

        if value_paths and inner is not None:
            resolver = _ValueAnnotationResolver(inner, value_paths, all_mappings)
            for mapping in all_mappings:
                for binding in mapping.bindings.values():
                    binding._value_resolver = resolver

        return cls(data["conforms"], mappings)


def _build_mapping(
    statement_py,
    shape_name: Optional[str],
    focus_py,
    focus: dict,
    source_catalog: _Catalog,
    normalized_catalog: _Catalog,
    inner,
    names_table: "dict[int, list[str]]",
    shape_name_of: "Optional[Callable[[int], Optional[str]]]",
) -> Mapping:
    conforms = focus["evidence"]["status"] == "pass"
    root = focus["evidence"]["evidence"]
    progress = (focus.get("progress") or {}).get("evaluated_children", [])

    # The statement's direct children, keyed by their logical constraint id.
    # Progress children are the authored conjunction's members; a child absent
    # from a failure tree is a passing sibling the witness elided.
    subtrees = {_details(root).get("shape"): root}
    for child in _direct_children(root):
        subtrees[_details(child).get("shape")] = child

    entries: "list[tuple[_KeyInfo, str, int, Optional[int], Optional[dict]]]" = []
    if progress:
        for child in progress:
            source_id = child["source_constraint_ref"]
            info = _derive_key_info(source_catalog, shape_name_of, source_id)
            normalized_ref = child.get("normalized_constraint_ref")
            logical = normalized_catalog.unwrap(normalized_ref)
            entries.append(
                (info, child["status"], source_id, normalized_ref, subtrees.get(logical))
            )
    else:
        source_id = statement_py.source_constraint_id
        info = _derive_key_info(source_catalog, shape_name_of, source_id)
        entries.append(
            (
                info,
                focus["evidence"]["status"],
                source_id,
                statement_py.normalized_constraint_id,
                root,
            )
        )

    bindings: "dict[Key, Binding]" = {}
    ordinals: "dict[tuple, int]" = {}
    for info, status, source_id, normalized_ref, subtree in entries:
        dedup_key = (info.path, info.qualifier, info.kind)
        ordinals[dedup_key] = ordinals.get(dedup_key, 0) + 1
        key = Key(info.path, info.qualifier, ordinal=ordinals[dedup_key], kind=info.kind)

        resolve = None
        if subtree is None and status == "pass":
            if normalized_ref is None:
                # Normalized away as trivially true: bound with nothing to show.
                subtree = {"type": "irrefutable", "details": {}}
            elif inner is not None:
                focus_term = focus_py.focus
                ref = normalized_ref
                resolve = lambda f=focus_term, r=ref: inner.evidence_for(f, r)["evidence"]

        bounds = _collect_bounds(source_catalog, source_id)
        severity = _severity_of(
            normalized_catalog, normalized_ref, statement_py.normalized_constraint_id
        )
        names = names_table.get(source_id)

        bindings[key] = Binding(
            key, status, source_id, normalized_ref, subtree, resolve, bounds, severity, names
        )

    return Mapping(
        _normalize_focus(focus_py.focus),
        shape_name,
        statement_py.target,
        conforms,
        bindings,
        focus_py,
    )


def shape_map(
    shacl_graph,
    data_graph=None,
    *,
    name_path: Optional[str] = "sh:name",
    value_paths: "Optional[dict[str, str]]" = None,
    shape_names: Optional[Sequence[str]] = None,
    minimum_severity: str = "info",
    infer: bool = True,
    graph_mode: str = "union",
    base: Optional[str] = None,
) -> ShapeMap:
    """Validate and return the :class:`ShapeMap` for the snapshot.

    A convenience over ``EvidenceSession(...).validate()`` +
    :meth:`ShapeMap.from_run`; accepts the same graph inputs as every other
    entry point. Keep an :class:`~shifty.EvidenceSession` yourself and call
    :meth:`ShapeMap.from_run` to reuse the prepared snapshot across calls.
    """
    from . import EvidenceSession

    session = EvidenceSession(
        shacl_graph,
        data_graph,
        infer=infer,
        graph_mode=graph_mode,
        base=base,
    )
    run = session.validate(
        shape_names=shape_names, minimum_severity=minimum_severity
    )
    return ShapeMap.from_run(run, session, name_path=name_path, value_paths=value_paths)
