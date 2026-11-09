"""Violation generation for knowledge-graph repair benchmarks.

This module injects SHACL violations with known ground-truth repairs into a
*reference* graph that conforms to a shapes graph. It follows the generator
of Lin et al., "Systematic Evaluation of Knowledge Graph Repair with Large
Language Models" (arXiv 2507.22419), extended to multiple interacting
violations as in "Selective Retrieval Routing for Multi-Violation Knowledge
Graph Repair" (arXiv 2611.07342). Shifty's evidence engine and shape algebra
do the work that the original implementation hand-wrote per SHACL component.

The pipeline has three stages:

1. **Expansion.** For every passing ``(focus, statement)`` pair, the
   satisfaction evidence explains *why* the shape holds. ``VIO(x, φ)`` rewrites
   that evidence into alternative edit sets that would make ``φ`` fail at
   ``x``. A failure inside ``¬φ`` flips to the dual ``SAT(x, φ)``, which makes
   a failing sub-shape hold. The rules run over shifty's normalized algebra
   (``∧ ∨ ¬ ∃≥n ∃≤n ∀ test closed``), so they cover every SHACL component that
   lowers to it. The leaves are :class:`ViolationExpression` values: normalized
   conjunctions of atomic :class:`Operation` s. Traversal is lazy, so no
   expansion graph is built.
2. **Sampling.** A weighted reservoir with Efraimidis–Spirakis keys,
   ``key(v) = -ln h(v) / w(v)``, keeps the ``M`` best distinct expressions.
   Here ``h`` is a stable hash and ``w`` is the geometric mean of the
   per-predicate weights an expression edits. Per-predicate weights let a
   benchmark suite emphasize one predicate class.
3. **Composition.** The reservoir is scanned in key order. An expression is
   admitted only if it is compatible with the expressions already chosen, and
   stops at the injection budget ``K``. Compatible means disjoint edit
   footprints, no opposing requirements on one ``(node, constraint)``, and a
   distinct target. Each admitted expression is checked by revalidating its
   own delta. The composed delta is then revalidated, so every recorded
   violation is one the engine reports. The inverse of the composed delta is
   the ground-truth repair.

The usual entry point is :class:`ViolationGenerator`::

    gen = shifty.multivio.ViolationGenerator(shapes, reference_graph, seed=7)
    for case in gen.generate(budget=5, cases=10):
        case.save(f"suite/case-{case.index:03d}")

The generator does not perform graph repair.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import itertools
import json
import math
import pathlib
import random
import re
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from . import EvidenceSession, RepairDelta, RepairSession, _coalesce_graph_input
from . import infer as _infer

__all__ = [
    "ExpansionLimits",
    "Operation",
    "TestCase",
    "ViolationExpression",
    "ViolationGenerator",
    "inverse_frequency_weights",
    "main",
]

Triple = tuple[str, str, str]
"""A triple of N-Triples-syntax terms."""

Requirement = tuple[str, str, int]
"""``(polarity, node, constraint)``: an expression needs ``node`` to
``"violate"`` or ``"satisfy"`` normalized constraint ``constraint``."""

_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
_XSD = "http://www.w3.org/2001/XMLSchema#"
_XSD_STRING = _XSD + "string"
_NUMERIC = {
    _XSD + name
    for name in (
        "integer",
        "decimal",
        "double",
        "float",
        "int",
        "long",
        "short",
        "byte",
        "nonNegativeInteger",
        "positiveInteger",
        "negativeInteger",
        "nonPositiveInteger",
        "unsignedInt",
        "unsignedLong",
        "unsignedShort",
        "unsignedByte",
    )
}
_INTEGRAL = _NUMERIC - {_XSD + "decimal", _XSD + "double", _XSD + "float"}


# ── Public data model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Operation:
    """One leaf of a violation expression: an atomic violating edit.

    ``kind`` is ``"remove"`` (cut an edge that supports a value),
    ``"substitute"`` (replace a value with one that fails or satisfies the
    governing qualifier), ``"add_value"`` (add a value along a path), or
    ``"add_forbidden"`` (add a triple a closed shape disallows). The operation
    makes ``node`` reach ``polarity`` for normalized constraint
    ``constraint``. ``requires`` lists the polarities this edit depends on,
    for example that a substituted value fails its qualifier.
    """

    kind: str
    node: str
    constraint: int
    polarity: str
    deletes: tuple[Triple, ...] = ()
    adds: tuple[Triple, ...] = ()
    requires: tuple[Requirement, ...] = ()
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "node": self.node,
            "constraint": self.constraint,
            "polarity": self.polarity,
            "delete": [list(t) for t in self.deletes],
            "add": [list(t) for t in self.adds],
            "requires": [list(r) for r in self.requires],
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ViolationExpression:
    """A normalized violation expression: the operations that together make
    one selected ``(focus, statement)`` pair fail.

    Two expressions with the same net delta are the same expression. The
    generator keeps the first target that produced the delta.
    """

    focus: str
    statement: int
    constraint: int
    shape: Optional[str]
    component: int
    """The top-level conjunct of the statement that this expression breaks.
    For a node shape this is usually one property shape. Validation reports
    it as its own result."""
    operations: tuple[Operation, ...]

    @property
    def deletes(self) -> frozenset[Triple]:
        return frozenset(t for op in self.operations for t in op.deletes)

    @property
    def adds(self) -> frozenset[Triple]:
        return frozenset(t for op in self.operations for t in op.adds)

    @property
    def footprint(self) -> frozenset[Triple]:
        return self.deletes | self.adds

    @property
    def target(self) -> tuple[str, int]:
        """The ``(focus, statement)`` pair the expression makes fail."""
        return (self.focus, self.statement)

    @property
    def slot(self) -> tuple[str, int, int]:
        """``(focus, statement, component)``, the unit of a distinct violation."""
        return (self.focus, self.statement, self.component)

    @property
    def predicates(self) -> frozenset[str]:
        """The predicates this expression edits, which are its targets in the
        predicate-weighted sampling."""
        return frozenset(t[1] for t in self.footprint)

    @property
    def requirements(self) -> frozenset[Requirement]:
        out: set[Requirement] = set()
        for op in self.operations:
            out.add((op.polarity, op.node, op.constraint))
            out.update(op.requires)
        return frozenset(out)

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(op.kind for op in self.operations)

    @property
    def key(self) -> str:
        """The canonical normal form used for deduplication and hashing."""
        lines = [f"- {_line(t)}" for t in sorted(self.deletes)]
        lines += [f"+ {_line(t)}" for t in sorted(self.adds)]
        return "\n".join(lines)

    def delta(self) -> RepairDelta:
        return _delta(self.adds, self.deletes)

    def conflicts_with(self, other: ViolationExpression) -> bool:
        """Whether the two cannot be injected together.

        Overlapping edit footprints conflict, since one edit would undo or
        duplicate the other. Opposing requirements on one ``(node,
        constraint)`` also conflict, such as one expression needing a value to
        satisfy a qualifier while another breaks that qualifier at the value.
        """
        if self.footprint & other.footprint:
            return True
        mine = self.requirements
        for polarity, node, constraint in other.requirements:
            if (_flip(polarity), node, constraint) in mine:
                return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "focus": self.focus,
            "statement": self.statement,
            "constraint": self.constraint,
            "shape": self.shape,
            "component": self.component,
            "key": self.key,
            "predicates": sorted(self.predicates),
            "operations": [op.to_dict() for op in self.operations],
        }


@dataclass(frozen=True)
class ExpansionLimits:
    """Bounds on the lazy expansion. Each bound caps a branching factor of the
    rewrite system. Without them a large graph would have an astronomical
    universe, since every value can be replaced in many ways."""

    max_alternatives: int = 24
    """Alternatives retained at any one choice point."""
    max_values: int = 6
    """Values sampled per count or universal before branching."""
    max_candidates: int = 3
    """Replacement or new terms tried per value context."""
    max_combinations: int = 12
    """Joint choices kept when several values must change together."""
    max_depth: int = 12
    """Nesting depth across ``sh:node`` and ``sh:qualifiedValueShape``."""
    max_per_target: int = 64
    """Expressions emitted per ``(focus, statement)`` pair."""
    max_expressions: int = 200_000
    """Total expressions the traversal emits before it stops."""


@dataclass
class TestCase:
    """One generated benchmark case: an invalid graph and its ground truth.

    ``deletes``/``adds`` form the injected delta ``Δ`` with
    ``G_test = G_ref ⊕ Δ``. ``repair`` is ``Δ⁻¹``, which recovers ``G_ref``.
    ``violations`` lists the failing ``(focus, statement)`` pairs in
    ``G_test`` as the engine reports them. It can exceed the budget, because
    one edit can fail several statements.
    """

    __test__ = False  # not a pytest class

    index: int
    budget: int
    salt: str
    expressions: list[ViolationExpression]
    deletes: frozenset[Triple]
    adds: frozenset[Triple]
    violations: list[dict[str, Any]]
    reference: frozenset[Triple] = field(repr=False)

    @property
    def repair(self) -> RepairDelta:
        """The ground-truth repair ``Δ⁻¹`` as a :class:`RepairDelta`."""
        return _delta(self.deletes, self.adds)

    @property
    def delta(self) -> RepairDelta:
        """The injected delta ``Δ``."""
        return _delta(self.adds, self.deletes)

    @property
    def amplification(self) -> int:
        """The number of failing pairs per injected expression, summed."""
        return len(self.violations)

    def invalid_ntriples(self) -> str:
        triples = (self.reference - self.deletes) | self.adds
        return "".join(f"{_line(t)}\n" for t in sorted(triples))

    def repair_sparql(self) -> str:
        """The ground-truth repair as a SPARQL 1.1 Update.

        SPARQL forbids blank nodes in ``DELETE DATA``. When the repair deletes
        a blank-node triple, this falls back to ``DELETE WHERE``, which binds
        blank nodes as variables.
        """
        parts = []
        removed = sorted(self.adds)
        if removed:
            body = "\n".join(f"  {_line(t)}" for t in removed)
            if any(term.startswith("_:") for t in removed for term in t):
                parts.append("DELETE WHERE {\n" + _bnodes_as_vars(body) + "\n}")
            else:
                parts.append("DELETE DATA {\n" + body + "\n}")
        restored = sorted(self.deletes)
        if restored:
            body = "\n".join(f"  {_line(t)}" for t in restored)
            parts.append("INSERT DATA {\n" + body + "\n}")
        return " ;\n".join(parts) + "\n"

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "budget": self.budget,
            "salt": self.salt,
            "injected": len(self.expressions),
            "delta": {
                "delete": [list(t) for t in sorted(self.deletes)],
                "add": [list(t) for t in sorted(self.adds)],
            },
            "repair": {
                "delete": [list(t) for t in sorted(self.adds)],
                "add": [list(t) for t in sorted(self.deletes)],
            },
            "expressions": [e.to_dict() for e in self.expressions],
            "violations": self.violations,
        }

    def save(self, directory: str | pathlib.Path) -> pathlib.Path:
        """Write ``invalid.nt``, ``repair.ru``, and ``case.json``."""
        out = pathlib.Path(directory)
        out.mkdir(parents=True, exist_ok=True)
        (out / "invalid.nt").write_text(self.invalid_ntriples(), encoding="utf-8")
        (out / "repair.ru").write_text(self.repair_sparql(), encoding="utf-8")
        (out / "case.json").write_text(
            json.dumps(self.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return out


# ── Generator ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Target:
    focus: str
    statement: int
    constraint: int
    shape: Optional[str]
    evidence: dict[str, Any]


class ViolationGenerator:
    """Generate multi-violation test cases from a conforming reference graph.

    Parameters
    ----------
    shacl_graph, data_graph:
        The shapes and the reference graph, in any form
        :class:`shifty.EvidenceSession` accepts. The data is first normalized
        to N-Triples, so blank-node labels stay stable across revalidations
        and in the emitted files.
    seed:
        Seeds candidate sampling during expansion and the per-case hash salts.
    predicate_weights:
        Positive weight ``ω_p`` per predicate. Keys may be bare IRIs or
        N-Triples ``<iri>`` terms. A predicate without an entry gets
        ``default_weight``.
    kind_weights:
        Optional weight per :attr:`Operation.kind`, multiplied into
        ``w(v)`` as a geometric mean over the expression's operations. 223P
        universes are dominated by ``add_value``, since nearly every property
        has a ``sh:class`` and a ``sh:maxCount``. Down-weighting it gives a
        mix closer to real modeling errors.
    materialize:
        Run SHACL-AF rules once and use the inferred closure as the reference
        graph. Models that rely on rules, such as ASHRAE 223P models, need
        this to conform, and edits may then touch inferred triples.
    infer:
        Re-run SHACL-AF rules on every validation instead. Edits target only
        the asserted reference triples, so values supported only by an
        inferred triple cannot be cut.
    include_external_foci:
        Also target focus nodes that have no outgoing triple in the reference
        graph, such as vocabulary terms that the shapes graph describes.
    limits:
        Bounds on the expansion. See :class:`ExpansionLimits`.
    """

    def __init__(
        self,
        shacl_graph: Any,
        data_graph: Any,
        *,
        seed: int = 0,
        predicate_weights: Optional[Mapping[str, float]] = None,
        default_weight: float = 1.0,
        kind_weights: Optional[Mapping[str, float]] = None,
        materialize: bool = False,
        infer: bool = False,
        include_external_foci: bool = False,
        graph_mode: str = "union",
        on_unsupported: str = "error",
        limits: Optional[ExpansionLimits] = None,
    ) -> None:
        shapes = _coalesce_graph_input(shacl_graph)
        data = _coalesce_graph_input(data_graph)
        if materialize:
            reference_nt = _infer(
                data, shapes, on_unsupported=on_unsupported
            ).graph_ntriples
        else:
            reference_nt = RepairSession(
                shapes, data, infer=False
            )._inner.current_ntriples()
        self.reference: frozenset[Triple] = frozenset(_parse_ntriples(reference_nt))
        self.session = EvidenceSession(
            shapes,
            reference_nt.encode("utf-8"),
            infer=infer,
            graph_mode=graph_mode,
            on_unsupported=on_unsupported,
        )
        self.seed = seed
        self.include_external_foci = include_external_foci
        self.limits = limits or ExpansionLimits()
        self.default_weight = default_weight
        self.predicate_weights: dict[str, float] = {}
        self.kind_weights: dict[str, float] = {}
        self.reweight(predicate_weights, kind_weights)
        self._catalog = {
            entry["id"]: entry["constraint"]
            for entry in self.session.constraints()["normalized"]
        }
        self._index = _Index(self.reference)
        self._status_cache: dict[tuple[str, int], Optional[dict[str, Any]]] = {}
        self._shape_keys: dict[int, str] = {}
        self._baseline_run = self.session.validate()
        self.baseline_failures = _failing_pairs(self._baseline_run)
        self._expressions: Optional[list[ViolationExpression]] = None
        self._verified: dict[str, Optional[dict[tuple[str, int], set[int]]]] = {}

    # ── stage 1: expansion ──

    def targets(self) -> list[_Target]:
        """Every passing ``(focus, statement)`` pair in the reference graph."""
        out = []
        for st in self._baseline_run.statements:
            for fe in st.selected_foci:
                if fe.status != "pass":
                    continue
                if not self.include_external_foci and fe.focus not in self._index.out:
                    continue
                out.append(
                    _Target(
                        focus=fe.focus,
                        statement=st.source_statement_id,
                        constraint=st.normalized_constraint_id
                        if st.normalized_constraint_id is not None
                        else st.source_constraint_id,
                        shape=st.shape_name,
                        evidence=fe.evidence.to_dict()["evidence"],
                    )
                )
        out.sort(key=lambda t: (t.shape or "", t.focus, t.statement))
        return out

    def iter_expressions(self) -> Iterator[ViolationExpression]:
        """Lazily traverse the expansion graph, yielding each distinct
        normalized violation expression once."""
        seen: set[str] = set()
        emitted = 0
        for target in self.targets():
            expander = _Expander(self, target)
            for component, alt in itertools.islice(
                expander.components(), self.limits.max_per_target
            ):
                expr = ViolationExpression(
                    focus=target.focus,
                    statement=target.statement,
                    constraint=target.constraint,
                    shape=target.shape,
                    component=component,
                    operations=alt,
                )
                if not expr.footprint or expr.key in seen:
                    continue
                seen.add(expr.key)
                yield expr
                emitted += 1
                if emitted >= self.limits.max_expressions:
                    return

    def expressions(self) -> list[ViolationExpression]:
        """The materialized universe 𝒰 of expressions, computed once."""
        if self._expressions is None:
            self._expressions = list(self.iter_expressions())
        return self._expressions

    # ── stage 2: weighted reservoir sampling ──

    def reweight(
        self,
        predicate_weights: Optional[Mapping[str, float]] = None,
        kind_weights: Optional[Mapping[str, float]] = None,
    ) -> None:
        """Replace the sampling weights. The expanded universe is kept, so
        several suites with different predicate mixtures can share one
        traversal."""
        preds = {_iri_term(k): float(v) for k, v in (predicate_weights or {}).items()}
        kinds = {k: float(v) for k, v in (kind_weights or {}).items()}
        for name, w in [*preds.items(), *kinds.items()]:
            if not w > 0:
                raise ValueError(f"weight for {name} must be positive, got {w}")
        self.predicate_weights = preds
        self.kind_weights = kinds

    def weight(self, expr: ViolationExpression) -> float:
        """``w(v)``: geometric mean of ``ω_p`` over the predicates edited,
        times the geometric mean of the kind weights of its operations."""
        preds = expr.predicates
        logs = [
            math.log(self.predicate_weights.get(p, self.default_weight)) for p in preds
        ] or [math.log(self.default_weight)]
        w = math.exp(sum(logs) / len(logs))
        if self.kind_weights:
            kinds = [math.log(self.kind_weights.get(k, 1.0)) for k in expr.kinds]
            w *= math.exp(sum(kinds) / len(kinds))
        return w

    def sample(
        self,
        size: int,
        *,
        salt: str = "",
        expressions: Optional[Iterable[ViolationExpression]] = None,
    ) -> list[ViolationExpression]:
        """Weighted reservoir sample of ``size`` distinct expressions, ordered
        by ascending Efraimidis–Spirakis key."""
        stream = self.expressions() if expressions is None else expressions
        seen: set[str] = set()

        def keyed() -> Iterator[tuple[float, str, ViolationExpression]]:
            for expr in stream:
                if expr.key in seen:
                    continue
                seen.add(expr.key)
                k = -math.log(_unit_hash(salt, expr.key)) / self.weight(expr)
                yield (k, expr.key, expr)

        best = heapq.nsmallest(size, keyed(), key=lambda item: (item[0], item[1]))
        return [expr for _, _, expr in best]

    # ── stage 3: conflict-free composition ──

    def compose(
        self,
        budget: int,
        *,
        reservoir: Optional[Sequence[ViolationExpression]] = None,
        salt: str = "",
        index: int = 0,
        reservoir_size: Optional[int] = None,
        verify: bool = True,
        distinct_targets: bool = True,
    ) -> TestCase:
        """Compose one test case with at most ``budget`` expressions.

        With ``verify`` set, each candidate must make its own target fail when
        applied alone. The composed delta is then revalidated, and any
        expression whose target the combination masked is dropped and
        replaced from the reservoir.
        """
        if budget < 1:
            raise ValueError("budget must be at least 1")
        pool = (
            list(reservoir)
            if reservoir is not None
            else self.sample(reservoir_size or max(8 * budget, 64), salt=salt)
        )
        chosen: list[ViolationExpression] = []
        rejected: set[str] = set()
        final: dict[tuple[str, int], set[int]] = {}
        cursor = 0
        while True:
            while len(chosen) < budget and cursor < len(pool):
                cand = pool[cursor]
                cursor += 1
                if cand.key in rejected:
                    continue
                if distinct_targets and any(c.slot == cand.slot for c in chosen):
                    continue
                if any(cand.conflicts_with(c) for c in chosen):
                    continue
                if verify and not self.verify(cand):
                    rejected.add(cand.key)
                    continue
                chosen.append(cand)
            if not chosen:
                final = {}
                break
            final = _failing_components(self.session.revalidate(_union_delta(chosen)))
            if not verify:
                break
            masked = [c for c in chosen if not _realized(c, final)]
            if not masked:
                break
            # The combination masked these, for example when another edit
            # restored support they removed. Drop them, refill from the
            # reservoir, and revalidate. Each round rejects at least one
            # expression for good, so the loop ends.
            rejected.update(c.key for c in masked)
            chosen = [c for c in chosen if _realized(c, final)]
        deletes = frozenset(t for c in chosen for t in c.deletes)
        adds = frozenset(t for c in chosen for t in c.adds)
        names = {
            st.source_statement_id: st.shape_name
            for st in self._baseline_run.statements
        }
        violations = []
        for (focus, stmt), components in sorted(final.items()):
            if (focus, stmt) in self.baseline_failures:
                continue
            violations.append(
                {
                    "focus": focus,
                    "statement": stmt,
                    "shape": names.get(stmt),
                    "expressions": [
                        i
                        for i, c in enumerate(chosen)
                        if c.target == (focus, stmt) and c.component in components
                    ],
                }
            )
        return TestCase(
            index=index,
            budget=budget,
            salt=salt,
            expressions=chosen,
            deletes=deletes,
            adds=adds,
            violations=violations,
            reference=self.reference,
        )

    def generate(
        self,
        budget: int,
        cases: int = 1,
        *,
        reservoir_size: Optional[int] = None,
        verify: bool = True,
        distinct_targets: bool = True,
    ) -> list[TestCase]:
        """Draw ``cases`` test cases from one traversal. Each case reuses the
        materialized universe with its own hash salt."""
        out = []
        universe = self.expressions()
        for i in range(cases):
            salt = f"{self.seed}:{budget}:{i}"
            reservoir = self.sample(
                reservoir_size or max(8 * budget, 64), salt=salt, expressions=universe
            )
            out.append(
                self.compose(
                    budget,
                    reservoir=reservoir,
                    salt=salt,
                    index=i,
                    verify=verify,
                    distinct_targets=distinct_targets,
                )
            )
        return out

    def verify(self, expr: ViolationExpression) -> bool:
        """Whether applying ``expr`` alone makes its target fail."""
        if expr.target in self.baseline_failures:
            return False
        if expr.key not in self._verified:
            try:
                run = self.session.revalidate(expr.delta())
                self._verified[expr.key] = _failing_components(run)
            except Exception:  # noqa: BLE001 - an unparseable edit is unusable
                self._verified[expr.key] = None
        failures = self._verified[expr.key]
        return failures is not None and _realized(expr, failures)

    # ── engine probes used by the expander ──

    def _evidence(self, term: str, constraint: int) -> Optional[dict[str, Any]]:
        key = (term, constraint)
        if key not in self._status_cache:
            try:
                node = self.session.evidence_for(term, constraint).to_dict()
            except Exception:  # noqa: BLE001 - ill-formed candidate terms
                node = None
            self._status_cache[key] = node
        return self._status_cache[key]

    def _shape_key(self, sid: int) -> str:
        """A digest of normalized shape ``sid`` by structure, not by id."""
        return self._shape_key_in(sid, ())[0]

    def _shape_key_in(self, sid: int, stack: tuple[int, ...]) -> tuple[str, bool]:
        cached = self._shape_keys.get(sid)
        if cached is not None:
            return cached, False
        if sid in stack:
            return "recursive", True
        shape = self._catalog.get(sid)
        cyclic = False

        def sub(child: int) -> str:
            nonlocal cyclic
            key, cut = self._shape_key_in(child, (*stack, sid))
            cyclic = cyclic or cut
            return key

        body: Any = shape
        if isinstance(shape, dict):
            if "Annotated" in shape:
                inner = shape["Annotated"]
                body = {"Annotated": {**inner, "shape": sub(inner["shape"])}}
            elif "Not" in shape:
                body = {"Not": sub(shape["Not"])}
            elif "And" in shape or "Or" in shape:
                op = "And" if "And" in shape else "Or"
                body = {op: sorted(sub(c) for c in shape[op])}
            elif "Count" in shape:
                count = shape["Count"]
                body = {"Count": {**count, "qualifier": sub(count["qualifier"])}}
        key = hashlib.blake2b(
            json.dumps(body, sort_keys=True, default=str).encode(), digest_size=12
        ).hexdigest()
        # A key computed below a cut cycle depends on where the walk entered,
        # so only acyclic keys are shared.
        if not cyclic:
            self._shape_keys[sid] = key
        return key, cyclic

    def _holds(self, term: str, constraint: int) -> Optional[bool]:
        node = self._evidence(term, constraint)
        return None if node is None else node["status"] == "pass"


# ── Expansion rules ──────────────────────────────────────────────────────────

Alt = tuple[Operation, ...]


@dataclass(frozen=True)
class _Value:
    """A value ``v`` reached from ``owner`` along a path, with the qualifier
    that governs it."""

    owner: str
    term: str
    edge: Optional[Triple]
    support: tuple[Triple, ...]
    qualifier: int
    constraint: int


class _Expander:
    """The ``VIO``/``SAT`` rewrite system for one target."""

    def __init__(self, gen: ViolationGenerator, target: _Target) -> None:
        self.gen = gen
        self.lim = gen.limits
        self.index = gen._index
        self.target = target
        self.rng = random.Random(f"{gen.seed}\x00{target.focus}\x00{target.shape}")
        # (node, path) -> universals that hold on that path, as (qualifier,
        # status a value needs). New values prefer to keep them holding.
        self._universals: dict[tuple[str, str], set[tuple[int, bool]]] = {}
        self._memo: dict[int, str] = {}

    def _stable(self, evs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            evs, key=lambda ev: _content_key(ev, self._memo, self.gen._shape_key)
        )

    def _register(self, children: Iterable[dict[str, Any]]) -> None:
        for child in children:
            d = child.get("details") or {}
            if child["type"] == "for_all_held":
                rule = (d["qualifier"], True)
            elif (
                child["type"] == "count_held" and d.get("max") == 0 and not d.get("min")
            ):
                rule = (d["qualifier"], False)
            else:
                continue
            key = (_nt(d["node"]), json.dumps(d["path"], sort_keys=True))
            self._universals.setdefault(key, set()).add(rule)

    def components(self) -> Iterator[tuple[int, Alt]]:
        """``VIO`` of the target, tagged with the top-level conjunct each
        alternative breaks, interleaved across conjuncts."""
        root = self.target.evidence
        d = root.get("details") or {}
        if root["type"] == "all_held":
            self._register(d["children"])
            parts = [
                ((c.get("details") or {}).get("shape", d["shape"]), c)
                for c in self._stable(d["children"])
            ]
        else:
            parts = [(d.get("shape", self.target.constraint), root)]
        live = [_tagged(cid, self.vio(ev, 1)) for cid, ev in parts]
        while live:
            nxt = []
            for g in live:
                item = next(g, None)
                if item is not None:
                    nxt.append(g)
                    yield item
            live = nxt

    # combinators

    def _union(self, gens: Iterable[Iterator[Alt]]) -> Iterator[Alt]:
        """Round-robin interleaving, so a capped prefix samples every branch."""
        live = list(gens)
        produced = 0
        while live and produced < self.lim.max_alternatives:
            nxt = []
            for g in live:
                alt = next(g, None)
                if alt is None:
                    continue
                nxt.append(g)
                yield alt
                produced += 1
                if produced >= self.lim.max_alternatives:
                    return
            live = nxt

    def _product(
        self, factories: Sequence[Callable[[], Iterator[Alt]]]
    ) -> Iterator[Alt]:
        """Joint choice: one alternative from every factory, merged."""
        pools = []
        for f in factories:
            pool = list(itertools.islice(f(), self.lim.max_alternatives))
            if not pool:
                return
            self.rng.shuffle(pool)
            pools.append(pool)
        produced = 0
        for combo in itertools.product(*pools):
            merged = _merge(combo)
            if merged is None:
                continue
            yield merged
            produced += 1
            if produced >= self.lim.max_combinations:
                return

    def _choose(self, items: Sequence[Any], k: int) -> list[Any]:
        if len(items) <= k:
            return list(items)
        return self.rng.sample(list(items), k)

    # VIO: make a holding shape fail

    def vio(self, ev: dict[str, Any], depth: int) -> Iterator[Alt]:
        if depth > self.lim.max_depth:
            return iter(())
        kind = ev["type"]
        d = ev.get("details") or {}
        if kind == "all_held":
            self._register(d["children"])
            return self._union(
                self.vio(c, depth + 1) for c in self._stable(d["children"])
            )
        if kind == "any_held":
            return self._product(
                [_bind(self.vio, c, depth + 1) for c in self._stable(d["satisfied"])]
            )
        if kind == "not_held":
            return self.sat(d["inner_fails"], depth + 1)
        if kind == "for_all_held":
            return self._vio_forall(d, depth)
        if kind == "count_held":
            return self._vio_count(d, depth)
        if kind == "blocked" and d.get("reason") == "ClosedNeedsAdd":
            return self._vio_closed(d)
        # irrefutable, coinductive, focus-level atoms, relational, and SPARQL
        # constraints have no edit that falsifies them here.
        return iter(())

    def _vio_forall(self, d: dict[str, Any], depth: int) -> Iterator[Alt]:
        owner = _nt(d["node"])
        q = d["qualifier"]
        branches: list[Iterator[Alt]] = []
        for value, support, trace in self._choose(
            _by_term(d["values"]), self.lim.max_values
        ):
            val = self._value(owner, value, support, q, d["shape"])
            branches.append(self._substitute(val, want=False, polarity="violate"))
            branches.append(self.vio(trace, depth + 1))
        existing = {_nt(v[0]) for v in d["values"]}
        branches.append(
            self._add_values(
                owner,
                d["path"],
                q,
                d["shape"],
                want=False,
                need=1,
                exclude=existing,
                polarity="violate",
                depth=depth,
            )
        )
        return self._union(branches)

    def _vio_count(self, d: dict[str, Any], depth: int) -> Iterator[Alt]:
        owner = _nt(d["node"])
        q = d["qualifier"]
        lo, hi = d.get("min"), d.get("max")
        observed = d["observed_count"]
        branches: list[Iterator[Alt]] = []
        if lo:
            # ∃≥n π.q holds: knock out observed - n + 1 of the qualifying values.
            need = observed - lo + 1
            matches = self._choose(
                _by_term(d["matches"]), max(self.lim.max_values, need)
            )
            per_value = []
            for value, support, trace in matches:
                val = self._value(owner, value, support, q, d["shape"])
                per_value.append((val, trace))
            branches.append(self._knock_out(per_value, need, depth, q))
        if hi is not None:
            # ∃≤n π.q holds: make n - observed + 1 more values qualify.
            need = hi - observed + 1
            qualifying = {_nt(m[0]) for m in d["matches"]}
            branches.append(
                self._add_values(
                    owner,
                    d["path"],
                    q,
                    d["shape"],
                    want=True,
                    need=need,
                    exclude=qualifying,
                    polarity="violate",
                    depth=depth,
                )
            )
        return self._union(branches)

    def _knock_out(
        self,
        per_value: list[tuple[_Value, dict[str, Any]]],
        need: int,
        depth: int,
        q: int,
    ) -> Iterator[Alt]:
        """Every way to make ``need`` of these values stop counting (Rule 2):
        cut the edge that reaches it, substitute a non-qualifying value, or
        break the qualifier at the value."""
        top = self._is_top(q)

        def options(val: _Value, trace: dict[str, Any]) -> Callable[[], Iterator[Alt]]:
            def gen() -> Iterator[Alt]:
                parts = [self._cut(val)]
                if not top:
                    parts.append(self._substitute(val, want=False, polarity="violate"))
                    parts.append(self.vio(trace, depth + 1))
                return self._union(parts)

            return gen

        subsets = itertools.combinations(range(len(per_value)), need)
        alts: list[Iterator[Alt]] = []
        for subset in itertools.islice(subsets, self.lim.max_combinations):
            alts.append(self._product([options(*per_value[i]) for i in subset]))
        return self._union(alts)

    def _vio_closed(self, d: dict[str, Any]) -> Iterator[Alt]:
        owner = _nt(d["node"])
        if owner.startswith('"'):
            return
        allowed = {f"<{p['value']}>" for p in self._shape(d["shape"]).get("Closed", [])}
        preds = [p for p in self.index.predicates() if p not in allowed]
        for p in self._choose(sorted(preds), self.lim.max_candidates):
            objects = self.index.sorted_objects_of(p)
            if not objects:
                continue
            o = self.rng.choice(objects)
            t = (owner, p, o)
            if t in self.index.triples:
                continue
            yield (
                Operation(
                    kind="add_forbidden",
                    node=owner,
                    constraint=d["shape"],
                    polarity="violate",
                    adds=(t,),
                    detail=f"add {_short(p)} outside the closed shape",
                ),
            )

    # SAT: make a failing shape hold

    def sat(self, ev: dict[str, Any], depth: int) -> Iterator[Alt]:
        if depth > self.lim.max_depth:
            return iter(())
        kind = ev["type"]
        d = ev.get("details") or {}
        if kind == "all":
            return self._product(
                [_bind(self.sat, c, depth + 1) for c in self._stable(d["failed"])]
            )
        if kind == "any":
            return self._union(
                self.sat(c, depth + 1) for c in self._stable(d["branches"])
            )
        if kind == "not":
            return self.vio(d["inner"], depth + 1)
        if kind == "count_low":
            return self._sat_count_low(d, depth)
        if kind == "count_high":
            return self._sat_count_high(d, depth)
        return iter(())

    def _sat_count_low(self, d: dict[str, Any], depth: int) -> Iterator[Alt]:
        owner = _nt(d["node"])
        q = d["qualifier"]
        need = d["min"] - d["have"]
        units: list[tuple[str, Iterator[Alt]]] = []
        rejected = sorted(d["rejected_candidates"], key=lambda c: _nt(c["value"]))
        for cand in self._choose(rejected, self.lim.max_values):
            val = self._value(owner, cand["value"], cand["path_support"], q, d["shape"])
            units.append(
                (
                    val.term,
                    self._union(
                        [
                            self._substitute(
                                val,
                                want=True,
                                polarity="satisfy",
                                also=d.get("sibling_qualifiers", []),
                            ),
                            self.sat(cand["failure"], depth + 1),
                        ]
                    ),
                )
            )
        exclude = {_nt(m["value"]) for m in d.get("qualifying_matches", [])}
        exclude |= {u for u, _ in units}
        return self._union(
            [
                self._combine(units, need) if units else iter(()),
                self._add_values(
                    owner,
                    d["path"],
                    q,
                    d["shape"],
                    want=True,
                    need=need,
                    exclude=exclude,
                    polarity="satisfy",
                    also=d.get("sibling_qualifiers", []),
                ),
            ]
        )

    def _sat_count_high(self, d: dict[str, Any], depth: int) -> Iterator[Alt]:
        owner = _nt(d["node"])
        q = d["qualifier"]
        excess = len(d["matched"]) - d["max"]
        per_value = []
        for value, support in _by_term(d["matched"]):
            val = self._value(owner, value, support, q, d["shape"])
            holding = self.gen._evidence(val.term, q)
            trace = (
                holding["evidence"] if holding and holding["status"] == "pass" else None
            )
            per_value.append((val, trace))

        def options(
            val: _Value, trace: Optional[dict[str, Any]]
        ) -> Callable[[], Iterator[Alt]]:
            def gen() -> Iterator[Alt]:
                parts = [self._cut(val, polarity="satisfy")]
                if trace is not None:
                    parts.append(self.vio(trace, depth + 1))
                return self._union(parts)

            return gen

        chosen = self._choose(per_value, max(self.lim.max_values, excess))
        subsets = itertools.combinations(range(len(chosen)), excess)
        return self._union(
            self._product([options(*chosen[i]) for i in subset])
            for subset in itertools.islice(subsets, self.lim.max_combinations)
        )

    # leaf operations

    def _cut(self, val: _Value, polarity: str = "violate") -> Iterator[Alt]:
        for edge in val.support:
            if edge in self.index.triples:
                yield (
                    Operation(
                        kind="remove",
                        node=val.owner,
                        constraint=val.constraint,
                        polarity=polarity,
                        deletes=(edge,),
                        detail=f"remove {_short(edge[1])} {_short(edge[2])}",
                    ),
                )

    def _substitute(
        self,
        val: _Value,
        *,
        want: bool,
        polarity: str,
        also: Sequence[int] = (),
    ) -> Iterator[Alt]:
        """Replace ``val`` on its incoming edge with a term whose qualifier
        status is ``want``."""
        edge = val.edge
        if edge is None or edge not in self.index.triples:
            return
        if edge[2] == val.term:
            position = 2
        elif edge[0] == val.term:
            position = 0
        else:
            return
        for cand in self._candidates(val.term, val.qualifier, want, edge[1], also):
            if position == 0 and cand.startswith('"'):
                continue
            new = (
                (cand, edge[1], edge[2]) if position == 0 else (edge[0], edge[1], cand)
            )
            if new in self.index.triples:
                continue
            yield (
                Operation(
                    kind="substitute",
                    node=val.owner,
                    constraint=val.constraint,
                    polarity=polarity,
                    deletes=(edge,),
                    adds=(new,),
                    requires=(("satisfy" if want else "violate", cand, val.qualifier),),
                    detail=f"replace {_short(val.term)} with {_short(cand)}",
                ),
            )

    def _add_values(
        self,
        owner: str,
        path: Any,
        q: int,
        constraint: int,
        *,
        want: bool,
        need: int,
        exclude: set[str],
        polarity: str,
        also: Sequence[int] = (),
        depth: int = 0,
    ) -> Iterator[Alt]:
        """Make ``need`` more values reached along ``path`` have qualifier
        status ``want``: link new values, or change existing reached ones."""
        if need < 1:
            return iter(())
        word = _minimal_word(path)
        units: list[tuple[str, Iterator[Alt]]] = []
        reached = self.index.eval_path(owner, path)
        # Reached values not yet counted can be fixed in place.
        if want:
            for v in self._choose(sorted(reached - exclude), self.lim.max_values):
                ev = self.gen._evidence(v, q)
                if ev is None or ev["status"] != "fail":
                    continue
                fixes = [self.sat(ev["evidence"], depth + 1)]
                edge = _single_edge(owner, word, v)
                if edge is not None:
                    # Or swap the value for one that counts, e.g. replace a
                    # status in sh:in with one outside it.
                    val = _Value(owner, v, edge, (edge,), q, constraint)
                    fixes.append(
                        self._substitute(val, want=True, polarity=polarity, also=also)
                    )
                units.append((v, self._union(fixes)))
        if word is not None:
            seed_value = next(iter(sorted(reached)), None)
            pred = word[-1][0] if word else None
            key = (owner, json.dumps(path, sort_keys=True))
            prefer = sorted(
                self._universals.get(key, set()) - {(q, want)},
                key=lambda rule: (self.gen._shape_key(rule[0]), rule[1]),
            )
            cands = self._candidates(
                seed_value,
                q,
                want,
                pred,
                also,
                limit=max(self.lim.max_candidates, need),
                prefer=prefer,
                need=need,
            )
            for cand in cands:
                if cand in reached or cand in exclude:
                    continue
                triples = self._materialize(owner, word, cand)
                if triples is None or any(t in self.index.triples for t in triples):
                    continue
                op = Operation(
                    kind="add_value",
                    node=owner,
                    constraint=constraint,
                    polarity=polarity,
                    adds=triples,
                    requires=(("satisfy" if want else "violate", cand, q),),
                    detail=f"add {_short(cand)} via {' / '.join(_short(p) for p, _ in word)}",
                )
                units.append((cand, iter([(op,)])))
        if not units:
            return iter(())
        return self._combine(units, need)

    def _combine(
        self, units: list[tuple[str, Iterator[Alt]]], need: int
    ) -> Iterator[Alt]:
        """Choose ``need`` units over distinct values and merge their edits."""
        pools = [
            (term, list(itertools.islice(it, self.lim.max_candidates)))
            for term, it in units
        ]
        pools = [(term, alts) for term, alts in pools if alts]
        if len(pools) < need:
            return
        produced = 0
        for subset in itertools.combinations(pools, need):
            for combo in itertools.product(*(alts for _, alts in subset)):
                merged = _merge(combo)
                if merged is None:
                    continue
                yield merged
                produced += 1
                if produced >= self.lim.max_combinations:
                    return
                break  # one joint choice per subset keeps subsets diverse

    def _materialize(
        self, owner: str, word: list[tuple[str, bool]], value: str
    ) -> Optional[tuple[Triple, ...]]:
        nodes = [owner]
        for i in range(len(word) - 1):
            nodes.append(self._fresh(f"{owner}|{value}|{i}"))
        nodes.append(value)
        out = []
        for (pred, inverse), a, b in zip(word, nodes, nodes[1:]):
            s, o = (b, a) if inverse else (a, b)
            if s.startswith('"'):
                return None
            out.append((s, pred, o))
        return tuple(out)

    # candidate terms

    def _candidates(
        self,
        current: Optional[str],
        q: int,
        want: bool,
        pred: Optional[str],
        also: Sequence[int] = (),
        limit: Optional[int] = None,
        prefer: Sequence[tuple[int, bool]] = (),
        need: int = 1,
    ) -> list[str]:
        """Terms whose status for qualifier ``q`` is ``want``, checked by the
        engine. ``also`` lists sibling qualifiers they must satisfy too.

        Terms that also keep each ``prefer`` rule come first, and the others
        fill in only when fewer than ``need`` do. A value added to break a
        maximum count then stays well formed for the other constraints on
        that path, so the case is a cardinality error rather than a mix."""
        limit = limit or self.lim.max_candidates
        pool = self._proposals(current, q, pred, [r for r, _ in prefer])
        good: list[str] = []
        fallback: list[str] = []
        for cand in pool:
            if cand == current:
                continue
            if self.gen._holds(cand, q) is not want:
                continue
            if any(self.gen._holds(cand, s) is not True for s in also):
                continue
            if all(self.gen._holds(cand, r) is w for r, w in prefer):
                good.append(cand)
                if len(good) >= limit:
                    break
            elif len(fallback) < limit:
                fallback.append(cand)
        if len(good) >= need:
            return good
        return (good + fallback)[:limit]

    def _proposals(
        self,
        current: Optional[str],
        q: int,
        pred: Optional[str],
        hints: Sequence[int] = (),
    ) -> list[str]:
        consts, facets, classes = self._leaves(q)
        for hint in hints:
            more = self._leaves(hint)
            consts += more[0]
            facets += more[1]
            classes += more[2]
        consts = sorted(set(consts))
        classes = sorted(set(classes))
        facets.sort(key=lambda f: json.dumps(f, sort_keys=True))
        # With no current value, a value of the same predicate elsewhere
        # tells what kind of term the predicate takes.
        siblings = self.index.sorted_objects_of(pred) if pred is not None else []
        ref = current if current is not None else (siblings[0] if siblings else None)
        lex, dt, lang = _literal_parts(ref) if ref else ("x", None, None)
        if ref and not ref.startswith('"'):
            lex = _local_name(ref)
        props: list[str] = []
        props.extend(consts)
        for cls in classes:
            props.extend(self._choose(self.index.sorted_instances_of(cls), 3))
        for facet in facets:
            props.extend(_facet_proposals(facet, lex, dt, lang))
        props.extend(_mutations(ref, lex, dt, lang))
        props.extend(self._choose(siblings, 4))
        if current is not None and not current.startswith('"'):
            for cls in sorted(self.index.types_of(current)):
                props.extend(self._choose(self.index.sorted_instances_of(cls), 3))
        props.extend(self._choose(self.index.sorted_typed_nodes(), 4))
        props.append(self._fresh(f"{current}|{pred}"))
        seen: set[str] = set()
        out = []
        for p in props:
            if p not in seen:
                seen.add(p)
                out.append(p)
        if ref is not None:
            # Prefer replacements of the same kind, so an object property
            # keeps getting nodes and a datatype property keeps getting
            # literals. Other kinds remain as a fallback.
            literal = ref.startswith('"')
            out.sort(key=lambda t: t.startswith('"') != literal)
        return out

    def _leaves(self, q: int) -> tuple[list[str], list[Any], list[str]]:
        """Constants, value-type facets, and classes mentioned under ``q``."""
        consts: list[str] = []
        facets: list[Any] = []
        classes: list[str] = []
        stack, seen = [q], set()
        while stack:
            sid = stack.pop()
            if sid in seen:
                continue
            seen.add(sid)
            shape = self._shape(sid)
            if not isinstance(shape, dict):
                continue
            if "TestConst" in shape:
                consts.append(_nt(shape["TestConst"]))
            elif "TestType" in shape:
                facets.extend(_flatten_facets(shape["TestType"]))
            elif "Not" in shape:
                stack.append(shape["Not"])
            elif "And" in shape or "Or" in shape:
                stack.extend(shape.get("And") or shape.get("Or") or [])
            elif "Count" in shape:
                # Only class membership speaks about the value itself. Other
                # counts constrain the value's own neighbours.
                count = shape["Count"]
                if _is_type_path(count["path"]):
                    inner = self._shape(count["qualifier"])
                    if isinstance(inner, dict) and "TestConst" in inner:
                        classes.append(_nt(inner["TestConst"]))
        facets.sort(key=lambda f: json.dumps(f, sort_keys=True))
        return sorted(set(consts)), facets, sorted(set(classes))

    def _shape(self, sid: int) -> Any:
        shape = self.gen._catalog.get(sid)
        while isinstance(shape, dict) and "Annotated" in shape:
            shape = self.gen._catalog.get(shape["Annotated"]["shape"])
        return shape

    def _is_top(self, sid: int) -> bool:
        return self._shape(sid) == "Top"

    def _value(
        self, owner: str, value: Any, support: Any, q: int, constraint: int
    ) -> _Value:
        edges = tuple(_support_edges(support))
        return _Value(
            owner=owner,
            term=_nt(value),
            edge=edges[-1] if edges else None,
            support=edges,
            qualifier=q,
            constraint=constraint,
        )

    def _fresh(self, hint: str) -> str:
        focus = self.target.focus
        base = focus[1:-1] if focus.startswith("<") else "urn:multivio:"
        cut = max(base.rfind("/"), base.rfind("#"), base.rfind(":"))
        ns = base[: cut + 1] if cut >= 0 else base
        digest = hashlib.blake2b(hint.encode(), digest_size=4).hexdigest()
        return f"<{ns}vio_{digest}>"


def _content_key(ev: Any, memo: dict[int, str], shape_key: Callable[[int], str]) -> str:
    """A digest of an evidence subtree that names shapes by structure.

    Normalized ids can differ between processes, and the expansion caps keep
    a prefix of each choice point, so siblings are ordered by content for a
    seed to reproduce. Each arena id is replaced by ``shape_key`` of the
    shape it names, so two counts that differ only in their qualifier still
    order apart. ``memo`` caches digests by object identity for one
    traversal."""
    if isinstance(ev, dict):
        cached = memo.get(id(ev))
        if cached is not None:
            return cached
        parts = []
        for k, v in sorted(ev.items()):
            if k in ("shape", "qualifier") and isinstance(v, int):
                parts.append(f"{k}={shape_key(v)}")
            elif k == "sibling_qualifiers" and isinstance(v, list):
                parts.append(f"{k}={sorted(shape_key(i) for i in v)}")
            else:
                parts.append(f"{k}={_content_key(v, memo, shape_key)}")
        key = hashlib.blake2b(
            ("{" + ",".join(parts) + "}").encode(), digest_size=12
        ).hexdigest()
        memo[id(ev)] = key
        return key
    if isinstance(ev, list):
        return "[" + ",".join(_content_key(v, memo, shape_key) for v in ev) + "]"
    return json.dumps(ev)


def _by_term(rows: Iterable[Sequence[Any]]) -> list[Sequence[Any]]:
    return sorted(rows, key=lambda row: _nt(row[0]))


def _tagged(tag: int, alts: Iterator[Alt]) -> Iterator[tuple[int, Alt]]:
    for alt in alts:
        yield (tag, alt)


def _bind(
    fn: Callable[[dict[str, Any], int], Iterator[Alt]], ev: dict[str, Any], depth: int
) -> Callable[[], Iterator[Alt]]:
    return lambda: fn(ev, depth)


def _merge(combo: Iterable[Alt]) -> Optional[Alt]:
    """Merge a joint choice. Returns ``None`` when the parts contradict: one
    deletes what another adds, or the parts demand opposite polarities."""
    ops: list[Operation] = []
    seen_ops: set[Operation] = set()
    deletes: set[Triple] = set()
    adds: set[Triple] = set()
    reqs: set[Requirement] = set()
    replaced: dict[Triple, tuple[Triple, ...]] = {}
    for alt in combo:
        for op in alt:
            if op in seen_ops:
                continue
            if deletes & set(op.adds) or adds & set(op.deletes):
                return None
            if op.kind == "substitute":
                # Two different replacements for one value would add both.
                if replaced.setdefault(op.deletes[0], op.adds) != op.adds:
                    return None
            new_reqs = {(op.polarity, op.node, op.constraint), *op.requires}
            if any((_flip(p), n, c) in reqs for p, n, c in new_reqs):
                return None
            seen_ops.add(op)
            ops.append(op)
            deletes.update(op.deletes)
            adds.update(op.adds)
            reqs.update(new_reqs)
    return tuple(ops)


def _flip(polarity: str) -> str:
    return "satisfy" if polarity == "violate" else "violate"


# ── Graph index and paths ────────────────────────────────────────────────────


class _Index:
    def __init__(self, triples: frozenset[Triple]) -> None:
        self.triples = triples
        self.out: dict[str, dict[str, set[str]]] = {}
        self.inc: dict[str, dict[str, set[str]]] = {}
        for s, p, o in triples:
            self.out.setdefault(s, {}).setdefault(p, set()).add(o)
            self.inc.setdefault(o, {}).setdefault(p, set()).add(s)
        self.by_pred: dict[str, set[str]] = {}
        for _, p, o in triples:
            self.by_pred.setdefault(p, set()).add(o)
        self._preds = sorted(self.by_pred)
        self._typed = {s for (s, p, _) in triples if p == _RDF_TYPE}
        self._sorted_cache: dict[tuple[str, str], list[str]] = {}

    def predicates(self) -> list[str]:
        return self._preds

    def objects_of(self, pred: str) -> set[str]:
        return self.by_pred.get(pred, set())

    def sorted_objects_of(self, pred: str) -> list[str]:
        return self._sorted(("objects", pred), lambda: self.objects_of(pred))

    def sorted_instances_of(self, cls: str) -> list[str]:
        return self._sorted(("instances", cls), lambda: self.instances_of(cls))

    def sorted_typed_nodes(self) -> list[str]:
        return self._sorted(("typed", ""), lambda: self._typed)

    def _sorted(self, key: tuple[str, str], items: Callable[[], set[str]]) -> list[str]:
        cached = self._sorted_cache.get(key)
        if cached is None:
            cached = self._sorted_cache[key] = sorted(items())
        return cached

    def types_of(self, node: str) -> set[str]:
        return self.out.get(node, {}).get(_RDF_TYPE, set())

    def instances_of(self, cls: str) -> set[str]:
        return self.inc.get(cls, {}).get(_RDF_TYPE, set())

    def typed_nodes(self) -> set[str]:
        return self._typed

    def step(self, nodes: set[str], path: Any) -> set[str]:
        if path == "Id":
            return set(nodes)
        if "Pred" in path:
            p = f"<{path['Pred']['value']}>"
            return {o for n in nodes for o in self.out.get(n, {}).get(p, ())}
        if "Inverse" in path:
            inner = path["Inverse"]
            if isinstance(inner, dict) and "Pred" in inner:
                p = f"<{inner['Pred']['value']}>"
                return {s for n in nodes for s in self.inc.get(n, {}).get(p, ())}
            return self._inverse(nodes, inner)
        if "Seq" in path:
            cur = set(nodes)
            for part in path["Seq"]:
                cur = self.step(cur, part)
            return cur
        if "Alt" in path:
            return set().union(*(self.step(nodes, part) for part in path["Alt"]))
        if "Star" in path:
            seen = set(nodes)
            frontier = set(nodes)
            while frontier:
                frontier = self.step(frontier, path["Star"]) - seen
                seen |= frontier
            return seen
        return set()

    def _inverse(self, nodes: set[str], path: Any) -> set[str]:
        # Inverse of a complex path: test every subject that reaches a node.
        subjects = set(self.out)
        return {s for s in subjects if self.step({s}, path) & nodes}

    def eval_path(self, node: str, path: Any) -> set[str]:
        return self.step({node}, path)


def _single_edge(
    owner: str, word: Optional[list[tuple[str, bool]]], value: str
) -> Optional[Triple]:
    """The one edge linking ``owner`` to ``value`` along a one-step path."""
    if word is None or len(word) != 1:
        return None
    pred, inverse = word[0]
    return (value, pred, owner) if inverse else (owner, pred, value)


def _minimal_word(path: Any) -> Optional[list[tuple[str, bool]]]:
    """The shortest predicate word through ``path``: stars taken reflexively,
    the first alternative taken. ``None`` when no edit can realize it."""
    if path == "Id":
        return []
    if "Pred" in path:
        return [(f"<{path['Pred']['value']}>", False)]
    if "Inverse" in path:
        inner = _minimal_word(path["Inverse"])
        return None if inner is None else [(p, not inv) for p, inv in reversed(inner)]
    if "Seq" in path:
        out: list[tuple[str, bool]] = []
        for part in path["Seq"]:
            w = _minimal_word(part)
            if w is None:
                return None
            out.extend(w)
        return out or None
    if "Alt" in path:
        for part in path["Alt"]:
            w = _minimal_word(part)
            if w:
                return w
        return None
    if "Star" in path:
        return []
    return None


def _is_type_path(path: Any) -> bool:
    word = _minimal_word(path)
    return word == [(_RDF_TYPE, False)]


def _support_edges(support: Any) -> Iterator[Triple]:
    if not isinstance(support, dict):
        return
    kind = support.get("type")
    if kind == "edge":
        d = support["details"]
        yield (_nt(d["subject"]), _nt(d["predicate"]), _nt(d["object"]))
    elif kind == "chain":
        for part in support.get("details") or []:
            yield from _support_edges(part)
    elif kind == "alt":
        parts = support.get("details") or []
        if parts:
            yield from _support_edges(parts[0])


# ── Candidate proposals ──────────────────────────────────────────────────────


def _flatten_facets(vt: Any) -> list[Any]:
    if isinstance(vt, dict) and "And" in vt:
        return [f for part in vt["And"] for f in _flatten_facets(part)]
    return [vt]


def _facet_proposals(
    facet: Any, lex: str, dt: Optional[str], lang: Optional[str]
) -> list[str]:
    if not isinstance(facet, dict):
        return []
    out: list[str] = []
    if "Datatype" in facet:
        target = facet["Datatype"]["value"]
        out.append(_literal(lex, target))
        out.extend(_literal(sample, target) for sample in _samples(target))
        if target == _XSD_STRING:
            out.append(_literal(lex, None, "en"))
            out.append(_literal("1", _XSD + "integer"))
        else:
            out.append(_literal(lex, None))
    elif "Length" in facet:
        lo, hi = facet["Length"].get("min"), facet["Length"].get("max")
        if lo:
            out.append(_literal(lex[: lo - 1], dt, lang))
            out.append(_literal((lex or "x").ljust(lo, "x"), dt, lang))
        if hi is not None:
            out.append(_literal(lex.ljust(hi + 1, "x"), dt, lang))
            out.append(_literal(lex[:hi], dt, lang))
    elif "Pattern" in facet:
        out.append(_literal("", dt, lang))
        out.append(_literal(lex + " ~", dt, lang))
    elif "NumericRange" in facet:
        for side in ("lo", "hi"):
            bound = facet["NumericRange"].get(side)
            if not bound:
                continue
            value = bound["value"]
            bdt = value.get("datatype", _XSD + "integer")
            for delta in (-1, 0, 1):
                shifted = _shift_number(value["value"], bdt, delta)
                if shifted is not None:
                    out.append(_literal(shifted, bdt))
    elif "LangIn" in facet:
        for tag in facet["LangIn"]:
            out.append(_literal(lex, None, tag))
        out.append(_literal(lex, None, "zz"))
        out.append(_literal(lex, None))
    return out


_SAMPLES = {
    "boolean": ("true", "false"),
    "decimal": ("1.0",),
    "double": ("1.0E0",),
    "float": ("1.0E0",),
    "dateTime": ("2024-01-01T00:00:00",),
    "date": ("2024-01-01",),
    "time": ("00:00:00",),
    "duration": ("PT1H",),
    "anyURI": ("http://example.org/x",),
    "gYear": ("2024",),
}


def _samples(datatype: str) -> tuple[str, ...]:
    """Well-formed lexical forms for common XSD datatypes, so a proposal can
    satisfy ``sh:datatype`` when no existing value has that type."""
    if datatype in _INTEGRAL:
        return ("0", "1")
    if datatype.startswith(_XSD):
        return _SAMPLES.get(datatype[len(_XSD) :], ())
    return ()


def _mutations(
    current: Optional[str], lex: str, dt: Optional[str], lang: Optional[str]
) -> list[str]:
    if current is None:
        return [_literal("x", None)]
    if current.startswith('"'):
        out = []
        if dt in _NUMERIC:
            shifted = _shift_number(lex, dt, 1)
            if shifted is not None:
                out.append(_literal(shifted, dt))
        out.append(_literal(lex + "_x", dt, lang))
        out.append(_literal(lex[: max(0, len(lex) // 2)], dt, lang))
        return out
    return [_literal(lex, None)]


def _shift_number(lex: str, dt: str, delta: int) -> Optional[str]:
    try:
        if dt in _INTEGRAL:
            return str(int(lex) + delta)
        return repr(float(lex) + delta)
    except ValueError:
        return None


# ── Terms and N-Triples ──────────────────────────────────────────────────────

_TERM_RE = re.compile(
    r'<[^>]*>|_:[^\s]+|"(?:[^"\\]|\\.)*"(?:@[A-Za-z0-9-]+|\^\^<[^>]*>)?'
)
_LIT_RE = re.compile(r'^"((?:[^"\\]|\\.)*)"(?:@([A-Za-z0-9-]+)|\^\^<([^>]*)>)?$')


def _parse_ntriples(text: str) -> Iterator[Triple]:
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        terms = _TERM_RE.findall(line)
        if len(terms) >= 3:
            yield (terms[0], terms[1], terms[2])


def _line(t: Triple) -> str:
    return f"{t[0]} {t[1]} {t[2]} ."


def _delta(add: Iterable[Triple], delete: Iterable[Triple]) -> RepairDelta:
    return RepairDelta.from_ntriples(
        add="".join(f"{_line(t)}\n" for t in sorted(add)),
        delete="".join(f"{_line(t)}\n" for t in sorted(delete)),
    )


def _union_delta(exprs: Iterable[ViolationExpression]) -> RepairDelta:
    exprs = list(exprs)
    return _delta(
        {t for e in exprs for t in e.adds}, {t for e in exprs for t in e.deletes}
    )


def _nt(term: Any) -> str:
    kind = term["type"]
    value = term["value"]
    if kind == "uri":
        return f"<{value}>"
    if kind == "bnode":
        return f"_:{value}"
    return _literal(value, term.get("datatype"), term.get("language"))


def _escape(lex: str) -> str:
    return (
        lex.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _unescape(lex: str) -> str:
    def repl(m: re.Match[str]) -> str:
        s = m.group(0)
        if s[1] in "uU":
            return chr(int(s[2:], 16))
        return {"n": "\n", "r": "\r", "t": "\t", '"': '"', "\\": "\\"}.get(s[1], s[1])

    return re.sub(r"\\(?:u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)", repl, lex)


def _literal(lex: str, dt: Optional[str] = None, lang: Optional[str] = None) -> str:
    body = f'"{_escape(lex)}"'
    if lang:
        return f"{body}@{lang}"
    if dt and dt != _XSD_STRING:
        return f"{body}^^<{dt}>"
    return body


def _literal_parts(term: str) -> tuple[str, Optional[str], Optional[str]]:
    m = _LIT_RE.match(term)
    if not m:
        return (_local_name(term), None, None)
    return (_unescape(m.group(1)), m.group(3), m.group(2))


def _local_name(term: str) -> str:
    iri = term.strip("<>")
    cut = max(iri.rfind("/"), iri.rfind("#"), iri.rfind(":"))
    return iri[cut + 1 :] or iri


def _short(term: str) -> str:
    if term.startswith("<"):
        return _local_name(term)
    return term


def _iri_term(iri: str) -> str:
    return iri if iri.startswith("<") else f"<{iri}>"


def _bnodes_as_vars(body: str) -> str:
    return re.sub(r"_:([^\s]+)", lambda m: "?b_" + re.sub(r"\W", "_", m.group(1)), body)


def _unit_hash(salt: str, key: str) -> float:
    digest = hashlib.blake2b(f"{salt}\x00{key}".encode(), digest_size=8).digest()
    return (int.from_bytes(digest, "big") + 1) / 2.0**64


def _failing_pairs(run: Any) -> set[tuple[str, int]]:
    return {
        (fe.focus, st.source_statement_id)
        for st in run.statements
        for fe in st.selected_foci
        if fe.status == "fail"
    }


def _failing_components(run: Any) -> dict[tuple[str, int], set[int]]:
    """Failing pairs mapped to the ids of their failing top-level conjuncts."""
    out: dict[tuple[str, int], set[int]] = {}
    for st in run.statements:
        for fe in st.selected_foci:
            if fe.status != "fail":
                continue
            ev = fe.evidence.to_dict()["evidence"]
            d = ev.get("details") or {}
            ids = {d.get("shape")}
            if ev["type"] == "all":
                ids |= {
                    (c.get("details") or {}).get("shape") for c in d.get("failed", [])
                }
            out[(fe.focus, st.source_statement_id)] = {
                i for i in ids if isinstance(i, int)
            }
    return out


def _realized(
    expr: ViolationExpression, failures: dict[tuple[str, int], set[int]]
) -> bool:
    return expr.component in failures.get(expr.target, ())


def inverse_frequency_weights(
    expressions: Iterable[ViolationExpression],
) -> dict[str, float]:
    """Per-predicate weights ``1 / frequency`` over a universe of expressions.
    Feeding them back as ``predicate_weights`` spreads a sample across
    predicates instead of following their natural frequency."""
    freq: Counter[str] = Counter()
    for expr in expressions:
        freq.update(expr.predicates)
    total = sum(freq.values()) or 1
    return {p: total / n for p, n in freq.items()}


# ── CLI ──────────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> int:
    """``python -m shifty.multivio``: generate a benchmark suite."""
    parser = argparse.ArgumentParser(
        prog="python -m shifty.multivio",
        description="Inject SHACL violations with known repairs into a conforming graph.",
    )
    parser.add_argument("--shapes", required=True, help="shapes graph (path or URL)")
    parser.add_argument("--data", required=True, help="conforming reference graph")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument(
        "-k",
        "--budget",
        type=int,
        action="append",
        help="injection budget K; repeat for several levels (default 5)",
    )
    parser.add_argument("-n", "--cases", type=int, default=10, help="cases per budget")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--reservoir", type=int, help="reservoir size M (default 8K, min 64)"
    )
    parser.add_argument(
        "--weights", help="JSON object mapping predicate IRIs to weights"
    )
    parser.add_argument(
        "--kind-weights",
        help="JSON object mapping operation kinds (remove, substitute, add_value, "
        "add_forbidden) to weights",
    )
    parser.add_argument(
        "--inverse-frequency",
        action="store_true",
        help="weight predicates by inverse frequency in the universe",
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="run SHACL-AF rules once and use the closure as the reference graph",
    )
    parser.add_argument(
        "--infer", action="store_true", help="re-run SHACL-AF rules on every validation"
    )
    parser.add_argument("--no-verify", action="store_true", help="skip revalidation")
    args = parser.parse_args(argv)

    def load(path: Optional[str]) -> dict[str, float]:
        if not path:
            return {}
        return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))

    gen = ViolationGenerator(
        args.shapes,
        args.data,
        seed=args.seed,
        predicate_weights=load(args.weights),
        kind_weights=load(args.kind_weights),
        materialize=args.materialize,
        infer=args.infer,
    )
    if gen.baseline_failures:
        print(
            f"warning: reference graph has {len(gen.baseline_failures)} failing pairs;"
            " they are excluded from injection",
            file=sys.stderr,
        )
    universe = gen.expressions()
    if args.inverse_frequency:
        gen.reweight(
            inverse_frequency_weights(universe) | gen.predicate_weights,
            gen.kind_weights,
        )
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reference.nt").write_text(
        "".join(f"{_line(t)}\n" for t in sorted(gen.reference)), encoding="utf-8"
    )
    summary = []
    for budget in args.budget or [5]:
        for case in gen.generate(
            budget, args.cases, reservoir_size=args.reservoir, verify=not args.no_verify
        ):
            path = case.save(out / f"k{budget:02d}" / f"case-{case.index:03d}")
            summary.append(
                {
                    "path": str(path.relative_to(out)),
                    "budget": budget,
                    "injected": len(case.expressions),
                    "violations": len(case.violations),
                }
            )
    suite = {
        "universe": len(universe),
        "operation_kinds": dict(
            sorted(Counter(k for e in universe for k in e.kinds).items())
        ),
        "baseline_failures": len(gen.baseline_failures),
        "cases": summary,
    }
    (out / "suite.json").write_text(
        json.dumps(suite, indent=2) + "\n", encoding="utf-8"
    )
    print(f"{len(universe)} expressions; wrote {len(summary)} cases to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
