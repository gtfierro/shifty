"""Violation generation for knowledge-graph repair benchmarks.

**Experimental**: this module may change in any release.

It injects SHACL violations with known ground-truth repairs into a
*reference* graph that conforms to a shapes graph. It follows Lin et al.,
"Systematic Evaluation of Knowledge Graph Repair with Large Language Models"
(arXiv 2507.22419), extended to multiple interacting violations as in
"Selective Retrieval Routing for Multi-Violation Knowledge Graph Repair"
(arXiv 2611.07342). The work runs in Rust (``shifty_experimental::vio``).

There are three nouns:

:class:`Universe`
    Every way to break the reference graph, as a sequence of
    :class:`Injection` s. For every passing ``(focus, statement)`` pair, the
    satisfaction evidence explains *why* the shape holds, and ``VIO(x, φ)``
    rewrites it into edit sets that make ``φ`` fail at ``x``. A failure
    inside ``¬φ`` flips to the dual ``SAT(x, φ)``, which makes a failing
    sub-shape hold. The expansion streams: draws sample it as it runs, and
    listing the universe stores it. :meth:`Universe.where` and
    :meth:`Universe.weighted` derive views.
:class:`Injection`
    One member: :class:`Operation` s that together make one pair fail,
    identified by its net delta, :attr:`Injection.key`.
:class:`Case`
    What :meth:`Universe.draw` returns for a budget ``k`` and a seed: up to
    ``k`` compatible injections, the broken graph, and the ground-truth
    repair. Candidates come from a weighted reservoir with
    Efraimidis–Spirakis keys ``-ln h(v) / w(v)``, where ``h`` is a stable
    hash. Every injection is equally likely unless predicates or operation
    kinds are weighted. A candidate is admitted only if its edits can be
    applied with those already chosen and it makes its target fail alone.
    The composed delta is revalidated, so every :class:`CaseFailure` is one
    the engine reports.

::

    from shifty.experimental import vio

    universe = vio.Universe(reference_graph, shapes)
    zones = universe.where(shape=S223 + "Zone")
    for case in zones.draws([(5, seed) for seed in range(10)]):
        case.save(f"suite/case-{case.seed:03d}")

The generator does not repair graphs.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cached_property
from types import MappingProxyType
from typing import Any, Optional, Union, overload

from .. import RepairDelta, _coalesce_graph_input, _to_rdf_input
from .._shifty import VioCase as _VioCase
from .._shifty import VioUniverse as _VioUniverse

__all__ = [
    "Case",
    "CaseFailure",
    "ExpansionLimits",
    "Injection",
    "Operation",
    "Universe",
    "main",
]

Triple = tuple[str, str, str]
"""A triple of N-Triples-syntax terms."""

Names = Union[str, Iterable[str]]
"""One IRI, or several."""


@dataclass(frozen=True)
class Operation:
    """One atomic edit of an :class:`Injection`.

    ``kind`` is ``"remove"`` (cut an edge that supports a value),
    ``"substitute"`` (replace a value with one that fails or satisfies the
    governing qualifier), ``"add_value"`` (add a value along a path), or
    ``"add_forbidden"`` (add a triple a closed shape disallows). The edit
    makes ``node`` reach ``polarity`` for one sub-shape of the target:
    ``"violate"`` it, or ``"satisfy"`` it under a negation. ``detail``
    describes the edit for people.
    """

    kind: str
    node: str
    polarity: str
    deletes: tuple[Triple, ...] = ()
    adds: tuple[Triple, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class Injection:
    """One way to break the reference graph: operations that together make
    one ``(focus, statement)`` pair fail.

    ``focus`` is an N-Triples term, ``shape`` the statement's shape IRI when
    it has one. Two injections with the same net delta are the same
    injection, identified by :attr:`key`.
    """

    focus: str
    statement: int
    shape: Optional[str]
    operations: tuple[Operation, ...]
    key: str
    """The sorted net delta as ``- s p o .`` and ``+ s p o .`` lines.
    Stable across processes, and unique in a universe."""

    @cached_property
    def deletes(self) -> frozenset[Triple]:
        return frozenset(t for op in self.operations for t in op.deletes)

    @cached_property
    def adds(self) -> frozenset[Triple]:
        return frozenset(t for op in self.operations for t in op.adds)

    @cached_property
    def delta(self) -> RepairDelta:
        """The injection's edits as a :class:`shifty.RepairDelta`."""
        return _delta(self.adds, self.deletes)

    @cached_property
    def predicates(self) -> frozenset[str]:
        """The predicates the injection edits, as N-Triples IRIs."""
        return frozenset(t[1] for t in self.deletes | self.adds)

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(op.kind for op in self.operations)


@dataclass(frozen=True)
class CaseFailure:
    """One failing ``(focus, statement)`` pair of a case's graph, with the
    injections that realize it. ``injections`` is empty for a side effect:
    a pair that fails although no injection aimed at it."""

    focus: str
    statement: int
    shape: Optional[str]
    injections: tuple[Injection, ...]


class Case:
    """One benchmark case: a broken graph and its ground truth.

    :attr:`delta` is the injected ``Δ``, with ``G_test = G_ref ⊕ Δ``, and
    :attr:`repair` is ``Δ⁻¹``, which recovers ``G_ref``. :attr:`failures`
    lists every failing pair the engine reports for ``G_test``. It can
    exceed ``k``, since one edit can fail several statements.
    """

    def __init__(self, inner: _VioCase, shared: _Shared) -> None:
        self._inner = inner
        self.k: int = inner.k
        self.seed: int = inner.seed
        self.injections: tuple[Injection, ...] = tuple(
            shared.injection(row) for row in inner.injections
        )
        """The injections, in admission order. At most ``k``."""
        self.failures: tuple[CaseFailure, ...] = tuple(
            CaseFailure(focus, statement, shape, tuple(self.injections[i] for i in idx))
            for focus, statement, shape, idx in inner.failures
        )

    @cached_property
    def delta(self) -> RepairDelta:
        """The injected delta ``Δ``."""
        return self._inner.delta

    @cached_property
    def repair(self) -> RepairDelta:
        """The ground-truth repair ``Δ⁻¹``."""
        return self._inner.repair

    @cached_property
    def graph_ntriples(self) -> str:
        """``G_test`` as sorted N-Triples."""
        return self._inner.graph_ntriples()

    @cached_property
    def repair_sparql(self) -> str:
        """The ground-truth repair as a SPARQL 1.1 Update.

        SPARQL forbids blank nodes in ``DELETE DATA``. When the repair deletes
        a blank-node triple, this falls back to ``DELETE WHERE``, which binds
        blank nodes as variables.
        """
        return self._inner.repair_sparql()

    def to_dict(self) -> dict[str, Any]:
        """The manifest written as ``case.json``."""
        return json.loads(self._inner.to_json())

    def save(self, directory: Union[str, pathlib.Path]) -> pathlib.Path:
        """Write ``graph.nt``, ``repair.ru``, and ``case.json``."""
        out = pathlib.Path(directory)
        out.mkdir(parents=True, exist_ok=True)
        (out / "graph.nt").write_text(self.graph_ntriples, encoding="utf-8")
        (out / "repair.ru").write_text(self.repair_sparql, encoding="utf-8")
        (out / "case.json").write_text(
            json.dumps(self.to_dict(), indent=2) + "\n", encoding="utf-8"
        )
        return out

    def __repr__(self) -> str:
        return (
            f"Case(k={self.k}, seed={self.seed}, injections={len(self.injections)},"
            f" failures={len(self.failures)})"
        )


@dataclass(frozen=True)
class ExpansionLimits:
    """Bounds on the expansion. Each bound caps a branching factor of the
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
    """Injections emitted per ``(focus, statement)`` pair."""
    max_injections: int = 200_000
    """Total injections the traversal emits before it stops."""


class _Shared:
    """What every view of one universe shares, including one
    :class:`Injection` object per key."""

    def __init__(self, root: _VioUniverse) -> None:
        self.root = root
        self.by_key: dict[str, Injection] = {}
        self.baseline_failures: frozenset[tuple[str, int]] = frozenset(
            (focus, statement) for focus, statement in root.baseline_failures()
        )

    def injection(self, row: tuple) -> Injection:
        key = row[3]
        found = self.by_key.get(key)
        if found is None:
            found = self.by_key[key] = _injection(row)
        return found

    @cached_property
    def reference_ntriples(self) -> str:
        return self.root.reference_ntriples()


class Universe(Sequence[Injection]):
    """Every way to break a conforming reference graph, as a sequence of
    :class:`Injection` s, with the filter and weights :meth:`draw` samples
    them by.

    Construction compiles the shapes and validates the reference graph, but
    does not expand it. :meth:`draw` and :meth:`draws` stream the expansion
    into their reservoirs without storing it. ``len``, iteration, indexing,
    and ``in`` expand the universe once and cache it, and later draws read
    the cache. Either way a draw returns the same case.

    A universe is immutable: :meth:`where` and :meth:`weighted` return views
    that share the engine session, the cached expansion, and the
    verification cache, so deriving one is cheap.

    Parameters
    ----------
    data_graph, shacl_graph:
        The reference graph and the shapes, in any form
        :func:`shifty.validate` accepts.
    materialize:
        Run SHACL-AF rules once and use the inferred closure as the reference
        graph. Models that rely on rules, such as ASHRAE 223P models, need
        this to conform, and edits may then touch inferred triples.
    infer:
        Re-run SHACL-AF rules on every validation instead. Edits target only
        the asserted reference triples, so values supported only by an
        inferred triple cannot be cut.
    include_external_foci:
        Also break focus nodes that have no outgoing triple in the reference
        graph, such as vocabulary terms that the shapes graph describes.
    canonicalize_blank_nodes:
        Relabel the reference graph's blank nodes canonically, so a graph
        with anonymous blank nodes yields the same universe in every process.
        Parsers label them afresh on every parse. Turn it off to keep the
        input's labels.
    limits:
        Bounds on the expansion. See :class:`ExpansionLimits`.
    """

    _inner: _VioUniverse
    _shared: _Shared
    _predicate_weights: Mapping[str, float]
    _kind_weights: Mapping[str, float]

    def __init__(
        self,
        data_graph: Any,
        shacl_graph: Any,
        *,
        materialize: bool = False,
        infer: bool = False,
        include_external_foci: bool = False,
        canonicalize_blank_nodes: bool = True,
        graph_mode: str = "union",
        on_unsupported: str = "error",
        base: Optional[str] = None,
        limits: Optional[ExpansionLimits] = None,
    ) -> None:
        data = _to_rdf_input(_coalesce_graph_input(data_graph))
        shapes = _to_rdf_input(_coalesce_graph_input(shacl_graph))
        lim = limits or ExpansionLimits()
        inner = _VioUniverse(
            data.data,
            data.path,
            data.format,
            shapes.data,
            shapes.path,
            shapes.format,
            materialize,
            infer,
            include_external_foci,
            canonicalize_blank_nodes,
            graph_mode,
            on_unsupported,
            base,
            (
                lim.max_alternatives,
                lim.max_values,
                lim.max_candidates,
                lim.max_combinations,
                lim.max_depth,
                lim.max_per_target,
                lim.max_injections,
            ),
        )
        self._init(inner, _Shared(inner), {}, {})

    def _init(
        self,
        inner: _VioUniverse,
        shared: _Shared,
        predicate_weights: Mapping[str, float],
        kind_weights: Mapping[str, float],
    ) -> None:
        self._inner = inner
        self._shared = shared
        self._predicate_weights = MappingProxyType(dict(predicate_weights))
        self._kind_weights = MappingProxyType(dict(kind_weights))

    def _view(
        self,
        inner: _VioUniverse,
        predicate_weights: Mapping[str, float],
        kind_weights: Mapping[str, float],
    ) -> Universe:
        view = Universe.__new__(Universe)
        view._init(inner, self._shared, predicate_weights, kind_weights)
        return view

    # ── the sequence: expands once, then cached ──

    @cached_property
    def _members(self) -> tuple[Injection, ...]:
        return tuple(self._shared.injection(row) for row in self._inner.injections())

    def __len__(self) -> int:
        return len(self._members)

    @overload
    def __getitem__(self, index: int) -> Injection: ...
    @overload
    def __getitem__(self, index: slice) -> tuple[Injection, ...]: ...
    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[Injection, tuple[Injection, ...]]:
        return self._members[index]

    def __iter__(self) -> Iterator[Injection]:
        return iter(self._members)

    def __contains__(self, item: object) -> bool:
        return isinstance(item, Injection) and item.key in self._keys

    @cached_property
    def _keys(self) -> frozenset[str]:
        return frozenset(e.key for e in self._members)

    def __repr__(self) -> str:
        weights = len(self._predicate_weights) + len(self._kind_weights)
        return f"Universe(weights={weights})"

    # ── the reference graph ──

    @property
    def reference_ntriples(self) -> str:
        """The reference graph ``G_ref`` as sorted N-Triples: the closure,
        with ``materialize``, and with canonical blank-node labels."""
        return self._shared.reference_ntriples

    @property
    def baseline_failures(self) -> frozenset[tuple[str, int]]:
        """``(focus, statement)`` pairs that already fail in the reference
        graph. They are never broken, and never count as case failures."""
        return self._shared.baseline_failures

    # ── views ──

    def where(
        self,
        predicate_fn: Optional[Callable[[Injection], bool]] = None,
        /,
        *,
        shape: Optional[Names] = None,
        predicate: Optional[Names] = None,
        focus: Optional[Names] = None,
    ) -> Universe:
        """The injections that break ``shape``, edit any of ``predicate``,
        break ``focus``, and pass ``predicate_fn``. Each argument takes one
        IRI or several, bare or as ``<iri>``.

        This is a hard filter, unlike :meth:`weighted`. Filters compose,
        and the weights carry over. The filter runs as the expansion
        streams, so ``predicate_fn`` is called once per injection on each
        pass; an exception it raises propagates from the call that ran it.
        """
        shapes = None if shape is None else {n.strip("<>") for n in _names(shape)}
        preds = None if predicate is None else {_iri_term(n) for n in _names(predicate)}
        foci = None if focus is None else {_node_term(n) for n in _names(focus)}
        keep = None
        if predicate_fn is not None:
            fn, shared = predicate_fn, self._shared

            def keep(row: tuple) -> bool:
                return bool(fn(shared.injection(row)))

        inner = self._inner.filtered(shapes, preds, foci, keep)
        return self._view(inner, self._predicate_weights, self._kind_weights)

    def weighted(
        self,
        predicates: Optional[Mapping[str, float]] = None,
        kinds: Optional[Mapping[str, float]] = None,
    ) -> Universe:
        """The same injections, drawn by these weights instead of the current
        ones.

        ``w(v)`` is the geometric mean of ``predicates`` over the predicates
        an injection edits, times the geometric mean of ``kinds`` over its
        operation kinds. A predicate or kind without an entry weighs 1, and
        with no weights every injection is equally likely. Predicates are
        keyed by IRI, bare or as ``<iri>``; kinds by :attr:`Operation.kind`.
        Weights favour injections but never exclude them: use :meth:`where`
        for that.
        """
        preds = {_iri_term(k): float(v) for k, v in (predicates or {}).items()}
        kind_weights = {k: float(v) for k, v in (kinds or {}).items()}
        inner = self._inner.weighted(preds, kind_weights)
        return self._view(inner, preds, kind_weights)

    @property
    def predicate_weights(self) -> Mapping[str, float]:
        return self._predicate_weights

    @property
    def kind_weights(self) -> Mapping[str, float]:
        return self._kind_weights

    # ── drawing ──

    def draw(self, k: int, *, seed: int = 0) -> Case:
        """Compose a case of up to ``k`` injections. The same ``(k, seed)``
        draws the same case from the same universe in every process.

        The injections stream into a weighted reservoir of ``max(8k, 64)``,
        which is then scanned in key order. As in arXiv 2611.07342 (Sec. 6.3), an injection is admitted
        when it can be applied with those already chosen: it does not delete
        what another adds, replace a value another replaces differently, or
        need the opposite of another's requirement on a node. Several
        injections may break one property of one focus node, and an
        injection may fail pairs it did not aim at.

        Beyond the paper, each injection must make its target fail when
        applied alone, and the composed delta is revalidated: an injection
        whose targeted property no longer fails is replaced from the
        reservoir. The case holds fewer than ``k`` injections only when the
        reservoir runs out.
        """
        return self.draws([(k, seed)])[0]

    def draws(self, requests: Iterable[tuple[int, int]]) -> list[Case]:
        """One case per ``(k, seed)``, as :meth:`draw` would compose it,
        from a single pass over the expansion. Use it to build a suite::

            universe.draws([(k, s) for k in (2, 5, 10) for s in range(10)])
        """
        requests = [(int(k), int(seed)) for k, seed in requests]
        if any(seed < 0 for _, seed in requests):
            raise ValueError("seed must be non-negative")
        return [Case(c, self._shared) for c in self._inner.draws(requests)]


# ── helpers ──


def _injection(row: tuple) -> Injection:
    focus, statement, shape, key, ops = row
    return Injection(
        focus=focus,
        statement=statement,
        shape=shape,
        operations=tuple(
            Operation(
                kind=kind,
                node=node,
                polarity=polarity,
                deletes=tuple(deletes),
                adds=tuple(adds),
                detail=detail,
            )
            for kind, node, polarity, deletes, adds, detail in ops
        ),
        key=key,
    )


def _names(names: Names) -> list[str]:
    return [names] if isinstance(names, str) else list(names)


def _iri_term(iri: str) -> str:
    return iri if iri.startswith("<") else f"<{iri}>"


def _node_term(node: str) -> str:
    return node if node.startswith("_:") else _iri_term(node)


def _line(t: Triple) -> str:
    return f"{t[0]} {t[1]} {t[2]} ."


def _delta(add: Iterable[Triple], delete: Iterable[Triple]) -> RepairDelta:
    return RepairDelta.from_ntriples(
        add="".join(f"{_line(t)}\n" for t in sorted(add)),
        delete="".join(f"{_line(t)}\n" for t in sorted(delete)),
    )


# ── CLI ──────────────────────────────────────────────────────────────────────


def main(argv: Optional[Sequence[str]] = None) -> int:
    """``python -m shifty.experimental.vio``: generate a benchmark suite."""
    parser = argparse.ArgumentParser(
        prog="python -m shifty.experimental.vio",
        description="Inject SHACL violations with known repairs into a conforming graph.",
    )
    parser.add_argument("--data", required=True, help="conforming reference graph")
    parser.add_argument("--shapes", required=True, help="shapes graph (path or URL)")
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument(
        "-k",
        type=int,
        action="append",
        help="injections per case; repeat for several levels (default 5)",
    )
    parser.add_argument("-n", "--cases", type=int, default=10, help="cases per k")
    parser.add_argument(
        "--seed", type=int, default=0, help="seed of the first case; cases count up"
    )
    parser.add_argument(
        "--shape", action="append", help="only break this shape (IRI); repeat"
    )
    parser.add_argument(
        "--predicate",
        action="append",
        help="only inject edits of this predicate (IRI); repeat",
    )
    parser.add_argument("--weights", help="JSON file mapping predicate IRIs to weights")
    parser.add_argument(
        "--kind-weights",
        help="JSON file mapping operation kinds (remove, substitute, add_value, "
        "add_forbidden) to weights",
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="run SHACL-AF rules once and use the closure as the reference graph",
    )
    parser.add_argument(
        "--infer", action="store_true", help="re-run SHACL-AF rules on every validation"
    )
    args = parser.parse_args(argv)

    def load(path: Optional[str]) -> dict[str, float]:
        if not path:
            return {}
        return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))

    universe = Universe(
        args.data, args.shapes, materialize=args.materialize, infer=args.infer
    )
    if universe.baseline_failures:
        print(
            f"warning: reference graph has {len(universe.baseline_failures)} failing"
            " pairs; they are never broken",
            file=sys.stderr,
        )
    pool = universe.where(shape=args.shape, predicate=args.predicate).weighted(
        load(args.weights), load(args.kind_weights)
    )
    # One streamed pass fills every case's reservoir; nothing else is stored.
    requests = [(k, args.seed + i) for k in args.k or [5] for i in range(args.cases)]
    try:
        cases = pool.draws(requests)
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reference.nt").write_text(universe.reference_ntriples, encoding="utf-8")
    summary = []
    for case in cases:
        i = case.seed - args.seed
        path = case.save(out / f"k{case.k:02d}" / f"case-{i:03d}")
        summary.append(
            {
                "path": str(path.relative_to(out)),
                "k": case.k,
                "seed": case.seed,
                "injected": len(case.injections),
                "failures": len(case.failures),
            }
        )
    suite = {
        "baseline_failures": len(universe.baseline_failures),
        "operation_kinds": dict(
            sorted(
                Counter(k for c in cases for e in c.injections for k in e.kinds).items()
            )
        ),
        "cases": summary,
    }
    (out / "suite.json").write_text(
        json.dumps(suite, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(summary)} cases to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
