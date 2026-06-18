#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "rdflib",
#   "pyshacl",
#   "brick-tq-shacl>=0.4.1",
# ]
# ///
"""Compare SHACL validation reports across shifty, TopQuadrant, and pySHACL.

Runs all three validators against the same shapes + data graphs, then
performs a 3-way diff that:
  - ignores blank-node identity (content-hashes each sh:ValidationResult subtree)
  - drops sh:resultMessage by default (--exact-message to compare verbatim)
  - groups results into buckets: all-agree, only-in-X, only-in-X+Y, etc.

Expects pre-closure graphs (all owl:imports already inlined) for both shapes
and data — no import resolution is performed.

Usage:
    uv run scripts/compare_implementations.py -s shapes-closure.ttl -d data.ttl

shifty must be importable (build it first: cd python && maturin develop).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import rdflib
from rdflib.namespace import RDF, SH
from rdflib.term import BNode, Identifier, Literal, URIRef
from rdflib.util import guess_format

# Locate shifty from the sibling python/ directory so the script works when
# run via "uv run scripts/..." from the repo root.
_SCRIPTS_DIR = Path(__file__).resolve().parent
_SHIFTY_PYTHON = _SCRIPTS_DIR.parent / "python"
if _SHIFTY_PYTHON.exists() and str(_SHIFTY_PYTHON) not in sys.path:
    sys.path.insert(0, str(_SHIFTY_PYTHON))

try:
    import shifty as _shifty_mod
    _HAS_SHIFTY = True
except ImportError:
    _HAS_SHIFTY = False

# ── SHACL predicates we care about in summary output ─────────────────────────

_SHACL_NS = str(SH)
_SUMMARY_PREDS = {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "type",
    str(SH.focusNode): "focusNode",
    str(SH.resultPath): "resultPath",
    str(SH.value): "value",
    str(SH.sourceConstraintComponent): "constraintComponent",
    str(SH.sourceShape): "sourceShape",
    str(SH.resultSeverity): "severity",
    str(SH.detail): "detail",
}


# ── canonical-signature helpers ───────────────────────────────────────────────

def _canonical_term(term: Identifier) -> dict[str, Any]:
    if isinstance(term, URIRef):
        return {"uri": str(term)}
    if isinstance(term, Literal):
        return {
            "literal": {
                "lexical": str(term),
                "datatype": str(term.datatype) if term.datatype is not None else None,
                "language": term.language,
            }
        }
    if isinstance(term, BNode):
        return {"blank": True}
    raise TypeError(f"Unsupported RDF term: {term!r}")


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_object(
    graph: rdflib.Graph,
    term: Identifier,
    visited: frozenset[Identifier],
) -> dict[str, Any]:
    if isinstance(term, BNode):
        return _canonical_resource(graph, term, visited)
    return _canonical_term(term)


def _canonical_resource(
    graph: rdflib.Graph,
    node: Identifier,
    visited: frozenset[Identifier],
) -> dict[str, Any]:
    if node in visited:
        return {"cycle": True}
    next_visited = visited | {node}
    triples: list[list[dict[str, Any]]] = []
    for predicate, obj in graph.predicate_objects(node):
        triples.append([_canonical_term(predicate), _canonical_object(graph, obj, next_visited)])
    triples.sort(key=_stable_json)
    return {"triples": triples}


def _canonical_result_signature(
    graph: rdflib.Graph,
    result_node: Identifier,
    *,
    fudge_message: bool,
) -> dict[str, Any]:
    """Content hash of a sh:ValidationResult subtree.

    Blank-node identity is ignored (bnodes are hashed by their outgoing
    content). With fudge_message=True (default), sh:resultMessage is
    dropped entirely from the signature: implementations differ both in
    phrasing *and* in whether they emit a message at all, so presence
    alone would still cause spurious mismatches. Pass --exact-message to
    compare messages verbatim.
    """
    visited = frozenset({result_node})
    triples: list[list[dict[str, Any]]] = []
    for predicate, obj in graph.predicate_objects(result_node):
        if fudge_message and predicate == SH.resultMessage:
            continue
        # sh:sourceConstraint points to a blank node whose identity (and
        # content) varies between validators that implement SHACL-AF
        # differently. Excluding it lets us compare logical violations rather
        # than implementation-specific bookkeeping.
        if predicate == SH.sourceConstraint:
            continue
        triples.append([_canonical_term(predicate), _canonical_object(graph, obj, visited)])
    triples.sort(key=_stable_json)
    return {"triples": triples}


def result_counter(
    graph: rdflib.Graph,
    *,
    fudge_message: bool,
) -> Counter[str]:
    """Map each distinct canonical result signature to its occurrence count."""
    report_nodes = list(graph.subjects(RDF.type, SH.ValidationReport))
    if not report_nodes:
        raise ValueError("No sh:ValidationReport found in graph")
    counter: Counter[str] = Counter()
    for report_node in report_nodes:
        for result_node in graph.objects(report_node, SH.result):
            sig = _canonical_result_signature(graph, result_node, fudge_message=fudge_message)
            counter[_stable_json(sig)] += 1
    return counter


# ── result-summary display ────────────────────────────────────────────────────

def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:10]


def _local_name(uri: str) -> str:
    return uri.rsplit("#", 1)[-1] if "#" in uri else uri.rsplit("/", 1)[-1]


def _obj_short(obj: dict[str, Any]) -> str:
    if "uri" in obj:
        return "<" + _local_name(obj["uri"]) + ">"
    if "literal" in obj:
        lit = obj["literal"]
        lexical = lit.get("lexical", "")
        dt = lit.get("datatype")
        lang = lit.get("language")
        if lang:
            return f'"{lexical}"@{lang}'
        if dt:
            return f'"{lexical}"^^{_local_name(dt)}'
        return f'"{lexical}"'
    if "message_present" in obj:
        return "(message)"
    if "triples" in obj:
        return f"[bnode/{len(obj['triples'])} props]"
    return str(obj)


def _summarize_result(sig_json: str) -> str:
    parsed = json.loads(sig_json)
    triples = parsed.get("triples", [])
    fields: list[str] = []
    for pair in triples:
        if len(pair) != 2:
            continue
        pred_d, obj_d = pair
        pred_uri = pred_d.get("uri", "")
        label = _SUMMARY_PREDS.get(pred_uri)
        if label is None:
            continue
        fields.append(f"{label}={_obj_short(obj_d)}")
    h = _short_hash(sig_json)
    body = "  ".join(fields) if fields else "(no recognized predicates)"
    return f"[{h}] {body}"


# ── validators ────────────────────────────────────────────────────────────────

def _load_graph(path: str) -> rdflib.Graph:
    p = Path(path)
    fmt = guess_format(str(p)) or "turtle"
    g = rdflib.Graph()
    g.parse(p, format=fmt)
    return g


def run_shifty(
    shapes_path: str,
    data_path: str,
    **_kwargs: Any,
) -> tuple[bool, rdflib.Graph]:
    if not _HAS_SHIFTY:
        raise RuntimeError(
            "shifty is not importable — build it first:\n"
            "  cd python && maturin develop"
        )
    # pyshacl and topquadrant both merge the shapes graph into the data graph
    # before validating. Match that behavior so focus-node discovery (e.g.
    # sh:targetObjectsOf on predicates that only appear in the shapes file)
    # finds the same nodes as the other validators.
    shapes_graph = _load_graph(shapes_path)
    data_graph = _load_graph(data_path)
    data_graph += shapes_graph

    conforms, report, _ = _shifty_mod.validate(
        data_graph,
        shapes_path,
        graph_mode="union",
        infer=True,
    )
    return conforms, report


def run_pyshacl(
    shapes_path: str,
    data_path: str,
    **_kwargs: Any,
) -> tuple[bool, rdflib.Graph]:
    import pyshacl

    shapes_graph = _load_graph(shapes_path)
    data_graph = _load_graph(data_path)
    data_graph += shapes_graph

    conforms, report, _ = pyshacl.validate(
        data_graph=data_graph,
        shacl_graph=shapes_graph,
        advanced=True,
        inplace=True,
        allow_warnings=True,
    )
    return conforms, report


def run_topquadrant(
    shapes_path: str,
    data_path: str,
    **_kwargs: Any,
) -> tuple[bool, rdflib.Graph]:
    from brick_tq_shacl import infer as tq_infer, validate as tq_validate

    shapes_graph = _load_graph(shapes_path)
    data_graph = _load_graph(data_path)
    data_graph += shapes_graph

    inferred = tq_infer(data_graph, shapes_graph)
    valid, report, _ = tq_validate(inferred, shapes_graph)
    return valid, report


_RUNNERS = {
    "shifty": run_shifty,
    "topquadrant": run_topquadrant,
    "pyshacl": run_pyshacl,
}

# ── 3-way diff ────────────────────────────────────────────────────────────────

def three_way_diff(
    counters: dict[str, Counter[str]],
    *,
    max_show: int,
    verbose: bool,
) -> int:
    """Print a 3-way comparison; return 0 if all agree, 1 if they differ."""
    names = list(counters.keys())
    all_names = frozenset(names)

    # Collect all seen signatures and group by which validators have each.
    groups: dict[frozenset[str], list[tuple[str, int]]] = {}
    for sig in set().union(*counters.values()):  # type: ignore[arg-type]
        present = frozenset(n for n in names if counters[n][sig] > 0)
        count = max(counters[n][sig] for n in names)
        groups.setdefault(present, []).append((sig, count))

    print(f"\n{'='*64}")
    print(f"  3-WAY COMPARISON: {', '.join(names)}")
    print(f"{'='*64}")
    for name, ctr in counters.items():
        total = sum(ctr.values())
        distinct = len(ctr)
        print(f"  {name:16s}: {total:4d} result(s), {distinct:3d} distinct signature(s)")
    print()

    if set(groups) == {all_names}:
        total = sum(sum(ctr.values()) for ctr in counters.values()) // len(names)
        print(f"ALL AGREE: all {len(names)} implementations produce equivalent reports")
        print(f"           ({total} result(s) total, all matched).")
        return 0

    print("DISAGREEMENTS FOUND:\n")

    # Sort buckets: bigger agreement sets first, then alphabetically.
    for present, items in sorted(
        groups.items(),
        key=lambda kv: (-len(kv[0]), sorted(kv[0])),
    ):
        total_in_bucket = sum(cnt for _, cnt in items)
        if present == all_names:
            print(f"  [all {len(names)} agree]  {total_in_bucket} result(s) match across all validators\n")
            continue

        absent = sorted(all_names - present)
        present_list = sorted(present)
        label = " + ".join(present_list)
        missing_label = " + ".join(absent)
        print(f"  [only in {label}] (absent from {missing_label}) — {total_in_bucket} result(s):")

        shown = 0
        for sig, _cnt in items:
            if shown >= max_show:
                remaining = len(items) - shown
                print(f"      ... {remaining} more (use --max-show to increase or --verbose for details)")
                break
            if verbose:
                print(f"    {json.dumps(json.loads(sig), indent=6, sort_keys=True)}")
            else:
                print(f"    {_summarize_result(sig)}")
            shown += 1
        print()

    return 1


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--shapes", "-s", required=True, metavar="FILE",
                   help="SHACL shapes graph (Turtle or N-Triples)")
    p.add_argument("--data", "-d", required=True, metavar="FILE",
                   help="RDF data graph to validate")
    p.add_argument(
        "--skip", nargs="*", choices=list(_RUNNERS), default=[],
        metavar="IMPL", help="Skip one or more implementations (shifty topquadrant pyshacl)",
    )
    p.add_argument(
        "--exact-message", action="store_true",
        help="Compare sh:message content exactly (default: normalize to presence-only)",
    )
    p.add_argument(
        "--max-show", type=int, default=5, metavar="N",
        help="Maximum result signatures to print per disagreement bucket (default: 5)",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Print full canonical signatures instead of one-line summaries",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    fudge = not args.exact_message

    counters: dict[str, Counter[str]] = {}

    for name, runner in _RUNNERS.items():
        if name in (args.skip or []):
            print(f"[{name}] skipped")
            continue

        print(f"[{name}] running ...", end=" ", flush=True)
        t0 = time.perf_counter()
        try:
            conforms, report_graph = runner(args.shapes, args.data)
            elapsed = time.perf_counter() - t0
            ctr = result_counter(report_graph, fudge_message=fudge)
            total = sum(ctr.values())
            status = "conforms" if conforms else f"{total} violation(s)"
            print(f"{status}  ({elapsed:.2f}s)")
            counters[name] = ctr
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            print(f"ERROR ({elapsed:.2f}s): {exc}")
            if args.verbose:
                import traceback
                traceback.print_exc()

    if len(counters) < 2:
        print("\nNeed at least 2 successful validators to compare; aborting.")
        return 2

    return three_way_diff(
        counters,
        max_show=args.max_show,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    sys.exit(main())
