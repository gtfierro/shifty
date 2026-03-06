#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "rdflib",
# ]
# ///
"""Compare SHACL validation reports by content, ignoring blank node IDs.

This script compares only report content rooted at `sh:ValidationReport`:
- report-level triples except `sh:result`
- each `sh:result` subtree (including nested blank-node structures)

Blank node identifiers are ignored by canonicalizing each subtree into a
deterministic JSON representation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import rdflib
from rdflib.namespace import RDF, SH
from rdflib.term import BNode, Identifier, Literal, URIRef
from rdflib.util import guess_format


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare two SHACL validation reports by content, with blank-node "
            "identity ignored."
        )
    )
    parser.add_argument("left_report", help="Path to first report graph.")
    parser.add_argument("right_report", help="Path to second report graph.")
    parser.add_argument(
        "--left-format",
        default=None,
        help="RDF format for first report (default: guessed from extension).",
    )
    parser.add_argument(
        "--right-format",
        default=None,
        help="RDF format for second report (default: guessed from extension).",
    )
    parser.add_argument(
        "--show-normalized",
        action="store_true",
        help="Print canonicalized report signatures for both inputs.",
    )
    parser.add_argument(
        "--max-diff-items",
        type=int,
        default=5,
        help="Maximum missing/extra signatures to print for mismatches.",
    )
    parser.add_argument(
        "--show-full-diff",
        action="store_true",
        help="Print full canonical signatures for missing/extra items.",
    )
    return parser.parse_args()


def parse_graph(path_str: str, explicit_format: str | None) -> rdflib.Graph:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Report file not found: {path}")
    rdf_format = explicit_format or guess_format(str(path)) or "turtle"
    graph = rdflib.Graph()
    graph.parse(path, format=rdf_format)
    return graph


def canonical_term(term: Identifier) -> dict[str, Any]:
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


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def canonical_object(
    graph: rdflib.Graph,
    term: Identifier,
    visited: frozenset[Identifier],
) -> dict[str, Any]:
    if isinstance(term, BNode):
        return canonical_resource(graph, term, visited)
    return canonical_term(term)


def canonical_resource(
    graph: rdflib.Graph,
    node: Identifier,
    visited: frozenset[Identifier],
) -> dict[str, Any]:
    if node in visited:
        # Defensive cycle marker: SHACL reports should be acyclic here.
        return {"cycle": True}

    next_visited = visited | {node}
    triples: list[list[dict[str, Any]]] = []
    for predicate, obj in graph.predicate_objects(node):
        predicate_repr = canonical_term(predicate)
        object_repr = canonical_object(graph, obj, next_visited)
        triples.append([predicate_repr, object_repr])

    triples.sort(key=stable_json)
    return {"triples": triples}


def canonical_result_signature(graph: rdflib.Graph, result_node: Identifier) -> dict[str, Any]:
    # Result nodes are compared by outgoing content only, not by node identity.
    return canonical_resource(graph, result_node, frozenset())


def canonical_report_signature(graph: rdflib.Graph, report_node: Identifier) -> dict[str, Any]:
    report_triples: list[list[dict[str, Any]]] = []
    for predicate, obj in graph.predicate_objects(report_node):
        if predicate == SH.result:
            continue
        predicate_repr = canonical_term(predicate)
        object_repr = canonical_object(graph, obj, frozenset({report_node}))
        report_triples.append([predicate_repr, object_repr])
    report_triples.sort(key=stable_json)

    result_counter: Counter[str] = Counter()
    for result_node in graph.objects(report_node, SH.result):
        result_sig = canonical_result_signature(graph, result_node)
        result_counter[stable_json(result_sig)] += 1

    return {
        "report_triples": report_triples,
        "results": [
            {"count": count, "signature": json.loads(signature)}
            for signature, count in sorted(result_counter.items())
        ],
    }


def graph_report_signature_counter(graph: rdflib.Graph) -> Counter[str]:
    report_nodes = set(graph.subjects(RDF.type, SH.ValidationReport))
    if not report_nodes:
        raise ValueError("No sh:ValidationReport node found in graph")

    signatures: Counter[str] = Counter()
    for report_node in report_nodes:
        signature = canonical_report_signature(graph, report_node)
        signatures[stable_json(signature)] += 1
    return signatures


def short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def summarize_signature(signature: str) -> dict[str, Any]:
    parsed = json.loads(signature)
    result_entries = parsed.get("results", [])
    total_results = sum(entry.get("count", 0) for entry in result_entries)
    predicate_uris = sorted(
        triple[0]["uri"]
        for triple in parsed.get("report_triples", [])
        if isinstance(triple, list)
        and len(triple) == 2
        and isinstance(triple[0], dict)
        and "uri" in triple[0]
    )
    top_result_fingerprints = [
        {
            "count": entry.get("count", 0),
            "result_hash": short_hash(stable_json(entry.get("signature"))),
        }
        for entry in result_entries[:3]
    ]
    return {
        "report_hash": short_hash(signature),
        "report_predicates": predicate_uris,
        "total_results": total_results,
        "distinct_result_signatures": len(result_entries),
        "sample_result_hashes": top_result_fingerprints,
    }


def print_diff(
    *,
    missing_from_right: Counter[str],
    missing_from_left: Counter[str],
    max_items: int,
    show_full_diff: bool,
) -> None:
    def show(title: str, items: Iterable[tuple[str, int]]) -> None:
        print(title)
        shown = 0
        for signature, count in items:
            shown += 1
            if shown > max_items:
                print("  ...")
                break
            print(f"  count={count}")
            print(json.dumps(summarize_signature(signature), indent=2, sort_keys=True))
            if show_full_diff:
                print("  full_signature:")
                print(json.dumps(json.loads(signature), indent=2, sort_keys=True))

    show("Present only in LEFT:", missing_from_right.items())
    show("Present only in RIGHT:", missing_from_left.items())


def main() -> int:
    args = parse_args()

    left_graph = parse_graph(args.left_report, args.left_format)
    right_graph = parse_graph(args.right_report, args.right_format)

    left_counter = graph_report_signature_counter(left_graph)
    right_counter = graph_report_signature_counter(right_graph)

    if args.show_normalized:
        print("LEFT normalized signatures:")
        print(
            json.dumps(
                [{"count": c, "signature": json.loads(s)} for s, c in sorted(left_counter.items())],
                indent=2,
                sort_keys=True,
            )
        )
        print("RIGHT normalized signatures:")
        print(
            json.dumps(
                [{"count": c, "signature": json.loads(s)} for s, c in sorted(right_counter.items())],
                indent=2,
                sort_keys=True,
            )
        )

    if left_counter == right_counter:
        print("MATCH: report contents are equivalent (blank-node IDs ignored).")
        return 0

    print("MISMATCH: report contents differ.")
    print_diff(
        missing_from_right=left_counter - right_counter,
        missing_from_left=right_counter - left_counter,
        max_items=args.max_diff_items,
        show_full_diff=args.show_full_diff,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
