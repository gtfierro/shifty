#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Attribute the bytes of a serialized EvidenceRun, and price the compactions.

Reads a JSON dump of one `EvidenceRun` (see `probe_evidence_cost --dump-json`)
and answers two questions:

  1. Where do the bytes go? Serialized size is attributed to each field name,
     aggregated over every depth at which it occurs.
  2. What would each candidate compaction save?
       * dropping `expected`   — a full `Shape` clone at every atom, already
         recoverable from the node's `shape` id via the run's catalog;
       * interning terms       — every RDF term spelled out in full, every time;
       * interning paths       — `reached_by` re-spelled at every leaf;
       * hash-consing subtrees — identical evidence subtrees emitted repeatedly,
         which is the same redundancy that makes materialization slow;
       * dropping the catalog  — a fixed per-run cost independent of findings;
       * failures only         — passing pairs are the overwhelming majority.

Savings are reported independently (each against the full size) and are not
additive: interning terms and hash-consing subtrees overlap.

Usage:
  ./benchmark/analyze_evidence_size.py run.json [--top 25]
"""

import argparse
import json
from collections import defaultdict


def blob(value) -> str:
    """Compact serialization, matching what the engine emits."""
    return json.dumps(value, separators=(",", ":"), sort_keys=False)


def attribute(node, key_bytes: defaultdict, path: str = "") -> None:
    """Charge each field's serialized size to its name, at every depth."""
    if isinstance(node, dict):
        for key, value in node.items():
            key_bytes[key] += len(blob(value)) + len(key) + 3  # "key":value,
            attribute(value, key_bytes, f"{path}.{key}")
    elif isinstance(node, list):
        for item in node:
            attribute(item, key_bytes, path)


def walk_evidence(node, out: list) -> None:
    """Every evidence node in the run, as (kind, subtree) pairs."""
    if isinstance(node, dict):
        if "type" in node and "details" in node:
            out.append((node["type"], node))
        for value in node.values():
            walk_evidence(value, out)
    elif isinstance(node, list):
        for item in node:
            walk_evidence(item, out)


def collect_terms(node, terms: list) -> None:
    """Every RDF term occurrence. oxrdf terms serialize as tagged objects."""
    if isinstance(node, dict):
        keys = set(node)
        if keys <= {"NamedNode", "BlankNode", "Literal"} and len(keys) == 1:
            terms.append(blob(node))
            return
        if "value" in keys and keys <= {"value", "datatype", "language"}:
            terms.append(blob(node))
            return
        for value in node.values():
            collect_terms(value, terms)
    elif isinstance(node, list):
        for item in node:
            collect_terms(item, terms)


def intern_nodes(node, table: dict[str, int]):
    """Replace every evidence node with an id into `table`, bottom-up.

    An evidence node is any tagged `{"type": ..., "details": ...}` object; its
    children are interned first, so structurally identical subtrees collapse to
    the same id regardless of where they occur.
    """
    if isinstance(node, dict):
        interned = {k: intern_nodes(v, table) for k, v in node.items()}
        if "type" in node and "details" in node:
            key = blob(interned)
            return {"$": table.setdefault(key, len(table))}
        return interned
    if isinstance(node, list):
        return [intern_nodes(item, table) for item in node]
    return node


def strip_key(node, target: str):
    """The run with every occurrence of `target` removed."""
    if isinstance(node, dict):
        return {k: strip_key(v, target) for k, v in node.items() if k != target}
    if isinstance(node, list):
        return [strip_key(item, target) for item in node]
    return node


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", help="JSON dump of one EvidenceRun")
    parser.add_argument("--top", type=int, default=25)
    args = parser.parse_args()

    with open(args.run) as handle:
        run = json.load(handle)

    total = len(blob(run))
    print(f"total serialized: {total / 1e6:.2f} MB")

    catalog_bytes = len(blob(run.get("constraints", {})))
    statements = run.get("statements", [])
    pairs = sum(len(s.get("selected_foci", [])) for s in statements)
    print(f"statements: {len(statements)}, selected pairs: {pairs}")
    print()

    # ---- where the bytes go ------------------------------------------------
    key_bytes = defaultdict(int)
    attribute(run, key_bytes)
    # Nested fields are charged at every level they appear under, so these
    # shares overlap and sum past 100%. They rank contributors, not partition.
    print(f"{'bytes':>14}  {'share':>7}  field (overlapping: nested fields counted at each level)")
    for key, size in sorted(key_bytes.items(), key=lambda kv: -kv[1])[: args.top]:
        print(f"{size:>14}  {100 * size / total:>6.1f}%  {key}")
    print()

    # ---- what each compaction would save -----------------------------------
    print("candidate compactions (each measured independently, not additive):")

    expected_bytes = key_bytes.get("expected", 0)
    print(
        f"  drop `expected` shape clones      "
        f"{expected_bytes / 1e6:>8.2f} MB  ({100 * expected_bytes / total:>5.1f}%)"
    )

    reached_bytes = key_bytes.get("reached_by", 0)
    print(
        f"  intern `reached_by` paths         "
        f"{reached_bytes / 1e6:>8.2f} MB  ({100 * reached_bytes / total:>5.1f}%)"
    )

    terms = []
    collect_terms(run, terms)
    term_bytes = sum(len(t) for t in terms)
    distinct = {t for t in terms}
    interned = sum(len(t) for t in distinct) + 6 * len(terms)  # table + u32 refs
    term_saving = max(0, term_bytes - interned)
    print(
        f"  intern terms ({len(terms)} occurrences, {len(distinct)} distinct)"
        f"{'':>2}{term_saving / 1e6:>8.2f} MB  ({100 * term_saving / total:>5.1f}%)"
    )

    # Build the interned form for real: bottom-up, every evidence node becomes
    # an id into one table, so identical subtrees are stored once. Counting
    # bytes of nested subtrees instead would charge inner nodes at every level
    # they sit under and overstate the saving.
    table: dict[str, int] = {}
    interned_run = {
        **run,
        "statements": intern_nodes(run.get("statements", []), table),
    }
    consed_bytes = len(blob(interned_run)) + len(blob(list(table)))
    nodes = []
    walk_evidence(run.get("statements", []), nodes)
    cons_saving = max(0, total - consed_bytes)
    print(
        f"  hash-cons subtrees ({len(nodes)} nodes, {len(table)} distinct)"
        f"{'':>3}{cons_saving / 1e6:>8.2f} MB  ({100 * cons_saving / total:>5.1f}%)"
    )

    print(
        f"  drop constraint catalog           "
        f"{catalog_bytes / 1e6:>8.2f} MB  ({100 * catalog_bytes / total:>5.1f}%)"
    )

    failures = {
        **run,
        "statements": [
            {
                **s,
                # Polarity lives on the nested Evidence tag, not on the focus.
                "selected_foci": [
                    f
                    for f in s.get("selected_foci", [])
                    if f.get("evidence", {}).get("status") == "fail"
                ],
            }
            for s in statements
        ],
    }
    fail_saving = total - len(blob(failures))
    fail_pairs = sum(len(s["selected_foci"]) for s in failures["statements"])
    print(
        f"  failures only ({fail_pairs} of {pairs} pairs)"
        f"{'':>12}{fail_saving / 1e6:>8.2f} MB  ({100 * fail_saving / total:>5.1f}%)"
    )

    # ---- the combination that matters --------------------------------------
    compact = strip_key(run, "expected")
    compact = strip_key(compact, "reached_by")
    compact_bytes = len(blob(compact))
    print()
    print(
        f"drop `expected` + `reached_by` together: "
        f"{compact_bytes / 1e6:.2f} MB "
        f"({100 * (total - compact_bytes) / total:.1f}% smaller)"
    )
    if pairs:
        print(f"bytes per selected pair: {total / pairs:.0f} -> {compact_bytes / pairs:.0f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
