#!/usr/bin/env python3
"""Print the observed sh:property bindings for conforming focus nodes.

This is the inverse of validation: instead of reporting violations, it
reports what each sh:property shape's sh:path resolved to, for every focus
node that conforms to a target/profile node shape.

Usage:
    python witnesses.py <shapes> <data> [--key-path PATH] [--mode union|data|union-all]

Example:
    python witnesses.py profile.ttl building.ttl --key-path zea:roleName

    # The key can live behind an arbitrary property path, not just a direct
    # annotation on the sh:property shape itself:
    python witnesses.py profile.ttl building.ttl --key-path zea:role/zea:roleName
"""

import argparse
import sys

import shifty


def main():
    parser = argparse.ArgumentParser(
        description="Print conforming sh:property bindings (the inverse of validation)"
    )
    parser.add_argument("shapes", help="Shapes graph (Turtle file) — the application profile(s)")
    parser.add_argument("data", help="Data graph to check (Turtle file)")
    parser.add_argument(
        "--key-path",
        default=None,
        help=(
            "A SPARQL 1.1 property path expression (sequence /, alternation "
            "|, inverse ^, and the Kleene forms */+/? are all supported), "
            "evaluated from each sh:property shape's own node, over the "
            "shapes graph, to produce a stable key. E.g. 'zea:roleName' for "
            'a direct zea:roleName "outsideAirTemp" annotation, or '
            "'zea:role/zea:roleName' if the key lives one hop further away, "
            "through an intermediate role-descriptor node. Falls back to "
            "the property shape's own IRI/blank-node id when omitted or "
            "when the path resolves to no value."
        ),
    )
    parser.add_argument(
        "--mode",
        default="union",
        choices=["union", "data", "union-all"],
        help="Graph mode for focus discovery and evaluation (default: union)",
    )
    parser.add_argument(
        "--no-infer",
        action="store_true",
        help="Skip SHACL-AF rule inference before checking",
    )
    args = parser.parse_args()

    validator = shifty.PreparedValidator(args.shapes)
    witnesses = validator.witnesses(
        args.data,
        key_path=args.key_path,
        graph_mode=args.mode,
        infer=not args.no_infer,
    )

    if not witnesses:
        print("No conforming focus nodes with sh:property bindings.")
        sys.exit(0)

    by_focus = {}
    for w in witnesses:
        by_focus.setdefault((w.focus, w.shape), []).append(w)

    for (focus, shape), bindings in by_focus.items():
        print(f"{focus}  (conforms to {shape})")
        for w in bindings:
            values = ", ".join(w.values) if w.values else "(none)"
            print(f"  {w.key}: {values}")

    sys.exit(0)


if __name__ == "__main__":
    main()
