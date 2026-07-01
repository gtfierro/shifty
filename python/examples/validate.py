#!/usr/bin/env python3
"""Validate a data graph against a SHACL shapes graph.

Usage:
    python validate.py <shapes> <data> [--mode union|data|union-all] [--algebra]

Examples:
    python validate.py shapes.ttl data.ttl
    python validate.py shapes.ttl data.ttl --algebra
    python validate.py shapes.ttl data.ttl --mode data
"""

import argparse
import sys

import shifty


def main():
    parser = argparse.ArgumentParser(description="Validate RDF data against SHACL shapes")
    parser.add_argument("shapes", help="Shapes graph (Turtle file)")
    parser.add_argument("data", help="Data graph to validate (Turtle file)")
    parser.add_argument(
        "--mode",
        default="union",
        choices=["union", "data", "union-all"],
        help="Graph mode for focus discovery and evaluation (default: union)",
    )
    parser.add_argument(
        "--no-infer",
        action="store_true",
        help="Skip SHACL-AF rule inference before validation",
    )
    parser.add_argument(
        "--algebra",
        action="store_true",
        help="Use algebra path (structured violations) instead of W3C report",
    )
    args = parser.parse_args()

    run_infer = not args.no_infer

    if args.algebra:
        result = shifty.validate_algebra(
            args.data,
            args.shapes,
            graph_mode=args.mode,
            infer=run_infer,
        )
        print(f"Conforms: {result.conforms}")
        if result.violations:
            print(f"Violations ({len(result.violations)}):")
            for v in result.violations:
                shape = f" [{v.shape_name}]" if v.shape_name else ""
                print(f"  {v.focus_node}{shape}")
                for r in v.reasons:
                    path = f" on {r.path}" if r.path else ""
                    # Lead with the author's sh:message when the shape supplied
                    # one, keeping the engine-generated message in parentheses so
                    # both are always available.
                    if r.author_message:
                        message = f"{r.author_message} (generated message: {r.message})"
                    else:
                        message = r.message
                    print(f"    - {message}{path}")
        sys.exit(0 if result.conforms else 1)

    else:
        conforms, report_graph, results_text = shifty.validate(
            args.data,
            args.shapes,
            graph_mode=args.mode,
            infer=run_infer,
        )
        print(results_text, end="")
        sys.exit(0 if conforms else 1)


if __name__ == "__main__":
    main()
