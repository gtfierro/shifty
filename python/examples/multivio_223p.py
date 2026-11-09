#!/usr/bin/env python3
"""Generate multi-violation repair benchmarks for an ASHRAE 223P model.

This mirrors the suite design of "Selective Retrieval Routing for
Multi-Violation Knowledge Graph Repair" (arXiv 2611.07342). The paper uses
three predicate mixtures. Each suite gives its predicate class a large
sampling weight and runs every injection budget K with several cases per
level. 223P models depend on SHACL-AF rules, so the reference graph is the
materialized closure.

Usage:
    python multivio_223p.py [MODEL] [OUT] [--cases N]

Examples:
    python multivio_223p.py ../../benchmark/s223/models/NIST-IBAL.ttl suites/
"""

import argparse
import json
import pathlib
from collections import Counter

from shifty.multivio import ViolationGenerator

S223 = "http://data.ashrae.org/standard223#"
QUDT = "http://qudt.org/schema/qudt/"

# Sec. 5.1: the predicates that distinguish each suite.
SUITES = {
    "evidential": [
        S223 + p for p in ("observes", "hasPhysicalLocation", "actuates", "contains")
    ],
    "terminological": [
        S223 + "hasProperty",
        S223 + "actuatedByProperty",
        S223 + "hasDomainSpace",
        QUDT + "hasQuantityKind",
    ],
    "representational": [
        S223 + p for p in ("cnx", "connectsThrough", "hasConnectionPoint")
    ],
}
BUDGETS = (2, 5, 10, 15, 20, 25, 30, 35)
HERE = pathlib.Path(__file__).resolve().parent
S223_DIR = HERE.parent.parent / "benchmark" / "s223"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "model", nargs="?", default=str(S223_DIR / "models" / "NIST-IBAL.ttl")
    )
    parser.add_argument("out", nargs="?", default="multivio-suites")
    parser.add_argument("--shapes", default=str(S223_DIR / "223p-closure.ttl"))
    parser.add_argument("--cases", type=int, default=10, help="cases per budget level")
    parser.add_argument(
        "--weight", type=float, default=50.0, help="weight of the suite's predicates"
    )
    parser.add_argument("--budgets", type=int, nargs="*", default=list(BUDGETS))
    args = parser.parse_args()

    out = pathlib.Path(args.out)
    gen = ViolationGenerator(args.shapes, args.model, seed=0, materialize=True)
    universe = gen.expressions()  # one traversal shared by every suite
    print(
        f"{len(universe)} expressions; {len(gen.baseline_failures)} pre-existing"
        " failing pairs are left alone"
    )
    for suite, predicates in SUITES.items():
        gen.reweight({p: args.weight for p in predicates})
        print(suite)
        for budget in args.budgets:
            for case in gen.generate(budget, args.cases):
                case.save(out / suite / f"k{budget:02d}" / f"case-{case.index:03d}")
                hits = Counter(
                    p in {f"<{q}>" for q in predicates}
                    for e in case.expressions
                    for p in e.predicates
                )
                print(
                    f"  K={budget:2d} case {case.index}: {len(case.expressions)} injected,"
                    f" {len(case.violations)} failing pairs, {hits[True]}/{sum(hits.values())} suite predicates"
                )
        (out / suite / "reference.nt").write_text(
            "".join(f"{s} {p} {o} .\n" for s, p, o in sorted(gen.reference)),
            encoding="utf-8",
        )
        (out / suite / "suite.json").write_text(
            json.dumps(
                {
                    "predicates": predicates,
                    "budgets": args.budgets,
                    "cases": args.cases,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
