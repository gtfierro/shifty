#!/usr/bin/env python3
"""Generate a multi-violation repair benchmark for an ASHRAE 223P model.

One streamed pass over the universe feeds every budget K, with several
cases per level. 223P models depend on SHACL-AF rules, so the reference
graph is the materialized closure. Every predicate is treated alike: draws are uniform over the
universe of injections.

Usage:
    python vio_223p.py [MODEL] [OUT] [--cases N] [--budgets K ...]

Examples:
    python vio_223p.py ../../benchmark/s223/models/NIST-IBAL.ttl suite/
"""

import argparse
import json
import pathlib
from collections import Counter

from shifty.experimental import vio

BUDGETS = (2, 5, 10, 15, 20, 25, 30, 35)
HERE = pathlib.Path(__file__).resolve().parent
S223_DIR = HERE.parent.parent / "benchmark" / "s223"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "model", nargs="?", default=str(S223_DIR / "models" / "NIST-IBAL.ttl")
    )
    parser.add_argument("out", nargs="?", default="vio-suite")
    parser.add_argument("--shapes", default=str(S223_DIR / "223p-closure.ttl"))
    parser.add_argument("--cases", type=int, default=10, help="cases per budget level")
    parser.add_argument("--budgets", type=int, nargs="*", default=list(BUDGETS))
    args = parser.parse_args()

    out = pathlib.Path(args.out)
    universe = vio.Universe(args.model, args.shapes, materialize=True)
    print(
        f"{len(universe.baseline_failures)} pre-existing failing pairs are left alone"
    )
    # One streamed pass over the expansion fills a reservoir per case.
    requests = [(k, seed) for k in args.budgets for seed in range(args.cases)]
    summary = []
    for case in universe.draws(requests):
        path = case.save(out / f"k{case.k:02d}" / f"case-{case.seed:03d}")
        predicates = Counter(p for e in case.injections for p in e.predicates)
        summary.append(
            {
                "path": str(path.relative_to(out)),
                "k": case.k,
                "seed": case.seed,
                "injected": len(case.injections),
                "failures": len(case.failures),
            }
        )
        print(
            f"K={case.k:2d} seed {case.seed}: {len(case.injections)} injected,"
            f" {len(case.failures)} failing pairs,"
            f" {len(predicates)} distinct predicates edited"
        )
    (out / "reference.nt").write_text(universe.reference_ntriples, encoding="utf-8")
    (out / "suite.json").write_text(
        json.dumps({"cases": summary}, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
