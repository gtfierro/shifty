#!/usr/bin/env python3
"""Generate a violation test suite for an ASHRAE 223P model.

223P models rely on SHACL-AF rules, so ``materialize=True`` runs them once
and uses the inferred graph as the starting point. ``vio.generate`` draws
ten cases for each budget k across four worker processes, and writes them
with ``reference.nt`` and ``suite.json``.

Usage:
    python vio_223p.py OUT [MODEL]

OUT is required. MODEL defaults to the NIST-IBAL model in benchmark/s223.
"""

import pathlib
import sys

from shifty.experimental import vio

S223 = pathlib.Path(__file__).resolve().parents[2] / "benchmark" / "s223"


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: python vio_223p.py OUT [MODEL]")
    out = sys.argv[1]
    model = sys.argv[2] if len(sys.argv) > 2 else S223 / "models" / "NIST-IBAL.ttl"
    suite = vio.generate(
        model,
        S223 / "223p-closure.ttl",
        out,
        budgets=(2, 5, 10),
        cases=10,
        materialize=True,
        jobs=4,  # each worker holds its own universe, about 550 MB here
    )
    for row in suite["cases"]:
        print(
            f"k={row['k']:2d} seed={row['seed']}: {row['injected']} injected,"
            f" {row['failures']} failing"
        )
    print(f"Saved under {out}")


if __name__ == "__main__":
    main()
