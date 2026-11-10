#!/usr/bin/env python3
"""Draw a violation suite for an ASHRAE 223P model.

223P models rely on SHACL-AF rules, so ``materialize=True`` runs them once
and uses the inferred graph as the starting point. Ten cases are drawn for
each budget k, in one pass over the universe.

Usage:
    python vio_223p.py [MODEL] [OUT]
"""

import pathlib
import sys

from shifty.experimental import vio

S223 = pathlib.Path(__file__).resolve().parents[2] / "benchmark" / "s223"
MODEL = sys.argv[1] if len(sys.argv) > 1 else S223 / "models" / "NIST-IBAL.ttl"
OUT = pathlib.Path(sys.argv[2] if len(sys.argv) > 2 else "vio-suite")


def main() -> None:
    universe = vio.Universe(MODEL, S223 / "223p-closure.ttl", materialize=True)
    requests = [(k, seed) for k in (2, 5, 10) for seed in range(10)]
    for case in universe.draws(requests):
        case.save(OUT / f"k{case.k:02d}" / f"seed{case.seed}")
        print(
            f"k={case.k:2d} seed={case.seed}: {len(case.injections)} injected,"
            f" {len(case.failures)} failing"
        )
    print(f"Saved under {OUT}")


if __name__ == "__main__":
    main()
