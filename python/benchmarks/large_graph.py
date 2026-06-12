"""Large-graph binding benchmark.

Run from the repository root after `maturin develop`:

    python python/benchmarks/large_graph.py --triples 10000 100000
"""

from __future__ import annotations

import argparse
import pathlib
import resource
import tempfile
import time

import rdflib

import shifty


SHAPES = b"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:p .
"""


def timed(label: str, operation):
    start = time.perf_counter()
    result = operation()
    elapsed = time.perf_counter() - start
    rss_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(f"{label:30} {elapsed:8.3f}s  peak RSS {rss_mib:9.1f} MiB")
    return result


def turtle_bytes(count: int) -> bytes:
    rows = ["@prefix ex: <http://example.org/> ."]
    rows.extend(f'ex:s{i} ex:p "value {i}" .' for i in range(count))
    return "\n".join(rows).encode()


def run(count: int) -> None:
    print(f"\n{count:,} triples")
    data = timed("build Turtle bytes", lambda: turtle_bytes(count))

    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "data.ttl"
        path.write_bytes(data)
        timed(
            "validate_algebra(path)",
            lambda: shifty.validate_algebra(path, SHAPES, infer=False),
        )

    graph = timed(
        "parse rdflib.Graph",
        lambda: rdflib.Graph().parse(data=data, format="turtle"),
    )
    timed(
        "serialize rdflib as N-Triples",
        lambda: graph.serialize(format="nt", encoding="utf-8"),
    )
    timed(
        "validate_algebra(rdflib)",
        lambda: shifty.validate_algebra(graph, SHAPES, infer=False),
    )

    prepared = timed("prepare shapes", lambda: shifty.PreparedValidator(SHAPES))
    timed(
        "prepared.validate_algebra(bytes)",
        lambda: prepared.validate_algebra(data, infer=False),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--triples", nargs="+", type=int, default=[10_000, 100_000])
    args = parser.parse_args()
    for count in args.triples:
        run(count)


if __name__ == "__main__":
    main()
