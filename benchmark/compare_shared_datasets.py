"""Alternate two optimized CLIs over the same fixtures and record exact output.

Example (from the repository root)::

    python/.venv/bin/python benchmark/compare_shared_datasets.py \
      --old /private/tmp/shifty-baseline/target/release/shifty \
      --new target/release/shifty \
      --old-lock /private/tmp/shifty-baseline/Cargo.lock \
      --new-lock Cargo.lock \
      --shapes benchmark/s223/223p-closure.ttl \
      --models benchmark/s223/models/nist-bdg1-1.ttl \
      --output /private/tmp/shifty-comparison.json

Pass ``--models-dir benchmark/brick/models`` to cover a whole suite. Use
``--manifest /private/tmp/shifty-synthetic-cases/manifest.json`` for generated
cases with a different shapes graph per data graph. Each condition runs a
warmup and five samples per binary by default. Process
versions alternate within a condition; conditions run sequentially. Output
digests are over complete stdout bytes. Inferred triples and report graphs are
also compared up to blank-node isomorphism using the locked Python development
environment's rdflib. Validation's human-readable output is compared as text.
Report message literals can still differ when they spell process-local blank
node IDs. Mismatches retain both outputs in the optional artifact directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

MAC_RSS = re.compile(r"^\s*(\d+)\s+maximum resident set size\s*$", re.MULTILINE)
LINUX_RSS = re.compile(
    r"^\s*Maximum resident set size \(kbytes\):\s*(\d+)", re.MULTILINE
)
STAGE = re.compile(r"^profile: stage: (.+?): ([0-9.]+) ms$", re.MULTILINE)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(
    binary: Path, shapes: Path, model: Path, operation: str, profile: bool = False
) -> dict:
    command = [str(binary), "infer" if operation == "infer" else "validate"]
    command += ["--shapes", str(shapes), "--data", str(model)]
    if operation == "report":
        command.append("--report")
    if profile:
        command.append("--profile")
    timer = ["/usr/bin/time", "-l" if sys.platform == "darwin" else "-v"]
    start = time.perf_counter()
    result = subprocess.run(timer + command, capture_output=True, check=False)
    elapsed_ms = (time.perf_counter() - start) * 1_000
    stderr = result.stderr.decode("utf-8", errors="replace")
    match = (MAC_RSS if sys.platform == "darwin" else LINUX_RSS).search(stderr)
    rss_bytes = (
        int(match.group(1)) * (1 if sys.platform == "darwin" else 1024)
        if match
        else None
    )
    if result.returncode:
        raise RuntimeError(f"{' '.join(command)} exited {result.returncode}: {stderr}")
    stdout = result.stdout
    return {
        "elapsed_ms": elapsed_ms,
        "peak_rss_bytes": rss_bytes,
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stdout": stdout,
    }


def summarize(samples: list[dict]) -> dict:
    times = [sample["elapsed_ms"] for sample in samples]
    rss = [sample["peak_rss_bytes"] for sample in samples]
    median = statistics.median(times)
    return {
        "elapsed_ms": times,
        "median_ms": median,
        "spread_ms": max(abs(value - median) for value in times),
        "peak_rss_bytes": rss,
        "median_rss_bytes": statistics.median(rss)
        if all(value is not None for value in rss)
        else None,
        "stdout_sha256": sorted({sample["stdout_sha256"] for sample in samples}),
    }


def semantic_signatures(samples: list[dict], operation: str) -> list[str]:
    if operation == "validate":
        # Human-readable findings include source blank-node labels and query
        # binding details. The report condition below compares RDF semantics.
        return sorted({sample["stdout_sha256"] for sample in samples})
    try:
        from rdflib import BNode, Graph
        from rdflib.compare import to_isomorphic
        from rdflib.namespace import SH
    except ImportError as error:
        raise RuntimeError(
            "RDF comparison needs rdflib; run this script with python/.venv/bin/python"
        ) from error

    def unique_color_signature(graph: Graph) -> str | None:
        """Canonically label a graph when structural refinement separates all bnodes.

        Distinct colors identify every blank node independently of its source
        label, so the resulting sorted triples are an exact isomorphism key.
        Ambiguous color classes fall through to rdflib's full canonicalizer.
        """
        triples = list(graph)
        blank_nodes = {
            term
            for subject, _, value in triples
            for term in (subject, value)
            if isinstance(term, BNode)
        }
        colors = dict.fromkeys(blank_nodes, "blank")
        distinct = 0
        for _ in range(16):
            incidents = {node: [] for node in blank_nodes}

            def key(term, palette=colors):
                return palette[term] if isinstance(term, BNode) else term.n3()

            for subject, predicate, value in triples:
                if isinstance(subject, BNode):
                    incidents[subject].append(f"out {predicate.n3()} {key(value)}")
                if isinstance(value, BNode):
                    incidents[value].append(f"in {predicate.n3()} {key(subject)}")
            colors = {
                node: hashlib.sha256(
                    (colors[node] + "\n" + "\n".join(sorted(rows))).encode()
                ).hexdigest()
                for node, rows in incidents.items()
            }
            count = len(set(colors.values()))
            if count == len(blank_nodes):
                break
            if count == distinct:
                return None
            distinct = count
        if len(set(colors.values())) != len(blank_nodes):
            return None

        def canonical(term):
            return f"_:{colors[term]}" if isinstance(term, BNode) else term.n3()

        rows = sorted(
            " ".join(canonical(term) for term in triple) for triple in triples
        )
        return hashlib.sha256("\n".join(rows).encode()).hexdigest()

    def graph_signature(sample: dict) -> str:
        output = sample["stdout"].decode("utf-8")
        if operation == "infer":
            lines = [
                line.strip() + " ."
                for line in output.splitlines()
                if line.startswith("  ")
            ]
            graph = Graph().parse(data="\n".join(lines), format="nt")
            links = []
        else:
            graph = Graph().parse(data=output, format="turtle")
            fast_signature = unique_color_signature(graph)
            if fast_signature is not None:
                return fast_signature
            # A SHACL report root connects every result through sh:result.
            # Remove those links before canonicalization and retain them as
            # component relationships, avoiding factorial symmetry among
            # otherwise identical findings.
            links = list(graph.triples((None, SH.result, None)))
            for link in links:
                graph.remove(link)

        parent = {}

        def root(node):
            parent.setdefault(node, node)
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node

        for subject, _, value in graph:
            if isinstance(subject, BNode) and isinstance(value, BNode):
                parent[root(value)] = root(subject)
            elif isinstance(subject, BNode):
                root(subject)
            elif isinstance(value, BNode):
                root(value)
        components = {}
        ground = []
        for triple in graph:
            nodes = [term for term in (triple[0], triple[2]) if isinstance(term, BNode)]
            if nodes:
                components.setdefault(root(nodes[0]), Graph()).add(triple)
            else:
                ground.append(" ".join(term.n3() for term in triple))
        hashes = {
            node: format(to_isomorphic(component).graph_digest(), "x")
            for node, component in components.items()
        }

        def term_key(term):
            return hashes[root(term)] if isinstance(term, BNode) else term.n3()

        component_digests = sorted(hashes.values())
        link_digests = sorted(f"{term_key(s)} {term_key(o)}" for s, _, o in links)
        encoded = "\n".join(sorted(ground) + component_digests + link_digests)
        return hashlib.sha256(encoded.encode()).hexdigest()

    return sorted({graph_signature(sample) for sample in samples})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--new", type=Path, required=True)
    parser.add_argument("--old-lock", type=Path, required=True)
    parser.add_argument("--new-lock", type=Path, required=True)
    parser.add_argument("--shapes", type=Path)
    parser.add_argument("--manifest", type=Path)
    model_source = parser.add_mutually_exclusive_group()
    model_source.add_argument("--models", type=Path, nargs="+")
    model_source.add_argument("--models-dir", type=Path)
    parser.add_argument(
        "--operations",
        nargs="+",
        choices=["infer", "validate", "report"],
        default=["infer", "validate", "report"],
    )
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--artifacts-dir", type=Path)
    args = parser.parse_args()
    if args.samples < 1 or args.warmups < 0:
        parser.error("--samples must be positive and --warmups nonnegative")
    lock_hash = digest(args.old_lock)
    if lock_hash != digest(args.new_lock):
        parser.error("old and new Cargo.lock files differ; use fixed dependencies")
    old = args.old.resolve(strict=True)
    new = args.new.resolve(strict=True)
    if args.manifest:
        if args.shapes or args.models or args.models_dir:
            parser.error("--manifest cannot be combined with --shapes or --models")
        manifest = json.loads(args.manifest.read_text())
        workloads = [
            (
                Path(case["shapes"]).resolve(strict=True),
                Path(case["data"]).resolve(strict=True),
                case["name"],
            )
            for case in manifest
        ]
    else:
        if not args.shapes or not (args.models or args.models_dir):
            parser.error("provide --shapes and --models/--models-dir, or --manifest")
        shapes = args.shapes.resolve(strict=True)
        model_paths = (
            sorted(args.models_dir.glob("*.ttl")) if args.models_dir else args.models
        )
        workloads = [
            (shapes, model.resolve(strict=True), model.stem) for model in model_paths
        ]
    if not workloads:
        parser.error("no Turtle workloads found")
    if args.artifacts_dir:
        args.artifacts_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "lock_sha256": lock_hash,
        "old_binary": str(old),
        "new_binary": str(new),
        "samples_per_binary": args.samples,
        "warmups_per_binary": args.warmups,
    }
    if args.resume and args.output.exists():
        output = json.loads(args.output.read_text())
        for key, value in config.items():
            if output.get(key) != value:
                parser.error(f"cannot resume: {key} differs from existing output")
        records = output["conditions"]
    else:
        records = []
        output = {
            **config,
            "platform": platform.platform(),
            "shapes": str(args.shapes.resolve(strict=True)) if args.shapes else None,
            "manifest": str(args.manifest.resolve(strict=True))
            if args.manifest
            else None,
            "complete": False,
            "conditions": records,
        }
    completed = {(row["model"], row["operation"]) for row in records}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for shapes, model, case_name in workloads:
        for operation in args.operations:
            if (str(model), operation) in completed:
                continue
            paths = {"old": old, "new": new}
            for warmup in range(args.warmups):
                for version in ["old", "new"] if warmup % 2 == 0 else ["new", "old"]:
                    run(paths[version], shapes, model, operation)
            samples = {"old": [], "new": []}
            for index in range(args.samples):
                for version in ["old", "new"] if index % 2 == 0 else ["new", "old"]:
                    samples[version].append(
                        run(paths[version], shapes, model, operation)
                    )
            old_summary = summarize(samples["old"])
            new_summary = summarize(samples["new"])
            equal = old_summary["stdout_sha256"] == new_summary["stdout_sha256"]
            byte_deterministic = all(
                len(summary["stdout_sha256"]) == 1
                for summary in [old_summary, new_summary]
            )
            old_semantic = semantic_signatures(samples["old"], operation)
            new_semantic = semantic_signatures(samples["new"], operation)
            semantic_equal = old_semantic == new_semantic
            semantic_deterministic = len(old_semantic) == len(new_semantic) == 1
            if args.artifacts_dir and not (equal and byte_deterministic):
                stem = f"{case_name}-{operation}"
                for version in ["old", "new"]:
                    (args.artifacts_dir / f"{stem}-{version}.stdout").write_bytes(
                        samples[version][0]["stdout"]
                    )
            old_profile = run(old, shapes, model, operation, profile=True)[
                "stdout"
            ].decode("utf-8", errors="replace")
            new_profile = run(new, shapes, model, operation, profile=True)[
                "stdout"
            ].decode("utf-8", errors="replace")
            record = {
                "case": case_name,
                "shapes": str(shapes),
                "model": str(model),
                "operation": operation,
                "old": old_summary,
                "new": new_summary,
                "exact_stdout_equal": equal,
                "deterministic_stdout": byte_deterministic,
                "semantic_equal": semantic_equal,
                "semantic_comparison": (
                    "CLI text" if operation == "validate" else "RDF graph isomorphism"
                ),
                "deterministic_semantics": semantic_deterministic,
                "old_semantic_signatures": old_semantic,
                "new_semantic_signatures": new_semantic,
                "old_profile_stages_ms": {
                    name: float(value) for name, value in STAGE.findall(old_profile)
                },
                "new_profile_stages_ms": {
                    name: float(value) for name, value in STAGE.findall(new_profile)
                },
                "new_profile_storage": [
                    line
                    for line in new_profile.splitlines()
                    if line.startswith(
                        ("profile: storage", "profile: graph materialization")
                    )
                ],
                "new_profile_indexes": [
                    line
                    for line in new_profile.splitlines()
                    if line.startswith("profile: index:")
                ],
            }
            records.append(record)
            args.output.write_text(json.dumps(output, indent=2) + "\n")
            print(
                f"{model.name} {operation}: {old_summary['median_ms']:.1f} -> "
                f"{new_summary['median_ms']:.1f} ms; exact={equal}; semantic={semantic_equal}",
                flush=True,
            )
    output["complete"] = True
    args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
