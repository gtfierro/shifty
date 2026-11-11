#!/usr/bin/env python3
"""Build the documentation's release history from recorded CLI timings.

Run with Python 3.10 or newer. The script reads legacy text tables and optional
run metadata. It never starts measurements or parses RDF fixtures.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "benchmark/results"
DATASETS = {"brick": "Brick", "s223": "ASHRAE 223P"}
VERSION = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)(?:-(alpha|beta|rc)\.(\d+))?$")
ROW = re.compile(
    r"^(\S+\.ttl)\s+"
    + r"\s+".join([r"(\d+(?:\.\d+)?)"] * 4)
    + r"(?:\s+\d+(?:\.\d+)?\s+\d+(?:\.\d+)?)?\s*$"
)


def version_key(version: str) -> tuple[int, ...]:
    if version == "HEAD":
        return (1,)
    match = VERSION.fullmatch(version)
    if match is None:
        raise ValueError(f"Invalid release version: {version}")
    major, minor, patch, stage, number = match.groups()
    return (
        0,
        int(major),
        int(minor),
        int(patch),
        {"alpha": 0, "beta": 1, "rc": 2, None: 3}[stage],
        int(number or 0),
    )


def release_tags() -> list[str]:
    result = subprocess.run(
        ["git", "tag", "--list", "v*"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted(
        (tag for tag in result.stdout.splitlines() if VERSION.fullmatch(tag)),
        key=version_key,
    )


def parse_bench_file(path: Path) -> dict[str, dict]:
    models = {}
    for line in path.read_text().splitlines():
        match = ROW.fullmatch(line.strip())
        if match is None:
            continue
        name, infer, infer_spread, validate, validate_spread = match.groups()
        if name in models:
            raise ValueError(f"Duplicate model {name} in {path}")
        if float(validate) <= 0:
            continue
        models[name] = {
            "infer_ms": float(infer) or None,
            "infer_spread_ms": float(infer_spread),
            "val_ms": float(validate),
            "val_spread_ms": float(validate_spread),
        }
    return models


def geomean(values: list[float]) -> float:
    return (
        math.exp(sum(math.log(value) for value in values) / len(values))
        if values
        else 0.0
    )


def collect_versions(results_dir: Path = RESULTS_DIR) -> list[str]:
    if not results_dir.exists():
        return []
    return sorted(
        (
            path.name
            for path in results_dir.iterdir()
            if path.is_dir() and (VERSION.fullmatch(path.name) or path.name == "HEAD")
        ),
        key=version_key,
    )


def build_data(results_dir: Path = RESULTS_DIR) -> dict:
    versions = collect_versions(results_dir)
    datasets = []
    revisions_file = results_dir / "revisions.json"
    revisions = (
        json.loads(revisions_file.read_text()) if revisions_file.exists() else {}
    )
    for dataset, label in DATASETS.items():
        expected = {
            path.name
            for path in (ROOT / "benchmark" / dataset / "models").glob("*.ttl")
        }
        runs = []
        for version in versions:
            path = results_dir / version / f"{dataset}.txt"
            metadata_path = path.with_suffix(".json")
            if not path.exists() and not metadata_path.exists():
                continue
            metadata = (
                json.loads(metadata_path.read_text())
                if metadata_path.exists()
                else None
            )
            models = (
                metadata["models"]
                if metadata and "models" in metadata
                else parse_bench_file(path)
            )
            if not models:
                continue
            infer = [row["infer_ms"] for row in models.values()]
            runs.append(
                {
                    "version": version,
                    "n_models": len(models),
                    "complete": set(models) == expected,
                    "mean_ms": sum(row["val_ms"] for row in models.values())
                    / len(models),
                    "geomean_ms": geomean([row["val_ms"] for row in models.values()]),
                    "infer_mean_ms": sum(infer) / len(infer)
                    if all(value is not None for value in infer)
                    else None,
                    "models": models,
                    "metadata": metadata,
                    "revision": (metadata or {}).get("revision")
                    or revisions.get(version),
                }
            )
        datasets.append(
            {
                "dataset": dataset,
                "label": label,
                "n_models": len(expected),
                "runs": runs,
            }
        )
    return {"versions": versions, "datasets": datasets}


def write_data(output: Path, results_dir: Path = RESULTS_DIR) -> None:
    data = json.dumps(build_data(results_dir), indent=2, allow_nan=False) + "\n"
    if output.suffix == ".js":
        data = "window.SHIFTY_BENCHMARK_DATA = " + data.rstrip() + ";\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=RESULTS_DIR)
    parser.add_argument(
        "--output", type=Path, default=RESULTS_DIR / "benchmark_data.json"
    )
    args = parser.parse_args()
    write_data(args.output, args.results)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
