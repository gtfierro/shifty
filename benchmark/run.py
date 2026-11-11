#!/usr/bin/env python3
"""Record fresh-process inference and validation timings for Shifty releases.

Examples:
    python3 benchmark/run.py
    python3 benchmark/run.py v0.5.0 v0.5.1 v0.6.0-alpha.4
    python3 benchmark/run.py --history --list
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from process_results import (
    DATASETS,
    RESULTS_DIR,
    ROOT,
    parse_bench_file,
    release_tags,
    version_key,
)


def checked(command: list[str], cwd: Path = ROOT) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def fingerprint(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode())
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def write_release_revisions(output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    destination = output / "revisions.json"
    revisions = json.loads(destination.read_text()) if destination.exists() else {}
    revisions.update(
        {
            tag: checked(["git", "rev-parse", f"refs/tags/{tag}^{{commit}}"])
            for tag in release_tags()
        }
    )
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        staged = Path(temporary) / "revisions.json"
        staged.write_text(json.dumps(revisions, indent=2, sort_keys=True) + "\n")
        staged.replace(destination)


@contextmanager
def binary_for(version: str, binary: Path | None):
    if binary is not None:
        yield binary.resolve()
        return
    if version == "HEAD":
        subprocess.run(
            ["cargo", "build", "--release", "--locked", "-p", "shifty-cli"],
            cwd=ROOT,
            check=True,
        )
        yield ROOT / "target/release/shifty"
        return
    with tempfile.TemporaryDirectory(prefix="shifty-benchmark-") as temporary:
        worktree = Path(temporary) / "source"
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(worktree), version],
            cwd=ROOT,
            check=True,
        )
        try:
            subprocess.run(
                ["cargo", "build", "--release", "--locked", "-p", "shifty-cli"],
                cwd=worktree,
                check=True,
            )
            yield worktree / "target/release/shifty"
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=ROOT,
                check=True,
            )


def measure(command: list[str], iterations: int) -> tuple[float, float, list[float]]:
    samples = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        elapsed = (time.perf_counter() - start) * 1000
        validation = command[1] == "validate"
        completed = result.returncode == 0
        if validation:
            completed = result.returncode in (0, 1) and result.stdout.startswith(
                ("conforms: true", "conforms: false")
            )
        if not completed:
            raise RuntimeError(
                f"Command failed ({result.returncode}): {' '.join(command)}\n{result.stderr}"
            )
        samples.append(elapsed)
    median = statistics.median(samples)
    spread = max(abs(value - median) for value in samples)
    return median, spread, samples


def suite_inputs(suite: str) -> tuple[Path, list[Path]]:
    directory = ROOT / "benchmark" / suite
    shapes = directory / (
        "Brick-closure.ttl" if suite == "brick" else "223p-closure.ttl"
    )
    models = sorted((directory / "models").glob("*.ttl"))
    if not models:
        raise ValueError(f"No models found for {suite}")
    return shapes, models


def reusable(suite: str, version: str, output: Path, iterations: int) -> bool:
    destination = output / version / f"{suite}.txt"
    metadata_file = destination.with_suffix(".json")
    if not destination.exists() and not metadata_file.exists():
        return False
    shapes, models = suite_inputs(suite)
    metadata = json.loads(metadata_file.read_text()) if metadata_file.exists() else None
    recorded = (
        metadata["models"]
        if metadata and "models" in metadata
        else parse_bench_file(destination)
    )
    if set(recorded) != {model.name for model in models}:
        return False
    return metadata is None or (
        metadata["inputs_sha256"] == fingerprint([shapes, *models])
        and metadata["iterations"] == iterations
    )


def record_suite(
    binary: Path, suite: str, version: str, iterations: int, output: Path
) -> None:
    shapes, models = suite_inputs(suite)
    destination = output / version / f"{suite}.txt"
    metadata_file = destination.with_suffix(".json")
    inputs = fingerprint([shapes, *models])
    help_text = checked([str(binary), "validate", "--help"])
    validation_options = (
        ["--on-unsupported", "ignore"] if "--on-unsupported" in help_text else []
    )
    rows = [f"{'model':32}  infer ms  +/-  infer+val ms  +/-"]
    samples = {}
    summaries = {}
    for index, model in enumerate(models, 1):
        print(
            f"{version} {suite} [{index}/{len(models)}] {model.name}", file=sys.stderr
        )
        common = ["--shapes", str(shapes), "--data", str(model)]
        infer, infer_spread, infer_samples = measure(
            [str(binary), "infer", *common], iterations
        )
        validate, validate_spread, validate_samples = measure(
            [str(binary), "validate", *common, *validation_options], iterations
        )
        rows.append(
            f"{model.name:32}  {infer:.3f}  {infer_spread:.3f}  {validate:.3f}  {validate_spread:.3f}"
        )
        samples[model.name] = {
            "infer_ms": infer_samples,
            "validate_ms": validate_samples,
        }
        summaries[model.name] = {
            "infer_ms": infer,
            "infer_spread_ms": infer_spread,
            "val_ms": validate,
            "val_spread_ms": validate_spread,
        }
    if fingerprint([shapes, *models]) != inputs:
        raise RuntimeError("Benchmark inputs changed during collection")
    metadata = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "revision": checked(["git", "rev-parse", f"{version}^{{commit}}"]),
        "dirty": version == "HEAD"
        and bool(
            checked(
                [
                    "git",
                    "status",
                    "--porcelain",
                    "--untracked-files=no",
                    "--",
                    ":!benchmark/results",
                ]
            )
        ),
        "binary_version": checked([str(binary), "version"]),
        "binary_sha256": fingerprint([binary]),
        "inputs_sha256": inputs,
        "iterations": iterations,
        "machine": platform.platform(),
        "cpu": platform.processor(),
        "rustc": checked(["rustc", "--version"]),
        "on_unsupported": "ignore (historical default)"
        if not validation_options
        else "ignore",
        "samples": samples,
        "models": summaries,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    # JSON is the complete recording. The text table keeps older captures and
    # tools readable, and is also replaced only after collection succeeds.
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        staged = Path(temporary)
        (staged / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        (staged / "results.txt").write_text("\n".join(rows) + "\n")
        (staged / "metadata.json").replace(metadata_file)
        (staged / "results.txt").replace(destination)
    print(f"Wrote {destination}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "versions", nargs="*", help="Release tags or HEAD (default HEAD)"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="All release tags from v0.1.1 onward, including prereleases",
    )
    parser.add_argument(
        "--suite",
        choices=DATASETS,
        action="append",
        help="Repeat to select suites (default both)",
    )
    parser.add_argument(
        "--iterations", type=int, default=int(os.environ.get("BENCH_ITERS", "3"))
    )
    parser.add_argument("--output", type=Path, default=RESULTS_DIR)
    parser.add_argument(
        "--binary", type=Path, help="Use an existing binary for one requested version"
    )
    parser.add_argument(
        "--force", action="store_true", help="Replace existing tagged measurements"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List selected versions without building or measuring",
    )
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.history and args.versions:
        parser.error("Choose --history or explicit versions")
    versions = args.versions or ["HEAD"]
    if args.history:
        versions = [
            tag for tag in release_tags() if version_key(tag) >= version_key("v0.1.1")
        ]
    for version in versions:
        version_key(version)
        checked(["git", "rev-parse", "--verify", f"{version}^{{commit}}"])
    if args.binary and len(versions) != 1:
        parser.error("--binary requires exactly one version")
    if args.list:
        print("\n".join(versions))
        return 0
    write_release_revisions(args.output.resolve())
    for version in versions:
        suites = []
        for suite in args.suite or list(DATASETS):
            if (
                not args.force
                and version != "HEAD"
                and reusable(suite, version, args.output.resolve(), args.iterations)
            ):
                print(f"{version} {suite}: reusing recorded results", file=sys.stderr)
            else:
                suites.append(suite)
        if not suites:
            continue
        with binary_for(version, args.binary) as binary:
            actual_version = checked([str(binary), "version"])
            if version != "HEAD" and actual_version != version.removeprefix("v"):
                raise ValueError(f"Expected {version}, binary reports {actual_version}")
            for suite in suites:
                record_suite(
                    binary, suite, version, args.iterations, args.output.resolve()
                )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
