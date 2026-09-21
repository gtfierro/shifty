"""Compare cold compilation and warm-session lifecycle costs in release examples.

Build the same example from the baseline and current worktrees first. With
``--data-b``, each fresh process runs ``bench_sessions``: one compilation serves
two data graphs, repeated validation, and an edited snapshot. With
``--many-count``, it runs ``bench_many_sessions`` and retains that many sessions
sharing one compilation.
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


def run(binary: Path, arguments: tuple[str, str, str], infer: bool) -> dict:
    command = [str(binary), *arguments]
    if infer:
        command.append("--infer")
    timer = ["/usr/bin/time", "-l" if sys.platform == "darwin" else "-v"]
    started = time.perf_counter()
    result = subprocess.run(timer + command, capture_output=True, check=False)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if result.returncode:
        raise RuntimeError(
            f"{' '.join(command)} exited {result.returncode}: "
            f"{result.stderr.decode(errors='replace')}"
        )
    line = result.stdout.decode().strip().splitlines()[-1]
    fields = dict(part.split("=", 1) for part in line.split(","))
    metrics = {
        key: float(value) for key, value in fields.items() if key.endswith("_ms")
    }
    stderr = result.stderr.decode(errors="replace")
    match = (MAC_RSS if sys.platform == "darwin" else LINUX_RSS).search(stderr)
    return {
        "elapsed_ms": elapsed_ms,
        "stages_ms": metrics,
        "peak_rss_bytes": (
            int(match.group(1)) * (1 if sys.platform == "darwin" else 1024)
            if match
            else None
        ),
    }


def summarize(samples: list[dict]) -> dict:
    elapsed = [sample["elapsed_ms"] for sample in samples]
    rss = [sample["peak_rss_bytes"] for sample in samples]
    stages = samples[0]["stages_ms"]
    return {
        "samples": samples,
        "elapsed_median_ms": statistics.median(elapsed),
        "elapsed_spread_ms": max(elapsed) - min(elapsed),
        "peak_rss_median_bytes": statistics.median(rss)
        if all(value is not None for value in rss)
        else None,
        "stage_median_ms": {
            name: statistics.median(sample["stages_ms"][name] for sample in samples)
            for name in stages
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--new", type=Path, required=True)
    parser.add_argument("--old-lock", type=Path, required=True)
    parser.add_argument("--new-lock", type=Path, required=True)
    parser.add_argument("--shapes", type=Path, required=True)
    parser.add_argument("--data-a", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--data-b", type=Path)
    mode.add_argument("--many-count", type=int)
    parser.add_argument("--infer", action="store_true")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.samples < 1 or args.warmups < 0:
        parser.error("--samples must be positive and --warmups nonnegative")
    if args.many_count is not None and args.many_count < 1:
        parser.error("--many-count must be positive")
    old_lock = hashlib.sha256(args.old_lock.read_bytes()).hexdigest()
    if old_lock != hashlib.sha256(args.new_lock.read_bytes()).hexdigest():
        parser.error("old and new Cargo.lock files differ")

    binaries = {
        "old": args.old.resolve(strict=True),
        "new": args.new.resolve(strict=True),
    }
    shapes = args.shapes.resolve(strict=True)
    data_a = args.data_a.resolve(strict=True)
    third = (
        str(args.data_b.resolve(strict=True)) if args.data_b else str(args.many_count)
    )
    arguments = (str(shapes), str(data_a), third)
    for index in range(args.warmups):
        for version in ["old", "new"] if index % 2 == 0 else ["new", "old"]:
            run(binaries[version], arguments, args.infer)
    samples = {"old": [], "new": []}
    for index in range(args.samples):
        for version in ["old", "new"] if index % 2 == 0 else ["new", "old"]:
            samples[version].append(run(binaries[version], arguments, args.infer))
    output = {
        "platform": platform.platform(),
        "lock_sha256": old_lock,
        "binaries": {name: str(path) for name, path in binaries.items()},
        "arguments": arguments,
        "mode": "many" if args.many_count else "lifecycle",
        "inference": args.infer,
        "samples_per_binary": args.samples,
        "warmups_per_binary": args.warmups,
        "old": summarize(samples["old"]),
        "new": summarize(samples["new"]),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
