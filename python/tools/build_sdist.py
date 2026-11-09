"""Build a source archive whose Cargo.lock matches its reduced workspace.

Run from the locked Python development environment (Python 3.12).
"""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[2]


def dependency_keys(package: dict, packages: dict) -> set[tuple]:
    """Resolve Cargo's shortened dependency references to exact package keys."""
    keys = set()
    for reference in package.get("dependencies", []):
        parts = reference.split(" ", 2)
        matches = [
            key
            for key in packages
            if key[0] == parts[0]
            and (len(parts) < 2 or key[1] == parts[1])
            and (
                len(parts) < 3 or key[2] == parts[2].removeprefix("(").removesuffix(")")
            )
        ]
        if len(matches) != 1:
            raise ValueError(f"Ambiguous or missing locked dependency: {reference}")
        keys.add(matches[0])
    return keys


def check_pruned_lock(original: bytes, pruned: bytes) -> None:
    """Allow removal of unused packages/edges, never dependency replacement."""
    before = tomllib.loads(original.decode())
    after = tomllib.loads(pruned.decode())
    if before.get("version") != after.get("version"):
        raise ValueError("Cargo changed the lockfile format")
    packages = {
        (package["name"], package["version"], package.get("source")): package
        for package in before["package"]
    }
    retained = {
        (package["name"], package["version"], package.get("source")): package
        for package in after["package"]
    }
    for package in after["package"]:
        key = (package["name"], package["version"], package.get("source"))
        previous = packages.get(key)
        if previous is None:
            raise ValueError(f"Cargo added or replaced a locked dependency: {key}")
        # Cargo also removes unused feature edges and shortens unambiguous
        # dependency references. All other metadata, including checksums, stays.
        previous_metadata = {k: v for k, v in previous.items() if k != "dependencies"}
        metadata = {k: v for k, v in package.items() if k != "dependencies"}
        if previous_metadata != metadata:
            raise ValueError(f"Cargo changed locked dependency metadata: {key}")
        if not dependency_keys(package, retained) <= dependency_keys(
            previous, packages
        ):
            raise ValueError(f"Cargo changed locked dependency edges: {key}")


def prepare_archive(archive: Path, output: Path, env: dict[str, str]) -> Path:
    with tempfile.TemporaryDirectory(prefix="shifty-sdist-") as temporary:
        staging = Path(temporary)
        with tarfile.open(archive) as source:
            source.extractall(staging, filter="data")
        roots = list(staging.iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise ValueError("Expected one source-archive root directory")
        workspace = roots[0]
        lock = workspace / "Cargo.lock"
        original = lock.read_bytes()
        command = ["cargo", "metadata", "--offline", "--format-version", "1"]
        # Keep existing selections while Cargo prunes the omitted CLI/C++/Wasm
        # members and their feature edges. Never touch the checkout's lockfile.
        subprocess.run(
            command, cwd=workspace, env=env, stdout=subprocess.DEVNULL, check=True
        )
        pruned = lock.read_bytes()
        check_pruned_lock(original, pruned)
        subprocess.run(
            [*command, "--locked"],
            cwd=workspace,
            env=env,
            stdout=subprocess.DEVNULL,
            check=True,
        )
        destination = output / archive.name
        with (
            tarfile.open(archive) as source,
            tempfile.NamedTemporaryFile(
                dir=output, suffix=".tar.gz", delete=False
            ) as pending,
        ):
            temporary_archive = Path(pending.name)
            try:
                with tarfile.open(fileobj=pending, mode="w:gz") as target:
                    for member in source:
                        if member.name == f"{workspace.name}/Cargo.lock":
                            member.size = len(pruned)
                            target.addfile(member, io.BytesIO(pruned))
                        else:
                            target.addfile(
                                member,
                                source.extractfile(member) if member.isfile() else None,
                            )
                pending.close()
                temporary_archive.replace(destination)
            finally:
                temporary_archive.unlink(missing_ok=True)
        return destination


def verify_archive(archive: Path) -> None:
    """Build a wheel from a fresh extraction without changing its lockfile."""
    with tempfile.TemporaryDirectory(prefix="shifty-sdist-check-") as temporary:
        staging = Path(temporary)
        with tarfile.open(archive) as source:
            source.extractall(staging, filter="data")
        workspace = next(staging.iterdir())
        subprocess.run(
            [
                sys.executable,
                "-m",
                "maturin",
                "build",
                "--release",
                "--locked",
                "--offline",
                "--manifest-path",
                str(workspace / "python/Cargo.toml"),
                "--out",
                str(staging / "wheels"),
            ],
            cwd=workspace,
            check=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=ROOT / "target/sdist")
    parser.add_argument(
        "--verify", action="store_true", help="Rebuild a wheel offline with --locked"
    )
    args = parser.parse_args()
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=True)
    checkout_lock = (ROOT / "Cargo.lock").read_bytes()
    with tempfile.TemporaryDirectory(prefix="shifty-maturin-") as temporary:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "maturin",
                "sdist",
                "--manifest-path",
                str(ROOT / "python/Cargo.toml"),
                "--out",
                temporary,
            ],
            cwd=ROOT,
            check=True,
        )
        if (ROOT / "Cargo.lock").read_bytes() != checkout_lock:
            raise ValueError("Maturin changed the checkout's Cargo.lock")
        archives = list(Path(temporary).glob("*.tar.gz"))
        if len(archives) != 1:
            raise ValueError("Expected maturin to produce one source archive")
        archive = prepare_archive(archives[0], output, os.environ.copy())
        if args.verify:
            verify_archive(archive)
        print(archive)


if __name__ == "__main__":
    main()
