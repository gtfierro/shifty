"""Source archives must retain the workspace's exact dependency selections."""

import io
import os
import subprocess
import tarfile

import pytest

from tools.build_sdist import check_pruned_lock, prepare_archive

LOCK = b"""version = 4

[[package]]
name = "bindings"
version = "0.1.0"

[[package]]
name = "omitted-cli"
version = "0.1.0"
"""


def test_packaged_lock_builds_without_initial_rewrite(tmp_path):
    members = {
        "Cargo.toml": b'[workspace]\nmembers = ["python"]\nresolver = "2"\n',
        "Cargo.lock": LOCK,
        "python/Cargo.toml": b'[package]\nname = "bindings"\nversion = "0.1.0"\n',
        "python/src/lib.rs": b"",
        "LICENSE": b"license text\n",
    }
    original = tmp_path / "source.tar.gz"
    with tarfile.open(original, "w:gz") as archive:
        for name, contents in members.items():
            entry = tarfile.TarInfo(f"source/{name}")
            entry.size = len(contents)
            archive.addfile(entry, io.BytesIO(contents))
    output = tmp_path / "dist"
    output.mkdir()
    prepared = prepare_archive(original, output, os.environ.copy())
    extracted = tmp_path / "extracted"
    with tarfile.open(prepared) as archive:
        archive.extractall(extracted, filter="data")
        assert archive.extractfile("source/LICENSE").read() == members["LICENSE"]
    workspace = extracted / "source"
    lock = workspace / "Cargo.lock"
    before = lock.read_bytes()
    assert b"omitted-cli" not in before
    subprocess.run(
        ["cargo", "metadata", "--offline", "--locked", "--format-version", "1"],
        cwd=workspace,
        stdout=subprocess.DEVNULL,
        check=True,
    )
    assert lock.read_bytes() == before
    with tarfile.open(original) as archive:
        assert archive.extractfile("source/Cargo.lock").read() == LOCK
    for name, contents in members.items():
        if name != "Cargo.lock":
            assert (workspace / name).read_bytes() == contents


@pytest.mark.parametrize(
    "change",
    [
        lambda data: data.replace(b'"1.2.3"', b'"1.2.4"'),
        lambda data: data.replace(b'checksum = "original"', b'checksum = "changed"'),
        lambda data: data.replace(b'"registry+original"', b'"registry+other"'),
    ],
)
def test_pruning_cannot_replace_locked_dependencies(change):
    original = b"""version = 4
[[package]]
name = "dependency"
version = "1.2.3"
source = "registry+original"
checksum = "original"
"""
    with pytest.raises(ValueError, match="locked dependency"):
        check_pruned_lock(original, change(original))


def test_pruning_cannot_switch_to_another_version_already_in_the_lock():
    original = b"""version = 4
[[package]]
name = "bindings"
version = "0.1.0"
dependencies = ["library 1.0.0"]
[[package]]
name = "library"
version = "1.0.0"
[[package]]
name = "library"
version = "2.0.0"
"""
    pruned = original.replace(b'"library 1.0.0"', b'"library 2.0.0"')
    with pytest.raises(ValueError, match="locked dependency edges"):
        check_pruned_lock(original, pruned)


def test_pruning_can_shorten_unambiguous_dependency_references():
    original = b"""version = 4
[[package]]
name = "bindings"
version = "0.1.0"
dependencies = ["library 1.0.0"]
[[package]]
name = "library"
version = "1.0.0"
[[package]]
name = "library"
version = "2.0.0"
"""
    pruned = original.split(b'[[package]]\nname = "library"\nversion = "2.0.0"')[0]
    pruned = pruned.replace(b'"library 1.0.0"', b'"library"')
    check_pruned_lock(original, pruned)
