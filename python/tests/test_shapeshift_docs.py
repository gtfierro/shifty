"""The Shapeshift code in the docs builds, and the tutorial's Python matches it."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

from shifty import _shifty

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs" / "experimental" / "shapeshift"

# Prefixes a fragment may use without declaring them.
PREFIXES = {
    "brick": "https://brickschema.org/schema/Brick#",
    "ex": "http://example.org/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
}
TOP_LEVEL = re.compile(r"(base|prefix|import|shape|path|doc)\b")


def _blocks():
    for rst in sorted(DOCS.glob("*.rst")):
        lines = rst.read_text().splitlines()
        for i, line in enumerate(lines):
            if line.strip() != ".. code-block:: shapeshift":
                continue
            body = []
            for text in lines[i + 1 :]:
                if text.strip() and not text.startswith("   "):
                    break
                if not text.strip().startswith(":"):
                    body.append(text)
            src = textwrap.dedent("\n".join(body)).strip("\n")
            yield pytest.param(src, id=f"{rst.name}:{i + 1}")


def _as_module(src: str) -> str:
    """A fragment as a whole file: wrapped in a shape, with its prefixes."""
    first = next(
        line for line in src.splitlines() if line.strip() and not line.startswith("--")
    )
    if not TOP_LEVEL.match(first):
        src = "shape snippet {\n" + textwrap.indent(src, "  ") + "\n}"
    head = ["base <urn:example/docs/>"] if "base " not in src else []
    head += [
        f"prefix {name}: <{iri}>"
        for name, iri in PREFIXES.items()
        if f"prefix {name}:" not in src
    ]
    return "\n".join(head) + "\n" + src + "\n"


@pytest.mark.parametrize("src", list(_blocks()))
def test_doc_snippet_builds(src: str) -> None:
    if re.search(r"^\s*import\b", src, re.M):
        pytest.skip("imports files that are not in the docs")
    with tempfile.TemporaryDirectory() as d:
        diagnostics = _shifty._shapeshift_check(_as_module(src), "snippet.shm", d, {})
    errors = [r for r in diagnostics if r[0] == "error"]
    assert not errors, "\n".join(r[-1] for r in errors)


def test_snippets_found() -> None:
    assert len(list(_blocks())) >= 15


def test_tutorial_python_writes_the_tutorial_shapes() -> None:
    tutorial = ROOT / "docs" / "examples" / "shapeshift" / "tutorial"
    out = subprocess.run(
        [sys.executable, "vav.py"],
        cwd=tutorial,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert out == (tutorial / "vav.shm").read_text()
