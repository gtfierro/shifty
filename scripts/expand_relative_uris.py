#!/usr/bin/env python3
"""
Replace Turtle document IRI shorthands (`<>`) with explicit base IRIs.

Several SHACL-AF fixtures rely on `<>` to refer to the current document. Our loader
doesn't pass the file URL to the parser, so those files fail to parse. This script
scans the advanced test suite, discovers each file's base IRI (via `# baseURI:` or
`@base`), and rewrites bare `<>` occurrences to `<base-iri>`.

Usage:
    python scripts/expand_relative_uris.py [--root PATH] [--dry-run]

By default the script rewrites files in-place under
`lib/tests/test-suite/advanced`. Pass `--dry-run` to preview the changes
without modifying anything.
"""

from __future__ import annotations

import argparse
import sys
import textwrap
from pathlib import Path
from typing import Iterable, Optional
import re


BASE_COMMENT_RE = re.compile(
    r"^\s*#\s*baseURI:\s*(\S+)\s*$", re.IGNORECASE | re.MULTILINE
)
BASE_DIRECTIVE_RE = re.compile(r"@base\s*<([^>]+)>", re.IGNORECASE)
DOCUMENT_IRI_RE = re.compile(r"<>")


def find_base_iri(text: str, file_path: Path) -> Optional[str]:
    """Extract the document base IRI from comments or directives."""
    comment = BASE_COMMENT_RE.search(text)
    if comment:
        return comment.group(1).strip()

    directive = BASE_DIRECTIVE_RE.search(text)
    if directive:
        return directive.group(1).strip()

    return None


def replace_document_iris(text: str, base: str) -> tuple[str, int]:
    """Swap every Turtle document IRI shorthand (`<>`) with `<base>`."""
    replacement = f"<{base}>"
    new_text, count = DOCUMENT_IRI_RE.subn(replacement, text)
    return new_text, count


def iter_turtle_files(root: Path) -> Iterable[Path]:
    yield from root.rglob("*.ttl")


def process_file(path: Path, dry_run: bool) -> None:
    text = path.read_text(encoding="utf-8")
    base = find_base_iri(text, path)
    if not base:
        return

    updated, replacements = replace_document_iris(text, base)
    if replacements == 0 or updated == text:
        return

    if dry_run:
        sys.stdout.write(
            textwrap.dedent(
                f"""\
                --- {path}
                +++ {path} (expanded)
                applied {replacements} replacement(s) against base {base}
                """
            )
        )
    else:
        path.write_text(updated, encoding="utf-8")
        sys.stdout.write(
            f"[info] {path}: expanded {replacements} relative IRI(s) using base {base}\n"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expand relative IRIs in Turtle fixtures."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("lib/tests/test-suite/advanced"),
        help="Root directory to scan (default: lib/tests/test-suite/advanced)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print a summary of changes without modifying any files.",
    )

    args = parser.parse_args()
    if not args.root.exists():
        parser.error(f"Root directory {args.root} does not exist")

    for ttl_file in iter_turtle_files(args.root):
        process_file(ttl_file, args.dry_run)


if __name__ == "__main__":
    main()
