"""The docs' Pygments lexer for Shapeshift agrees with the language."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from pygments.token import Error, Keyword, Literal, Name, Operator, Punctuation, String

from shifty import _shifty

ROOT = Path(__file__).resolve().parents[2]
SHM_FILES = sorted(
    p
    for p in ROOT.rglob("*.shm")
    if not {"target", ".venv", "node_modules", "_build"} & set(p.parts)
)


@pytest.fixture(scope="module")
def lexer():
    spec = importlib.util.spec_from_file_location(
        "shapeshift_lexer", ROOT / "docs" / "_ext" / "shapeshift_lexer.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tokens(lexer, text: str):
    """The tokens of `text`, without whitespace."""
    return [(t, v) for t, v in lexer.ShapeshiftLexer().get_tokens(text) if v.strip()]


def test_the_lexer_knows_every_reserved_word(lexer):
    assert set(_shifty._shapeshift_keywords()) <= lexer.KEYWORDS


@pytest.mark.parametrize("path", SHM_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_every_shm_file_lexes(lexer, path):
    text = path.read_text()
    lexed = list(lexer.ShapeshiftLexer().get_tokens(text))
    assert [v for t, v in lexed if t in Error] == []
    # the tokens cover the text exactly (Pygments ends it with a newline)
    assert "".join(v for _, v in lexed) == text.rstrip("\n") + "\n"


def test_tokens(lexer):
    assert tokens(lexer, "shape vav for every brick:VAV { ztemp: with-unit(x) }") == [
        (Keyword, "shape"),
        (Name.Class, "vav"),
        (Keyword, "for"),
        (Keyword, "every"),
        (Name.Constant, "brick:VAV"),
        (Punctuation, "{"),
        (Name.Label, "ztemp:"),
        (Name.Function, "with-unit"),
        (Punctuation, "("),
        (Name, "x"),
        (Punctuation, ")"),
        (Punctuation, "}"),
    ]
    # a keyword inside a hyphenated name is part of the name
    assert tokens(lexer, "points each zone-co2-level") == [
        (Name, "points"),
        (Operator.Word, "each"),
        (Name, "zone-co2-level"),
    ]
    assert tokens(lexer, 'prefix brick: <urn:b#>\ndoc "x"@en') == [
        (Keyword, "prefix"),
        (Name.Namespace, "brick:"),
        (String.Other, "<urn:b#>"),
        (Name.Decorator, "doc"),
        (String, '"x"@en'),
    ]
    # dates and times are literals, not numbers and names
    assert tokens(
        lexer, "each from 2024-01-31 below 2024-02-01T08:00:00Z, up to 18:00:00"
    ) == [
        (Operator.Word, "each"),
        (Operator.Word, "from"),
        (Literal.Date, "2024-01-31"),
        (Operator.Word, "below"),
        (Literal.Date, "2024-02-01T08:00:00Z"),
        (Punctuation, ","),
        (Operator.Word, "up"),
        (Operator.Word, "to"),
        (Literal.Date, "18:00:00"),
    ]
    assert tokens(lexer, "each a brick:AHU or brick:RTU") == [
        (Operator.Word, "each"),
        (Operator.Word, "a"),
        (Name.Constant, "brick:AHU"),
        (Operator.Word, "or"),
        (Name.Constant, "brick:RTU"),
    ]
