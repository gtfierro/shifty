"""Pygments lexer for Shapeshift (``.shm``), and the Sphinx extension that registers it.

Use it in the docs as ``.. code-block:: shapeshift`` (or ``shm``). The token
classes follow the tree-sitter grammar's ``queries/highlights.scm``.
``python/tests/test_shapeshift_lexer.py`` checks that the lexer knows every
keyword the parser reserves, and lexes every ``.shm`` file in the repository
without an error token.
"""

from __future__ import annotations

from pygments.lexer import RegexLexer, bygroups
from pygments.token import (
    Comment,
    Keyword,
    Literal,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Whitespace,
)

#: Words that begin a declaration, a target, or a frame.
DECLARATIONS = frozenset(
    """
    base prefix import as shape path for every node value of with matching sparql
    only these properties when then any none one
    """.split()
)

#: Words inside lines: counts, qualifiers, and tests.
OPERATORS = frozenset(
    """
    required optional exactly at least most between and each is a matches same
    different from above below unique language length to up pattern flags datatype
    iri literal blank or
    """.split()
)

#: Line annotations.
ANNOTATIONS = frozenset({"doc", "message", "severity"})

#: Severity levels, booleans, and `self`.
CONSTANTS = frozenset({"info", "warning", "violation", "true", "false", "self"})

#: The built-in datatype names, as tests.
DATATYPES = frozenset(
    """
    string integer decimal double float boolean date dateTime time anyURI langString
    """.split()
)

KEYWORDS = DECLARATIONS | OPERATORS | ANNOTATIONS | CONSTANTS

_IDENT = r"[A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*"
_PREFIXED = (
    r"(?:[A-Za-z_][A-Za-z0-9_-]*)?:[A-Za-z0-9_%](?:[A-Za-z0-9_.%-]*[A-Za-z0-9_%-])?"
)


def _word(lexer, match):
    """A bare identifier: a keyword, a datatype, or a name."""
    word = match.group(0)
    if word in DECLARATIONS:
        token = Keyword
    elif word in OPERATORS:
        token = Operator.Word
    elif word in ANNOTATIONS:
        token = Name.Decorator
    elif word in CONSTANTS:
        token = Keyword.Constant
    elif word in DATATYPES:
        token = Keyword.Type
    else:
        token = Name
    yield match.start(), token, word


class ShapeshiftLexer(RegexLexer):
    """Shapeshift, the shape language of Shifty."""

    name = "Shapeshift"
    aliases = ["shapeshift", "shm"]
    filenames = ["*.shm"]
    mimetypes = ["text/x-shapeshift"]

    tokens = {
        "root": [
            (r"[ \t\r]+", Whitespace),
            (r"\n", Whitespace),
            (r"--[^\n]*", Comment.Single),
            (r'"""(?:[^"\\]|\\.|"(?!""))*"""(?:@[A-Za-z]+(?:-[A-Za-z0-9]+)*)?', String),
            (r'"(?:[^"\\\n]|\\.)*"(?:@[A-Za-z]+(?:-[A-Za-z0-9]+)*)?', String),
            (r"<[^<>\"{}|^`\\\s]*>", String.Other),
            # `prefix brick: <...>`: the label is the prefix being declared
            (
                r"(prefix)(\s+)([A-Za-z_][A-Za-z0-9_-]*:)",
                bygroups(Keyword, Whitespace, Name.Namespace),
            ),
            # the name a declaration defines
            (rf"(shape)(\s+)({_IDENT})", bygroups(Keyword, Whitespace, Name.Class)),
            (
                rf"(path)(\s+)({_IDENT})(?=\s*[(=])",
                bygroups(Keyword, Whitespace, Name.Function),
            ),
            # a date, dateTime, or time, written bare: `2024-01-31`, `08:00:00`
            (
                r"(?:\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?)?"
                r"|\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?:Z|[+-]\d{2}:\d{2})?(?![\w:.-])",
                Literal.Date,
            ),
            (r"-?(?:\d+\.\d+|\d+(?:\.\d+)?[eE][+-]?\d+|\.\d+)", Number.Float),
            (r"-?\d+", Number.Integer),
            (_PREFIXED, Name.Constant),
            # `label:` names a line, a row, or a field
            (r"[A-Za-z_][A-Za-z0-9_-]*:", Name.Label),
            (rf"{_IDENT}(?=\()", Name.Function),
            (_IDENT, _word),
            (r"[{}()]", Punctuation),
            (r"[,;]", Punctuation),
            (r"=", Operator),
            (r"[\^/*+?]", Operator),
        ],
    }


def setup(app):
    app.add_lexer("shapeshift", ShapeshiftLexer)
    app.add_lexer("shm", ShapeshiftLexer)
    return {"parallel_read_safe": True, "parallel_write_safe": True}


__all__ = ["ShapeshiftLexer", "KEYWORDS", "DATATYPES"]
