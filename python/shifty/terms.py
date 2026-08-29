"""Typed RDF terms: hashable, pattern-matchable, replacing rendered strings.

Everywhere the rest of the API exposed a term as a rendered N-Triples string
(``mapping.focus``, a binding's ``values``), it now speaks these instead::

    match term:
        case Iri(value):
            ...
        case Literal(value, datatype, language):
            ...
        case BNode(id):
            ...

``from_json`` decodes the SPARQL-JSON term encoding evidence trees use
(``{"type": "uri"|"bnode"|"literal", "value": …, "datatype"?: …,
"xml:lang"?: …}``); ``parse`` decodes the N-Triples spelling the pyclasses
render focus nodes as (``<…>`` / ``"…"`` / ``_:…``). Both are plain functions,
not methods, since ``Term`` is a bare :class:`typing.Union` and cannot carry
attributes.
"""

from __future__ import annotations

import decimal
import re
from dataclasses import dataclass
from typing import Optional, Union

__all__ = ["Iri", "Literal", "BNode", "Term", "from_json", "parse"]

_XSD = "http://www.w3.org/2001/XMLSchema#"
_XSD_STRING = _XSD + "string"

_XSD_INT_LOCALS = {
    "integer",
    "int",
    "long",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "nonPositiveInteger",
    "negativeInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
}


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _unescape(value: str) -> str:
    out: list[str] = []
    i = 0
    n = len(value)
    while i < n:
        c = value[i]
        if c == "\\" and i + 1 < n:
            nxt = value[i + 1]
            mapped = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\", "'": "'"}.get(nxt)
            if mapped is not None:
                out.append(mapped)
                i += 2
                continue
        out.append(c)
        i += 1
    return "".join(out)


@dataclass(frozen=True)
class Iri:
    __match_args__ = ("value",)
    value: str  # the IRI text, no angle brackets

    def n3(self) -> str:
        return f"<{self.value}>"

    def __str__(self) -> str:
        return self.n3()

    def to_rdflib(self):
        import rdflib

        return rdflib.URIRef(self.value)


@dataclass(frozen=True)
class Literal:
    __match_args__ = ("value", "datatype", "language")
    value: str  # lexical form
    datatype: Optional[str] = None  # IRI text; None for plain/xsd:string
    language: Optional[str] = None

    def n3(self) -> str:
        escaped = _escape(self.value)
        if self.language:
            return f'"{escaped}"@{self.language}'
        if self.datatype and self.datatype != _XSD_STRING:
            return f'"{escaped}"^^<{self.datatype}>'
        return f'"{escaped}"'

    def __str__(self) -> str:
        return self.value

    def to_python(self):
        """Coerce by datatype: int/float/bool/decimal, falling back to the
        lexical form for everything else (including plain/xsd:string)."""
        dt = self.datatype
        if not dt or not dt.startswith(_XSD):
            return self.value
        local = dt[len(_XSD) :]
        try:
            if local in _XSD_INT_LOCALS:
                return int(self.value)
            if local == "decimal":
                return decimal.Decimal(self.value)
            if local in ("double", "float"):
                return float(self.value)
            if local == "boolean":
                return self.value.strip().lower() in ("true", "1")
        except (ValueError, decimal.InvalidOperation):
            return self.value
        return self.value

    def to_rdflib(self):
        import rdflib

        return rdflib.Literal(
            self.value,
            datatype=rdflib.URIRef(self.datatype) if self.datatype else None,
            lang=self.language,
        )


@dataclass(frozen=True)
class BNode:
    __match_args__ = ("id",)
    id: str

    def n3(self) -> str:
        return f"_:{self.id}"

    def __str__(self) -> str:
        return self.n3()

    def to_rdflib(self):
        import rdflib

        return rdflib.BNode(self.id)


Term = Union[Iri, Literal, BNode]


def from_json(term: dict) -> Term:
    """SPARQL-JSON term (``{"type": ..., "value": ..., ...}``) -> `Term`."""
    kind = term.get("type")
    value = term.get("value", "")
    if kind == "uri":
        return Iri(value)
    if kind == "bnode":
        return BNode(value)
    datatype = term.get("datatype")
    language = term.get("xml:lang") or term.get("lang")
    return Literal(value, datatype=datatype or None, language=language or None)


_IRI_RE = re.compile(r"^<(.*)>$", re.DOTALL)
_BNODE_RE = re.compile(r"^_:(.+)$", re.DOTALL)
_LITERAL_RE = re.compile(
    r'^"((?:[^"\\]|\\.)*)"(?:\^\^<([^>]*)>|@([A-Za-z][A-Za-z0-9-]*))?$', re.DOTALL
)


def parse(text: str) -> Term:
    """N-Triples spelling (``<…>`` / ``"…"`` / ``_:…``) -> `Term`."""
    m = _IRI_RE.match(text)
    if m:
        return Iri(m.group(1))
    m = _BNODE_RE.match(text)
    if m:
        return BNode(m.group(1))
    m = _LITERAL_RE.match(text)
    if m:
        lexical = _unescape(m.group(1))
        datatype, language = m.group(2), m.group(3)
        return Literal(lexical, datatype=datatype or None, language=language or None)
    raise ValueError(f"cannot parse term: {text!r}")
