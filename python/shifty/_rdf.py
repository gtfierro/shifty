"""Lossless N-Triples decoding for reports returned by the Rust engine."""

from __future__ import annotations

from io import StringIO
from typing import Optional

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.plugins.parsers.ntriples import (
    NTGraphSink,
    W3CNTriplesParser,
    r_literal,
    unquote,
    uriquote,
)


class _TermPreservingParser(W3CNTriplesParser):
    def literal(self):
        if not self.peek('"'):
            return False
        lexical, language, datatype = self.eat(r_literal).groups()
        return Literal(
            unquote(lexical),
            lang=language or None,
            datatype=URIRef(uriquote(unquote(datatype))) if datatype else None,
            normalize=False,
        )


def parse_ntriples(
    data: str, bnode_context: Optional[dict[str, BNode]] = None
) -> Graph:
    """Preserve lexical forms without changing RDFLib's process-wide settings.

    RDFLib's default N-Triples parser normalizes typed literals. This can turn
    even an ill-formed dateTime into a different term, or change an offending
    value such as "01"^^xsd:integer. Its parser/sink handle syntax and blank
    nodes; only literal construction needs an explicit normalize=False.
    """
    graph = Graph()
    parser = _TermPreservingParser(NTGraphSink(graph))
    parser.parse(StringIO(data), bnode_context=dict(bnode_context or {}))
    return graph
