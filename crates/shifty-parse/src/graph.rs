//! Loading an RDF graph from Turtle and convenience accessors over it.

use crate::diagnostics::ParseError;
use crate::vocab;
use oxrdf::{Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use oxrdfio::RdfParser;
use oxttl::{NTriplesParser, TurtleParser};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    Turtle,
    NTriples,
    RdfXml,
    NQuads,
    TriG,
    N3,
}

impl RdfFormat {
    pub fn from_media_type(media_type: &str) -> Option<Self> {
        match oxrdfio::RdfFormat::from_media_type(media_type)? {
            oxrdfio::RdfFormat::Turtle => Some(Self::Turtle),
            oxrdfio::RdfFormat::NTriples => Some(Self::NTriples),
            oxrdfio::RdfFormat::RdfXml => Some(Self::RdfXml),
            oxrdfio::RdfFormat::NQuads => Some(Self::NQuads),
            oxrdfio::RdfFormat::TriG => Some(Self::TriG),
            oxrdfio::RdfFormat::N3 => Some(Self::N3),
            _ => None,
        }
    }

    pub fn from_extension(extension: &str) -> Option<Self> {
        match oxrdfio::RdfFormat::from_extension(extension)? {
            oxrdfio::RdfFormat::Turtle => Some(Self::Turtle),
            oxrdfio::RdfFormat::NTriples => Some(Self::NTriples),
            oxrdfio::RdfFormat::RdfXml => Some(Self::RdfXml),
            oxrdfio::RdfFormat::NQuads => Some(Self::NQuads),
            oxrdfio::RdfFormat::TriG => Some(Self::TriG),
            oxrdfio::RdfFormat::N3 => Some(Self::N3),
            _ => None,
        }
    }

    fn to_oxrdfio(self) -> oxrdfio::RdfFormat {
        match self {
            Self::Turtle => oxrdfio::RdfFormat::Turtle,
            Self::NTriples => oxrdfio::RdfFormat::NTriples,
            Self::RdfXml => oxrdfio::RdfFormat::RdfXml,
            Self::NQuads => oxrdfio::RdfFormat::NQuads,
            Self::TriG => oxrdfio::RdfFormat::TriG,
            Self::N3 => oxrdfio::RdfFormat::N3,
        }
    }
}

/// A loaded shapes graph plus the prefixes declared in the document.
pub struct Loaded {
    pub graph: Graph,
    pub prefixes: Vec<(String, String)>,
    pub base: Option<String>,
}

impl Loaded {
    /// Parse a Turtle document into an in-memory graph.
    pub fn from_turtle(data: &[u8], base: Option<&str>) -> Result<Self, ParseError> {
        Self::from_turtle_reader(data, base)
    }

    pub fn from_ntriples(data: &[u8]) -> Result<Self, ParseError> {
        Self::from_ntriples_reader(data)
    }

    pub fn from_rdf(
        data: &[u8],
        format: RdfFormat,
        base: Option<&str>,
    ) -> Result<Self, ParseError> {
        match format {
            RdfFormat::Turtle => Self::from_turtle_reader(data, base),
            RdfFormat::NTriples => Self::from_ntriples_reader(data),
            _ => Self::from_oxrdfio(data, format, base),
        }
    }

    pub fn from_rdf_auto(
        data: &[u8],
        content_type: Option<&str>,
        source: Option<&str>,
        base: Option<&str>,
    ) -> Result<Self, ParseError> {
        let sniffed = sniff_format(data);
        let hinted = content_type
            .and_then(RdfFormat::from_media_type)
            .or_else(|| source.and_then(format_from_source))
            .or(sniffed);

        if let Some(format) = hinted {
            match Self::from_rdf(data, format, base) {
                Ok(loaded) => return Ok(loaded),
                Err(first_error)
                    if content_type.is_some() || source.is_some() || sniffed.is_some() =>
                {
                    return Err(first_error);
                }
                Err(_) => {}
            }
        }

        let mut errors = Vec::new();
        for format in [
            RdfFormat::Turtle,
            RdfFormat::RdfXml,
            RdfFormat::NTriples,
            RdfFormat::NQuads,
            RdfFormat::TriG,
            RdfFormat::N3,
        ] {
            match Self::from_rdf(data, format, base) {
                Ok(loaded) => return Ok(loaded),
                Err(e) => errors.push(format!("{format:?}: {e}")),
            }
        }
        Err(ParseError(format!(
            "failed to parse RDF using common formats: {}",
            errors.join("; ")
        )))
    }

    pub fn from_path(
        path: &Path,
        format: RdfFormat,
        base: Option<&str>,
    ) -> Result<Self, ParseError> {
        let file = File::open(path)
            .map_err(|e| ParseError(format!("failed to open {}: {e}", path.display())))?;
        let reader = BufReader::new(file);
        match format {
            RdfFormat::Turtle => Self::from_turtle_reader(reader, base),
            RdfFormat::NTriples => Self::from_ntriples_reader(reader),
            _ => {
                let mut bytes = Vec::new();
                let mut reader = reader;
                reader
                    .read_to_end(&mut bytes)
                    .map_err(|e| ParseError(format!("failed to read RDF input: {e}")))?;
                Self::from_oxrdfio(&bytes, format, base)
            }
        }
    }

    fn from_turtle_reader(reader: impl Read, base: Option<&str>) -> Result<Self, ParseError> {
        let mut parser = TurtleParser::new();
        if let Some(b) = base {
            parser = parser
                .with_base_iri(b)
                .map_err(|e| ParseError(format!("invalid base IRI: {e}")))?;
        }
        let mut reader = parser.for_reader(reader);
        let mut graph = Graph::new();
        for triple in reader.by_ref() {
            let triple = triple.map_err(|e| ParseError(format!("turtle syntax error: {e}")))?;
            graph.insert(&triple);
        }
        let prefixes = reader
            .prefixes()
            .map(|(p, iri)| (p.to_string(), iri.to_string()))
            .collect();
        let base = reader.base_iri().map(|s| s.to_string());
        Ok(Self {
            graph,
            prefixes,
            base,
        })
    }

    fn from_ntriples_reader(reader: impl Read) -> Result<Self, ParseError> {
        let mut graph = Graph::new();
        for triple in NTriplesParser::new().for_reader(reader) {
            let triple = triple.map_err(|e| ParseError(format!("N-Triples syntax error: {e}")))?;
            graph.insert(&triple);
        }
        Ok(Self {
            graph,
            prefixes: Vec::new(),
            base: None,
        })
    }

    fn from_oxrdfio(
        data: &[u8],
        format: RdfFormat,
        base: Option<&str>,
    ) -> Result<Self, ParseError> {
        let mut parser = RdfParser::from_format(format.to_oxrdfio()).without_named_graphs();
        if let Some(base) = base {
            parser = parser
                .with_base_iri(base)
                .map_err(|e| ParseError(format!("invalid base IRI: {e}")))?;
        }
        let mut graph = Graph::new();
        for quad in parser.for_slice(data) {
            let quad = quad.map_err(|e| ParseError(format!("{format:?} syntax error: {e}")))?;
            graph.insert(&Triple {
                subject: quad.subject,
                predicate: quad.predicate,
                object: quad.object,
            });
        }
        Ok(Self {
            graph,
            prefixes: Vec::new(),
            base: base.map(ToOwned::to_owned),
        })
    }

    /// All objects of `(subject, predicate)`.
    pub fn objects(&self, subject: &NamedOrBlankNode, predicate: NamedNodeRef) -> Vec<Term> {
        self.graph
            .objects_for_subject_predicate(subject, predicate)
            .map(|t| t.into_owned())
            .collect()
    }

    /// The first object of `(subject, predicate)`, if any.
    pub fn object(&self, subject: &NamedOrBlankNode, predicate: NamedNodeRef) -> Option<Term> {
        self.graph
            .object_for_subject_predicate(subject, predicate)
            .map(|t| t.into_owned())
    }

    /// Does the subject have `(subject, rdf:type, ty)`?
    pub fn has_type(&self, subject: &NamedOrBlankNode, ty: NamedNodeRef) -> bool {
        self.objects(subject, vocab::RDF_TYPE)
            .iter()
            .any(|t| matches!(t, Term::NamedNode(n) if n.as_ref() == ty))
    }

    /// Does the subject have `ty` through `rdf:type/rdfs:subClassOf*`?
    pub fn is_instance_of(&self, subject: &NamedOrBlankNode, ty: NamedNodeRef) -> bool {
        let mut pending: Vec<NamedNode> = self
            .objects(subject, vocab::RDF_TYPE)
            .into_iter()
            .filter_map(|term| match term {
                Term::NamedNode(node) => Some(node),
                _ => None,
            })
            .collect();
        let mut seen = HashSet::new();
        while let Some(class) = pending.pop() {
            if class.as_ref() == ty {
                return true;
            }
            if !seen.insert(class.clone()) {
                continue;
            }
            pending.extend(
                self.objects(&NamedOrBlankNode::NamedNode(class), vocab::RDFS_SUBCLASSOF)
                    .into_iter()
                    .filter_map(|term| match term {
                        Term::NamedNode(node) => Some(node),
                        _ => None,
                    }),
            );
        }
        false
    }

    /// Merge all triples from `other` into this graph.
    pub fn merge_from(&mut self, other: &Loaded) {
        for triple in other.graph.iter() {
            self.graph.insert(triple);
        }
    }

    /// Read an `rdf:List` starting at `head` into its member terms.
    pub fn read_list(&self, head: &Term) -> Vec<Term> {
        let mut out = Vec::new();
        let mut cursor = head.clone();
        while let Some(node) = term_to_node(&cursor) {
            if is_nil(&cursor) {
                break;
            }
            if let Some(first) = self.object(&node, vocab::RDF_FIRST) {
                out.push(first);
            }
            match self.object(&node, vocab::RDF_REST) {
                Some(rest) => cursor = rest,
                None => break,
            }
        }
        out
    }
}

fn format_from_source(source: &str) -> Option<RdfFormat> {
    let path = source.split(['?', '#']).next().unwrap_or(source);
    let extension = path.rsplit_once('.')?.1;
    RdfFormat::from_extension(extension)
}

fn sniff_format(data: &[u8]) -> Option<RdfFormat> {
    // Only examine a small prefix — format signatures are always near the top.
    let prefix = &data[..data.len().min(4096)];
    let text = std::str::from_utf8(prefix).ok()?;

    // Walk past blank lines and # comment lines to find the first real token.
    let first_token = text
        .lines()
        .map(|l| l.trim_start())
        .find(|l| !l.is_empty() && !l.starts_with('#'))?;

    if first_token.starts_with("<?xml") || first_token.starts_with("<rdf:RDF") {
        return Some(RdfFormat::RdfXml);
    }
    if first_token.starts_with("@prefix")
        || first_token.starts_with("@base")
        || first_token.starts_with("PREFIX")
        || first_token.starts_with("BASE")
    {
        return Some(RdfFormat::Turtle);
    }
    // Check only the first real line, not the whole buffer.
    if first_token.starts_with('<') && first_token.contains("> <") {
        return Some(RdfFormat::NTriples);
    }
    None
}

/// Convert an object term into a subject position node, if it is not a literal.
pub fn term_to_node(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n.clone())),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b.clone())),
        Term::Literal(_) => None,
    }
}

/// Is this term `rdf:nil`?
pub fn is_nil(term: &Term) -> bool {
    matches!(term, Term::NamedNode(n) if n.as_ref() == vocab::RDF_NIL)
}

/// The IRI of a named node, or `None` for blanks/literals.
pub fn as_named(term: &Term) -> Option<NamedNode> {
    match term {
        Term::NamedNode(n) => Some(n.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // sniff_format tests

    #[test]
    fn sniff_rdfxml_xml_declaration() {
        assert_eq!(
            sniff_format(b"<?xml version=\"1.0\"?>\n<rdf:RDF>"),
            Some(RdfFormat::RdfXml)
        );
    }

    #[test]
    fn sniff_rdfxml_bare_tag() {
        assert_eq!(sniff_format(b"<rdf:RDF xmlns:rdf=\"...\">"), Some(RdfFormat::RdfXml));
    }

    #[test]
    fn sniff_turtle_at_prefix() {
        assert_eq!(
            sniff_format(b"@prefix sh: <http://www.w3.org/ns/shacl#> ."),
            Some(RdfFormat::Turtle)
        );
    }

    #[test]
    fn sniff_turtle_at_base() {
        assert_eq!(
            sniff_format(b"@base <http://example.org/> ."),
            Some(RdfFormat::Turtle)
        );
    }

    #[test]
    fn sniff_turtle_sparql_prefix() {
        assert_eq!(sniff_format(b"PREFIX sh: <http://www.w3.org/ns/shacl#>"), Some(RdfFormat::Turtle));
    }

    #[test]
    fn sniff_turtle_sparql_base() {
        assert_eq!(sniff_format(b"BASE <http://example.org/>"), Some(RdfFormat::Turtle));
    }

    #[test]
    fn sniff_turtle_after_comment_lines() {
        let data = b"# Copyright 2024\n# Licensed under ...\n@prefix ex: <http://ex/> .";
        assert_eq!(sniff_format(data), Some(RdfFormat::Turtle));
    }

    #[test]
    fn sniff_turtle_after_blank_and_comment_lines() {
        let data = b"\n\n# A comment\n\nPREFIX ex: <http://ex/>";
        assert_eq!(sniff_format(data), Some(RdfFormat::Turtle));
    }

    #[test]
    fn sniff_ntriples_first_line() {
        let data = b"<http://ex/s> <http://ex/p> <http://ex/o> .\n";
        assert_eq!(sniff_format(data), Some(RdfFormat::NTriples));
    }

    #[test]
    fn sniff_ntriples_after_comment() {
        let data = b"# generated by riot\n<http://ex/s> <http://ex/p> \"value\" .\n";
        // NTriples line has `> <` pattern
        assert_eq!(sniff_format(data), Some(RdfFormat::NTriples));
    }

    #[test]
    fn sniff_ntriples_contains_check_uses_first_line_only() {
        // The "> <" pattern only appears in the second line; first real line has no prefix,
        // so sniff should return None rather than scanning deep into the buffer.
        let data = b"# comment\n_:b0 <http://ex/p> _:b1 .\n<http://ex/s> <http://ex/p> <http://ex/o> .\n";
        assert_eq!(sniff_format(data), None);
    }

    #[test]
    fn sniff_returns_none_for_unknown() {
        assert_eq!(sniff_format(b"SELECT ?s WHERE { ?s a ex:Thing }"), None);
    }

    #[test]
    fn sniff_only_reads_prefix() {
        // Build a buffer >4096 bytes whose format marker is at the very start.
        let mut data = b"@prefix ex: <http://ex/> .\n".to_vec();
        data.extend(std::iter::repeat(b'x').take(8000));
        assert_eq!(sniff_format(&data), Some(RdfFormat::Turtle));
    }

    #[test]
    fn sniff_ignores_format_marker_beyond_prefix_window() {
        // Format marker buried past the 4096-byte sniff window — should not be detected.
        let mut data = vec![b' '; 5000];
        data.extend_from_slice(b"@prefix ex: <http://ex/> .");
        assert_eq!(sniff_format(&data), None);
    }
}
