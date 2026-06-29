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
    let text = std::str::from_utf8(data).ok()?.trim_start();
    if text.starts_with("<?xml") || text.starts_with("<rdf:RDF") {
        return Some(RdfFormat::RdfXml);
    }
    if text.starts_with("@prefix")
        || text.starts_with("@base")
        || text.starts_with("PREFIX")
        || text.starts_with("BASE")
    {
        return Some(RdfFormat::Turtle);
    }
    if text.starts_with('<') && text.contains("> <") {
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
