//! Loading an RDF graph from Turtle and convenience accessors over it.

use crate::diagnostics::ParseError;
use crate::vocab;
use oxrdf::{Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, Term};
use oxttl::TurtleParser;
use std::collections::HashSet;

/// A loaded shapes graph plus the prefixes declared in the document.
pub struct Loaded {
    pub graph: Graph,
    pub prefixes: Vec<(String, String)>,
    pub base: Option<String>,
}

impl Loaded {
    /// Parse a Turtle document into an in-memory graph.
    pub fn from_turtle(data: &[u8], base: Option<&str>) -> Result<Self, ParseError> {
        let mut parser = TurtleParser::new();
        if let Some(b) = base {
            parser = parser
                .with_base_iri(b)
                .map_err(|e| ParseError(format!("invalid base IRI: {e}")))?;
        }
        let mut reader = parser.for_slice(data);
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
        Ok(Self { graph, prefixes, base })
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
                self.objects(
                    &NamedOrBlankNode::NamedNode(class),
                    vocab::RDFS_SUBCLASSOF,
                )
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
