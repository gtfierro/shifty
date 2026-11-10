//! Import declarations that travel with a schema.
//!
//! A schema built from a text front end may depend on other documents: other
//! Shapeshift files, SHACL shapes graphs, or ontologies. The declaration is
//! data on the [`Schema`](crate::Schema); *resolving* it (fetching, caching,
//! pinning) happens outside this crate, so a parsed schema can list its imports
//! without any network access.

use crate::term::NamedNode;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImportSource {
    /// `import <https://...>`
    Iri(NamedNode),
    /// `import "./relative/path.shm"`, relative to the importing file.
    Path(String),
}

impl fmt::Display for ImportSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iri(iri) => write!(f, "<{}>", iri.as_str()),
            Self::Path(path) => write!(f, "\"{path}\""),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImportKind {
    /// Another Shapeshift (`.shm`) file.
    Shapeshift,
    /// An RDF graph: a SHACL shapes graph, an ontology, or both.
    Rdf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Import {
    pub source: ImportSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// `None` until resolved; sniffed from the extension or content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<ImportKind>,
    /// Canonical identity after resolution, for deduplication and export.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_iri: Option<NamedNode>,
}

impl Import {
    pub fn new(source: ImportSource, alias: Option<String>) -> Self {
        Self {
            source,
            alias,
            kind: None,
            resolved_iri: None,
        }
    }
}
