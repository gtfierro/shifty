//! Opaque SPARQL escape-hatch leaves (gap-analysis **AF-C / AF-T / AF-R**).
//!
//! These carry raw query text the algebra does not reason about. The parser is
//! expected to inline prefixes so the stored query is self-contained; a later
//! pass may recognize simple shapes/BGPs and lift them back into the algebra.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SparqlQueryKind {
    Ask,
    Select,
}

/// `sh:sparql` constraint (an `ASK` or `SELECT` producing violations).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlConstraint {
    pub kind: SparqlQueryKind,
    pub query: String,
}

/// `sh:target` + `sh:select` — a SPARQL-based target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlTarget {
    pub query: String,
}

/// `sh:SPARQLRule` — a `CONSTRUCT` rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlConstruct {
    pub query: String,
}
