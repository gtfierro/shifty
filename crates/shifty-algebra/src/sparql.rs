//! Opaque SPARQL escape-hatch leaves (gap-analysis **AF-C / AF-T / AF-R**).
//!
//! These carry parsed-and-canonicalized query text the algebra does not yet
//! reason about. Prefixes are resolved so the stored query is self-contained;
//! later passes may rewrite its Spargebra AST or recognize simple BGPs and lift
//! them back into the algebra.

use crate::path::Path;
use oxrdf::Term;
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
    /// The owning property shape's path. Simple predicate paths are prebound
    /// to `$PATH`; complex paths require an AST rewrite before execution.
    pub path: Option<Path>,
    /// The RDF node of the shape that declares this constraint, pre-bound to
    /// `$currentShape`. `None` when provenance is unavailable.
    pub shape: Option<Term>,
    /// `sh:message` literals declared on the SPARQL constraint (falling back to
    /// the owning shape). Used as the violation message when a `SELECT` result
    /// does not bind its own `?message`. Empty when none is declared.
    pub messages: Vec<Term>,
    /// Extra pre-bound variables injected before `$this`. Used by custom
    /// constraint component lowering to substitute parameter values (e.g.
    /// `$exactCount`, `$class`) that are static per activating shape.
    /// Empty for plain `sh:sparql` constraints.
    pub extra_bindings: Vec<(String, Term)>,
    /// When `true`, rename `?value` to `?this` in the compiled query before
    /// binding `$this` per focus. Set for ASK validators activated by node
    /// shapes (no `sh:path`), where `$value` equals the focus node (`$this`).
    pub bind_value_to_this: bool,
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
