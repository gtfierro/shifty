//! Selectors (doc 00 §4, Def. 5) — which focus nodes a shape applies to.
//!
//! The paper's selectors are very restricted; `HasPath` and `Sparql` are our
//! additions for class targets (gap-analysis **C1**) and SPARQL-based targets
//! (**AF-T**).

use crate::path::Path;
use crate::shape::ShapeId;
use crate::sparql::SparqlTarget;
use crate::term::{NamedNode, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Selector {
    /// `∃q.⊤` — nodes with an outgoing `q` edge (`sh:targetSubjectsOf`).
    HasOut(NamedNode),
    /// `∃q⁻.⊤` — nodes with an incoming `q` edge (`sh:targetObjectsOf`).
    HasIn(NamedNode),
    /// `test(c)` — a specific node/value (`sh:targetNode`).
    IsConst(Term),
    /// `∃≥¹ π.φ` — path-shaped selector, used for `sh:targetClass` and the
    /// implicit class target via `rdf:type · rdfs:subClassOf*` (gap-analysis
    /// **C1**).
    HasPath(Path, ShapeId),
    /// SPARQL-based target (gap-analysis **AF-T**).
    Sparql(SparqlTarget),
}

impl Selector {
    /// This selector with its shape reference (if any) passed through `f`.
    pub fn remap(&self, f: &impl Fn(ShapeId) -> ShapeId) -> Selector {
        match self {
            Selector::HasPath(path, shape) => Selector::HasPath(path.clone(), f(*shape)),
            other => other.clone(),
        }
    }
}
