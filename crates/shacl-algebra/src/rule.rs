//! SHACL-AF rule skeleton (gap-analysis **AF-R**).
//!
//! A rule fires on the focus nodes picked by `selector` for which all
//! `conditions` hold, and produces head triples (built from node expressions)
//! or a SPARQL `CONSTRUCT`. Rules are evaluated to a fixpoint, honoring
//! `sh:order`. Execution semantics land in Layer 6 (`shacl-engine`); the
//! recursion/fixpoint framework is fixed in Layer 4 (`shacl-opt`).

use crate::expr::NodeExpr;
use crate::selector::Selector;
use crate::shape::ShapeId;
use crate::sparql::SparqlConstruct;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleHead {
    /// `sh:TripleRule` — a single inferred triple from node expressions.
    Triple {
        subject: NodeExpr,
        predicate: NodeExpr,
        object: NodeExpr,
    },
    /// `sh:SPARQLRule` — an opaque `CONSTRUCT`.
    Sparql(SparqlConstruct),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// Focus nodes the rule applies to (the owning shape's target).
    pub selector: Selector,
    /// `sh:condition` shapes that must hold before the rule fires.
    pub conditions: Vec<ShapeId>,
    pub head: RuleHead,
    /// `sh:order` (default 0); rules execute in ascending order, ties together.
    pub order: Option<i64>,
    pub deactivated: bool,
}
