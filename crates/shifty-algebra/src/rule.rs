//! SHACL-AF rule skeleton (gap-analysis **AF-R**).
//!
//! A rule fires on the focus nodes picked by `selector` for which all
//! `conditions` hold, and produces head triples (built from node expressions)
//! or a SPARQL `CONSTRUCT`. Rules are evaluated to a fixpoint, honoring
//! `sh:order`. Execution semantics land in Layer 6 (`shifty-engine`); the
//! recursion/fixpoint framework is fixed in Layer 4 (`shifty-opt`).

/// Exact numeric ordering, serialized as its decimal lexical representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Order(bigdecimal::BigDecimal);

impl TryFrom<String> for Order {
    type Error = bigdecimal::ParseBigDecimalError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(Self)
    }
}
impl From<Order> for String {
    fn from(value: Order) -> Self {
        value.0.to_string()
    }
}
impl From<i64> for Order {
    fn from(value: i64) -> Self {
        Self(value.into())
    }
}
impl std::fmt::Display for Order {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

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
    /// Authored rule and enclosing shape, retained for execution diagnostics.
    #[serde(default)]
    pub source: Option<String>,
    /// Owning authored shape; absent for programmatically constructed rules.
    #[serde(default)]
    pub owner: Option<oxrdf::NamedOrBlankNode>,
    #[serde(default)]
    pub shape_order: Option<Order>,
    /// Focus nodes the rule applies to (the owning shape's target).
    pub selector: Selector,
    /// `sh:condition` shapes that must hold before the rule fires.
    pub conditions: Vec<ShapeId>,
    pub head: RuleHead,
    /// `sh:order` (default 0); rules execute in ascending order, ties together.
    pub order: Option<Order>,
    pub deactivated: bool,
}
