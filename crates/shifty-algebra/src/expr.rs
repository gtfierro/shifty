//! SHACL-AF node expressions (gap-analysis **AF-E**).
//!
//! Node expressions are *value-producing*: evaluated at a focus node they yield
//! a set of terms. They are the shared substrate for rule heads, computed
//! values, and (eventually) AF targets. They map closely onto the [`Path`]
//! algebra plus constants and function application.
//!
//! This is a skeleton; evaluation lands with the rule engine (Layer 6).

use crate::path::Path;
use crate::shape::ShapeId;
use crate::term::{NamedNode, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeExpr {
    /// `sh:this` — the focus node itself.
    This,
    /// A literal/IRI constant.
    Constant(Term),
    /// Values reachable from the focus node along a path (`sh:path` expression).
    Path(Path),
    /// `sh:filterShape` — keep only inputs satisfying `shape`.
    Filter {
        input: Box<NodeExpr>,
        shape: ShapeId,
    },
    /// `sh:intersection` of sub-expression result sets.
    Intersection(Vec<NodeExpr>),
    /// `sh:union` of sub-expression result sets.
    Union(Vec<NodeExpr>),
    /// Application of a SHACL function (gap-analysis **AF-F**).
    Function { iri: NamedNode, args: Vec<NodeExpr> },
}
