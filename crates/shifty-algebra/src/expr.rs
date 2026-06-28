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

impl NodeExpr {
    /// Append every [`ShapeId`] this expression references (via `Filter`
    /// sub-expressions) to `out`. Used by reachability, dependency, and
    /// normalization passes that must follow shape references inside
    /// expressions just as they do inside the shape grammar.
    pub fn referenced_shapes(&self, out: &mut Vec<ShapeId>) {
        match self {
            NodeExpr::Filter { input, shape } => {
                out.push(*shape);
                input.referenced_shapes(out);
            }
            NodeExpr::Intersection(es) | NodeExpr::Union(es) => {
                for e in es {
                    e.referenced_shapes(out);
                }
            }
            NodeExpr::Function { args, .. } => {
                for e in args {
                    e.referenced_shapes(out);
                }
            }
            NodeExpr::This | NodeExpr::Constant(_) | NodeExpr::Path(_) => {}
        }
    }
}
