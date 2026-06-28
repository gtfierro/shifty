//! Polarity-aware shape dependency graph (Layer 4).
//!
//! An edge `A → B` means shape `A`'s satisfaction depends on shape `B`'s. The
//! polarity is the *semantic* monotonicity of that dependency, not the surface
//! `¬`: because our IR encodes `∀π.φ` as `∃≤0 π.¬φ`, a positive SHACL constraint
//! looks syntactically negative, and two anti-monotone operators compose back to
//! monotone (see `docs/03-recursion-semantics.md`).
//!
//! Per-node polarity:
//! - `And` / `Or`        → children **positive** (monotone)
//! - `Not`               → child **negative**
//! - `Count` lower bound → qualifier **positive** (monotone in the qualifier)
//! - `Count` upper bound → qualifier **negative** (anti-monotone)
//!
//! A `Count{min,max}` with both bounds emits both a positive and a negative edge
//! to its qualifier (un-fusing), so a genuinely two-sided qualified count reads
//! as non-monotone.

use serde::{Deserialize, Serialize};
use shifty_algebra::{Shape, ShapeArena, ShapeId};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Polarity {
    Positive,
    Negative,
}

impl Polarity {
    /// `+1` / `-1`, so a cycle's net polarity is the product of its edges'.
    pub fn sign(self) -> i8 {
        match self {
            Polarity::Positive => 1,
            Polarity::Negative => -1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepEdge {
    pub from: ShapeId,
    pub to: ShapeId,
    pub polarity: Polarity,
}

/// All polarity-annotated dependency edges of an arena.
pub fn dependency_edges(arena: &ShapeArena) -> Vec<DepEdge> {
    let mut edges = Vec::new();
    for i in 0..arena.len() {
        let from = ShapeId(i as u32);
        match arena.get(from) {
            Shape::Annotated { shape, .. } => edges.push(DepEdge {
                from,
                to: *shape,
                polarity: Polarity::Positive,
            }),
            Shape::Not(c) => edges.push(DepEdge {
                from,
                to: *c,
                polarity: Polarity::Negative,
            }),
            Shape::And(cs) | Shape::Or(cs) => {
                for c in cs {
                    edges.push(DepEdge {
                        from,
                        to: *c,
                        polarity: Polarity::Positive,
                    });
                }
            }
            Shape::Count {
                min,
                max,
                qualifier,
                ..
            } => {
                if min.is_some() {
                    edges.push(DepEdge {
                        from,
                        to: *qualifier,
                        polarity: Polarity::Positive,
                    });
                }
                if max.is_some() {
                    edges.push(DepEdge {
                        from,
                        to: *qualifier,
                        polarity: Polarity::Negative,
                    });
                }
            }
            Shape::Expression(e) => {
                // A `Filter` inside the expression keeps inputs satisfying its
                // shape — monotone, so the edge is positive.
                let mut refs = Vec::new();
                e.referenced_shapes(&mut refs);
                for to in refs {
                    edges.push(DepEdge {
                        from,
                        to,
                        polarity: Polarity::Positive,
                    });
                }
            }
            _ => {}
        }
    }
    edges
}
