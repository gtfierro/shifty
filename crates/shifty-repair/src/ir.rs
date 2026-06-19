//! The repair template IR (`docs/06-repair.md` §3.1).
//!
//! A [`RepairTree`] is a parametric, inspectable description of the repair space
//! for one violation: an AND/OR/repeat tree whose leaves are graph [`Edit`]s with
//! typed [`Hole`]s. It is the central artifact a driver fills (see
//! [`crate::instantiate`]). Every node carries a stable [`NodeId`] so a
//! [`crate::Plan`] can address it.

use oxrdf::{NamedNode, Term};
use serde::{Deserialize, Serialize};
use shifty_algebra::{NodeKindSet, ShapeId, ValueType};
use std::collections::BTreeSet;

/// Stable identity for a [`RepairTree`] node, assigned by synthesis. A
/// [`crate::Plan`] references branch/count choices by `NodeId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u32);

/// A typed placeholder a driver binds to a term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hole(pub u32);

/// What a bound term must satisfy — the self-describing constraint on a hole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HoleConstraint {
    /// Any term.
    AnyNode,
    /// A newly-minted node (mint, don't reuse an existing one).
    Fresh,
    /// Must equal this term (`sh:hasValue` / `test(c)`).
    Const(Term),
    /// A value type: datatype / numeric range / length / pattern.
    Typed(ValueType),
    /// `sh:nodeKind` — IRI | blank | literal.
    Kind(NodeKindSet),
    /// A finite domain (`sh:in`), treated as a set (order not significant).
    OneOf(Vec<Term>),
    /// The bound node must itself satisfy a sub-shape.
    ConformsTo(ShapeId),
    /// The bound node must satisfy *every* listed sub-shape at once. Carried when
    /// several independent obligations govern one freshly-added value — a count
    /// qualifier together with the universal `∀π.φ` siblings on the same path that
    /// were vacuously satisfied while the value set was empty. Rendering and
    /// drivers treat it as the conjunction of its members.
    ConformsToAll(Vec<ShapeId>),
}

/// A triple slot: a fixed term or an open hole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Slot {
    Bound(Term),
    Open(Hole),
}

/// A triple whose slots may be holes or fixed terms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriplePattern {
    pub s: Slot,
    pub p: Slot,
    pub o: Slot,
}

impl TriplePattern {
    pub fn new(s: Slot, p: Slot, o: Slot) -> Self {
        Self { s, p, o }
    }

    /// The three slots, for hole collection / resolution.
    pub fn slots(&self) -> [&Slot; 3] {
        [&self.s, &self.p, &self.o]
    }
}

/// A synthesis-assigned default cost a driver may use or override when ranking
/// plans for minimality. Reuse-vs-mint cost is a separate, driver-side weighting
/// over [`HoleConstraint`], not encoded here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Cost(pub u32);

impl Default for Cost {
    fn default() -> Self {
        Cost(1)
    }
}

/// Add or delete a triple pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EditOp {
    Add(TriplePattern),
    Delete(TriplePattern),
}

impl EditOp {
    pub fn pattern(&self) -> &TriplePattern {
        match self {
            EditOp::Add(p) | EditOp::Delete(p) => p,
        }
    }
}

/// One edit, with its default cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edit {
    pub op: EditOp,
    pub cost: Cost,
}

impl Edit {
    pub fn add(pattern: TriplePattern) -> Self {
        Self {
            op: EditOp::Add(pattern),
            cost: Cost::default(),
        }
    }

    pub fn delete(pattern: TriplePattern) -> Self {
        Self {
            op: EditOp::Delete(pattern),
            cost: Cost::default(),
        }
    }
}

/// Why a branch admits no data repair in scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockReason {
    /// `sh:sparql` — not algebraically repairable.
    OpaqueSparql,
    /// A focus-scoped nodeKind/identity test; data edits can't change it.
    CannotMutateIdentity,
    /// A gfp back-edge support with no finite facts to delete.
    Coinductive,
    /// A constraint this cut does not yet synthesize.
    Unsupported(String),
}

/// The repair space for one (sub)shape, as an AND/OR/repeat tree. Every node
/// carries a stable [`NodeId`].
///
/// Invariant maintained by synthesis: `Blocked` is normalized away inside a
/// satisfiable branch (`All` with any `Blocked` ⇒ `Blocked`; `Any` drops
/// `Blocked` children), so a driver never has to reason around a dead branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairTree {
    /// Already satisfied — the empty repair.
    Noop(NodeId),
    /// Unrepairable in scope.
    Blocked(NodeId, BlockReason),
    /// A concrete set of edits and the holes they introduce.
    Edits {
        id: NodeId,
        edits: Vec<Edit>,
        holes: Vec<(Hole, HoleConstraint)>,
    },
    /// Satisfy every child (conjunction).
    All {
        id: NodeId,
        children: Vec<RepairTree>,
    },
    /// Satisfy any one child (disjunction).
    Any {
        id: NodeId,
        children: Vec<RepairTree>,
    },
    /// Instantiate `body` between `min` and `max` times (the variadic case).
    Repeat {
        id: NodeId,
        body: Box<RepairTree>,
        min: u64,
        max: Option<u64>,
    },
}

impl RepairTree {
    /// This node's stable address.
    pub fn id(&self) -> NodeId {
        match self {
            RepairTree::Noop(id)
            | RepairTree::Blocked(id, _)
            | RepairTree::Edits { id, .. }
            | RepairTree::All { id, .. }
            | RepairTree::Any { id, .. }
            | RepairTree::Repeat { id, .. } => *id,
        }
    }

    /// Whether this whole subtree is `Blocked` (no data repair in scope).
    pub fn is_blocked(&self) -> bool {
        matches!(self, RepairTree::Blocked(..))
    }

    /// Collect every hole declared or referenced in this subtree.
    pub fn holes(&self) -> BTreeSet<Hole> {
        let mut out = BTreeSet::new();
        collect_holes(self, &mut out);
        out
    }
}

fn collect_holes(tree: &RepairTree, out: &mut BTreeSet<Hole>) {
    match tree {
        RepairTree::Edits { edits, holes, .. } => {
            for (h, _) in holes {
                out.insert(*h);
            }
            for e in edits {
                for slot in e.op.pattern().slots() {
                    if let Slot::Open(h) = slot {
                        out.insert(*h);
                    }
                }
            }
        }
        RepairTree::All { children, .. } | RepairTree::Any { children, .. } => {
            for c in children {
                collect_holes(c, out);
            }
        }
        RepairTree::Repeat { body, .. } => collect_holes(body, out),
        RepairTree::Noop(_) | RepairTree::Blocked(..) => {}
    }
}

/// Convenience: a `Bound` IRI slot.
pub fn iri_slot(iri: &NamedNode) -> Slot {
    Slot::Bound(Term::NamedNode(iri.clone()))
}
