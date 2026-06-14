//! Applying a driver's [`Plan`] to a [`RepairTree`] (`docs/06-repair.md` §3.2).
//!
//! [`instantiate`] is a pure fold: it resolves the choices a driver has made
//! (hole bindings, `Any` branches, `Repeat` counts) into concrete graph edits,
//! and reports whatever is still open. It never validates and never chooses, so a
//! driver can fill a plan in one shot or interactively — instantiate with the
//! counts set but no bindings to discover the holes (including the per-instance
//! holes a `Repeat` unrolls to), bind them, then instantiate again.
//!
//! Hole renaming for `Repeat` instances is deterministic in `(tree, counts)`, so
//! the holes reported by a discovery pass are exactly the holes a binding pass
//! resolves.

use crate::ir::{Edit, EditOp, Hole, HoleConstraint, NodeId, RepairTree, Slot};
use oxrdf::{NamedOrBlankNode, Term, Triple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A set of triple additions and deletions — the `ΔG` a driver applies.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDelta {
    pub add: Vec<Triple>,
    pub delete: Vec<Triple>,
}

/// A driver's choices over a [`RepairTree`], keyed by `NodeId`/`Hole` so it is
/// serializable and position-stable. Partial plans are allowed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    /// Which child to take at an `Any` node.
    pub branch: HashMap<NodeId, usize>,
    /// How many instances to materialize at a `Repeat` node.
    pub count: HashMap<NodeId, u64>,
    /// The chosen term for each hole (post-renaming ids for `Repeat` instances).
    pub binding: HashMap<Hole, Term>,
}

/// The result of [`instantiate`]: resolved edits plus what remains unfilled.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instantiated {
    pub delta: GraphDelta,
    /// Holes that still need a term, with their constraints.
    pub open_holes: Vec<(Hole, HoleConstraint)>,
    /// `Any`/`Repeat` nodes that still need a choice.
    pub open_choices: Vec<NodeId>,
}

/// Apply a (possibly partial) `plan` to `tree`.
pub fn instantiate(tree: &RepairTree, plan: &Plan) -> Instantiated {
    let mut out = Instantiated::default();
    // Fresh ids for Repeat-instance holes start above every static hole id.
    let mut next = tree.holes().iter().map(|h| h.0).max().map_or(0, |m| m + 1);
    let remap: HashMap<Hole, Hole> = HashMap::new();
    go(tree, plan, &remap, &mut next, &mut out);
    out
}

fn go(
    tree: &RepairTree,
    plan: &Plan,
    remap: &HashMap<Hole, Hole>,
    next: &mut u32,
    out: &mut Instantiated,
) {
    match tree {
        RepairTree::Noop(_) | RepairTree::Blocked(..) => {}
        RepairTree::Edits { edits, holes, .. } => {
            for (h, c) in holes {
                let rh = rename(*h, remap);
                if !plan.binding.contains_key(&rh) {
                    out.open_holes.push((rh, c.clone()));
                }
            }
            for e in edits {
                if let Some(triple) = resolve_edit(e, remap, plan) {
                    match &e.op {
                        EditOp::Add(_) => out.delta.add.push(triple),
                        EditOp::Delete(_) => out.delta.delete.push(triple),
                    }
                }
            }
        }
        RepairTree::All { children, .. } => {
            for c in children {
                go(c, plan, remap, next, out);
            }
        }
        RepairTree::Any { id, children } => match plan.branch.get(id) {
            Some(&i) if i < children.len() => go(&children[i], plan, remap, next, out),
            _ => out.open_choices.push(*id),
        },
        RepairTree::Repeat { id, body, .. } => match plan.count.get(id) {
            Some(&k) => {
                let body_holes = body.holes();
                for _ in 0..k {
                    let mut instance = remap.clone();
                    for h in &body_holes {
                        instance.insert(*h, Hole(*next));
                        *next += 1;
                    }
                    go(body, plan, &instance, next, out);
                }
            }
            None => out.open_choices.push(*id),
        },
    }
}

fn rename(h: Hole, remap: &HashMap<Hole, Hole>) -> Hole {
    remap.get(&h).copied().unwrap_or(h)
}

/// Resolve an edit's three slots; `Some` only when all are bound and form a valid
/// triple. Unresolved holes are reported separately (via the `holes` list).
fn resolve_edit(edit: &Edit, remap: &HashMap<Hole, Hole>, plan: &Plan) -> Option<Triple> {
    let p = edit.op.pattern();
    let s = resolve_slot(&p.s, remap, plan)?;
    let pr = resolve_slot(&p.p, remap, plan)?;
    let o = resolve_slot(&p.o, remap, plan)?;
    make_triple(s, pr, o)
}

fn resolve_slot(slot: &Slot, remap: &HashMap<Hole, Hole>, plan: &Plan) -> Option<Term> {
    match slot {
        Slot::Bound(t) => Some(t.clone()),
        Slot::Open(h) => plan.binding.get(&rename(*h, remap)).cloned(),
    }
}

/// Build a triple from three terms, honoring RDF position constraints (subject is
/// a node, predicate is an IRI). Returns `None` for an ill-typed combination.
fn make_triple(s: Term, p: Term, o: Term) -> Option<Triple> {
    let subject: NamedOrBlankNode = match s {
        Term::NamedNode(n) => n.into(),
        Term::BlankNode(b) => b.into(),
        Term::Literal(_) => return None,
    };
    let predicate = match p {
        Term::NamedNode(n) => n,
        _ => return None,
    };
    Some(Triple::new(subject, predicate, o))
}
