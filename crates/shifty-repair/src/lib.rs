//! Symbolic repair template IR for shifty (`docs/06-repair.md`).
//!
//! This crate is the **library API surface** for symbolic repair: the
//! [`RepairTree`] IR (a parametric, inspectable description of a violation's
//! repair space), a driver's [`Plan`] of choices, and [`instantiate`] to turn a
//! plan into concrete graph edits ([`GraphDelta`]).
//!
//! It is a pure, leaf crate — it depends only on the algebra IR and `oxrdf`, and
//! decides nothing. Synthesis (witness → `RepairTree`) and the re-validation gate
//! live in `shifty-engine`, which depends on these types. Reference drivers
//! (monomorphism / enumeration / ASP / LLM) are specified in
//! `docs/07-repair-drivers.md`.

pub mod instantiate;
pub mod ir;

pub use instantiate::{GraphDelta, Instantiated, Plan, instantiate};
pub use ir::{
    BlockReason, Cost, Edit, EditOp, Hole, HoleConstraint, NodeId, RepairTree, Slot, TriplePattern,
    iri_slot,
};

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{NamedNode, Term};
    use std::collections::HashMap;

    fn iri(s: &str) -> NamedNode {
        NamedNode::new(s).unwrap()
    }
    fn term(s: &str) -> Term {
        Term::NamedNode(iri(s))
    }

    /// `Edits` adding `(focus, p, ?h)` with `?h` a hole.
    fn one_add(node: u32, hole: u32) -> RepairTree {
        let pat = TriplePattern::new(
            iri_slot(&iri("http://ex/focus")),
            iri_slot(&iri("http://ex/p")),
            Slot::Open(Hole(hole)),
        );
        RepairTree::Edits {
            id: NodeId(node),
            edits: vec![Edit::add(pat)],
            holes: vec![(Hole(hole), HoleConstraint::AnyNode)],
        }
    }

    #[test]
    fn bound_hole_yields_a_concrete_triple() {
        let tree = one_add(0, 1);
        let mut plan = Plan::default();
        plan.binding.insert(Hole(1), term("http://ex/v"));
        let r = instantiate(&tree, &plan);
        assert!(r.open_holes.is_empty());
        assert_eq!(r.delta.add.len(), 1);
        assert!(r.delta.delete.is_empty());
        assert_eq!(
            r.delta.add[0].object,
            oxrdf::Term::NamedNode(iri("http://ex/v"))
        );
    }

    #[test]
    fn unbound_hole_is_reported_open_and_emits_nothing() {
        let tree = one_add(0, 1);
        let r = instantiate(&tree, &Plan::default());
        assert_eq!(r.open_holes, vec![(Hole(1), HoleConstraint::AnyNode)]);
        assert!(r.delta.add.is_empty());
    }

    #[test]
    fn any_takes_the_chosen_branch_else_reports_open_choice() {
        let tree = RepairTree::Any {
            id: NodeId(10),
            children: vec![one_add(0, 1), one_add(2, 3)],
        };
        // no choice → open_choices
        let r0 = instantiate(&tree, &Plan::default());
        assert_eq!(r0.open_choices, vec![NodeId(10)]);
        assert!(r0.open_holes.is_empty());
        // choose branch 1, bind its hole
        let mut plan = Plan {
            branch: HashMap::from([(NodeId(10), 1)]),
            ..Default::default()
        };
        plan.binding.insert(Hole(3), term("http://ex/w"));
        let r1 = instantiate(&tree, &plan);
        assert!(r1.open_choices.is_empty());
        assert_eq!(r1.delta.add.len(), 1);
    }

    #[test]
    fn repeat_unrolls_to_distinct_per_instance_holes() {
        // Repeat over a body that adds (focus, p, ?1).
        let tree = RepairTree::Repeat {
            id: NodeId(5),
            body: Box::new(one_add(0, 1)),
            min: 2,
            max: None,
        };
        // discovery pass: count set, no bindings.
        let plan = Plan {
            count: HashMap::from([(NodeId(5), 2)]),
            ..Default::default()
        };
        let disc = instantiate(&tree, &plan);
        // two instances ⇒ two distinct fresh holes, none equal to the static id 1.
        assert_eq!(disc.open_holes.len(), 2);
        let ids: Vec<u32> = disc.open_holes.iter().map(|(h, _)| h.0).collect();
        assert_ne!(ids[0], ids[1]);
        assert!(ids.iter().all(|&i| i > 1));

        // binding pass: bind the discovered holes ⇒ two distinct triples.
        let mut bound = plan.clone();
        bound.binding.insert(Hole(ids[0]), term("http://ex/a"));
        bound.binding.insert(Hole(ids[1]), term("http://ex/b"));
        let r = instantiate(&tree, &bound);
        assert!(r.open_holes.is_empty());
        assert_eq!(r.delta.add.len(), 2);
        let objs: std::collections::HashSet<_> =
            r.delta.add.iter().map(|t| t.object.to_string()).collect();
        assert_eq!(objs.len(), 2);
    }

    #[test]
    fn blocked_and_noop_emit_nothing() {
        let tree = RepairTree::All {
            id: NodeId(0),
            children: vec![
                RepairTree::Noop(NodeId(1)),
                RepairTree::Blocked(NodeId(2), BlockReason::OpaqueSparql),
            ],
        };
        let r = instantiate(&tree, &Plan::default());
        assert!(r.delta.add.is_empty() && r.delta.delete.is_empty());
        assert!(r.open_holes.is_empty() && r.open_choices.is_empty());
    }

    #[test]
    fn repair_tree_serde_roundtrips() {
        let tree = RepairTree::Any {
            id: NodeId(10),
            children: vec![one_add(0, 1), RepairTree::Noop(NodeId(9))],
        };
        let json = serde_json::to_string(&tree).unwrap();
        let back: RepairTree = serde_json::from_str(&json).unwrap();
        assert_eq!(tree, back);
    }
}
