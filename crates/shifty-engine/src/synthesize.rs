//! Synthesis: `FocusWitness` → [`RepairTree`] (`docs/06-repair.md` §6).
//!
//! Two mutually-recursive folds turn the failed sub-DAG of `φ` into the inspectable
//! repair space a driver fills:
//! - [`Synth::repair`] (additive) walks a [`Witness`], emitting adds.
//! - [`Synth::break_`] (deletive) walks a [`SatTrace`], emitting deletes.
//!
//! They cross at `Not`. The third design direction — a structural `build` that
//! expands a fresh value's qualifier shape — is represented in this first cut by a
//! single `ConformsTo` hole (value-type leaves get the precise `Typed`/`Kind`
//! constraint), deferring structural expansion to the driver. So this cut needs no
//! fuel: the recursion bottoms out at the witness/trace leaves.

use crate::witness::{
    BlockReason as WBlock, FocusWitness, PathSupport, RelKind, SatTrace, Witness,
};
use oxrdf::{NamedOrBlankNode, Term, Triple};
use shifty_algebra::{Path, Shape, ShapeArena, ShapeId};
use shifty_repair::{
    BlockReason, Edit, Hole, HoleConstraint, NodeId, RepairTree, Slot, TriplePattern,
};

/// Add-edges plus the fresh interior holes they introduce.
type Materialized = (Vec<Edit>, Vec<(Hole, HoleConstraint)>);

/// Synthesize the repair space for one focus node failing one statement.
pub fn synthesize(arena: &ShapeArena, w: &FocusWitness) -> RepairTree {
    let mut s = Synth::new(arena);
    s.repair(&w.failure)
}

/// Per-focus grouping: `All` over a focus's statement-witnesses, so a driver can
/// fill one plan that fixes everything a focus violates at once.
pub fn synthesize_focus(arena: &ShapeArena, ws: &[FocusWitness]) -> RepairTree {
    let mut s = Synth::new(arena);
    let children: Vec<RepairTree> = ws.iter().map(|w| s.repair(&w.failure)).collect();
    s.all(children)
}

struct Synth<'a> {
    arena: &'a ShapeArena,
    next_node: u32,
    next_hole: u32,
}

impl<'a> Synth<'a> {
    fn new(arena: &'a ShapeArena) -> Self {
        Self {
            arena,
            next_node: 0,
            next_hole: 0,
        }
    }

    fn node(&mut self) -> NodeId {
        let n = NodeId(self.next_node);
        self.next_node += 1;
        n
    }

    fn hole(&mut self) -> Hole {
        let h = Hole(self.next_hole);
        self.next_hole += 1;
        h
    }

    // ── smart constructors maintaining the Blocked-propagation invariant ──

    /// `All`: any `Blocked` child ⇒ `Blocked`; drop `Noop`; collapse trivials.
    fn all(&mut self, children: Vec<RepairTree>) -> RepairTree {
        let mut kept = Vec::with_capacity(children.len());
        for c in children {
            match c {
                RepairTree::Blocked(_, r) => return RepairTree::Blocked(self.node(), r),
                RepairTree::Noop(_) => {}
                other => kept.push(other),
            }
        }
        match kept.len() {
            0 => RepairTree::Noop(self.node()),
            1 => kept.pop().unwrap(),
            _ => RepairTree::All {
                id: self.node(),
                children: kept,
            },
        }
    }

    /// `Any`: drop `Blocked` children; all-`Blocked` ⇒ `Blocked`; collapse trivials.
    fn any(&mut self, children: Vec<RepairTree>) -> RepairTree {
        let had = !children.is_empty();
        let mut reason = None;
        let mut kept = Vec::with_capacity(children.len());
        for c in children {
            match c {
                RepairTree::Blocked(_, r) => reason = Some(r),
                other => kept.push(other),
            }
        }
        match kept.len() {
            0 if had => RepairTree::Blocked(
                self.node(),
                reason.unwrap_or(BlockReason::Unsupported("no repair branch".into())),
            ),
            0 => RepairTree::Noop(self.node()),
            1 => kept.pop().unwrap(),
            _ => RepairTree::Any {
                id: self.node(),
                children: kept,
            },
        }
    }

    fn blocked(&mut self, reason: BlockReason) -> RepairTree {
        RepairTree::Blocked(self.node(), reason)
    }

    fn edits(&mut self, edits: Vec<Edit>, holes: Vec<(Hole, HoleConstraint)>) -> RepairTree {
        RepairTree::Edits {
            id: self.node(),
            edits,
            holes,
        }
    }

    // ── additive: Witness → RepairTree ──

    fn repair(&mut self, w: &Witness) -> RepairTree {
        match w {
            Witness::Atom {
                shape, produced_by, ..
            } => self.repair_atom(*shape, produced_by.as_ref()),
            Witness::Relational {
                kind,
                lhs,
                rhs,
                offending,
                ..
            } => self.repair_relational(*kind, lhs, rhs, offending),
            Witness::Closed {
                node, offenders, ..
            } => {
                let edits = offenders
                    .iter()
                    .map(|(p, o)| {
                        Edit::delete(TriplePattern::new(
                            Slot::Bound(node.clone()),
                            Slot::Bound(Term::NamedNode(p.clone())),
                            Slot::Bound(o.clone()),
                        ))
                    })
                    .collect();
                self.edits(edits, vec![])
            }
            Witness::Not { inner, .. } => self.break_(inner),
            Witness::All { failed, .. } => {
                let cs = failed.iter().map(|c| self.repair(c)).collect();
                self.all(cs)
            }
            Witness::Any { branches, .. } => {
                let cs = branches.iter().map(|c| self.repair(c)).collect();
                self.any(cs)
            }
            Witness::CountLow {
                node,
                path,
                qualifier,
                have,
                min,
                sibling_qualifiers,
                ..
            } => {
                let effective = self.effective_qualifiers(*qualifier, sibling_qualifiers);
                let body = self.build_value(node, path, &effective);
                if body.is_blocked() {
                    return body;
                }
                RepairTree::Repeat {
                    id: self.node(),
                    body: Box::new(body),
                    min: min - have,
                    max: None,
                }
            }
            Witness::CountHigh {
                node,
                path,
                matched,
                max,
                per_value,
                ..
            } => self.repair_count_high(node, path, matched, *max, per_value),
            Witness::Opaque { .. } => self.blocked(BlockReason::OpaqueSparql),
        }
    }

    fn repair_atom(&mut self, shape: ShapeId, produced_by: Option<&PathSupport>) -> RepairTree {
        // Replace-in-place: delete the offending value's edge, add a good one.
        let Some(ps) = produced_by else {
            // focus-scoped (e.g. nodeKind on the focus) — data edits can't fix it.
            return self.blocked(BlockReason::CannotMutateIdentity);
        };
        let Some(edge) = cut_edge(ps) else {
            return self.blocked(BlockReason::CannotMutateIdentity);
        };
        let del = Edit::delete(delete_pattern(&edge));
        let hole = self.hole();
        let add = Edit::add(TriplePattern::new(
            Slot::Bound(subj_term(&edge)),
            Slot::Bound(Term::NamedNode(edge.predicate.clone())),
            Slot::Open(hole),
        ));
        let constraint = self.leaf_constraint(shape);
        self.edits(vec![del, add], vec![(hole, constraint)])
    }

    fn repair_relational(
        &mut self,
        kind: RelKind,
        lhs: &[(Term, PathSupport)],
        rhs: &[(Term, PathSupport)],
        offending: &[(Term, Term)],
    ) -> RepairTree {
        match kind {
            RelKind::Eq => {
                self.blocked(BlockReason::Unsupported("sh:equals reconciliation".into()))
            }
            RelKind::Disj => {
                // shared value: delete it from the lhs path or the rhs predicate.
                let parts = offending
                    .iter()
                    .map(|(v, _)| {
                        let mut opts = Vec::new();
                        opts.extend(self.delete_value(v, lhs));
                        opts.extend(self.delete_value(v, rhs));
                        self.any(opts)
                    })
                    .collect();
                self.all(parts)
            }
            RelKind::UniqueLang => {
                // each duplicated pair: delete one of the two.
                let parts = offending
                    .iter()
                    .map(|(a, b)| {
                        let mut opts = Vec::new();
                        opts.extend(self.delete_value(a, lhs));
                        opts.extend(self.delete_value(b, lhs));
                        self.any(opts)
                    })
                    .collect();
                self.all(parts)
            }
            RelKind::Lt | RelKind::Le => {
                // delete the offending lhs value (value-shrinking deferred).
                let parts = offending
                    .iter()
                    .map(|(a, _)| {
                        let opts = self.delete_value(a, lhs);
                        self.any(opts)
                    })
                    .collect();
                self.all(parts)
            }
        }
    }

    /// A delete edit for `v` if it appears (with cuttable support) in `set`.
    fn delete_value(&mut self, v: &Term, set: &[(Term, PathSupport)]) -> Vec<RepairTree> {
        set.iter()
            .filter(|(val, _)| val == v)
            .filter_map(|(_, ps)| cut_edge(ps))
            .map(|edge| self.edits(vec![Edit::delete(delete_pattern(&edge))], vec![]))
            .collect()
    }

    fn repair_count_high(
        &mut self,
        node: &Term,
        path: &Path,
        matched: &[(Term, PathSupport)],
        max: u64,
        per_value: &[(Term, Witness)],
    ) -> RepairTree {
        if !per_value.is_empty() {
            // ∀-encoding: fix each offending value in place.
            let cs = per_value.iter().map(|(_, w)| self.repair(w)).collect();
            return self.all(cs);
        }
        // plain maxCount: delete (have - max) of the matched values.
        let d = (matched.len() as u64).saturating_sub(max);
        if d == 0 {
            return self.blocked(BlockReason::Unsupported(
                "maxCount: nothing to delete".into(),
            ));
        }
        let Path::Pred(p) = path else {
            return self.blocked(BlockReason::Unsupported(
                "maxCount over a compound path".into(),
            ));
        };
        let hole = self.hole();
        let values: Vec<Term> = matched.iter().map(|(v, _)| v.clone()).collect();
        let body = self.edits(
            vec![Edit::delete(TriplePattern::new(
                Slot::Bound(node.clone()),
                Slot::Bound(Term::NamedNode(p.clone())),
                Slot::Open(hole),
            ))],
            vec![(hole, HoleConstraint::OneOf(values))],
        );
        RepairTree::Repeat {
            id: self.node(),
            body: Box::new(body),
            min: d,
            max: Some(d),
        }
    }

    /// Materialize a fresh `π`-reachable value from `subject`, constrained to the
    /// `qualifiers` it must satisfy. The value hole carries `Typed`/`Kind`/`Const`
    /// for a single value-type qualifier, a `ConformsTo` for a single structural
    /// one, or a `ConformsToAll` conjunction for several (structural expansion
    /// deferred to the driver).
    fn build_value(&mut self, subject: &Term, path: &Path, qualifiers: &[ShapeId]) -> RepairTree {
        let vh = self.hole();
        let Some((edits, mut holes)) = self.materialize_path(subject.clone(), path, vh) else {
            return self.blocked(BlockReason::Unsupported("path materialization".into()));
        };
        holes.push((vh, self.value_constraint(qualifiers)));
        self.edits(edits, holes)
    }

    /// Add-edges realizing `path` from `subject` to the value hole `vh`, plus any
    /// fresh interior holes. Handles `Pred`, `Inverse(Pred)`, `Seq` of those, and
    /// `Star` — the last reflexively, which is what makes `sh:class` buildable:
    /// `rdf:type/rdfs:subClassOf*` materializes to `subject rdf:type ?vh` (a value
    /// bound to the class `C`, since `C subClassOf* C`).
    fn materialize_path(&mut self, subject: Term, path: &Path, vh: Hole) -> Option<Materialized> {
        match path {
            Path::Pred(p) => Some((
                vec![Edit::add(TriplePattern::new(
                    Slot::Bound(subject),
                    Slot::Bound(Term::NamedNode(p.clone())),
                    Slot::Open(vh),
                ))],
                vec![],
            )),
            Path::Inverse(inner) => {
                if let Path::Pred(p) = inner.as_ref() {
                    Some((
                        vec![Edit::add(TriplePattern::new(
                            Slot::Open(vh),
                            Slot::Bound(Term::NamedNode(p.clone())),
                            Slot::Bound(subject),
                        ))],
                        vec![],
                    ))
                } else {
                    None
                }
            }
            // `π*`: a single application is a valid member of the closure, so
            // materialize one hop of the inner path.
            Path::Star(inner) => self.materialize_path(subject, inner, vh),
            Path::Seq(steps) => self.materialize_seq(subject, steps, vh),
            _ => None,
        }
    }

    /// Materialize a `Seq` of steps from `subject` to `vh`. Trailing `Star` steps
    /// are dropped: they are satisfied reflexively, so the value sits at the result
    /// of the last concrete step. Each remaining step must be `Pred` or
    /// `Inverse(Pred)`.
    fn materialize_seq(&mut self, subject: Term, steps: &[Path], vh: Hole) -> Option<Materialized> {
        let mut end = steps.len();
        while end > 0 && matches!(steps[end - 1], Path::Star(_)) {
            end -= 1;
        }
        let steps = &steps[..end];
        if steps.is_empty() {
            return None; // wholly reflexive: nothing concrete to add
        }
        let mut edits = Vec::new();
        let mut holes = Vec::new();
        let mut cursor = Slot::Bound(subject);
        for (i, step) in steps.iter().enumerate() {
            let target = if i == steps.len() - 1 {
                Slot::Open(vh)
            } else {
                let m = self.hole();
                holes.push((m, HoleConstraint::Fresh));
                Slot::Open(m)
            };
            match step {
                Path::Pred(p) => edits.push(Edit::add(TriplePattern::new(
                    cursor.clone(),
                    Slot::Bound(Term::NamedNode(p.clone())),
                    target.clone(),
                ))),
                Path::Inverse(inner) => {
                    let Path::Pred(p) = inner.as_ref() else {
                        return None;
                    };
                    edits.push(Edit::add(TriplePattern::new(
                        target.clone(),
                        Slot::Bound(Term::NamedNode(p.clone())),
                        cursor.clone(),
                    )));
                }
                _ => return None, // interior Star / Alt / nested Seq not supported
            }
            cursor = target;
        }
        Some((edits, holes))
    }

    /// Every shape a value added to satisfy this `CountLow` must conform to: the
    /// count's own qualifier together with the inner shape of each sibling
    /// universal `∀π.φ` on the same path. Those universals are vacuously satisfied
    /// while `have == 0`, so they never witness as their own failures — but a
    /// freshly-added value is checked against all of them, so the hole must carry
    /// the *full* conjunction, not just the base qualifier or a single sibling.
    /// `⊤` qualifiers contribute nothing and are dropped; duplicates are merged.
    fn effective_qualifiers(&self, qualifier: ShapeId, siblings: &[ShapeId]) -> Vec<ShapeId> {
        let mut out: Vec<ShapeId> = Vec::with_capacity(1 + siblings.len());
        for id in std::iter::once(qualifier).chain(siblings.iter().copied()) {
            if matches!(self.arena.get(id), Shape::Top | Shape::Pending) {
                continue;
            }
            if !out.contains(&id) {
                out.push(id);
            }
        }
        out
    }

    /// The constraint a freshly-built value hole carries, given every shape it must
    /// conform to. No effective qualifier (all were `⊤`) is `AnyNode`; a single one
    /// gets the precise leaf constraint or a `ConformsTo`; several are kept whole as
    /// a `ConformsToAll` conjunction (the gate still enforces each, but the hole now
    /// *describes* the complete obligation rather than an arbitrary representative).
    fn value_constraint(&self, qualifiers: &[ShapeId]) -> HoleConstraint {
        match qualifiers {
            [] => HoleConstraint::AnyNode,
            [single] => self.single_value_constraint(*single),
            many => HoleConstraint::ConformsToAll(many.to_vec()),
        }
    }

    fn single_value_constraint(&self, qualifier: ShapeId) -> HoleConstraint {
        match self.arena.get(qualifier) {
            Shape::Top | Shape::Pending => HoleConstraint::AnyNode,
            Shape::TestConst(c) => HoleConstraint::Const(c.clone()),
            Shape::TestType(t) => HoleConstraint::Typed(t.clone()),
            Shape::TestKind(k) => HoleConstraint::Kind(*k),
            _ => HoleConstraint::ConformsTo(qualifier),
        }
    }

    fn leaf_constraint(&self, shape: ShapeId) -> HoleConstraint {
        match self.arena.get(shape) {
            Shape::TestConst(c) => HoleConstraint::Const(c.clone()),
            Shape::TestType(t) => HoleConstraint::Typed(t.clone()),
            Shape::TestKind(k) => HoleConstraint::Kind(*k),
            _ => HoleConstraint::ConformsTo(shape),
        }
    }

    // ── deletive: SatTrace → RepairTree ──

    fn break_(&mut self, s: &SatTrace) -> RepairTree {
        match s {
            SatTrace::Atom { produced_by, .. } => match cut_edge(produced_by) {
                Some(edge) => self.edits(vec![Edit::delete(delete_pattern(&edge))], vec![]),
                None => self.blocked(BlockReason::CannotMutateIdentity),
            },
            SatTrace::AllHeld { children, .. } => {
                // break any one conjunct.
                let cs = children.iter().map(|c| self.break_(c)).collect();
                self.any(cs)
            }
            SatTrace::AnyHeld { satisfied, .. } => {
                // break every satisfied disjunct.
                let cs = satisfied.iter().map(|c| self.break_(c)).collect();
                self.all(cs)
            }
            SatTrace::CountHeld { matches, min, .. } => {
                // Drop below min by breaking the qualifier at the matches. First
                // cut breaks all of them (sound, non-minimal); needs a min>0 to be
                // falsifiable by deletion at all.
                if matches!(min, Some(m) if *m > 0) {
                    let cs = matches.iter().map(|(_, t)| self.break_(t)).collect();
                    self.all(cs)
                } else {
                    self.blocked(BlockReason::Unsupported(
                        "falsify maxCount needs adds".into(),
                    ))
                }
            }
            SatTrace::NotHeld { inner_fails, .. } => self.repair(inner_fails),
            SatTrace::Irrefutable { .. } => {
                self.blocked(BlockReason::Unsupported("tautology".into()))
            }
            SatTrace::Blocked { reason, .. } => {
                let r = match reason {
                    WBlock::OpaqueSparql => BlockReason::OpaqueSparql,
                    WBlock::ClosedNeedsAdd => {
                        BlockReason::Unsupported("closed needs an addition".into())
                    }
                    WBlock::Unsupported => {
                        BlockReason::Unsupported("relational falsification".into())
                    }
                };
                self.blocked(r)
            }
            SatTrace::Coinductive { .. } => self.blocked(BlockReason::Coinductive),
        }
    }
}

// ── PathSupport helpers ──

/// The immediate edge to a value (the final hop): the one to cut for a
/// replace/delete. `None` for reflexive (`Empty`) support.
fn cut_edge(ps: &PathSupport) -> Option<Triple> {
    match ps {
        PathSupport::Empty => None,
        PathSupport::Edge(t) => Some(t.clone()),
        PathSupport::Chain(v) => v.last().and_then(cut_edge),
        PathSupport::Alt(v) => v.first().and_then(cut_edge),
    }
}

fn delete_pattern(t: &Triple) -> TriplePattern {
    TriplePattern::new(
        Slot::Bound(subj_term(t)),
        Slot::Bound(Term::NamedNode(t.predicate.clone())),
        Slot::Bound(t.object.clone()),
    )
}

fn subj_term(t: &Triple) -> Term {
    match &t.subject {
        NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n.clone()),
        NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::witness::witness_violations;
    use shifty_parse::{load_turtle, parse_turtle};
    use shifty_repair::{Plan, instantiate};

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix ex:  <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    "#;

    fn synth_one(ttl: &str) -> (RepairTree, shifty_algebra::Schema) {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let ws = witness_violations(&loaded.graph, &parsed.schema).expect("stratifiable");
        assert_eq!(ws.len(), 1, "expected exactly one focus witness");
        (synthesize(&parsed.schema.arena, &ws[0]), parsed.schema)
    }

    #[test]
    fn min_count_yields_a_repeat_adding_one_value() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 2 ] .
            ex:x ex:p ex:y .
            "
        );
        let (tree, _) = synth_one(&ttl);
        // ∃≥2 with 1 present ⇒ add 1 more value reachable by ex:p.
        let id = match &tree {
            RepairTree::Repeat { id, min: 1, .. } => *id,
            other => panic!("expected Repeat{{min:1}}, got {other:?}"),
        };
        // one instance ⇒ one fresh value hole; binding it yields one add.
        let plan = Plan {
            count: std::collections::HashMap::from([(id, 1)]),
            ..Default::default()
        };
        let disc = instantiate(&tree, &plan);
        assert_eq!(disc.open_holes.len(), 1);
        let (hole, _) = disc.open_holes[0].clone();
        let mut bound = plan;
        bound.binding.insert(
            hole,
            Term::NamedNode(oxrdf::NamedNode::new("http://ex/z").unwrap()),
        );
        let out = instantiate(&tree, &bound);
        assert_eq!(out.delta.add.len(), 1);
        assert!(out.open_holes.is_empty());
    }

    #[test]
    fn datatype_value_replace_instantiates_to_delete_plus_add() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            "
        );
        let (tree, _) = synth_one(&ttl);
        // bind the replacement hole and check we get delete + add.
        let mut plan = Plan::default();
        // discover the hole id first.
        let disc = instantiate(&tree, &plan);
        assert_eq!(disc.open_holes.len(), 1, "one replacement hole");
        let (hole, constraint) = disc.open_holes[0].clone();
        assert!(matches!(constraint, HoleConstraint::Typed(_)));
        plan.binding.insert(
            hole,
            Term::from(oxrdf::Literal::new_typed_literal(
                "7",
                oxrdf::vocab::xsd::INTEGER,
            )),
        );
        let out = instantiate(&tree, &plan);
        assert_eq!(out.delta.delete.len(), 1, "deletes the bad value");
        assert_eq!(out.delta.add.len(), 1, "adds a good one");
    }

    #[test]
    fn focus_nodekind_is_blocked() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:nodeKind sh:Literal .
            ex:x ex:p ex:y .
            "
        );
        let (tree, _) = synth_one(&ttl);
        assert!(matches!(
            tree,
            RepairTree::Blocked(_, BlockReason::CannotMutateIdentity)
        ));
    }

    #[test]
    fn class_qualified_min_count_builds_a_type_assertion() {
        // ex:x needs a part conforming to (sh:class ex:Widget); none present.
        // The build must add `<part> rdf:type ex:Widget` — i.e. materialize the
        // `rdf:type/rdfs:subClassOf*` path reflexively, not give up on it.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:part ;
                    sh:qualifiedValueShape ex:PartShape ; sh:qualifiedMinCount 1 ] .
            ex:PartShape a sh:NodeShape ; sh:class ex:Widget .
            ex:x a ex:Thing .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let ws = witness_violations(&loaded.graph, &parsed.schema).unwrap();
        let parent = synthesize(&parsed.schema.arena, &ws[0]);
        // the parent hole is the part value (ConformsTo PartShape).
        let mut plan = Plan {
            count: std::collections::HashMap::new(),
            ..Default::default()
        };
        // bind the part hole to a fresh node, then build that node against PartShape.
        let parent_hole = {
            let disc = instantiate(
                &parent,
                &Plan {
                    count: std::collections::HashMap::from([(parent.id(), 1)]),
                    ..Default::default()
                },
            );
            disc.open_holes[0].clone()
        };
        // Find PartShape's id via the ConformsTo constraint, build the node.
        let HoleConstraint::ConformsTo(part_shape) = parent_hole.1 else {
            panic!("expected ConformsTo, got {:?}", parent_hole.1);
        };
        let fresh = Term::NamedNode(oxrdf::NamedNode::new("http://ex/part1").unwrap());
        let sub_fw =
            crate::witness::witness_node(&loaded.graph, &parsed.schema, &fresh, part_shape)
                .unwrap()
                .expect("fresh node fails PartShape");
        let sub = synthesize(&parsed.schema.arena, &sub_fw);
        assert!(
            !sub.is_blocked(),
            "class build must not be blocked: {sub:?}"
        );
        // instantiate the sub-build: one Repeat instance; the value hole is Const(Widget).
        let disc = instantiate(
            &sub,
            &Plan {
                count: std::collections::HashMap::from([(sub.id(), 1)]),
                ..Default::default()
            },
        );
        let (h, c) = disc.open_holes[0].clone();
        assert!(
            matches!(c, HoleConstraint::Const(_)),
            "type hole is Const(C): {c:?}"
        );
        plan.count.insert(sub.id(), 1);
        plan.binding.insert(
            h,
            Term::NamedNode(oxrdf::NamedNode::new("http://ex/Widget").unwrap()),
        );
        let out = instantiate(&sub, &plan);
        assert_eq!(out.delta.add.len(), 1);
        assert_eq!(
            out.delta.add[0].predicate.as_str(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
    }

    #[test]
    fn closed_violation_deletes_the_extra_triple() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:closed true ;
                sh:property [ sh:path ex:p ; sh:minCount 0 ] .
            ex:x ex:p ex:y ; ex:extra ex:z .
            "
        );
        let (tree, _) = synth_one(&ttl);
        let out = instantiate(&tree, &Plan::default());
        assert!(out.delta.add.is_empty());
        assert_eq!(out.delta.delete.len(), 1);
        assert_eq!(
            out.delta.delete[0].predicate,
            oxrdf::NamedNode::new("http://ex/extra").unwrap()
        );
    }

    /// The sole open hole of `tree`, taking exactly one instance of any `Repeat`.
    fn sole_hole(tree: &RepairTree) -> HoleConstraint {
        let disc = instantiate(
            tree,
            &Plan {
                count: std::collections::HashMap::from([(tree.id(), 1)]),
                ..Default::default()
            },
        );
        assert_eq!(disc.open_holes.len(), 1, "expected exactly one open hole");
        disc.open_holes[0].1.clone()
    }

    fn class_named(name: &str) -> Term {
        Term::NamedNode(oxrdf::NamedNode::new(name).unwrap())
    }

    #[test]
    fn min_count_with_class_value_constraint_carries_the_class() {
        // `sh:minCount 1 ; sh:class ex:C` on one property: the count's qualifier is
        // ⊤, but the `∀ex:p.(class ex:C)` universal is vacuously satisfied while no
        // value exists. The single effective qualifier is that class, so the hole
        // is a `ConformsTo` of it — not an unconstrained `AnyNode`.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] .
            ex:x a ex:Thing .
            "
        );
        let (tree, schema) = synth_one(&ttl);
        let HoleConstraint::ConformsTo(s) = sole_hole(&tree) else {
            panic!("expected a single ConformsTo hole");
        };
        assert_eq!(
            shifty_algebra::render::class_target_shape(s, &schema.arena),
            Some(class_named("http://ex/C")),
        );
    }

    #[test]
    fn qualified_min_count_carries_qualifier_and_sibling_universal() {
        // The motivating case: a non-⊤ count qualifier (`sh:qualifiedValueShape`)
        // *and* a sibling `∀ex:p.(class ex:C)`. The old synthesis kept only the
        // qualifier and dropped the class; now the hole is a `ConformsToAll`
        // carrying *both*, so the description reflects every obligation.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ;
                    sh:qualifiedValueShape ex:Q ; sh:qualifiedMinCount 1 ;
                    sh:class ex:C ] .
            ex:Q a sh:NodeShape ; sh:nodeKind sh:IRI .
            ex:x a ex:Thing .
            "
        );
        let (tree, schema) = synth_one(&ttl);
        let HoleConstraint::ConformsToAll(ss) = sole_hole(&tree) else {
            panic!("expected a ConformsToAll hole carrying every qualifier");
        };
        assert_eq!(ss.len(), 2, "qualifier + sibling universal");
        // Exactly one member is the class ex:C; the other is the qualifier shape.
        let classes: Vec<Term> = ss
            .iter()
            .filter_map(|s| shifty_algebra::render::class_target_shape(*s, &schema.arena))
            .collect();
        assert_eq!(classes, vec![class_named("http://ex/C")]);
        // …and the non-class member is the qualifier (nodeKind IRI), not dropped.
        assert!(
            ss.iter().any(|s| matches!(
                schema.arena.get(*s),
                Shape::Annotated { .. } | Shape::TestKind(_)
            )),
            "the qualifiedValueShape must still be present: {ss:?}",
        );
    }
}
