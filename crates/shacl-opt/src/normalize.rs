//! Semantics-preserving normalization of a [`Schema`] (Layer 4).
//!
//! Rebuilds the shape arena from the schema roots, hash-consing
//! structurally-identical nodes (CSE) and applying the sound Boolean/count
//! simplifications tracked in `docs/04-normalization.md`. Because the rebuild
//! interns only what the roots reach, the result is also compacted (no orphan
//! slots). Recursive SCCs (found via [`crate::strata`]) are rebuilt preserving
//! sharing but *not* collapsed, so cycles survive.
//!
//! All rewrites are per-node truth-functional, hence sound under the gfp
//! validation semantics; the W3C harness cross-checks `validate(normalize(S))
//! ≡ validate(S)` on every core test.

use crate::strata::analyze;
use shacl_algebra::{NodeExpr, Rule, RuleHead, Schema, Selector, Shape, ShapeArena, ShapeId, Statement};
use std::collections::{HashMap, HashSet};

/// Normalize a schema: CSE + compaction + Boolean/count simplification.
pub fn normalize(schema: &Schema) -> Schema {
    let mut z = Interner::new(&schema.arena);
    let statements = schema
        .statements
        .iter()
        .map(|st| Statement { selector: z.selector(&st.selector), shape: z.intern(st.shape) })
        .collect();
    let rules = schema.rules.iter().map(|r| z.rule(r)).collect();
    Schema { arena: z.dst, statements, rules }
}

struct Interner<'a> {
    src: &'a ShapeArena,
    dst: ShapeArena,
    /// src id → dst id
    memo: HashMap<ShapeId, ShapeId>,
    /// canonical dst node → its id (hash-consing)
    cons: HashMap<Shape, ShapeId>,
    /// src ids inside a recursive SCC (rebuilt, not CSE'd/collapsed)
    cyclic: HashSet<ShapeId>,
}

impl<'a> Interner<'a> {
    fn new(src: &'a ShapeArena) -> Self {
        let strat = analyze(src);
        let cyclic = strat
            .strata
            .iter()
            .filter(|s| s.recursive)
            .flat_map(|s| s.shapes.iter().copied())
            .collect();
        Self { src, dst: ShapeArena::new(), memo: HashMap::new(), cons: HashMap::new(), cyclic }
    }

    fn cons(&mut self, shape: Shape) -> ShapeId {
        if let Some(&d) = self.cons.get(&shape) {
            return d;
        }
        let d = self.dst.insert(shape.clone());
        self.cons.insert(shape, d);
        d
    }

    fn top(&mut self) -> ShapeId {
        self.cons(Shape::Top)
    }

    fn bottom(&mut self) -> ShapeId {
        let t = self.top();
        self.cons(Shape::Not(t))
    }

    fn is_top(&self, id: ShapeId) -> bool {
        matches!(self.dst.get(id), Shape::Top)
    }

    fn is_bottom(&self, id: ShapeId) -> bool {
        matches!(self.dst.get(id), Shape::Not(x) if matches!(self.dst.get(*x), Shape::Top))
    }

    fn intern(&mut self, id: ShapeId) -> ShapeId {
        if let Some(&d) = self.memo.get(&id) {
            return d;
        }
        if self.cyclic.contains(&id) {
            let d = self.dst.reserve();
            self.memo.insert(id, d);
            let shape = self.rebuild_cyclic(id);
            self.dst.set(d, shape);
            d
        } else {
            let r = self.simplify(id);
            self.memo.insert(id, r);
            r
        }
    }

    /// Full simplification for an acyclic node, returning a (possibly existing)
    /// canonical id.
    fn simplify(&mut self, id: ShapeId) -> ShapeId {
        match self.src.get(id).clone() {
            Shape::Top => self.top(),
            Shape::Not(c) => {
                let cn = self.intern(c);
                self.mk_not(cn)
            }
            Shape::And(cs) => {
                let ids = cs.iter().map(|c| self.intern(*c)).collect();
                self.mk_and(ids)
            }
            Shape::Or(cs) => {
                let ids = cs.iter().map(|c| self.intern(*c)).collect();
                self.mk_or(ids)
            }
            Shape::Count { path, min, max, qualifier } => {
                let q = self.intern(qualifier);
                self.mk_count(path, min, max, q)
            }
            // leaves (and the transient Pending) are interned verbatim
            leaf => self.cons(leaf),
        }
    }

    /// Light rebuild for a node inside a recursive SCC: intern children and keep
    /// the variant (dedup `And`/`Or` members) but never collapse.
    fn rebuild_cyclic(&mut self, id: ShapeId) -> Shape {
        match self.src.get(id).clone() {
            Shape::Not(c) => Shape::Not(self.intern(c)),
            Shape::And(cs) => Shape::And(self.intern_set(&cs)),
            Shape::Or(cs) => Shape::Or(self.intern_set(&cs)),
            Shape::Count { path, min, max, qualifier } => {
                Shape::Count { path, min, max, qualifier: self.intern(qualifier) }
            }
            leaf => leaf,
        }
    }

    fn intern_set(&mut self, cs: &[ShapeId]) -> Vec<ShapeId> {
        let mut v: Vec<ShapeId> = cs.iter().map(|c| self.intern(*c)).collect();
        v.sort();
        v.dedup();
        v
    }

    fn mk_not(&mut self, c: ShapeId) -> ShapeId {
        if let Shape::Not(x) = self.dst.get(c) {
            return *x; // ¬¬φ = φ
        }
        self.cons(Shape::Not(c))
    }

    fn mk_and(&mut self, ids: Vec<ShapeId>) -> ShapeId {
        let mut flat = Vec::new();
        for id in ids {
            if self.is_bottom(id) {
                return id; // φ ∧ ⊥ = ⊥
            }
            match self.dst.get(id) {
                Shape::Top => {}                                        // drop ⊤
                Shape::And(inner) => flat.extend(inner.iter().copied()), // flatten
                _ => flat.push(id),
            }
        }
        flat.sort();
        flat.dedup();
        if self.has_complement(&flat) {
            return self.bottom(); // φ ∧ ¬φ = ⊥
        }
        match flat.len() {
            0 => self.top(),
            1 => flat[0],
            _ => self.cons(Shape::And(flat)),
        }
    }

    fn mk_or(&mut self, ids: Vec<ShapeId>) -> ShapeId {
        let mut flat = Vec::new();
        for id in ids {
            if self.is_top(id) {
                return id; // φ ∨ ⊤ = ⊤
            }
            if self.is_bottom(id) {
                continue; // drop ⊥
            }
            match self.dst.get(id) {
                Shape::Or(inner) => flat.extend(inner.iter().copied()),
                _ => flat.push(id),
            }
        }
        flat.sort();
        flat.dedup();
        if self.has_complement(&flat) {
            return self.top(); // φ ∨ ¬φ = ⊤
        }
        match flat.len() {
            0 => self.bottom(),
            1 => flat[0],
            _ => self.cons(Shape::Or(flat)),
        }
    }

    /// Does `ids` contain some `X` and its negation `¬X`?
    fn has_complement(&self, ids: &[ShapeId]) -> bool {
        let set: HashSet<ShapeId> = ids.iter().copied().collect();
        ids.iter().any(|&id| match self.dst.get(id) {
            Shape::Not(x) => set.contains(x),
            _ => false,
        })
    }

    fn mk_count(
        &mut self,
        path: shacl_algebra::Path,
        min: Option<u64>,
        max: Option<u64>,
        q: ShapeId,
    ) -> ShapeId {
        if max.is_none() && matches!(min, None | Some(0)) {
            return self.top(); // ∃≥0 = ⊤
        }
        if let (Some(a), Some(b)) = (min, max)
            && a > b
        {
            return self.bottom(); // unsatisfiable bounds
        }
        self.cons(Shape::Count { path, min, max, qualifier: q })
    }

    fn selector(&mut self, sel: &Selector) -> Selector {
        match sel {
            Selector::HasPath(p, q) => Selector::HasPath(p.clone(), self.intern(*q)),
            other => other.clone(),
        }
    }

    fn rule(&mut self, r: &Rule) -> Rule {
        Rule {
            selector: self.selector(&r.selector),
            conditions: r.conditions.iter().map(|c| self.intern(*c)).collect(),
            head: self.head(&r.head),
            order: r.order,
            deactivated: r.deactivated,
        }
    }

    fn head(&mut self, h: &RuleHead) -> RuleHead {
        match h {
            RuleHead::Triple { subject, predicate, object } => RuleHead::Triple {
                subject: self.node_expr(subject),
                predicate: self.node_expr(predicate),
                object: self.node_expr(object),
            },
            RuleHead::Sparql(s) => RuleHead::Sparql(s.clone()),
        }
    }

    fn node_expr(&mut self, e: &NodeExpr) -> NodeExpr {
        match e {
            NodeExpr::Filter { input, shape } => NodeExpr::Filter {
                input: Box::new(self.node_expr(input)),
                shape: self.intern(*shape),
            },
            NodeExpr::Intersection(v) => {
                NodeExpr::Intersection(v.iter().map(|x| self.node_expr(x)).collect())
            }
            NodeExpr::Union(v) => NodeExpr::Union(v.iter().map(|x| self.node_expr(x)).collect()),
            NodeExpr::Function { iri, args } => NodeExpr::Function {
                iri: iri.clone(),
                args: args.iter().map(|x| self.node_expr(x)).collect(),
            },
            other => other.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shacl_algebra::{NodeKindSet, Path, Selector};

    fn schema_with(arena: ShapeArena, root: ShapeId) -> Schema {
        let mut s = Schema { arena, statements: Vec::new(), rules: Vec::new() };
        s.statements.push(Statement {
            selector: Selector::IsConst(shacl_algebra::Term::NamedNode(
                shacl_algebra::NamedNode::new("http://ex/x").unwrap(),
            )),
            shape: root,
        });
        s
    }

    #[test]
    fn cse_dedups_identical_subshapes() {
        let mut a = ShapeArena::new();
        let t1 = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let t2 = a.insert(Shape::TestKind(NodeKindSet::IRI)); // duplicate
        let root = a.insert(Shape::And(vec![t1, t2]));
        let n = normalize(&schema_with(a, root));
        // And([X, X]) → dedup → single → the TestKind itself
        let rooted = n.statements[0].shape;
        assert!(matches!(n.arena.get(rooted), Shape::TestKind(_)));
        // exactly one TestKind survives (plus nothing else reachable)
        assert_eq!(n.arena.len(), 1);
    }

    #[test]
    fn bottom_absorbs_conjunction() {
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let t = a.insert(Shape::Top);
        let bot = a.insert(Shape::Not(t));
        let root = a.insert(Shape::And(vec![k, bot]));
        let n = normalize(&schema_with(a, root));
        let rooted = n.statements[0].shape;
        assert!(matches!(n.arena.get(rooted), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn top_absorbs_disjunction() {
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let t = a.insert(Shape::Top);
        let root = a.insert(Shape::Or(vec![k, t]));
        let n = normalize(&schema_with(a, root));
        assert!(matches!(n.arena.get(n.statements[0].shape), Shape::Top));
    }

    #[test]
    fn complement_is_unsat() {
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let nk = a.insert(Shape::Not(k));
        let root = a.insert(Shape::And(vec![k, nk]));
        let n = normalize(&schema_with(a, root));
        assert!(matches!(n.arena.get(n.statements[0].shape), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn recursive_shape_survives_normalization() {
        // S := ∃≥1 p . S
        let mut a = ShapeArena::new();
        let s = a.reserve();
        a.set(
            s,
            Shape::Count {
                path: Path::Pred(shacl_algebra::NamedNode::new("http://ex/p").unwrap()),
                min: Some(1),
                max: None,
                qualifier: s,
            },
        );
        let n = normalize(&schema_with(a, s));
        let rooted = n.statements[0].shape;
        match n.arena.get(rooted) {
            Shape::Count { qualifier, .. } => assert_eq!(*qualifier, rooted),
            other => panic!("expected self-referential Count, got {other:?}"),
        }
    }
}
