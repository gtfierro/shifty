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
use shifty_algebra::{
    NodeExpr, NodeKindSet, Path, Rule, RuleHead, Schema, Selector, Shape, ShapeArena, ShapeId,
    Statement, ValueType,
};
use std::collections::{HashMap, HashSet};

/// Normalize a schema: CSE + compaction + Boolean/count simplification.
pub fn normalize(schema: &Schema) -> Schema {
    let mut z = Interner::new(&schema.arena);
    // dedup identical (selector, shape) pairs after normalization
    let mut seen: HashSet<(Selector, ShapeId)> = HashSet::new();
    let statements = schema
        .statements
        .iter()
        .map(|st| Statement {
            selector: z.selector(&st.selector),
            shape: z.intern(st.shape),
        })
        .filter(|st| seen.insert((st.selector.clone(), st.shape)))
        .collect();
    let rules = schema.rules.iter().map(|r| z.rule(r)).collect();
    // remap shape names through the CSE memo (CSE may collapse two named shapes)
    let names = schema
        .names
        .iter()
        .filter_map(|(old, name)| z.memo.get(old).map(|new| (*new, name.clone())))
        .collect();
    let normalized = Schema {
        arena: z.dst,
        statements,
        rules,
        names,
    };
    normalized.arena.debug_assert_finalized();
    normalized
}

/// Push `Inverse` inward one level, returning the canonical inverse of `path`
/// (only `Inverse(Pred(...))` leaves remain after full recursion).
fn push_inverse(path: Path) -> Path {
    match path {
        Path::Id => Path::Id,
        Path::Inverse(inner) => normalize_path(*inner), // (π⁻)⁻ = normalize(π)
        Path::Seq(steps) => {
            // (π₁·…·πₙ)⁻ = πₙ⁻·…·π₁⁻
            Path::seq(steps.into_iter().rev().map(push_inverse).collect())
        }
        Path::Alt(alts) => Path::alt(alts.into_iter().map(push_inverse).collect()),
        Path::Star(inner) => Path::star(push_inverse(*inner)), // (π*)⁻ = (π⁻)*
        pred => Path::Inverse(Box::new(pred)),                 // Pred: stays wrapped
    }
}

/// Recursively normalize a path so `Inverse` only wraps `Pred` leaves,
/// `Alt` members are deduped, and star laws are applied.
fn normalize_path(path: Path) -> Path {
    match path {
        Path::Inverse(inner) => push_inverse(*inner),
        Path::Seq(steps) => {
            let steps: Vec<Path> = steps.into_iter().map(normalize_path).collect();
            // π*·π* = π* — merge adjacent equal stars
            let mut merged: Vec<Path> = Vec::with_capacity(steps.len());
            for step in steps {
                match merged.last() {
                    Some(last) if matches!(last, Path::Star(_)) && last == &step => {}
                    _ => merged.push(step),
                }
            }
            Path::seq(merged)
        }
        Path::Alt(alts) => {
            // dedup while preserving first-occurrence order
            let mut seen = HashSet::new();
            let deduped: Vec<Path> = alts
                .into_iter()
                .map(normalize_path)
                .filter(|p| seen.insert(p.clone()))
                .collect();
            Path::alt(deduped)
        }
        Path::Star(inner) => {
            let inner = normalize_path(*inner);
            // (π∪id)* = π* — Id is implicit in the reflexive closure
            let inner = match inner {
                Path::Alt(alts) => {
                    let without_id: Vec<Path> =
                        alts.into_iter().filter(|p| *p != Path::Id).collect();
                    Path::alt(without_id)
                }
                other => other,
            };
            inner.star()
        }
        other => other,
    }
}

/// The tighter lower bound (larger min), treating `None` as no bound.
fn tighter_lower(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (s, None) | (None, s) => s,
    }
}

/// The tighter upper bound (smaller max), treating `None` as no bound.
fn tighter_upper(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (s, None) | (None, s) => s,
    }
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
    /// dst ids that are recursive; NNF must not push negation into these
    cyclic_dst: HashSet<ShapeId>,
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
        Self {
            src,
            dst: ShapeArena::new(),
            memo: HashMap::new(),
            cons: HashMap::new(),
            cyclic,
            cyclic_dst: HashSet::new(),
        }
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
            self.cyclic_dst.insert(d);
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
            Shape::Annotated { severity, shape } => {
                let shape = self.intern(shape);
                self.cons(Shape::Annotated { severity, shape })
            }
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
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                let q = self.intern(qualifier);
                self.mk_count(normalize_path(path), min, max, q)
            }
            // value-type facet tightening + same-family unsat (§4)
            Shape::TestType(vt) => match vt.normalize() {
                None => self.bottom(),              // facet unsat ⇒ ⊥
                Some(ValueType::Any) => self.top(), // any ⇒ ⊤
                Some(v) => self.cons(Shape::TestType(v)),
            },
            // path-bearing leaves: normalize their paths
            Shape::Eq(path, nn) => self.cons(Shape::Eq(normalize_path(path), nn)),
            Shape::Disj(path, nn) => self.cons(Shape::Disj(normalize_path(path), nn)),
            Shape::Lt(path, nn) => self.cons(Shape::Lt(normalize_path(path), nn)),
            Shape::Le(path, nn) => self.cons(Shape::Le(normalize_path(path), nn)),
            Shape::UniqueLang(path) => self.cons(Shape::UniqueLang(normalize_path(path))),
            // remaining leaves (and the transient Pending) are interned verbatim
            leaf => self.cons(leaf),
        }
    }

    /// Light rebuild for a node inside a recursive SCC: intern children and keep
    /// the variant (dedup `And`/`Or` members) but never collapse.
    fn rebuild_cyclic(&mut self, id: ShapeId) -> Shape {
        match self.src.get(id).clone() {
            Shape::Annotated { severity, shape } => Shape::Annotated {
                severity,
                shape: self.intern(shape),
            },
            Shape::Not(c) => Shape::Not(self.intern(c)),
            Shape::And(cs) => Shape::And(self.intern_set(&cs)),
            Shape::Or(cs) => Shape::Or(self.intern_set(&cs)),
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => Shape::Count {
                path: normalize_path(path),
                min,
                max,
                qualifier: self.intern(qualifier),
            },
            leaf => leaf,
        }
    }

    fn intern_set(&mut self, cs: &[ShapeId]) -> Vec<ShapeId> {
        let mut v: Vec<ShapeId> = cs.iter().map(|c| self.intern(*c)).collect();
        v.sort();
        v.dedup();
        v
    }

    /// `¬c`, pushed inward to negation normal form. `¬` only ever ends up on a
    /// leaf atom or on a recursive node (which we don't unfold).
    fn mk_not(&mut self, c: ShapeId) -> ShapeId {
        if let Shape::Not(x) = self.dst.get(c) {
            return *x; // ¬¬φ = φ (always safe, just an id lookup)
        }
        if self.cyclic_dst.contains(&c) {
            return self.cons(Shape::Not(c)); // don't push negation into a cycle
        }
        match self.dst.get(c).clone() {
            // De Morgan
            Shape::And(cs) => {
                let neg = cs.iter().map(|c| self.mk_not(*c)).collect();
                self.mk_or(neg)
            }
            Shape::Or(cs) => {
                let neg = cs.iter().map(|c| self.mk_not(*c)).collect();
                self.mk_and(neg)
            }
            // ¬(∃[min..max] π.q) = ∃≤(min-1) π.q ∨ ∃≥(max+1) π.q (qualifier stays positive)
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                let mut alts = Vec::new();
                if let Some(a) = min
                    && a > 0
                {
                    alts.push(self.mk_count(path.clone(), None, Some(a - 1), qualifier));
                }
                if let Some(b) = max {
                    alts.push(self.mk_count(path, Some(b + 1), None, qualifier));
                }
                self.mk_or(alts)
            }
            // ¬TestKind(K) = TestKind(K̄) — complement the node-kind bitset
            Shape::TestKind(k) => {
                let comp: NodeKindSet = k.complement();
                if comp.is_empty() {
                    self.bottom() // K covered all kinds ⇒ complement is ⊥
                } else {
                    self.cons(Shape::TestKind(comp))
                }
            }
            // leaf atom (and ⊤, which becomes ⊥ = ¬⊤)
            _ => self.cons(Shape::Not(c)),
        }
    }

    fn mk_and(&mut self, ids: Vec<ShapeId>) -> ShapeId {
        // flatten nested And
        let mut flat = Vec::new();
        for id in ids {
            match self.dst.get(id) {
                Shape::And(inner) => flat.extend(inner.iter().copied()),
                _ => flat.push(id),
            }
        }
        // merge counts on the same (path, qualifier) into one tightened bound
        let flat = self.merge_counts(flat);
        // fuse sibling value-type facets into one tightened test(τ)
        let flat = self.merge_value_types(flat);
        // intersect sibling node-kind sets; unsat intersection → ⊥
        let flat = self.merge_node_kinds(flat);
        // absorption (merging may have produced ⊤/⊥) + dedup + complement
        let mut acc = Vec::new();
        for id in flat {
            if self.is_bottom(id) {
                return id; // φ ∧ ⊥ = ⊥
            }
            if self.is_top(id) {
                continue; // φ ∧ ⊤ = φ
            }
            acc.push(id);
        }
        acc.sort();
        acc.dedup();
        if self.has_complement(&acc) {
            return self.bottom(); // φ ∧ ¬φ = ⊥
        }
        match acc.len() {
            0 => self.top(),
            1 => acc[0],
            _ => self.cons(Shape::And(acc)),
        }
    }

    /// Intersect sibling `TestKind` sets in a conjunction.  An empty intersection
    /// means no term can satisfy the shape ⇒ ⊥.  A full intersection (all three
    /// kinds) imposes no constraint ⇒ drops from ∧ (same as ⊤).
    fn merge_node_kinds(&mut self, flat: Vec<ShapeId>) -> Vec<ShapeId> {
        let mut acc: Option<NodeKindSet> = None;
        let mut others = Vec::new();
        for id in flat {
            match self.dst.get(id) {
                Shape::TestKind(k) => {
                    acc = Some(match acc {
                        None => *k,
                        Some(prev) => NodeKindSet {
                            iri: prev.iri && k.iri,
                            blank: prev.blank && k.blank,
                            literal: prev.literal && k.literal,
                        },
                    });
                }
                _ => others.push(id),
            }
        }
        if let Some(k) = acc {
            if k.is_empty() {
                others.push(self.bottom()); // empty intersection ⇒ ⊥
            } else if k.iri && k.blank && k.literal {
                // all kinds allowed ⇒ no constraint; drops from ∧ like ⊤
            } else {
                let id = self.cons(Shape::TestKind(k));
                others.push(id);
            }
        }
        others
    }

    /// Fuse conjoined counts over the same `(path, qualifier)`: the lower bounds
    /// take their max, the upper bounds their min (`∃≥a ∧ ∃≥b = ∃≥max`,
    /// `∃≤a ∧ ∃≤b = ∃≤min`, and a separate min/max count become one node).
    fn merge_counts(&mut self, flat: Vec<ShapeId>) -> Vec<ShapeId> {
        let mut keys: Vec<(Path, ShapeId)> = Vec::new();
        let mut bounds: Vec<(Option<u64>, Option<u64>)> = Vec::new();
        let mut index: HashMap<(Path, ShapeId), usize> = HashMap::new();
        let mut others = Vec::new();

        for id in flat {
            if let Shape::Count {
                path,
                min,
                max,
                qualifier,
            } = self.dst.get(id).clone()
            {
                let key = (path, qualifier);
                match index.get(&key) {
                    Some(&i) => {
                        bounds[i].0 = tighter_lower(bounds[i].0, min);
                        bounds[i].1 = tighter_upper(bounds[i].1, max);
                    }
                    None => {
                        index.insert(key.clone(), keys.len());
                        keys.push(key);
                        bounds.push((min, max));
                    }
                }
            } else {
                others.push(id);
            }
        }

        let mut result = others;
        for ((path, q), (min, max)) in keys.into_iter().zip(bounds) {
            let merged = self.mk_count(path, min, max, q);
            result.push(merged);
        }
        result
    }

    /// Fuse conjoined value-type facets (`test(τ)` siblings) into one tightened
    /// `test(τ₁ ∧ … ∧ τₙ)`, applying range/length bound-merging and same-family
    /// unsat ([`ValueType::normalize`]). An unsatisfiable combination becomes
    /// ⊥ (absorbed by the surrounding ∧); a vacuous one (`any`) drops out.
    fn merge_value_types(&mut self, flat: Vec<ShapeId>) -> Vec<ShapeId> {
        let mut facets: Vec<ValueType> = Vec::new();
        let mut others = Vec::new();
        for id in flat {
            match self.dst.get(id) {
                Shape::TestType(vt) => facets.push(vt.clone()),
                _ => others.push(id),
            }
        }
        if facets.is_empty() {
            return others;
        }
        match ValueType::and(facets).normalize() {
            None => others.push(self.bottom()), // unsat ⇒ ⊥ (mk_and's loop absorbs)
            Some(ValueType::Any) => {}          // vacuous ⇒ drops from ∧
            Some(v) => {
                let id = self.cons(Shape::TestType(v));
                others.push(id);
            }
        }
        others
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
        path: shifty_algebra::Path,
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
        // Empty-Alt path: Alt([]) matches no neighbors, so count is always 0
        if matches!(&path, Path::Alt(v) if v.is_empty()) {
            return if min.unwrap_or(0) >= 1 {
                self.bottom() // ∃≥1 ∅.φ = ⊥
            } else {
                self.top() // ∃[0..m] ∅.φ = ⊤
            };
        }
        // qualifier-⊥ collapse: no node ever satisfies ⊥, so count is always 0
        if self.is_bottom(q) {
            return if min.unwrap_or(0) >= 1 {
                self.bottom() // ∃≥1 π.⊥ = ⊥
            } else {
                self.top() // ∃[0..m] π.⊥ = ⊤
            };
        }
        // id-path collapse: id reaches exactly 1 node (the focus node itself)
        if path == Path::Id {
            let lo = min.unwrap_or(0);
            if lo >= 2 {
                return self.bottom(); // ∃≥2 id.φ = ⊥
            }
            return match (lo, max) {
                (0, Some(0)) => self.mk_not(q), // ∃[0..0] id.φ = ¬φ
                (1, _) => q,                    // ∃≥1 id.φ = φ
                _ => self.top(),                // ∃[0..≥1] id.φ = ⊤
            };
        }
        self.cons(Shape::Count {
            path,
            min,
            max,
            qualifier: q,
        })
    }

    fn selector(&mut self, sel: &Selector) -> Selector {
        match sel {
            Selector::HasPath(p, q) => {
                let path = normalize_path(p.clone());
                let shape = self.intern(*q);
                // HasPath(Pred(q), ⊤) ⇒ HasOut(q)
                // HasPath(Pred(q)⁻, ⊤) ⇒ HasIn(q)
                if self.is_top(shape) {
                    match &path {
                        Path::Pred(nn) => return Selector::HasOut(nn.clone()),
                        Path::Inverse(inner) => {
                            if let Path::Pred(nn) = inner.as_ref() {
                                return Selector::HasIn(nn.clone());
                            }
                        }
                        _ => {}
                    }
                }
                Selector::HasPath(path, shape)
            }
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
            RuleHead::Triple {
                subject,
                predicate,
                object,
            } => RuleHead::Triple {
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
    use shifty_algebra::{NodeKindSet, Path, Selector};

    fn schema_with(arena: ShapeArena, root: ShapeId) -> Schema {
        Schema {
            arena,
            statements: vec![Statement {
                selector: Selector::IsConst(shifty_algebra::Term::NamedNode(
                    shifty_algebra::NamedNode::new("http://ex/x").unwrap(),
                )),
                shape: root,
            }],
            rules: Vec::new(),
            names: Default::default(),
        }
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
        assert!(
            matches!(n.arena.get(rooted), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top))
        );
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
        assert!(
            matches!(n.arena.get(n.statements[0].shape), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top))
        );
    }

    #[test]
    fn nnf_pushes_negation_through_and() {
        // ¬(TestKind(IRI) ∧ Count(≥1 p.⊤)) → TestKind(Blank|Lit) ∨ Count(≤0 p.⊤)
        // Uses a TestKind + Count pair so merge_node_kinds can't short-circuit to ⊥.
        let mut a = ShapeArena::new();
        let p = Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap());
        let top = a.insert(Shape::Top);
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let cnt = a.insert(Shape::Count {
            path: p,
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let and = a.insert(Shape::And(vec![k, cnt]));
        let root = a.insert(Shape::Not(and));
        let n = normalize(&schema_with(a, root));
        match n.arena.get(n.statements[0].shape) {
            Shape::Or(cs) => {
                assert_eq!(cs.len(), 2);
                let kinds: Vec<_> = cs
                    .iter()
                    .map(|c| n.arena.get(*c))
                    .map(|s| matches!(s, Shape::TestKind(_) | Shape::Count { .. }))
                    .collect();
                assert!(
                    kinds.iter().all(|&b| b),
                    "expected Or of TestKind+Count, got something else"
                );
            }
            other => panic!("expected Or of two shapes, got {other:?}"),
        }
    }

    #[test]
    fn disjoint_node_kinds_in_and_is_unsat() {
        // TestKind(IRI) ∧ TestKind(Literal) = ⊥ (no term is both)
        let mut a = ShapeArena::new();
        let iri = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let lit = a.insert(Shape::TestKind(NodeKindSet::LITERAL));
        let root = a.insert(Shape::And(vec![iri, lit]));
        let n = normalize(&schema_with(a, root));
        assert!(
            matches!(n.arena.get(n.statements[0].shape), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top))
        );
    }

    #[test]
    fn nnf_flips_count_bound() {
        // ¬(∃≥2 p.⊤) → ∃≤1 p.⊤  (qualifier stays positive)
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let count = a.insert(Shape::Count {
            path: Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap()),
            min: Some(2),
            max: None,
            qualifier: top,
        });
        let root = a.insert(Shape::Not(count));
        let n = normalize(&schema_with(a, root));
        match n.arena.get(n.statements[0].shape) {
            Shape::Count { min, max, .. } => {
                assert_eq!((*min, *max), (None, Some(1)));
            }
            other => panic!("expected ∃≤1, got {other:?}"),
        }
    }

    #[test]
    fn merges_min_and_max_counts() {
        // (∃≥1 p.⊤) ∧ (∃≤1 p.⊤) → ∃[1..1] p.⊤
        let mut a = ShapeArena::new();
        let p = Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap());
        let t1 = a.insert(Shape::Top);
        let t2 = a.insert(Shape::Top);
        let lo = a.insert(Shape::Count {
            path: p.clone(),
            min: Some(1),
            max: None,
            qualifier: t1,
        });
        let hi = a.insert(Shape::Count {
            path: p,
            min: None,
            max: Some(1),
            qualifier: t2,
        });
        let root = a.insert(Shape::And(vec![lo, hi]));
        let n = normalize(&schema_with(a, root));
        match n.arena.get(n.statements[0].shape) {
            Shape::Count { min, max, .. } => assert_eq!((*min, *max), (Some(1), Some(1))),
            other => panic!("expected one fused ∃[1..1], got {other:?}"),
        }
    }

    #[test]
    fn merged_counts_can_be_unsat() {
        // (∃≥2 p.⊤) ∧ (∃≤1 p.⊤) → ⊥
        let mut a = ShapeArena::new();
        let p = Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap());
        let t1 = a.insert(Shape::Top);
        let t2 = a.insert(Shape::Top);
        let lo = a.insert(Shape::Count {
            path: p.clone(),
            min: Some(2),
            max: None,
            qualifier: t1,
        });
        let hi = a.insert(Shape::Count {
            path: p,
            min: None,
            max: Some(1),
            qualifier: t2,
        });
        let root = a.insert(Shape::And(vec![lo, hi]));
        let n = normalize(&schema_with(a, root));
        assert!(
            matches!(n.arena.get(n.statements[0].shape), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top))
        );
    }

    #[test]
    fn unsat_value_type_absorbs_conjunction() {
        // K(IRI) ∧ test([5,3])  →  ⊥   (the empty range folds to ⊥, absorbing ∧)
        use shifty_algebra::{Bound, Literal, NamedNode, ValueType};
        let int = |n: i64| {
            Literal::new_typed_literal(
                n.to_string(),
                NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
            )
        };
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let bad = a.insert(Shape::TestType(ValueType::NumericRange {
            lo: Some(Bound {
                value: int(5),
                inclusive: true,
            }),
            hi: Some(Bound {
                value: int(3),
                inclusive: true,
            }),
        }));
        let root = a.insert(Shape::And(vec![k, bad]));
        let n = normalize(&schema_with(a, root));
        assert!(matches!(n.arena.get(n.statements[0].shape),
            Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn conjoined_range_facets_merge() {
        // test(≥1) ∧ test(≤10)  →  single test([1,10])
        use shifty_algebra::{Bound, Literal, NamedNode, ValueType};
        let int = |n: i64| {
            Literal::new_typed_literal(
                n.to_string(),
                NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
            )
        };
        let mut a = ShapeArena::new();
        let lo = a.insert(Shape::TestType(ValueType::NumericRange {
            lo: Some(Bound {
                value: int(1),
                inclusive: true,
            }),
            hi: None,
        }));
        let hi = a.insert(Shape::TestType(ValueType::NumericRange {
            lo: None,
            hi: Some(Bound {
                value: int(10),
                inclusive: true,
            }),
        }));
        let root = a.insert(Shape::And(vec![lo, hi]));
        let n = normalize(&schema_with(a, root));
        match n.arena.get(n.statements[0].shape) {
            Shape::TestType(ValueType::NumericRange { lo, hi }) => {
                assert!(lo.is_some() && hi.is_some(), "expected fused [1,10]");
            }
            other => panic!("expected one fused range facet, got {other:?}"),
        }
    }

    #[test]
    fn negating_recursive_shape_terminates() {
        // T := ¬S where S := ∃≥1 p.S — must not loop; ¬ stays outside the cycle
        let mut a = ShapeArena::new();
        let s = a.reserve();
        a.set(
            s,
            Shape::Count {
                path: Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap()),
                min: Some(1),
                max: None,
                qualifier: s,
            },
        );
        let root = a.insert(Shape::Not(s));
        let n = normalize(&schema_with(a, root));
        assert!(matches!(n.arena.get(n.statements[0].shape), Shape::Not(_)));
    }

    #[test]
    fn qualifier_bottom_min1_is_bottom() {
        // ∃≥1 p.⊥ = ⊥
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let bot = a.insert(Shape::Not(top));
        let count = a.insert(Shape::Count {
            path: Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap()),
            min: Some(1),
            max: None,
            qualifier: bot,
        });
        let n = normalize(&schema_with(a, count));
        let r = n.statements[0].shape;
        assert!(matches!(n.arena.get(r), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn qualifier_bottom_max_bound_is_top() {
        // ∃[0..2] p.⊥ = ⊤
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let bot = a.insert(Shape::Not(top));
        let count = a.insert(Shape::Count {
            path: Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap()),
            min: None,
            max: Some(2),
            qualifier: bot,
        });
        let n = normalize(&schema_with(a, count));
        assert!(matches!(n.arena.get(n.statements[0].shape), Shape::Top));
    }

    #[test]
    fn id_path_min1_is_qualifier() {
        // ∃≥1 id.φ = φ
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let count = a.insert(Shape::Count {
            path: Path::Id,
            min: Some(1),
            max: None,
            qualifier: k,
        });
        let n = normalize(&schema_with(a, count));
        assert!(matches!(
            n.arena.get(n.statements[0].shape),
            Shape::TestKind(NodeKindSet::IRI)
        ));
    }

    #[test]
    fn id_path_max0_is_negation() {
        // ∃[0..0] id.φ = ¬φ; for TestKind(IRI) that becomes TestKind(Blank|Lit)
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let count = a.insert(Shape::Count {
            path: Path::Id,
            min: None,
            max: Some(0),
            qualifier: k,
        });
        let n = normalize(&schema_with(a, count));
        match n.arena.get(n.statements[0].shape) {
            Shape::TestKind(nk) => {
                assert_eq!(*nk, NodeKindSet::BLANK_NODE_OR_LITERAL);
            }
            other => panic!("expected TestKind(Blank|Lit), got {other:?}"),
        }
    }

    #[test]
    fn id_path_min2_is_bottom() {
        // ∃≥2 id.φ = ⊥
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let count = a.insert(Shape::Count {
            path: Path::Id,
            min: Some(2),
            max: None,
            qualifier: k,
        });
        let n = normalize(&schema_with(a, count));
        let r = n.statements[0].shape;
        assert!(matches!(n.arena.get(r), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn converse_pushdown_through_seq() {
        // ∃≥1 (p·q)⁻.⊤ → path normalized to q⁻·p⁻
        let p = shifty_algebra::NamedNode::new("http://ex/p").unwrap();
        let q = shifty_algebra::NamedNode::new("http://ex/q").unwrap();
        let inv_seq = Path::Inverse(Box::new(Path::Seq(vec![
            Path::Pred(p.clone()),
            Path::Pred(q.clone()),
        ])));
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let count = a.insert(Shape::Count {
            path: inv_seq,
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let n = normalize(&schema_with(a, count));
        match n.arena.get(n.statements[0].shape) {
            Shape::Count { path, .. } => {
                let expected = Path::Seq(vec![
                    Path::Inverse(Box::new(Path::Pred(q))),
                    Path::Inverse(Box::new(Path::Pred(p))),
                ]);
                assert_eq!(*path, expected, "expected (p·q)⁻ normalized to q⁻·p⁻");
            }
            other => panic!("expected Count with normalized path, got {other:?}"),
        }
    }

    #[test]
    fn converse_pushdown_through_alt_and_star() {
        // (p|q)⁻ = p⁻|q⁻;  (p*)⁻ = (p⁻)*
        let p = shifty_algebra::NamedNode::new("http://ex/p").unwrap();
        let q = shifty_algebra::NamedNode::new("http://ex/q").unwrap();
        let alt = Path::Alt(vec![Path::Pred(p.clone()), Path::Pred(q.clone())]);
        assert_eq!(
            normalize_path(Path::Inverse(Box::new(alt))),
            Path::Alt(vec![
                Path::Inverse(Box::new(Path::Pred(p.clone()))),
                Path::Inverse(Box::new(Path::Pred(q))),
            ])
        );
        let star_inv = Path::Inverse(Box::new(Path::Star(Box::new(Path::Pred(p.clone())))));
        assert_eq!(
            normalize_path(star_inv),
            Path::Star(Box::new(Path::Inverse(Box::new(Path::Pred(p)))))
        );
    }

    #[test]
    fn recursive_shape_survives_normalization() {
        // S := ∃≥1 p . S
        let mut a = ShapeArena::new();
        let s = a.reserve();
        a.set(
            s,
            Shape::Count {
                path: Path::Pred(shifty_algebra::NamedNode::new("http://ex/p").unwrap()),
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

    #[test]
    fn negkind_iri_becomes_blank_or_literal() {
        // ¬TestKind(IRI) = TestKind(Blank|Literal)
        let mut a = ShapeArena::new();
        let iri = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let root = a.insert(Shape::Not(iri));
        let n = normalize(&schema_with(a, root));
        assert!(matches!(
            n.arena.get(n.statements[0].shape),
            Shape::TestKind(NodeKindSet::BLANK_NODE_OR_LITERAL)
        ));
    }

    #[test]
    fn empty_alt_path_min1_is_bottom() {
        // ∃≥1 Alt([]).φ = ⊥
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let count = a.insert(Shape::Count {
            path: Path::Alt(vec![]),
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let n = normalize(&schema_with(a, count));
        let r = n.statements[0].shape;
        assert!(matches!(n.arena.get(r), Shape::Not(x) if matches!(n.arena.get(*x), Shape::Top)));
    }

    #[test]
    fn empty_alt_path_max_bound_is_top() {
        // ∃[0..3] Alt([]).φ = ⊤
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let count = a.insert(Shape::Count {
            path: Path::Alt(vec![]),
            min: None,
            max: Some(3),
            qualifier: top,
        });
        let n = normalize(&schema_with(a, count));
        assert!(matches!(n.arena.get(n.statements[0].shape), Shape::Top));
    }

    #[test]
    fn star_drops_id_from_alt() {
        // (p∪id)* = p*
        let p = shifty_algebra::NamedNode::new("http://ex/p").unwrap();
        let alt_id = Path::Alt(vec![Path::Pred(p.clone()), Path::Id]);
        let star = Path::Star(Box::new(alt_id));
        assert_eq!(normalize_path(star), Path::Star(Box::new(Path::Pred(p))));
    }

    #[test]
    fn seq_merges_adjacent_equal_stars() {
        // p*·p* = p*
        let p = shifty_algebra::NamedNode::new("http://ex/p").unwrap();
        let star = Path::Star(Box::new(Path::Pred(p.clone())));
        let seq = Path::Seq(vec![star.clone(), star]);
        assert_eq!(normalize_path(seq), Path::Star(Box::new(Path::Pred(p))));
    }

    #[test]
    fn alt_deduplicates_paths() {
        // Alt([p, p, q]) = Alt([p, q])
        let p = shifty_algebra::NamedNode::new("http://ex/p").unwrap();
        let q = shifty_algebra::NamedNode::new("http://ex/q").unwrap();
        let dup = Path::Alt(vec![
            Path::Pred(p.clone()),
            Path::Pred(p.clone()),
            Path::Pred(q.clone()),
        ]);
        let result = normalize_path(dup);
        assert_eq!(result, Path::Alt(vec![Path::Pred(p), Path::Pred(q)]));
    }

    #[test]
    fn selector_haspath_pred_top_becomes_hasout() {
        // HasPath(Pred(q), ⊤) ⇒ HasOut(q)
        let q = shifty_algebra::NamedNode::new("http://ex/q").unwrap();
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let schema = Schema {
            arena: a,
            statements: vec![Statement {
                selector: Selector::HasPath(Path::Pred(q.clone()), top),
                shape: top,
            }],
            rules: vec![],
            names: Default::default(),
        };
        let n = normalize(&schema);
        assert_eq!(n.statements[0].selector, Selector::HasOut(q));
    }

    #[test]
    fn selector_haspath_inv_pred_top_becomes_hasin() {
        // HasPath(Pred(q)⁻, ⊤) ⇒ HasIn(q)
        let q = shifty_algebra::NamedNode::new("http://ex/q").unwrap();
        let mut a = ShapeArena::new();
        let top = a.insert(Shape::Top);
        let schema = Schema {
            arena: a,
            statements: vec![Statement {
                selector: Selector::HasPath(Path::Inverse(Box::new(Path::Pred(q.clone()))), top),
                shape: top,
            }],
            rules: vec![],
            names: Default::default(),
        };
        let n = normalize(&schema);
        assert_eq!(n.statements[0].selector, Selector::HasIn(q));
    }

    #[test]
    fn statement_dedup_removes_identical() {
        let mut a = ShapeArena::new();
        let k = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let node =
            shifty_algebra::Term::NamedNode(shifty_algebra::NamedNode::new("http://ex/x").unwrap());
        let sel = Selector::IsConst(node);
        let schema = Schema {
            arena: a,
            statements: vec![
                Statement {
                    selector: sel.clone(),
                    shape: k,
                },
                Statement {
                    selector: sel.clone(),
                    shape: k,
                },
            ],
            rules: vec![],
            names: Default::default(),
        };
        let n = normalize(&schema);
        assert_eq!(n.statements.len(), 1);
    }
}
