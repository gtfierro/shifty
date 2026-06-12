//! Compiled, index-aware reachability plans for the path algebra.
//!
//! [`compile_fwd`] / [`compile_bwd`] translate a [`shifty_algebra::Path`] into a
//! [`ReachStep`] tree once, at executor-creation time. Two things happen during
//! compilation that would otherwise recur per seed node at eval time:
//!
//! 1. **Predicate interning** — each `Path::Pred(q)` is resolved to a `TermId`
//!    via `ds.intern(…)`, eliminating one hash-map lookup per seed per step.
//! 2. **Direction folding** — `Path::Inverse` is absorbed into the step type
//!    (`FwdPred` vs `BwdPred`) and `Path::Seq` elements are reversed for
//!    backward traversal, so the evaluator never re-dispatches on direction.
//!
//! [`eval_reach`] then walks the compiled tree with direct index calls:
//! - `FwdPred(q)` → SPO range `scan(node, q, _)` → objects
//! - `BwdPred(q)` → POS range `scan(_, q, node)` → subjects
//!
//! [`star_closure`] and [`plus_closure`] provide the BFS loops used both by
//! `eval_reach` for nested `Path::Star` and by the executor for the top-level
//! `ClosureKind`.

use crate::frozen::{FrozenIndexedDataset, GraphSel, TermId};
use oxrdf::Term;
use shifty_algebra::Path;
use shifty_opt::ClosureKind;
use std::collections::HashSet;
use std::rc::Rc;

/// A compiled, direction-resolved reachability step. `Inverse` never appears
/// here — it is folded into `FwdPred` ↔ `BwdPred` during compilation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum ReachStep {
    Id,
    /// `scan(node, q, _)` → objects — uses SPO range.
    FwdPred(TermId),
    /// `scan(_, q, node)` → subjects — uses POS range.
    BwdPred(TermId),
    Seq(Vec<ReachStep>),
    Alt(Vec<ReachStep>),
    /// Nested reflexive-transitive closure: handles `π*` inside a step path.
    Star(Box<ReachStep>),
}

/// Compile `path` for forward traversal (subject→object direction).
pub(crate) fn compile_fwd(path: &Path, ds: &FrozenIndexedDataset) -> ReachStep {
    match path {
        Path::Id => ReachStep::Id,
        Path::Pred(q) => ReachStep::FwdPred(ds.intern(&Term::NamedNode(q.clone()))),
        // Inverse flips direction: compile the inner path backward instead.
        Path::Inverse(p) => compile_bwd(p, ds),
        Path::Seq(ps) => ReachStep::Seq(ps.iter().map(|p| compile_fwd(p, ds)).collect()),
        Path::Alt(ps) => ReachStep::Alt(ps.iter().map(|p| compile_fwd(p, ds)).collect()),
        Path::Star(p) => ReachStep::Star(Box::new(compile_fwd(p, ds))),
    }
}

/// Compile `path` for backward traversal (object→subject direction).
///
/// Key difference from [`compile_fwd`]: `Seq` steps are reversed so the
/// evaluator can fold forward over the reversed list without per-call logic.
pub(crate) fn compile_bwd(path: &Path, ds: &FrozenIndexedDataset) -> ReachStep {
    match path {
        Path::Id => ReachStep::Id,
        Path::Pred(q) => ReachStep::BwdPred(ds.intern(&Term::NamedNode(q.clone()))),
        // Inverse of inverse cancels out.
        Path::Inverse(p) => compile_fwd(p, ds),
        // (u, node) ∈ ⟦p0·p1·…⟧ : peel predicates from the right.
        Path::Seq(ps) => ReachStep::Seq(ps.iter().rev().map(|p| compile_bwd(p, ds)).collect()),
        Path::Alt(ps) => ReachStep::Alt(ps.iter().map(|p| compile_bwd(p, ds)).collect()),
        Path::Star(p) => ReachStep::Star(Box::new(compile_bwd(p, ds))),
    }
}

/// Evaluate a compiled step from `node`, returning the set of reachable nodes.
pub(crate) fn eval_reach(
    step: &ReachStep,
    node: TermId,
    ds: &FrozenIndexedDataset,
    g: GraphSel,
) -> HashSet<TermId> {
    match step {
        ReachStep::Id => HashSet::from([node]),
        ReachStep::FwdPred(q) => ds
            .scan(Some(node), Some(*q), None, g)
            .map(|t| t[2])
            .collect(),
        ReachStep::BwdPred(q) => ds
            .scan(None, Some(*q), Some(node), g)
            .map(|t| t[0])
            .collect(),
        ReachStep::Seq(steps) => {
            let mut cursor: HashSet<TermId> = HashSet::from([node]);
            for s in steps {
                cursor = cursor
                    .iter()
                    .flat_map(|&x| eval_reach(s, x, ds, g))
                    .collect();
            }
            cursor
        }
        ReachStep::Alt(steps) => steps
            .iter()
            .flat_map(|s| eval_reach(s, node, ds, g))
            .collect(),
        ReachStep::Star(inner) => star_closure(node, inner, ds, g),
    }
}

/// Reflexive-transitive closure of `step` starting from `node` (`p*`).
pub(crate) fn star_closure(
    node: TermId,
    step: &ReachStep,
    ds: &FrozenIndexedDataset,
    g: GraphSel,
) -> HashSet<TermId> {
    let mut result: HashSet<TermId> = HashSet::from([node]);
    let mut frontier = vec![node];
    while let Some(x) = frontier.pop() {
        for y in eval_reach(step, x, ds, g) {
            if result.insert(y) {
                frontier.push(y);
            }
        }
    }
    result
}

/// Transitive closure of `step` starting from `node` (`p+`): one or more steps,
/// excluding the start node unless a cycle returns to it.
pub(crate) fn plus_closure(
    node: TermId,
    step: &ReachStep,
    ds: &FrozenIndexedDataset,
    g: GraphSel,
) -> HashSet<TermId> {
    let mut result = HashSet::new();
    let mut frontier: Vec<TermId> = eval_reach(step, node, ds, g).into_iter().collect();
    result.extend(frontier.iter().copied());
    while let Some(x) = frontier.pop() {
        for y in eval_reach(step, x, ds, g) {
            if result.insert(y) {
                frontier.push(y);
            }
        }
    }
    result
}

/// Apply the top-level `ClosureKind` repetition to a compiled `step` from `node`.
/// Direction (fwd vs bwd) is already encoded in `step`; callers pass whichever
/// compiled plan is appropriate.
pub(crate) fn apply_closure(
    node: TermId,
    step: &ReachStep,
    kind: ClosureKind,
    ds: &FrozenIndexedDataset,
    g: GraphSel,
) -> Rc<HashSet<TermId>> {
    if let Some(cached) = ds.cached_reach(node, step, kind, g) {
        return cached;
    }
    let result = Rc::new(match kind {
        ClosureKind::Star => star_closure(node, step, ds, g),
        ClosureKind::Plus => plus_closure(node, step, ds, g),
        ClosureKind::Opt => {
            let mut r = eval_reach(step, node, ds, g);
            r.insert(node);
            r
        }
    });
    ds.cache_reach(node, step, kind, g, result.clone());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frozen::FrozenIndexedDataset;
    use oxrdf::{Graph, NamedNode, Triple};
    use shifty_algebra::Path as AlgPath;

    fn nn(iri: &str) -> NamedNode {
        NamedNode::new(iri).unwrap()
    }

    fn t(s: &str, p: &str, o: &str) -> Triple {
        Triple::new(nn(s), nn(p), nn(o))
    }

    fn sample_ds() -> FrozenIndexedDataset {
        let mut g = Graph::new();
        for tr in [
            t("http://ex/a", "http://ex/p", "http://ex/b"),
            t("http://ex/b", "http://ex/p", "http://ex/c"),
            t("http://ex/c", "http://ex/q", "http://ex/d"),
        ] {
            g.insert(tr.as_ref());
        }
        FrozenIndexedDataset::from_graph(&g)
    }

    fn id(iri: &str, ds: &FrozenIndexedDataset) -> TermId {
        ds.intern(&Term::NamedNode(nn(iri)))
    }

    #[test]
    fn inverse_folds_to_bwd() {
        let ds = sample_ds();
        let inv = AlgPath::Inverse(Box::new(AlgPath::Pred(nn("http://ex/p"))));
        assert!(matches!(compile_fwd(&inv, &ds), ReachStep::BwdPred(_)));
    }

    #[test]
    fn double_inverse_cancels() {
        let ds = sample_ds();
        let p = AlgPath::Pred(nn("http://ex/p"));
        let inv_inv = AlgPath::Inverse(Box::new(AlgPath::Inverse(Box::new(p))));
        assert!(matches!(compile_fwd(&inv_inv, &ds), ReachStep::FwdPred(_)));
    }

    #[test]
    fn seq_bwd_reverses_and_flips() {
        let ds = sample_ds();
        let p = AlgPath::Pred(nn("http://ex/p"));
        let q = AlgPath::Pred(nn("http://ex/q"));
        let seq = AlgPath::Seq(vec![p, q]);
        let p_id = id("http://ex/p", &ds);
        let q_id = id("http://ex/q", &ds);
        if let ReachStep::Seq(steps) = compile_bwd(&seq, &ds) {
            assert_eq!(steps.len(), 2);
            assert!(matches!(steps[0], ReachStep::BwdPred(i) if i == q_id));
            assert!(matches!(steps[1], ReachStep::BwdPred(i) if i == p_id));
        } else {
            panic!("expected Seq");
        }
    }

    #[test]
    fn eval_reach_fwd_pred_returns_objects() {
        let ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let a = id("http://ex/a", &ds);
        let b = id("http://ex/b", &ds);
        let result = eval_reach(&ReachStep::FwdPred(p_id), a, &ds, GraphSel::Default);
        assert_eq!(result, HashSet::from([b]));
    }

    #[test]
    fn eval_reach_bwd_pred_returns_subjects() {
        let ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let b = id("http://ex/b", &ds);
        let a = id("http://ex/a", &ds);
        let result = eval_reach(&ReachStep::BwdPred(p_id), b, &ds, GraphSel::Default);
        assert_eq!(result, HashSet::from([a]));
    }

    #[test]
    fn star_closure_is_reflexive_transitive() {
        let ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let a = id("http://ex/a", &ds);
        let b = id("http://ex/b", &ds);
        let c = id("http://ex/c", &ds);
        let result = star_closure(a, &ReachStep::FwdPred(p_id), &ds, GraphSel::Default);
        assert!(result.contains(&a)); // reflexive
        assert!(result.contains(&b));
        assert!(result.contains(&c));
    }

    #[test]
    fn seq_bwd_with_nested_star() {
        // compile_bwd(Seq([Star(p), q])) should yield Seq([bwd(q), Star(bwd(p))])
        // i.e. reversed order and each element flipped to backward.
        let ds = sample_ds();
        let p = AlgPath::Pred(nn("http://ex/p"));
        let q = AlgPath::Pred(nn("http://ex/q"));
        let seq = AlgPath::Seq(vec![AlgPath::Star(Box::new(p)), q]);
        let p_id = id("http://ex/p", &ds);
        let q_id = id("http://ex/q", &ds);
        if let ReachStep::Seq(steps) = compile_bwd(&seq, &ds) {
            assert_eq!(steps.len(), 2);
            // first compiled step = bwd(q)
            assert!(matches!(steps[0], ReachStep::BwdPred(i) if i == q_id));
            // second compiled step = Star(bwd(p))
            if let ReachStep::Star(inner) = &steps[1] {
                assert!(matches!(inner.as_ref(), ReachStep::BwdPred(i) if *i == p_id));
            } else {
                panic!("expected Star as second element");
            }
        } else {
            panic!("expected Seq");
        }
    }

    #[test]
    fn plus_closure_excludes_start_unless_cyclic() {
        let ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let a = id("http://ex/a", &ds);
        let b = id("http://ex/b", &ds);
        let c = id("http://ex/c", &ds);
        let result = plus_closure(a, &ReachStep::FwdPred(p_id), &ds, GraphSel::Default);
        assert!(!result.contains(&a)); // not reflexive
        assert!(result.contains(&b));
        assert!(result.contains(&c));
    }

    #[test]
    fn apply_closure_reuses_cached_result() {
        let ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let a = id("http://ex/a", &ds);
        let step = ReachStep::FwdPred(p_id);

        let first = apply_closure(a, &step, ClosureKind::Star, &ds, GraphSel::Default);
        let second = apply_closure(a, &step, ClosureKind::Star, &ds, GraphSel::Default);

        assert!(Rc::ptr_eq(&first, &second));
    }

    #[test]
    fn extending_dataset_invalidates_cached_closure() {
        let mut ds = sample_ds();
        let p_id = id("http://ex/p", &ds);
        let a = id("http://ex/a", &ds);
        let c = id("http://ex/c", &ds);
        let step = ReachStep::FwdPred(p_id);
        let before = apply_closure(a, &step, ClosureKind::Plus, &ds, GraphSel::Default);
        assert!(before.contains(&c));

        let d = nn("http://ex/d");
        let added = Triple::new(nn("http://ex/c"), nn("http://ex/p"), d);
        ds.extend_triples([&added]);

        let d_id = id("http://ex/d", &ds);
        let after = apply_closure(a, &step, ClosureKind::Plus, &ds, GraphSel::Default);
        assert!(after.contains(&d_id));
        assert!(!Rc::ptr_eq(&before, &after));
    }
}
