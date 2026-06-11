//! Relational evaluation of the path algebra `⟦π⟧^G` (doc 00 §2, Table 1).
//!
//! Two mutually-recursive functions implement the relation by following it
//! forward ([`succ`]) or backward ([`pred`]); `Inverse` simply swaps between
//! them. This is the naive reference evaluation — correctness over speed.
//!
//! Both are generic over a [`PathBackend`]: the leaf `Path::Pred` arms dispatch
//! through the trait's two adjacency lookups, so the same evaluator runs over a
//! linear `oxrdf::Graph` (inference's growing graph) or the dictionary-encoded
//! [`FrozenIndexedDataset`] (the report and algebra validation paths). Every
//! other arm (`Id`/`Seq`/`Alt`/`Star`/`Inverse`) is pure combinator logic and is
//! backend-agnostic.

use crate::frozen::{FrozenIndexedDataset, GraphSel};
use oxrdf::{Graph, NamedNode, NamedOrBlankNode, Term};
use shifty_algebra::Path;
use std::collections::{BTreeSet, HashSet};

/// Storage backend for path evaluation. Exposes the three adjacency primitives
/// `succ`/`pred`/`sh:closed` need, so path evaluation can dispatch to either a
/// linear `oxrdf::Graph` or the indexed [`FrozenIndexedDataset`].
pub trait PathBackend {
    /// `{ o | (subject, predicate, o) ∈ G }` — forward step.
    fn objects(&self, subject: &Term, predicate: &NamedNode) -> HashSet<Term>;
    /// `{ s | (s, predicate, object) ∈ G }` — backward step.
    fn subjects(&self, predicate: &NamedNode, object: &Term) -> HashSet<Term>;
    /// Predicates of the outgoing triples of `subject` (used by `sh:closed`).
    fn out_predicates(&self, subject: &Term) -> BTreeSet<NamedNode>;
}

/// `{ u | (node, u) ∈ ⟦path⟧ }` — nodes reachable forward from `node`.
pub fn succ<B: PathBackend + ?Sized>(g: &B, node: &Term, path: &Path) -> HashSet<Term> {
    match path {
        Path::Id => once(node.clone()),
        Path::Pred(q) => g.objects(node, q),
        Path::Inverse(p) => pred(g, node, p),
        Path::Seq(ps) => fold_seq(g, node, ps, succ),
        Path::Alt(ps) => ps.iter().flat_map(|p| succ(g, node, p)).collect(),
        Path::Star(p) => closure(g, node, |g, n| succ(g, n, p)),
    }
}

/// `{ u | (u, node) ∈ ⟦path⟧ }` — nodes that reach `node`.
pub fn pred<B: PathBackend + ?Sized>(g: &B, node: &Term, path: &Path) -> HashSet<Term> {
    match path {
        Path::Id => once(node.clone()),
        Path::Pred(q) => g.subjects(q, node),
        Path::Inverse(p) => succ(g, node, p),
        // (u, node) ∈ ⟦p1·…·pk⟧ : peel predicates from the right.
        Path::Seq(ps) => {
            let mut cursor = once(node.clone());
            for p in ps.iter().rev() {
                cursor = cursor.iter().flat_map(|x| pred(g, x, p)).collect();
            }
            cursor
        }
        Path::Alt(ps) => ps.iter().flat_map(|p| pred(g, node, p)).collect(),
        Path::Star(p) => closure(g, node, |g, n| pred(g, n, p)),
    }
}

fn fold_seq<B: PathBackend + ?Sized>(
    g: &B,
    node: &Term,
    ps: &[Path],
    step: fn(&B, &Term, &Path) -> HashSet<Term>,
) -> HashSet<Term> {
    let mut cursor = once(node.clone());
    for p in ps {
        cursor = cursor.iter().flat_map(|x| step(g, x, p)).collect();
    }
    cursor
}

/// Reflexive-transitive closure of a one-step function (for `π*`).
fn closure<B, F>(g: &B, node: &Term, step: F) -> HashSet<Term>
where
    B: PathBackend + ?Sized,
    F: Fn(&B, &Term) -> HashSet<Term>,
{
    let mut result = once(node.clone());
    let mut frontier = vec![node.clone()];
    while let Some(x) = frontier.pop() {
        for y in step(g, &x) {
            if result.insert(y.clone()) {
                frontier.push(y);
            }
        }
    }
    result
}

fn once(t: Term) -> HashSet<Term> {
    let mut s = HashSet::with_capacity(1);
    s.insert(t);
    s
}

// ── Backends ─────────────────────────────────────────────────────────────────

/// Linear backend over oxrdf's B-tree indexes. Used by inference, whose graph
/// grows during forward chaining and so cannot share an immutable snapshot.
impl PathBackend for Graph {
    fn objects(&self, subject: &Term, predicate: &NamedNode) -> HashSet<Term> {
        match node_of(subject) {
            Some(s) => self
                .objects_for_subject_predicate(&s, predicate.as_ref())
                .map(|t| t.into_owned())
                .collect(),
            None => HashSet::new(),
        }
    }

    fn subjects(&self, predicate: &NamedNode, object: &Term) -> HashSet<Term> {
        self.subjects_for_predicate_object(predicate.as_ref(), object)
            .map(|s| term_of(s.into_owned()))
            .collect()
    }

    fn out_predicates(&self, subject: &Term) -> BTreeSet<NamedNode> {
        let mut preds = BTreeSet::new();
        if let Some(n) = node_of(subject) {
            for t in self.triples_for_subject(&n) {
                preds.insert(t.predicate.into_owned());
            }
        }
        preds
    }
}

/// Indexed backend over the dictionary-encoded post-inference snapshot. Built
/// once at the inference→validation boundary and shared across focus nodes; the
/// `u32`-keyed sorted indexes replace per-call term hashing and B-tree walks.
/// Unknown terms intern to fresh ids that match no stored triple — exactly the
/// empty-result semantics path evaluation needs.
impl PathBackend for FrozenIndexedDataset {
    fn objects(&self, subject: &Term, predicate: &NamedNode) -> HashSet<Term> {
        let s = self.intern(subject);
        let p = self.intern(&Term::NamedNode(predicate.clone()));
        self.scan(Some(s), Some(p), None, GraphSel::Default)
            .map(|t| self.externalize_id(t[2]))
            .collect()
    }

    fn subjects(&self, predicate: &NamedNode, object: &Term) -> HashSet<Term> {
        let p = self.intern(&Term::NamedNode(predicate.clone()));
        let o = self.intern(object);
        self.scan(None, Some(p), Some(o), GraphSel::Default)
            .map(|t| self.externalize_id(t[0]))
            .collect()
    }

    fn out_predicates(&self, subject: &Term) -> BTreeSet<NamedNode> {
        let s = self.intern(subject);
        self.scan(Some(s), None, None, GraphSel::Default)
            .filter_map(|t| match self.externalize_id(t[1]) {
                Term::NamedNode(p) => Some(p),
                _ => None,
            })
            .collect()
    }
}

/// A term usable in subject position, if it is not a literal.
pub fn node_of(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(n) => Some(NamedOrBlankNode::NamedNode(n.clone())),
        Term::BlankNode(b) => Some(NamedOrBlankNode::BlankNode(b.clone())),
        Term::Literal(_) => None,
    }
}

pub fn term_of(node: NamedOrBlankNode) -> Term {
    match node {
        NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n),
        NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{NamedNode, Triple};

    fn nn(iri: &str) -> NamedNode {
        NamedNode::new(iri).unwrap()
    }

    fn t(s: &str, p: &str, o: &str) -> Triple {
        Triple::new(nn(s), nn(p), nn(o))
    }

    fn term(iri: &str) -> Term {
        Term::NamedNode(nn(iri))
    }

    /// `a -p-> b -p-> c -q-> d`, with a side branch `a -p-> e`.
    fn sample_graph() -> Graph {
        let mut g = Graph::new();
        for tr in [
            t("http://ex/a", "http://ex/p", "http://ex/b"),
            t("http://ex/a", "http://ex/p", "http://ex/e"),
            t("http://ex/b", "http://ex/p", "http://ex/c"),
            t("http://ex/c", "http://ex/q", "http://ex/d"),
        ] {
            g.insert(tr.as_ref());
        }
        g
    }

    fn p(iri: &str) -> Path {
        Path::Pred(nn(iri))
    }

    /// The path shapes exercised by the Graph-vs-Frozen differential below.
    fn sample_paths() -> Vec<Path> {
        let pp = || p("http://ex/p");
        let qq = || p("http://ex/q");
        vec![
            Path::Id,
            pp(),
            Path::Inverse(Box::new(pp())),
            Path::Seq(vec![pp(), pp()]),
            Path::Seq(vec![pp(), qq()]),
            Path::Alt(vec![pp(), qq()]),
            Path::Star(Box::new(pp())),
            Path::Seq(vec![Path::Star(Box::new(pp())), qq()]),
            Path::Inverse(Box::new(Path::Seq(vec![pp(), pp()]))),
        ]
    }

    #[test]
    fn graph_and_frozen_backends_agree() {
        let g = sample_graph();
        let frozen = FrozenIndexedDataset::from_graph(&g);
        let nodes = ["http://ex/a", "http://ex/b", "http://ex/c", "http://ex/d"];
        for path in sample_paths() {
            for node in nodes {
                let n = term(node);
                assert_eq!(
                    succ(&g, &n, &path),
                    succ(&frozen, &n, &path),
                    "succ mismatch at {node} for {path:?}"
                );
                assert_eq!(
                    pred(&g, &n, &path),
                    pred(&frozen, &n, &path),
                    "pred mismatch at {node} for {path:?}"
                );
            }
        }
    }

    #[test]
    fn frozen_succ_pred_basics() {
        let g = sample_graph();
        let frozen = FrozenIndexedDataset::from_graph(&g);
        let a = term("http://ex/a");
        assert_eq!(
            succ(&frozen, &a, &p("http://ex/p")),
            HashSet::from([term("http://ex/b"), term("http://ex/e")])
        );
        let c = term("http://ex/c");
        assert_eq!(
            pred(&frozen, &c, &p("http://ex/p")),
            HashSet::from([term("http://ex/b")])
        );
    }

    #[test]
    fn star_is_reflexive_transitive_on_both_backends() {
        let g = sample_graph();
        let frozen = FrozenIndexedDataset::from_graph(&g);
        let a = term("http://ex/a");
        let star = Path::Star(Box::new(p("http://ex/p")));
        let expected = HashSet::from([
            term("http://ex/a"),
            term("http://ex/b"),
            term("http://ex/e"),
            term("http://ex/c"),
        ]);
        assert_eq!(succ(&g, &a, &star), expected);
        assert_eq!(succ(&frozen, &a, &star), expected);
    }

    #[test]
    fn out_predicates_agree() {
        let g = sample_graph();
        let frozen = FrozenIndexedDataset::from_graph(&g);
        let a = term("http://ex/a");
        assert_eq!(g.out_predicates(&a), BTreeSet::from([nn("http://ex/p")]));
        assert_eq!(
            frozen.out_predicates(&a),
            BTreeSet::from([nn("http://ex/p")])
        );
        // Literal subject → no outgoing predicates on either backend.
        let lit = Term::Literal(oxrdf::Literal::new_simple_literal("x"));
        assert!(g.out_predicates(&lit).is_empty());
        assert!(frozen.out_predicates(&lit).is_empty());
    }
}
