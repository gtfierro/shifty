//! Relational evaluation of the path algebra `⟦π⟧^G` (doc 00 §2, Table 1).
//!
//! Two mutually-recursive functions implement the relation by following it
//! forward ([`succ`]) or backward ([`pred`]); `Inverse` simply swaps between
//! them. This is the naive reference evaluation — correctness over speed.

use oxrdf::{Graph, NamedOrBlankNode, Term};
use shacl_algebra::Path;
use std::collections::HashSet;

/// `{ u | (node, u) ∈ ⟦path⟧ }` — nodes reachable forward from `node`.
pub fn succ(g: &Graph, node: &Term, path: &Path) -> HashSet<Term> {
    match path {
        Path::Id => once(node.clone()),
        Path::Pred(q) => match node_of(node) {
            Some(s) => g
                .objects_for_subject_predicate(&s, q.as_ref())
                .map(|t| t.into_owned())
                .collect(),
            None => HashSet::new(),
        },
        Path::Inverse(p) => pred(g, node, p),
        Path::Seq(ps) => fold_seq(g, node, ps, succ),
        Path::Alt(ps) => ps.iter().flat_map(|p| succ(g, node, p)).collect(),
        Path::Star(p) => closure(g, node, |g, n| succ(g, n, p)),
    }
}

/// `{ u | (u, node) ∈ ⟦path⟧ }` — nodes that reach `node`.
pub fn pred(g: &Graph, node: &Term, path: &Path) -> HashSet<Term> {
    match path {
        Path::Id => once(node.clone()),
        Path::Pred(q) => g
            .subjects_for_predicate_object(q.as_ref(), node)
            .map(|s| term_of(s.into_owned()))
            .collect(),
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

fn fold_seq(
    g: &Graph,
    node: &Term,
    ps: &[Path],
    step: fn(&Graph, &Term, &Path) -> HashSet<Term>,
) -> HashSet<Term> {
    let mut cursor = once(node.clone());
    for p in ps {
        cursor = cursor.iter().flat_map(|x| step(g, x, p)).collect();
    }
    cursor
}

/// Reflexive-transitive closure of a one-step function (for `π*`).
fn closure<F>(g: &Graph, node: &Term, step: F) -> HashSet<Term>
where
    F: Fn(&Graph, &Term) -> HashSet<Term>,
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
