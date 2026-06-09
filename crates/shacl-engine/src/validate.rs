//! Reference shape satisfaction `G, v ⊨ φ` and schema validation `G ⊨ S`
//! (doc 00 §3–§4, Table 2). This is the conformance *oracle*: the optimized
//! engines in later layers must agree with it.

use crate::path::{node_of, succ};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{Graph, Term};
use serde::{Deserialize, Serialize};
use shacl_algebra::{Path, Schema, Selector, Shape, ShapeArena, ShapeId};
use std::cmp::Ordering;
use std::collections::HashSet;

/// One focus node that failed its statement's shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultEntry {
    pub focus: Term,
    /// Index of the violated `(selector, shape)` statement in the schema.
    pub statement: usize,
}

/// The outcome of validating a data graph against a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationOutcome {
    pub conforms: bool,
    pub results: Vec<ResultEntry>,
}

/// Validate `data` against `schema`.
pub fn validate(data: &Graph, schema: &Schema) -> ValidationOutcome {
    let mut results = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        for v in focus_nodes(data, &st.selector, &schema.arena) {
            let mut stack = HashSet::new();
            if !holds(data, &schema.arena, &v, st.shape, &mut stack) {
                results.push(ResultEntry { focus: v, statement: i });
            }
        }
    }
    ValidationOutcome { conforms: results.is_empty(), results }
}

/// The focus nodes selected by a selector.
pub fn focus_nodes(data: &Graph, sel: &Selector, arena: &ShapeArena) -> Vec<Term> {
    match sel {
        Selector::HasOut(q) => {
            let mut seen = HashSet::new();
            data.triples_for_predicate(q.as_ref())
                .filter_map(|t| {
                    let term = subject_term(t.subject);
                    seen.insert(term.clone()).then_some(term)
                })
                .collect()
        }
        Selector::HasIn(q) => {
            let mut seen = HashSet::new();
            data.triples_for_predicate(q.as_ref())
                .filter_map(|t| {
                    let term = t.object.into_owned();
                    seen.insert(term.clone()).then_some(term)
                })
                .collect()
        }
        Selector::IsConst(c) => vec![c.clone()],
        Selector::HasPath(path, qual) => {
            // v is a focus node iff some path-successor satisfies the qualifier.
            all_nodes(data)
                .into_iter()
                .filter(|v| {
                    let mut stack = HashSet::new();
                    succ(data, v, path)
                        .iter()
                        .any(|u| holds(data, arena, u, *qual, &mut stack))
                })
                .collect()
        }
        Selector::Sparql(_) => Vec::new(),
    }
}

/// `G, v ⊨ φ`. The `stack` of in-progress `(shape, node)` pairs breaks cycles
/// in recursive schemas (provisional semantics; pinned down in Layer 4). For
/// non-recursive schemas the shape graph is acyclic, so it never triggers.
fn holds(
    g: &Graph,
    arena: &ShapeArena,
    v: &Term,
    id: ShapeId,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> bool {
    let key = (id, v.clone());
    if !stack.insert(key.clone()) {
        return true;
    }
    let result = match arena.get(id) {
        Shape::Top | Shape::Pending => true,
        Shape::TestConst(c) => v == c,
        Shape::TestType(t) => value_type_holds(t, v),
        Shape::TestKind(k) => k.matches(v),
        Shape::Closed(q) => match node_of(v) {
            Some(node) => g
                .triples_for_subject(&node)
                .all(|t| q.contains(&t.predicate.into_owned())),
            None => true,
        },
        Shape::Eq(path, p) => {
            succ(g, v, path) == succ(g, v, &Path::Pred(p.clone()))
        }
        Shape::Disj(path, p) => {
            let lhs = succ(g, v, path);
            let rhs = succ(g, v, &Path::Pred(p.clone()));
            lhs.is_disjoint(&rhs)
        }
        Shape::Lt(path, p) => all_pairs_ordered(g, v, path, p, false),
        Shape::Le(path, p) => all_pairs_ordered(g, v, path, p, true),
        Shape::UniqueLang(path) => unique_lang(&succ(g, v, path)),
        Shape::Not(c) => !holds(g, arena, v, *c, stack),
        Shape::And(cs) => cs.iter().all(|c| holds(g, arena, v, *c, stack)),
        Shape::Or(cs) => cs.iter().any(|c| holds(g, arena, v, *c, stack)),
        Shape::Count { path, min, max, qualifier } => {
            let n = succ(g, v, path)
                .iter()
                .filter(|u| holds(g, arena, u, *qualifier, stack))
                .count() as u64;
            min.is_none_or(|m| n >= m) && max.is_none_or(|m| n <= m)
        }
        // Unsupported leaf (diagnosed at parse): conservatively conforms.
        Shape::Sparql(_) => true,
    };
    stack.remove(&key);
    result
}

fn all_pairs_ordered(g: &Graph, v: &Term, path: &Path, p: &oxrdf::NamedNode, allow_eq: bool) -> bool {
    let lhs = succ(g, v, path);
    let rhs = succ(g, v, &Path::Pred(p.clone()));
    for a in &lhs {
        for b in &rhs {
            match compare_terms(a, b) {
                Some(Ordering::Less) => {}
                Some(Ordering::Equal) if allow_eq => {}
                _ => return false,
            }
        }
    }
    true
}

fn unique_lang(values: &HashSet<Term>) -> bool {
    let mut seen = HashSet::new();
    for term in values {
        if let Term::Literal(l) = term
            && let Some(lang) = l.language()
            && !seen.insert(lang.to_ascii_lowercase())
        {
            return false;
        }
    }
    true
}

fn subject_term(s: oxrdf::NamedOrBlankNodeRef) -> Term {
    crate::path::term_of(s.into_owned())
}

/// All distinct terms appearing as a subject or object in the graph.
fn all_nodes(g: &Graph) -> HashSet<Term> {
    let mut nodes = HashSet::new();
    for t in g.iter() {
        nodes.insert(subject_term(t.subject));
        nodes.insert(t.object.into_owned());
    }
    nodes
}
