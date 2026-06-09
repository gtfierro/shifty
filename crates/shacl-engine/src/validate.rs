//! Reference shape satisfaction `G, v ⊨ φ` and schema validation `G ⊨ S`
//! (doc 00 §3–§4, Table 2). This is the conformance *oracle*: the optimized
//! engines in later layers must agree with it.
//!
//! Two evaluators share the logic: [`holds`] returns a bare bool (used for
//! target selection and counting), while [`explain`] returns the specific
//! atomic constraints that failed, with the value node and path at which they
//! failed — enough for per-constraint reporting. The `∀π = ∃≤0 π.¬φ` encoding
//! lets a failed universal drill straight into the offending value node's inner
//! constraint.

use crate::path::{node_of, succ};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{Graph, NamedNode, Term};
use serde::{Deserialize, Serialize};
use shacl_algebra::render::{path_to_string, shape_to_string};
use shacl_algebra::{Path, Schema, Selector, Shape, ShapeArena, ShapeId};
use shacl_opt::analyze;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::fmt;

/// A single failed atomic constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reason {
    /// The node at which the constraint failed (a value node, or the focus).
    pub value: Term,
    /// The path from the enclosing focus to `value`, if the failure is
    /// value-scoped (rendered in `π` notation).
    pub path: Option<String>,
    /// The failing constraint's arena slot (cross-references the algebra dump).
    pub shape: ShapeId,
    pub message: String,
}

/// One focus node that failed its statement's shape, with the reasons why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    pub focus: Term,
    /// Index of the violated `(selector, shape)` statement in the schema.
    pub statement: usize,
    pub reasons: Vec<Reason>,
}

/// The outcome of validating a data graph against a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationOutcome {
    pub conforms: bool,
    pub violations: Vec<Violation>,
}

/// The schema is not stratifiable: it recurses through genuine negation, so it
/// has no defined 2-valued semantics (`docs/03-recursion-semantics.md`). We
/// diagnose rather than guess. Carries the offending shape components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonStratifiable {
    pub components: Vec<Vec<ShapeId>>,
}

impl fmt::Display for NonStratifiable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "non-stratifiable schema (recursion through negation): ")?;
        for (i, c) in self.components.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            let ids: Vec<String> = c.iter().map(|s| format!("@{}", s.0)).collect();
            write!(f, "{{{}}}", ids.join(" "))?;
        }
        Ok(())
    }
}

impl std::error::Error for NonStratifiable {}

/// Validate `data` against `schema`.
///
/// Honors the decided recursion semantics (`docs/03-recursion-semantics.md`):
/// the schema must be **stratifiable** (no recursion through net negation), else
/// we return [`NonStratifiable`]. For a stratifiable schema all recursion is
/// net-positive (monotone), and [`explain`]/[`holds`]'s "assume conforming on a
/// back-edge" cycle guard computes exactly the **greatest fixpoint** — the
/// coinductive validation reading we chose.
pub fn validate(data: &Graph, schema: &Schema) -> Result<ValidationOutcome, NonStratifiable> {
    let strat = analyze(&schema.arena);
    if !strat.stratifiable {
        let components = strat
            .strata
            .iter()
            .filter(|s| !s.stratifiable)
            .map(|s| s.shapes.clone())
            .collect();
        return Err(NonStratifiable { components });
    }

    let mut violations = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        for v in focus_nodes(data, &st.selector, &schema.arena) {
            let mut stack = HashSet::new();
            let mut reasons = explain(data, &schema.arena, &v, st.shape, None, &mut stack);
            dedup_reasons(&mut reasons);
            if !reasons.is_empty() {
                violations.push(Violation { focus: v, statement: i, reasons });
            }
        }
    }
    Ok(ValidationOutcome { conforms: violations.is_empty(), violations })
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
        Selector::HasPath(path, qual) => all_nodes(data)
            .into_iter()
            .filter(|v| {
                let mut stack = HashSet::new();
                succ(data, v, path)
                    .iter()
                    .any(|u| holds(data, arena, u, *qual, &mut stack))
            })
            .collect(),
        Selector::Sparql(_) => Vec::new(),
    }
}

/// `G, v ⊨ φ` (bare bool). Used for target selection and qualifier counting.
fn holds(
    g: &Graph,
    arena: &ShapeArena,
    v: &Term,
    id: ShapeId,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> bool {
    let key = (id, v.clone());
    if !stack.insert(key.clone()) {
        return true; // back-edge ⇒ assume conforming: the gfp choice (doc 03)
    }
    let result = match arena.get(id) {
        Shape::Top | Shape::Pending | Shape::Sparql(_) => true,
        Shape::TestConst(c) => v == c,
        Shape::TestType(t) => value_type_holds(t, v),
        Shape::TestKind(k) => k.matches(v),
        Shape::Closed(q) => closed_offenders(g, v, q).is_empty(),
        Shape::Eq(path, p) => succ(g, v, path) == objects(g, v, p),
        Shape::Disj(path, p) => succ(g, v, path).is_disjoint(&objects(g, v, p)),
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
    };
    stack.remove(&key);
    result
}

/// The reasons `φ` (slot `id`) fails at `node`. Empty iff it holds. `path_ctx`
/// is the rendered path by which `node` was reached from the enclosing focus.
fn explain(
    g: &Graph,
    arena: &ShapeArena,
    node: &Term,
    id: ShapeId,
    path_ctx: Option<&str>,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> Vec<Reason> {
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        return Vec::new(); // back-edge ⇒ assume conforming (gfp, doc 03)
    }
    let reasons = match arena.get(id) {
        Shape::Top | Shape::Pending | Shape::Sparql(_) => Vec::new(),
        Shape::TestConst(_)
        | Shape::TestType(_)
        | Shape::TestKind(_)
        | Shape::Eq(..)
        | Shape::Disj(..)
        | Shape::Lt(..)
        | Shape::Le(..)
        | Shape::UniqueLang(_) => {
            let mut s = HashSet::new();
            leaf(
                holds(g, arena, node, id, &mut s),
                node,
                id,
                path_ctx,
                format!("{} not satisfied", shape_to_string(arena, id)),
            )
        }
        Shape::Closed(q) => {
            let bad = closed_offenders(g, node, q);
            if bad.is_empty() {
                Vec::new()
            } else {
                let preds: Vec<String> = bad.iter().map(|p| p.to_string()).collect();
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    message: format!("closed: unexpected predicate(s) {}", preds.join(", ")),
                }]
            }
        }
        Shape::Not(c) => {
            if explain(g, arena, node, *c, path_ctx, stack).is_empty() {
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    message: "negated shape unexpectedly held".to_string(),
                }]
            } else {
                Vec::new()
            }
        }
        Shape::And(cs) => cs
            .iter()
            .flat_map(|c| explain(g, arena, node, *c, path_ctx, stack))
            .collect(),
        Shape::Or(cs) => {
            let mut acc = Vec::new();
            let mut satisfied = false;
            for c in cs {
                let sub = explain(g, arena, node, *c, path_ctx, stack);
                if sub.is_empty() {
                    satisfied = true;
                    break;
                }
                acc.extend(sub);
            }
            if satisfied {
                Vec::new()
            } else {
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    message: "no alternative satisfied".to_string(),
                }]
            }
        }
        Shape::Count { path, min, max, qualifier } => {
            explain_count(g, arena, node, id, path, *min, *max, *qualifier, stack)
        }
    };
    stack.remove(&key);
    reasons
}

#[allow(clippy::too_many_arguments)]
fn explain_count(
    g: &Graph,
    arena: &ShapeArena,
    node: &Term,
    id: ShapeId,
    path: &Path,
    min: Option<u64>,
    max: Option<u64>,
    qualifier: ShapeId,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> Vec<Reason> {
    let path_str = path_to_string(path);
    let matched: Vec<Term> = succ(g, node, path)
        .into_iter()
        .filter(|u| holds(g, arena, u, qualifier, stack))
        .collect();
    let n = matched.len() as u64;
    let mut reasons = Vec::new();

    if let Some(mx) = max
        && n > mx
    {
        match arena.get(qualifier) {
            // ∀path.inner encoded as ∃≤0 path.¬inner: drill into the offenders.
            Shape::Not(inner) if mx == 0 => {
                for u in &matched {
                    reasons.extend(explain(g, arena, u, *inner, Some(&path_str), stack));
                }
            }
            _ => reasons.push(Reason {
                value: node.clone(),
                path: Some(path_str.clone()),
                shape: id,
                message: format!("at most {mx} value(s) may match along {path_str}, found {n}"),
            }),
        }
    }

    if let Some(mn) = min
        && n < mn
    {
        reasons.push(Reason {
            value: node.clone(),
            path: Some(path_str.clone()),
            shape: id,
            message: format!("at least {mn} value(s) required along {path_str}, found {n}"),
        });
    }

    reasons
}

fn leaf(
    ok: bool,
    node: &Term,
    id: ShapeId,
    path_ctx: Option<&str>,
    message: String,
) -> Vec<Reason> {
    if ok {
        Vec::new()
    } else {
        vec![Reason {
            value: node.clone(),
            path: path_ctx.map(str::to_string),
            shape: id,
            message,
        }]
    }
}

fn all_pairs_ordered(g: &Graph, v: &Term, path: &Path, p: &NamedNode, allow_eq: bool) -> bool {
    let lhs = succ(g, v, path);
    let rhs = objects(g, v, p);
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

fn objects(g: &Graph, v: &Term, p: &NamedNode) -> HashSet<Term> {
    succ(g, v, &Path::Pred(p.clone()))
}

/// Predicates on `node` not allowed by a closed shape's set `q`.
fn closed_offenders(g: &Graph, node: &Term, q: &BTreeSet<NamedNode>) -> BTreeSet<NamedNode> {
    let mut bad = BTreeSet::new();
    if let Some(n) = node_of(node) {
        for t in g.triples_for_subject(&n) {
            let p = t.predicate.into_owned();
            if !q.contains(&p) {
                bad.insert(p);
            }
        }
    }
    bad
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

fn dedup_reasons(reasons: &mut Vec<Reason>) {
    let mut seen = HashSet::new();
    reasons.retain(|r| seen.insert((r.value.to_string(), r.message.clone())));
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
