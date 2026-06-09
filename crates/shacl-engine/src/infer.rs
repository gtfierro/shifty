//! SHACL-AF rule inference (Layer 6) — least-fixpoint forward chaining.
//!
//! A rule fires on its focus nodes for which all `sh:condition`s hold, producing
//! triples from its head's node expressions. Per the decided semantics
//! ([`docs/03-recursion-semantics.md`](../../../docs/03-recursion-semantics.md)),
//! inference is the **least fixpoint**: rules are grouped by `sh:order` (lower
//! first) and each order is saturated — applied until no new triple appears —
//! before the next. Because `sh:TripleRule` heads only ever combine existing
//! terms (no term invention), the fixpoint terminates.
//!
//! `sh:SPARQLRule` heads and function node expressions are not executed yet;
//! they are reported as diagnostics rather than silently skipped.

use crate::path::{node_of, succ};
use crate::validate::{focus_nodes, holds, NonStratifiable};
use oxrdf::{Graph, Term, Triple};
use shacl_algebra::{NodeExpr, RuleHead, Schema, ShapeArena};
use shacl_opt::analyze;
use std::collections::{BTreeSet, HashSet};

/// The result of running inference over a data graph.
pub struct InferenceOutcome {
    /// The data graph augmented with all inferred triples.
    pub graph: Graph,
    /// The triples that were newly inferred (not already asserted).
    pub inferred: Vec<Triple>,
    /// Unsupported rule features encountered (deduplicated).
    pub diagnostics: Vec<String>,
}

/// Run rule inference to a least fixpoint, ordered by `sh:order`.
pub fn infer(data: &Graph, schema: &Schema) -> Result<InferenceOutcome, NonStratifiable> {
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

    let mut graph = data.clone();
    let mut inferred = Vec::new();
    let mut diags: BTreeSet<String> = BTreeSet::new();

    // process rules grouped by ascending sh:order; saturate each group
    let mut orders: Vec<i64> = schema
        .rules
        .iter()
        .filter(|r| !r.deactivated)
        .map(|r| r.order.unwrap_or(0))
        .collect();
    orders.sort_unstable();
    orders.dedup();

    for order in orders {
        loop {
            let mut candidates = Vec::new();
            for rule in schema
                .rules
                .iter()
                .filter(|r| !r.deactivated && r.order.unwrap_or(0) == order)
            {
                fire_rule(&graph, &schema.arena, rule, &mut candidates, &mut diags);
            }
            let mut added = false;
            for t in candidates {
                if graph.insert(&t) {
                    inferred.push(t);
                    added = true;
                }
            }
            if !added {
                break;
            }
        }
    }

    Ok(InferenceOutcome { graph, inferred, diagnostics: diags.into_iter().collect() })
}

fn fire_rule(
    g: &Graph,
    arena: &ShapeArena,
    rule: &shacl_algebra::Rule,
    out: &mut Vec<Triple>,
    diags: &mut BTreeSet<String>,
) {
    for v in focus_nodes(g, &rule.selector, arena) {
        let conditions_hold = rule.conditions.iter().all(|c| {
            let mut stack = HashSet::new();
            holds(g, arena, &v, *c, &mut stack)
        });
        if !conditions_hold {
            continue;
        }
        match &rule.head {
            RuleHead::Triple { subject, predicate, object } => {
                let subjects = eval_node_expr(g, arena, &v, subject, diags);
                let predicates = eval_node_expr(g, arena, &v, predicate, diags);
                let objects = eval_node_expr(g, arena, &v, object, diags);
                for s in &subjects {
                    let Some(subj) = node_of(s) else { continue };
                    for p in &predicates {
                        let Term::NamedNode(pred) = p else { continue };
                        for o in &objects {
                            out.push(Triple::new(subj.clone(), pred.clone(), o.clone()));
                        }
                    }
                }
            }
            RuleHead::Sparql(_) => {
                diags.insert("sh:SPARQLRule execution not yet supported".to_string());
            }
        }
    }
}

/// Evaluate a node expression at focus node `v` to its set of result terms.
fn eval_node_expr(
    g: &Graph,
    arena: &ShapeArena,
    v: &Term,
    expr: &NodeExpr,
    diags: &mut BTreeSet<String>,
) -> HashSet<Term> {
    match expr {
        NodeExpr::This => once(v.clone()),
        NodeExpr::Constant(t) => once(t.clone()),
        NodeExpr::Path(p) => succ(g, v, p),
        NodeExpr::Filter { input, shape } => eval_node_expr(g, arena, v, input, diags)
            .into_iter()
            .filter(|x| {
                let mut stack = HashSet::new();
                holds(g, arena, x, *shape, &mut stack)
            })
            .collect(),
        NodeExpr::Intersection(es) => {
            let mut iter = es.iter();
            match iter.next() {
                Some(first) => {
                    let mut acc = eval_node_expr(g, arena, v, first, diags);
                    for e in iter {
                        let s = eval_node_expr(g, arena, v, e, diags);
                        acc.retain(|x| s.contains(x));
                    }
                    acc
                }
                None => HashSet::new(),
            }
        }
        NodeExpr::Union(es) => {
            let mut acc = HashSet::new();
            for e in es {
                acc.extend(eval_node_expr(g, arena, v, e, diags));
            }
            acc
        }
        NodeExpr::Function { .. } => {
            diags.insert("function node expressions not yet supported".to_string());
            HashSet::new()
        }
    }
}

fn once(t: Term) -> HashSet<Term> {
    let mut s = HashSet::with_capacity(1);
    s.insert(t);
    s
}
