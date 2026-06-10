//! SHACL-AF rule inference (Layer 6) — least-fixpoint forward chaining.
//!
//! A rule fires on its focus nodes for which all `sh:condition`s hold, producing
//! triples from its head's node expressions. Per the decided semantics
//! ([`docs/03-recursion-semantics.md`](../../../docs/03-recursion-semantics.md)),
//! inference is the **least fixpoint**. Rules run in ascending `sh:order`
//! groups, and output from a later group may reactivate an earlier group in the
//! next pass. Predicate-level delta scheduling avoids rerunning rules whose
//! graph reads cannot observe the newly added triples. Triple rules only
//! combine existing terms, and SPARQL `CONSTRUCT` results containing fresh
//! blank nodes are rejected, preserving termination for the supported subset.
//!
//! Function node expressions are not executed yet; they are reported as
//! diagnostics rather than silently skipped.

use crate::path::{node_of, succ};
use crate::sparql::SparqlExecutor;
use crate::validate::{NonStratifiable, focus_nodes_with, graph_union, holds};
use oxrdf::{Graph, Term, Triple};
use shacl_algebra::{NodeExpr, Rule, RuleHead, Schema, ShapeArena};
use shacl_opt::{RuleDependencies, analyze, rule_dependencies};
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
    infer_with_context(data, data, schema)
}

/// Run inference over split data and shapes graphs.
///
/// Rule focus nodes come from `data`, while class hierarchy, paths, conditions,
/// and SPARQL rule bodies see the RDF union of `data` and `shapes`.
pub fn infer_graphs(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
) -> Result<InferenceOutcome, NonStratifiable> {
    let context = graph_union(data, shapes);
    infer_with_context(data, &context, schema)
}

/// Run inference with data-scoped focus discovery and a broader execution
/// context. `context` should contain `data`; newly inferred triples are added to
/// both the returned data graph and the mutable execution context.
pub fn infer_with_context(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
) -> Result<InferenceOutcome, NonStratifiable> {
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
    let mut context = context.clone();
    let sparql =
        SparqlExecutor::new(&context).expect("building an in-memory Oxigraph store should succeed");
    let mut inferred = Vec::new();
    let mut diags: BTreeSet<String> = BTreeSet::new();

    let mut rules: Vec<ScheduledRule<'_>> = schema
        .rules
        .iter()
        .enumerate()
        .filter(|(_, rule)| !rule.deactivated)
        .map(|(index, rule)| ScheduledRule {
            index,
            order: rule.order.unwrap_or(0),
            dependencies: rule_dependencies(rule, &schema.arena),
            rule,
        })
        .collect();
    rules.sort_by_key(|scheduled| (scheduled.order, scheduled.index));

    // The first pass evaluates every rule. Later passes are semi-naive at rule
    // granularity: only rules that may read a changed predicate are active.
    let mut active: HashSet<usize> = (0..rules.len()).collect();
    loop {
        let mut changed_predicates = HashSet::new();
        let mut added = false;
        let mut start = 0;

        while start < rules.len() {
            let order = rules[start].order;
            let mut end = start + 1;
            while end < rules.len() && rules[end].order == order {
                end += 1;
            }

            // Tied rules observe the same graph snapshot. Their additions are
            // visible to subsequent order groups in this pass.
            let mut candidates = Vec::new();
            for (position, scheduled) in rules[start..end].iter().enumerate() {
                if !active.contains(&(start + position)) {
                    continue;
                }
                fire_rule(
                    &graph,
                    &context,
                    &schema.arena,
                    scheduled.rule,
                    &sparql,
                    &mut candidates,
                    &mut diags,
                );
            }
            for t in candidates {
                if !context.contains(&t) {
                    graph.insert(&t);
                    context.insert(&t);
                    if let Err(error) = sparql.insert(&t) {
                        diags.insert(format!("failed to update SPARQL inference store: {error}"));
                    }
                    changed_predicates.insert(t.predicate.clone());
                    inferred.push(t);
                    added = true;
                }
            }

            start = end;
        }

        if !added {
            break;
        }

        active.clear();
        for (position, scheduled) in rules.iter().enumerate() {
            if scheduled.dependencies.affected_by(&changed_predicates) {
                active.insert(position);
            }
        }
        if active.is_empty() {
            break;
        }
    }

    Ok(InferenceOutcome {
        graph,
        inferred,
        diagnostics: diags.into_iter().collect(),
    })
}

struct ScheduledRule<'a> {
    index: usize,
    order: i64,
    dependencies: RuleDependencies,
    rule: &'a Rule,
}

fn fire_rule(
    data: &Graph,
    context: &Graph,
    arena: &ShapeArena,
    rule: &shacl_algebra::Rule,
    sparql: &SparqlExecutor,
    out: &mut Vec<Triple>,
    diags: &mut BTreeSet<String>,
) {
    for v in focus_nodes_with(data, context, &rule.selector, arena, sparql) {
        let conditions_hold = rule.conditions.iter().all(|c| {
            let mut stack = HashSet::new();
            holds(context, arena, &v, *c, &mut stack, sparql)
        });
        if !conditions_hold {
            continue;
        }
        match &rule.head {
            RuleHead::Triple {
                subject,
                predicate,
                object,
            } => {
                let subjects = eval_node_expr(context, arena, &v, subject, sparql, diags);
                let predicates = eval_node_expr(context, arena, &v, predicate, sparql, diags);
                let objects = eval_node_expr(context, arena, &v, object, sparql, diags);
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
            RuleHead::Sparql(construct) => match sparql.construct(&construct.query, &v) {
                Ok(triples) => {
                    for triple in triples {
                        if matches!(triple.subject, oxrdf::NamedOrBlankNode::BlankNode(_))
                            || matches!(triple.object, Term::BlankNode(_))
                        {
                            diags.insert(
                                "sh:SPARQLRule CONSTRUCT blank nodes are not supported because \
                                 they can prevent fixpoint termination"
                                    .to_string(),
                            );
                        } else {
                            out.push(triple);
                        }
                    }
                }
                Err(error) => {
                    diags.insert(format!("sh:SPARQLRule evaluation failed: {error}"));
                }
            },
        }
    }
}

/// Evaluate a node expression at focus node `v` to its set of result terms.
fn eval_node_expr(
    g: &Graph,
    arena: &ShapeArena,
    v: &Term,
    expr: &NodeExpr,
    sparql: &SparqlExecutor,
    diags: &mut BTreeSet<String>,
) -> HashSet<Term> {
    match expr {
        NodeExpr::This => once(v.clone()),
        NodeExpr::Constant(t) => once(t.clone()),
        NodeExpr::Path(p) => succ(g, v, p),
        NodeExpr::Filter { input, shape } => eval_node_expr(g, arena, v, input, sparql, diags)
            .into_iter()
            .filter(|x| {
                let mut stack = HashSet::new();
                holds(g, arena, x, *shape, &mut stack, sparql)
            })
            .collect(),
        NodeExpr::Intersection(es) => {
            let mut iter = es.iter();
            match iter.next() {
                Some(first) => {
                    let mut acc = eval_node_expr(g, arena, v, first, sparql, diags);
                    for e in iter {
                        let s = eval_node_expr(g, arena, v, e, sparql, diags);
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
                acc.extend(eval_node_expr(g, arena, v, e, sparql, diags));
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
