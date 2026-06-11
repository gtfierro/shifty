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

use crate::frozen::FrozenIndexedDataset;
use crate::path::{node_of, succ};
use crate::sparql::SparqlExecutor;
use crate::validate::{NonStratifiable, focus_nodes_with, graph_union, holds};
use oxrdf::{Graph, NamedNode, Term, Triple};
use shifty_algebra::{NodeExpr, Rule, RuleHead, Schema, Selector, ShapeArena};
use shifty_opt::{RuleDependencies, analyze, rule_dependencies, rule_guard_dependencies};
use std::collections::{BTreeSet, HashMap, HashSet};

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
    let mut inferred: Vec<Triple> = Vec::new();
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
            guard_dependencies: rule_guard_dependencies(rule, &schema.arena),
            rule,
        })
        .collect();
    rules.sort_by_key(|scheduled| (scheduled.order, scheduled.index));
    let mut frozen = rules
        .iter()
        .any(|scheduled| matches!(scheduled.rule.head, RuleHead::Sparql(_)))
        .then(|| FrozenIndexedDataset::from_graph(&context));

    // The first pass evaluates every rule. Later passes are semi-naive at rule
    // granularity: only rules that may read a changed predicate are active.
    let mut active: HashSet<usize> = (0..rules.len()).collect();
    // Additions from each pass occupy one contiguous suffix of `inferred`.
    // `delta_start` avoids cloning RDF terms into separate delta buffers.
    let mut delta_start = 0;
    let mut first_pass = true;
    loop {
        let mut changed_predicates = HashSet::new();
        let mut added = false;
        let mut start = 0;
        let pass_start = inferred.len();
        let mut visible_changed: HashSet<NamedNode> = inferred[delta_start..]
            .iter()
            .map(|triple| triple.predicate.clone())
            .collect();

        // Focus node sets are recomputed at most once per selector per pass.
        // Entries are evicted lazily when a committed triple's predicate matches
        // the selector's read dependency.
        let mut focus_cache: HashMap<Selector, Vec<Term>> = HashMap::new();
        // Predicates of triples committed so far within this pass, used to
        // invalidate stale cache entries before they are read.
        let mut pass_changed: HashSet<NamedNode> = HashSet::new();

        while start < rules.len() {
            let order = rules[start].order;
            let mut end = start + 1;
            while end < rules.len() && rules[end].order == order {
                end += 1;
            }

            // Tied rules observe the same graph snapshot. Their additions are
            // visible to subsequent order groups in this pass.
            // HashSet deduplicates within the batch; fire_rule pre-filters
            // against the context so only genuinely new triples reach here.
            let mut candidates: HashSet<Triple> = HashSet::new();
            for (position, scheduled) in rules[start..end].iter().enumerate() {
                if !active.contains(&(start + position)) {
                    continue;
                }
                let sel = &scheduled.rule.selector;
                if selector_stale(sel, &pass_changed) {
                    focus_cache.remove(sel);
                }
                let focus_nodes = focus_cache.entry(sel.clone()).or_insert_with(|| {
                    focus_nodes_with(&graph, &context, sel, &schema.arena, &sparql)
                });
                let mut delta_focus_nodes = Vec::new();
                let execution_focus_nodes = match &scheduled.rule.head {
                    RuleHead::Sparql(construct)
                        if !first_pass
                            && !focus_nodes.is_empty()
                            // Differential BGP execution visits the delta once
                            // per scan. Above this crossover, the existing
                            // focus-bound batch is the cheaper access path.
                            && (inferred.len() - delta_start).saturating_mul(2)
                                < focus_nodes.len()
                            && !scheduled
                                .guard_dependencies
                                .affected_by(&visible_changed) =>
                    {
                        match sparql.construct_delta_foci(
                            &construct.query,
                            &inferred[delta_start..],
                            frozen.as_ref(),
                        ) {
                            Ok(Some(affected)) => {
                                delta_focus_nodes.extend(
                                    focus_nodes
                                        .iter()
                                        .filter(|focus| affected.contains(*focus))
                                        .cloned(),
                                );
                                delta_focus_nodes.as_slice()
                            }
                            Ok(None) | Err(_) => focus_nodes.as_slice(),
                        }
                    }
                    _ => focus_nodes.as_slice(),
                };
                let rule_label = format!("rule[{}]", start + position);
                let rule_t = std::time::Instant::now();
                fire_rule(
                    execution_focus_nodes,
                    &context,
                    &schema.arena,
                    scheduled.rule,
                    &sparql,
                    frozen.as_ref(),
                    &mut candidates,
                    &mut diags,
                );
                crate::profile::record_shape(&rule_label, rule_t.elapsed().as_micros() as u64);
            }
            if let Some(frozen) = frozen.as_mut() {
                frozen.extend_triples(candidates.iter());
            }
            for t in candidates {
                pass_changed.insert(t.predicate.clone());
                visible_changed.insert(t.predicate.clone());
                graph.insert(&t);
                context.insert(&t);
                if let Err(error) = sparql.insert(&t) {
                    diags.insert(format!("failed to update SPARQL inference store: {error}"));
                }
                changed_predicates.insert(t.predicate.clone());
                inferred.push(t);
                added = true;
            }

            start = end;
        }

        if !added {
            break;
        }

        delta_start = pass_start;
        first_pass = false;
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
    guard_dependencies: RuleDependencies,
    rule: &'a Rule,
}

/// Whether a cached focus-node set for `sel` may have become stale given the
/// predicates committed so far within the current pass.
fn selector_stale(sel: &Selector, pass_changed: &HashSet<NamedNode>) -> bool {
    if pass_changed.is_empty() {
        return false;
    }
    match sel {
        Selector::HasOut(p) | Selector::HasIn(p) => pass_changed.contains(p),
        Selector::IsConst(_) => false,
        // HasPath traversal and SPARQL queries can read any predicate.
        Selector::HasPath(..) | Selector::Sparql(_) => true,
    }
}

#[allow(clippy::too_many_arguments)]
fn fire_rule(
    focus_nodes: &[Term],
    context: &Graph,
    arena: &ShapeArena,
    rule: &shifty_algebra::Rule,
    sparql: &SparqlExecutor,
    frozen: Option<&FrozenIndexedDataset>,
    out: &mut HashSet<Triple>,
    diags: &mut BTreeSet<String>,
) {
    let eligible: Vec<&Term> = focus_nodes
        .iter()
        .filter(|v| {
            rule.conditions.iter().all(|c| {
                let mut stack = HashSet::new();
                holds(context, arena, v, *c, &mut stack, sparql)
            })
        })
        .collect();

    match &rule.head {
        RuleHead::Triple {
            subject,
            predicate,
            object,
        } => {
            for v in eligible {
                let subjects = eval_node_expr(context, arena, v, subject, sparql, diags);
                let predicates = eval_node_expr(context, arena, v, predicate, sparql, diags);
                let objects = eval_node_expr(context, arena, v, object, sparql, diags);
                for s in &subjects {
                    let Some(subj) = node_of(s) else { continue };
                    for p in &predicates {
                        let Term::NamedNode(pred) = p else { continue };
                        for o in &objects {
                            let t = Triple::new(subj.clone(), pred.clone(), o.clone());
                            if !context.contains(&t) {
                                out.insert(t);
                            }
                        }
                    }
                }
            }
        }
        RuleHead::Sparql(construct) => {
            let eligible: Vec<Term> = eligible.into_iter().cloned().collect();
            match sparql.construct_many(&construct.query, &eligible, frozen) {
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
                            out.insert(triple);
                        }
                    }
                }
                Err(error) => {
                    diags.insert(format!("sh:SPARQLRule evaluation failed: {error}"));
                }
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
