//! SHACL-AF rule inference (Layer 6) — least-fixpoint forward chaining.
//!
//! A rule fires on its focus nodes for which all `sh:condition`s hold, producing
//! triples from its head's node expressions. Per the decided semantics
//! ([`docs/03-recursion-semantics.md`](../../../docs/03-recursion-semantics.md)),
//! inference is the **least fixpoint**. Rules run in ascending `sh:order`
//! groups, and output from a later group may reactivate an earlier group in the
//! next pass. Predicate-level delta scheduling avoids rerunning rules whose
//! graph reads cannot observe the newly added triples. Triple rules only
//! combine existing terms. SPARQL `CONSTRUCT` may reuse blank nodes from the
//! input data or shapes graph, but results containing fresh blank nodes are
//! rejected.
//!
//! Rule inference runs only through compiled sessions. This keeps graph roles,
//! parsed queries, function definitions, and blank-node identities identical
//! across every public interface.
//!
//! The outer loop represents one least-fixpoint pass. Within a pass, rules with
//! the same `sh:order` read the same snapshot and contribute a deduplicated
//! candidate batch; committing that batch makes it visible to the next order
//! group. The next pass begins only after all groups have run, so an earlier
//! group can observe a later group's output without giving tied rules accidental
//! order dependence.
//!
//! The first pass evaluates every active rule. Later passes use predicate read
//! dependencies to schedule only rules that could observe the previous delta;
//! focus selections are also cached and invalidated by their own read surface.
//! These are scheduling reductions, not alternate semantics: every candidate is
//! still tested against the current read view before commit, and no triple leaves
//! a pass until its whole order group has finished reading its snapshot.

use crate::compiled::ParsedQueries;
use crate::focus::{FocusNodes, FocusScope, IndexedFocus};
use crate::frozen::{FrozenIndexedDataset, SourceStorage};
use crate::path::{node_of, succ};
use crate::sparql::{FunctionDef, SparqlExecutor};
use crate::validate::{EngineOptions, ShapeEvaluator, focus_nodes_with};
use oxrdf::{BlankNode, NamedNode, NamedOrBlankNode, Term, Triple};
use shifty_algebra::{NodeExpr, Rule, RuleHead, Schema, Selector, ShapeArena};
use shifty_opt::RuleDependencies;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

pub(crate) struct InferenceRun {
    pub inferred: Vec<Triple>,
    pub diagnostics: Vec<String>,
    pub dataset: FrozenIndexedDataset,
}

/// Run one compiled document session over an asserted data snapshot.
pub(crate) fn infer_with_compiled_functions(
    data: &oxrdf::Graph,
    schema: &Schema,
    options: &EngineOptions,
    functions: &[FunctionDef],
    schedule: &[crate::compiled::CompiledRuleSchedule],
    source: (Arc<SourceStorage>, bool, Arc<ParsedQueries>),
) -> InferenceRun {
    let (source_storage, separate, parsed_queries) = source;
    let dataset =
        FrozenIndexedDataset::from_data_with_source(data, source_storage, separate, separate);
    let mut sparql = SparqlExecutor::from_frozen_with_parsed(dataset, true, Some(parsed_queries));
    // Register sh:SPARQLFunctions so CONSTRUCT rule bodies can call them (node
    // expressions use the graph-aware call_sparql_function path separately).
    sparql.set_functions(functions.to_vec(), options.unsupported);
    let mut inferred: Vec<Triple> = Vec::new();
    let mut diags: BTreeSet<String> = BTreeSet::new();

    let rules: Vec<ScheduledRule<'_>> = schedule
        .iter()
        .map(|entry| ScheduledRule {
            order: entry.order,
            dependencies: entry.dependencies.clone(),
            guard_dependencies: entry.guard_dependencies.clone(),
            rule: &schema.rules[entry.index],
        })
        .collect();

    // Capture input graph identities before the first inference commit. In a
    // compiled session, the indexed dataset may alias data blank nodes whose
    // labels collide with the shapes source, so use its internalized terms.
    // This finite set permits CONSTRUCT rules to describe existing nodes while
    // excluding fresh template and BNODE() nodes on every fixpoint pass.
    let input_blank_nodes: HashSet<BlankNode> = if rules
        .iter()
        .any(|scheduled| matches!(scheduled.rule.head, RuleHead::Sparql(_)))
    {
        sparql
            .frozen()
            .expect("inference dataset")
            .input_blank_nodes()
    } else {
        HashSet::new()
    };

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
                let backend = sparql.frozen().expect("compiled inference dataset");
                let indexed_focus = IndexedFocus::new(
                    backend,
                    if separate {
                        FocusScope::Data
                    } else {
                        FocusScope::Default
                    },
                );
                let focus: &dyn FocusNodes = &indexed_focus;
                let focus_nodes = focus_cache.entry(sel.clone()).or_insert_with(|| {
                    focus_nodes_with(focus, backend, sel, &schema.arena, &sparql)
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
                            sparql.frozen(),
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
                let rule_t = web_time::Instant::now();
                let runtime = RuleRuntime {
                    backend,
                    arena: &schema.arena,
                    sparql: &sparql,
                    functions,
                    input_blank_nodes: &input_blank_nodes,
                };
                fire_rule(
                    execution_focus_nodes,
                    scheduled.rule,
                    &runtime,
                    &mut candidates,
                    &mut diags,
                );
                crate::profile::record_shape(&rule_label, rule_t.elapsed().as_micros() as u64);
            }
            sparql.extend_triples(candidates.iter());
            for t in candidates {
                pass_changed.insert(t.predicate.clone());
                visible_changed.insert(t.predicate.clone());
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

    InferenceRun {
        inferred,
        diagnostics: diags.into_iter().collect(),
        dataset: sparql.into_frozen().expect("compiled inference dataset"),
    }
}

struct ScheduledRule<'a> {
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

struct RuleRuntime<'a> {
    backend: &'a dyn crate::path::PathBackend,
    arena: &'a ShapeArena,
    sparql: &'a SparqlExecutor,
    functions: &'a [FunctionDef],
    input_blank_nodes: &'a HashSet<BlankNode>,
}

fn fire_rule(
    focus_nodes: &[Term],
    rule: &shifty_algebra::Rule,
    runtime: &RuleRuntime<'_>,
    out: &mut HashSet<Triple>,
    diags: &mut BTreeSet<String>,
) {
    let mut evaluator = ShapeEvaluator::new(
        runtime.backend,
        runtime.arena,
        shifty_algebra::Prefixes::empty(),
        runtime.sparql,
    );
    let eligible: Vec<&Term> = focus_nodes
        .iter()
        .filter(|v| rule.conditions.iter().all(|c| evaluator.holds(v, *c)))
        .collect();

    match &rule.head {
        RuleHead::Triple {
            subject,
            predicate,
            object,
        } => {
            for v in eligible {
                let subjects = eval_node_expr(
                    runtime.backend,
                    v,
                    subject,
                    &mut evaluator,
                    runtime.functions,
                    diags,
                );
                let predicates = eval_node_expr(
                    runtime.backend,
                    v,
                    predicate,
                    &mut evaluator,
                    runtime.functions,
                    diags,
                );
                let objects = eval_node_expr(
                    runtime.backend,
                    v,
                    object,
                    &mut evaluator,
                    runtime.functions,
                    diags,
                );
                for s in &subjects {
                    let Some(subj) = node_of(s) else { continue };
                    for p in &predicates {
                        let Term::NamedNode(pred) = p else { continue };
                        for o in &objects {
                            let t = Triple::new(subj.clone(), pred.clone(), o.clone());
                            if !runtime.backend.contains(s, pred, o) {
                                out.insert(t);
                            }
                        }
                    }
                }
            }
        }
        RuleHead::Sparql(construct) => {
            let eligible: Vec<Term> = eligible.into_iter().cloned().collect();
            match runtime.sparql.construct_many(
                &construct.query,
                &eligible,
                runtime.sparql.frozen(),
            ) {
                Ok(triples) => {
                    for triple in triples {
                        if matches!(&triple.subject, NamedOrBlankNode::BlankNode(node) if !runtime.input_blank_nodes.contains(node))
                            || matches!(&triple.object, Term::BlankNode(node) if !runtime.input_blank_nodes.contains(node))
                        {
                            diags.insert(
                                "sh:SPARQLRule CONSTRUCT fresh blank nodes are not supported because \
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
    backend: &dyn crate::path::PathBackend,
    v: &Term,
    expr: &NodeExpr,
    evaluator: &mut ShapeEvaluator<'_>,
    functions: &[FunctionDef],
    diags: &mut BTreeSet<String>,
) -> HashSet<Term> {
    match expr {
        NodeExpr::This => once(v.clone()),
        NodeExpr::Constant(t) => once(t.clone()),
        NodeExpr::Path(p) => succ(backend, v, p),
        NodeExpr::Filter { input, shape } => {
            eval_node_expr(backend, v, input, evaluator, functions, diags)
                .into_iter()
                .filter(|x| evaluator.holds(x, *shape))
                .collect()
        }
        NodeExpr::Intersection(es) => {
            let mut iter = es.iter();
            match iter.next() {
                Some(first) => {
                    let mut acc = eval_node_expr(backend, v, first, evaluator, functions, diags);
                    for e in iter {
                        let s = eval_node_expr(backend, v, e, evaluator, functions, diags);
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
                acc.extend(eval_node_expr(backend, v, e, evaluator, functions, diags));
            }
            acc
        }
        NodeExpr::Function { iri, args } => {
            // Evaluate arguments before borrowing evaluator for sparql().
            let arg_values: Vec<HashSet<Term>> = args
                .iter()
                .map(|a| eval_node_expr(backend, v, a, evaluator, functions, diags))
                .collect();

            let Some(function) = functions.iter().find(|function| function.iri == *iri) else {
                diags.insert(format!("function <{}> has no sh:select", iri.as_str()));
                return HashSet::new();
            };
            let sparql = evaluator.sparql();
            let mut results = HashSet::new();
            for combo in cartesian_product(&arg_values) {
                if combo.len() != function.params.len() {
                    continue;
                }
                let bindings: Vec<(String, Term)> = function
                    .params
                    .iter()
                    .zip(combo)
                    .map(|(name, val)| (name.clone(), val))
                    .collect();
                match sparql.call_sparql_function(&function.query, &bindings) {
                    Ok(terms) => results.extend(terms),
                    Err(e) => {
                        diags.insert(format!("function <{}> error: {e}", iri.as_str()));
                    }
                }
            }
            results
        }
    }
}

fn once(t: Term) -> HashSet<Term> {
    let mut s = HashSet::with_capacity(1);
    s.insert(t);
    s
}

/// Cartesian product of term sets — one arg combo per returned vec.
fn cartesian_product(sets: &[HashSet<Term>]) -> Vec<Vec<Term>> {
    sets.iter().fold(vec![vec![]], |acc, set| {
        acc.into_iter()
            .flat_map(|combo| {
                set.iter().map(move |item| {
                    let mut row = combo.clone();
                    row.push(item.clone());
                    row
                })
            })
            .collect()
    })
}
