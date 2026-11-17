//! SHACL-AF rule inference (Layer 6) — least-fixpoint forward chaining.
//!
//! A rule fires on its focus nodes for which all `sh:condition`s hold, producing
//! triples from its head's node expressions. Per the decided semantics
//! ([`docs/03-recursion-semantics.md`](../../../docs/03-recursion-semantics.md)),
//! inference is the **least fixpoint**. Rules run in ascending `sh:order`
//! groups within ordered shapes. Tied shapes read isolated branches, and output
//! from a later group may reactivate an earlier group in the
//! next pass. Predicate-level delta scheduling avoids rerunning rules whose
//! graph reads cannot observe the newly added triples. Triple rules only
//! combine existing terms. SPARQL `CONSTRUCT` may reuse blank nodes from the
//! input data or shapes graph and create fresh blank nodes. An iteration limit
//! bounds rules that keep generating new terms instead of reaching a fixpoint.
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
use crate::error::EvaluationError;
use crate::focus::{FocusNodes, FocusScope, IndexedFocus};
use crate::frozen::{FrozenIndexedDataset, SourceStorage};
use crate::path::{node_of, succ};
use crate::sparql::{FunctionDef, SparqlExecutor};
use crate::validate::{EngineOptions, ShapeEvaluator, focus_nodes_with};
use oxrdf::{BlankNode, NamedNode, NamedOrBlankNode, Term, Triple};
use shifty_algebra::{NodeExpr, Rule, RuleHead, Schema, Selector};
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
) -> Result<InferenceRun, EvaluationError> {
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
            order: entry.order.clone(),
            shape_order: entry.shape_order.clone(),
            owner: entry.owner.clone(),
            dependencies: entry.dependencies.clone(),
            guard_dependencies: entry.guard_dependencies.clone(),
            rule: &schema.rules[entry.index],
        })
        .collect();

    // Capture input graph identities before the first inference commit. In a
    // compiled session, the indexed dataset may alias data blank nodes whose
    // labels collide with the shapes source, so use its internalized terms.
    // Fresh template and BNODE() nodes require another pass even if none of
    // the rule's read predicates changed: another execution may create more.
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
    let mut iterations = 0;
    let mut generative_rules = HashSet::new();
    loop {
        if iterations >= options.max_inference_iterations {
            return Err(EvaluationError::InferenceLimit {
                iterations,
                rules: rules
                    .iter()
                    .enumerate()
                    .filter(|(position, _)| active.contains(position))
                    .map(|(position, scheduled)| {
                        scheduled
                            .rule
                            .source
                            .clone()
                            .unwrap_or_else(|| format!("rule[{position}]"))
                    })
                    .collect(),
            });
        }
        iterations += 1;
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
            let mut group_end = start + 1;
            while group_end < rules.len()
                && rules[group_end].shape_order == rules[start].shape_order
            {
                group_end += 1;
            }
            // Shapes with only one rule-order group have no private intermediate
            // additions. Evaluate all such tied shapes on the same executor and
            // commit once, preserving isolation without cloning a session per shape.
            let shared_snapshot = rules[start..group_end]
                .windows(2)
                .all(|pair| pair[0].owner != pair[1].owner || pair[0].order == pair[1].order);
            let mut shape_end = start + 1;
            while shape_end < group_end && rules[shape_end].owner == rules[start].owner {
                shape_end += 1;
            }
            // The common single-shape case retains the original executor/caches.
            let tied_shapes = !shared_snapshot && shape_end < group_end;
            let baseline = tied_shapes.then(|| sparql.fork());
            let baseline_len = inferred.len();
            let base_changed = pass_changed.clone();
            let base_visible = visible_changed.clone();
            let mut staged = HashSet::new();
            while start < group_end {
                shape_end = if shared_snapshot {
                    group_end
                } else {
                    start + 1
                };
                while shape_end < group_end && rules[shape_end].owner == rules[start].owner {
                    shape_end += 1;
                }
                // A mixed tie group can still contain long runs of shapes with
                // one rule order. Coalesce those runs into one read-only branch.
                let single_order = rules[start].order == rules[shape_end - 1].order;
                if single_order {
                    while shape_end < group_end {
                        let next_start = shape_end;
                        let mut next_end = next_start + 1;
                        while next_end < group_end
                            && rules[next_end].owner == rules[next_start].owner
                        {
                            next_end += 1;
                        }
                        if rules[next_start].order != rules[next_end - 1].order {
                            break;
                        }
                        shape_end = next_end;
                    }
                }
                if let Some(base) = &baseline {
                    sparql = base.fork();
                    focus_cache.clear();
                    pass_changed.clone_from(&base_changed);
                    visible_changed.clone_from(&base_visible);
                }
                while start < shape_end {
                    let order = rules[start].order.clone();
                    let mut end = if shared_snapshot || single_order {
                        shape_end
                    } else {
                        start + 1
                    };
                    while end < shape_end && rules[end].order == order {
                        end += 1;
                    }

                    // Tied rules observe the same graph snapshot. Their additions are
                    // visible to subsequent order groups in this pass.
                    // HashSet deduplicates within the batch; fire_rule pre-filters
                    // against the context so only genuinely new triples reach here.
                    let mut candidates: HashSet<Triple> = HashSet::new();
                    let mut evaluator = None;
                    for (position, scheduled) in rules[start..end].iter().enumerate() {
                        if !active.contains(&(start + position))
                            && !scheduled.dependencies.affected_by(&pass_changed)
                        {
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
                        let focus_nodes = sparql.checked(|| {
                            focus_cache.entry(sel.clone()).or_insert_with(|| {
                                focus_nodes_with(focus, backend, sel, &schema.arena, &sparql)
                            })
                        })?;
                        let mut delta_focus_nodes = Vec::new();
                        let execution_focus_nodes = match &scheduled.rule.head {
                            RuleHead::Sparql(construct)
                                if !first_pass
                            && !generative_rules.contains(&(start + position))
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
                        if execution_focus_nodes.is_empty() {
                            continue;
                        }
                        let evaluator = evaluator.get_or_insert_with(|| {
                            ShapeEvaluator::new(
                                backend,
                                &schema.arena,
                                shifty_algebra::Prefixes::empty(),
                                &sparql,
                            )
                        });
                        let rule_label = format!("rule[{}]", start + position);
                        let rule_t = web_time::Instant::now();
                        let runtime = RuleRuntime {
                            backend,
                            sparql: &sparql,
                            input_blank_nodes: &input_blank_nodes,
                        };
                        // Conditions and filter expressions use boolean shape evaluation.
                        // Check its retained failures before committing any candidate
                        // triples, including results produced through negation.
                        let generated = sparql.checked(|| {
                            fire_rule(
                                execution_focus_nodes,
                                scheduled.rule,
                                &runtime,
                                evaluator,
                                &mut candidates,
                                &mut diags,
                            )
                        })?;
                        if generated {
                            generative_rules.insert(start + position);
                        }
                        crate::profile::record_shape(
                            &rule_label,
                            rule_t.elapsed().as_micros() as u64,
                        );
                    }
                    drop(evaluator);
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
                if tied_shapes {
                    staged.extend(inferred.drain(baseline_len..));
                }
            }
            if let Some(base) = baseline {
                sparql = base;
                sparql.extend_triples(staged.iter());
                for triple in staged {
                    pass_changed.insert(triple.predicate.clone());
                    visible_changed.insert(triple.predicate.clone());
                    inferred.push(triple);
                }
                focus_cache.clear();
            }
        }

        if !added {
            break;
        }

        delta_start = pass_start;
        first_pass = false;
        active.clear();
        for (position, scheduled) in rules.iter().enumerate() {
            if generative_rules.contains(&position)
                || scheduled.dependencies.affected_by(&changed_predicates)
            {
                active.insert(position);
            }
        }
        if active.is_empty() {
            break;
        }
    }

    Ok(InferenceRun {
        inferred,
        diagnostics: diags.into_iter().collect(),
        dataset: sparql.into_frozen().expect("compiled inference dataset"),
    })
}

struct ScheduledRule<'a> {
    order: shifty_algebra::Order,
    shape_order: shifty_algebra::Order,
    owner: Option<String>,
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
    sparql: &'a SparqlExecutor,
    input_blank_nodes: &'a HashSet<BlankNode>,
}

fn fire_rule(
    focus_nodes: &[Term],
    rule: &shifty_algebra::Rule,
    runtime: &RuleRuntime<'_>,
    evaluator: &mut ShapeEvaluator<'_>,
    out: &mut HashSet<Triple>,
    diags: &mut BTreeSet<String>,
) -> bool {
    let mut generated = false;
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
                let subjects = eval_node_expr(runtime.backend, v, subject, evaluator, diags);
                let predicates = eval_node_expr(runtime.backend, v, predicate, evaluator, diags);
                let objects = eval_node_expr(runtime.backend, v, object, evaluator, diags);
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
                            generated = true;
                        }
                        out.insert(triple);
                    }
                }
                Err(error) => {
                    diags.insert(format!(
                        "sh:SPARQLRule evaluation failed{}: {error}",
                        rule.source
                            .as_ref()
                            .map(|source| format!(" ({source})"))
                            .unwrap_or_default()
                    ));
                }
            }
        }
    }
    generated
}

/// Evaluate a node expression at focus node `v` to its set of result terms.
fn eval_node_expr(
    backend: &dyn crate::path::PathBackend,
    v: &Term,
    expr: &NodeExpr,
    evaluator: &mut ShapeEvaluator<'_>,
    diags: &mut BTreeSet<String>,
) -> HashSet<Term> {
    match expr {
        NodeExpr::This => once(v.clone()),
        NodeExpr::Constant(t) => once(t.clone()),
        NodeExpr::Path(p) => succ(backend, v, p),
        NodeExpr::PathFrom { input, path } => eval_node_expr(backend, v, input, evaluator, diags)
            .iter()
            .flat_map(|node| succ(backend, node, path))
            .collect(),
        NodeExpr::Filter { input, shape } => eval_node_expr(backend, v, input, evaluator, diags)
            .into_iter()
            .filter(|x| evaluator.holds(x, *shape))
            .collect(),
        NodeExpr::Intersection(es) => {
            let mut iter = es.iter();
            match iter.next() {
                Some(first) => {
                    let mut acc = eval_node_expr(backend, v, first, evaluator, diags);
                    for e in iter {
                        let s = eval_node_expr(backend, v, e, evaluator, diags);
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
                acc.extend(eval_node_expr(backend, v, e, evaluator, diags));
            }
            acc
        }
        NodeExpr::Function { iri, args } => {
            let args: Vec<_> = args
                .iter()
                .map(|arg| eval_node_expr(backend, v, arg, evaluator, diags))
                .collect();
            match evaluator.sparql().node_function_values(iri, &args) {
                Ok(values) => values,
                Err(error) => {
                    diags.insert(error);
                    HashSet::new()
                }
            }
        }
    }
}

fn once(t: Term) -> HashSet<Term> {
    let mut values = HashSet::with_capacity(1);
    values.insert(t);
    values
}
