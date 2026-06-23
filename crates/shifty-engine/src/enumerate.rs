//! The enumeration reference driver (`docs/07-repair-drivers.md` §4) and the
//! `candidates` helper (`docs/06-repair.md` §3.3).
//!
//! This is the simplest driver: it walks the [`RepairTree`]'s finite choices —
//! `Any` branches, `Repeat` counts, and per-hole candidate terms drawn from the
//! data graph (reuse) or minted fresh — and returns the first plan whose `ΔG`
//! passes the [`gate`]. It is primarily a correctness check on the
//! IR and the gate; it cannot invent novel literals (those holes are filled only
//! by reuse).
//!
//! Like every driver, it decides nothing the library doesn't expose: it only
//! composes [`instantiate`], [`candidates`], and [`gate`].

use crate::gate::{RepairOutcome, apply, gate};
use crate::path::term_of;
use crate::synthesize::synthesize;
use crate::validate::NonStratifiable;
use crate::value::value_type_holds;
use crate::witness::witness_violations;
use oxrdf::{BlankNode, Graph, Term};
use shifty_algebra::Schema;
use shifty_repair::{GraphDelta, HoleConstraint, NodeId, Plan, RepairTree, instantiate};
use std::collections::{HashMap, HashSet};

/// Knobs for the enumeration search.
#[derive(Debug, Clone, Copy)]
pub struct EnumOptions {
    /// Maximum number of gated candidate `ΔG`s before giving up.
    pub budget: usize,
    /// Maximum candidate terms tried per hole (bounds branching).
    pub max_candidates: usize,
}

impl Default for EnumOptions {
    fn default() -> Self {
        Self {
            budget: 10_000,
            max_candidates: 64,
        }
    }
}

/// A repair the enumeration driver found and verified.
#[derive(Debug, Clone)]
pub struct RepairSolution {
    pub plan: Plan,
    pub delta: GraphDelta,
    pub outcome: RepairOutcome,
}

/// Search for the first sound, progress-making repair of `tree`. Reuse
/// candidates are drawn from `data`, while gating is evaluated against `context`
/// (which should contain `data`; pass `data` again when there is no separate
/// shapes graph) — so a candidate typed with a subclass of a required class
/// clears the gate. The enumeration dual of [`crate::validate_with_context`].
pub fn enumerate_repair(
    tree: &RepairTree,
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    opts: EnumOptions,
) -> Result<Option<RepairSolution>, NonStratifiable> {
    let mut choices = HashMap::new();
    index_choices(tree, &mut choices);
    let mut budget = opts.budget;
    solve(
        tree,
        data,
        context,
        schema,
        &choices,
        Plan::default(),
        &mut budget,
        opts,
    )
}

/// The outcome of repairing a graph to a fixpoint.
#[derive(Debug, Clone)]
pub struct FixpointResult {
    /// The repaired data graph (`G` with every applied `ΔG`).
    pub graph: Graph,
    /// The deltas applied, in order.
    pub applied: Vec<GraphDelta>,
    /// Iterations run (one per applied repair).
    pub iterations: usize,
    /// Violations still unrepaired when the loop stopped (0 ⟺ conforms).
    pub remaining: usize,
}

/// The reference fixpoint driver (`docs/07-repair-drivers.md` §1): witness →
/// synthesize → enumerate → gate → apply, re-witnessing after each accepted
/// repair until the graph conforms or no focus admits a sound, progress-making
/// repair. Each accepted `ΔG` is gated (introduces nothing) and strictly reduces
/// the violation count, so the loop terminates.
///
/// Witnessing and gating are evaluated against `context` (which should contain
/// `data`; pass `data` again when there is no separate shapes graph) while focus
/// and the emitted graph stay the data graph. Each accepted `ΔG` is applied to
/// both graphs so later iterations see `(data ⊕ …) ∪ shapes` for evaluation. The
/// returned `graph` is the repaired *data* graph — the shapes/context triples are
/// not emitted.
pub fn repair_to_fixpoint(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    opts: EnumOptions,
) -> Result<FixpointResult, NonStratifiable> {
    let mut graph = data.clone();
    let mut context = context.clone();
    let mut applied = Vec::new();
    let mut iterations = 0usize;
    // A safety bound; progress guarantees far fewer iterations in practice.
    let max_iterations = 100_000;

    loop {
        let witnesses = witness_violations(&graph, &context, schema)?;
        if witnesses.is_empty() {
            return Ok(FixpointResult {
                graph,
                applied,
                iterations,
                remaining: 0,
            });
        }
        if iterations >= max_iterations {
            return Ok(FixpointResult {
                remaining: witnesses.len(),
                graph,
                applied,
                iterations,
            });
        }

        let mut progressed = false;
        for fw in &witnesses {
            let tree = synthesize(&schema.arena, fw);
            if let Some(sol) = enumerate_repair(&tree, &graph, &context, schema, opts)? {
                graph = apply(&graph, &sol.delta);
                context = apply(&context, &sol.delta);
                applied.push(sol.delta);
                progressed = true;
                break; // re-witness after each applied repair
            }
        }
        if !progressed {
            return Ok(FixpointResult {
                remaining: witnesses.len(),
                graph,
                applied,
                iterations,
            });
        }
        iterations += 1;
    }
}

#[derive(Clone, Copy)]
enum Choice {
    Any(usize),
    Repeat(u64),
}

fn index_choices(tree: &RepairTree, out: &mut HashMap<NodeId, Choice>) {
    match tree {
        RepairTree::Any { id, children } => {
            out.insert(*id, Choice::Any(children.len()));
            for c in children {
                index_choices(c, out);
            }
        }
        RepairTree::Repeat { id, body, min, .. } => {
            // Minimal repair: take exactly `min` instances.
            out.insert(*id, Choice::Repeat(*min));
            index_choices(body, out);
        }
        RepairTree::All { children, .. } => {
            for c in children {
                index_choices(c, out);
            }
        }
        RepairTree::Noop(_) | RepairTree::Blocked(..) | RepairTree::Edits { .. } => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn solve(
    tree: &RepairTree,
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    choices: &HashMap<NodeId, Choice>,
    plan: Plan,
    budget: &mut usize,
    opts: EnumOptions,
) -> Result<Option<RepairSolution>, NonStratifiable> {
    if *budget == 0 {
        return Ok(None);
    }
    let inst = instantiate(tree, &plan);

    if let Some(node) = inst.open_choices.first().copied() {
        return match choices.get(&node) {
            Some(Choice::Any(n)) => {
                for i in 0..*n {
                    let mut p = plan.clone();
                    p.branch.insert(node, i);
                    if let Some(sol) = solve(tree, data, context, schema, choices, p, budget, opts)?
                    {
                        return Ok(Some(sol));
                    }
                }
                Ok(None)
            }
            Some(Choice::Repeat(count)) => {
                let mut p = plan;
                p.count.insert(node, *count);
                solve(tree, data, context, schema, choices, p, budget, opts)
            }
            None => Ok(None),
        };
    }

    if let Some((hole, constraint)) = inst.open_holes.first().cloned() {
        for cand in candidates(&constraint, data, opts.max_candidates) {
            let mut p = plan.clone();
            p.binding.insert(hole, cand);
            if let Some(sol) = solve(tree, data, context, schema, choices, p, budget, opts)? {
                return Ok(Some(sol));
            }
        }
        return Ok(None);
    }

    // Fully resolved: gate it.
    *budget -= 1;
    let outcome = gate(data, context, schema, &inst.delta)?;
    if outcome.is_progress() {
        Ok(Some(RepairSolution {
            plan,
            delta: inst.delta,
            outcome,
        }))
    } else {
        Ok(None)
    }
}

/// Existing terms in `data` that satisfy a hole's constraint, plus a fresh node
/// where minting is allowed — truncated to `cap`. The contract places no
/// requirement on where a binding comes from; this is the reuse-first default.
pub fn candidates(constraint: &HoleConstraint, data: &Graph, cap: usize) -> Vec<Term> {
    let mut out = match constraint {
        HoleConstraint::Const(t) => vec![t.clone()],
        HoleConstraint::OneOf(v) => v.clone(),
        HoleConstraint::Fresh => vec![fresh()],
        HoleConstraint::AnyNode => with_fresh(graph_nodes(data)),
        HoleConstraint::ConformsTo(_) | HoleConstraint::ConformsToAll(_) => {
            with_fresh(graph_nodes(data))
        }
        // Infinite domains: reuse only — enumeration cannot invent a novel literal.
        HoleConstraint::Typed(t) => graph_terms(data)
            .into_iter()
            .filter(|x| value_type_holds(t, x))
            .collect(),
        HoleConstraint::Kind(k) => graph_terms(data)
            .into_iter()
            .filter(|x| k.matches(x))
            .collect(),
    };
    out.truncate(cap);
    out
}

fn fresh() -> Term {
    Term::BlankNode(BlankNode::default())
}

fn with_fresh(mut nodes: Vec<Term>) -> Vec<Term> {
    nodes.push(fresh());
    nodes
}

/// All distinct terms in the graph, ordered for determinism.
fn graph_terms(data: &Graph) -> Vec<Term> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for t in data.iter() {
        let s = term_of(t.subject.into_owned());
        if seen.insert(s.clone()) {
            out.push(s);
        }
        let o = t.object.into_owned();
        if seen.insert(o.clone()) {
            out.push(o);
        }
    }
    out.sort_by_key(|t| t.to_string());
    out
}

/// Graph terms usable as nodes (not literals).
fn graph_nodes(data: &Graph) -> Vec<Term> {
    graph_terms(data)
        .into_iter()
        .filter(|t| !matches!(t, Term::Literal(_)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthesize::synthesize;
    use crate::witness::witness_violations;
    use shifty_parse::{load_turtle, parse_turtle};

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix ex:  <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    "#;

    /// witness → synthesize → enumerate the first repair for the sole violation.
    fn repair(ttl: &str) -> (Option<RepairSolution>, Schema, Graph) {
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        let ws = witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).unwrap();
        assert_eq!(ws.len(), 1);
        let tree = synthesize(&parsed.schema.arena, &ws[0]);
        let sol = enumerate_repair(
            &tree,
            &loaded.graph,
            &loaded.graph,
            &parsed.schema,
            EnumOptions::default(),
        )
        .unwrap();
        (sol, parsed.schema, loaded.graph)
    }

    #[test]
    fn min_count_repaired_by_reusing_an_existing_node() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:other ex:y .
            "
        );
        let (sol, _, _) = repair(&ttl);
        let sol = sol.expect("a repair exists");
        assert_eq!(sol.delta.add.len(), 1);
        assert!(sol.outcome.is_progress());
    }

    #[test]
    fn datatype_repaired_by_reusing_an_existing_integer() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            ex:z ex:n 7 .
            "
        );
        let (sol, _, _) = repair(&ttl);
        let sol = sol.expect("reuse the integer 7");
        // replace-in-place: delete the bad value, add a good one.
        assert_eq!(sol.delta.delete.len(), 1);
        assert_eq!(sol.delta.add.len(), 1);
    }

    #[test]
    fn fixpoint_repairs_multiple_violations_to_conformance() {
        // Two foci each missing an ex:p; reuse can satisfy both.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x, ex:y ;
                sh:property [ sh:path ex:p ; sh:minCount 1 ] .
            ex:x ex:other ex:n .
            ex:y ex:other ex:n .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = load_turtle(ttl.as_bytes(), None).unwrap();
        // baseline: two violations.
        assert_eq!(
            witness_violations(&loaded.graph, &loaded.graph, &parsed.schema)
                .unwrap()
                .len(),
            2
        );
        let result = repair_to_fixpoint(
            &loaded.graph,
            &loaded.graph,
            &parsed.schema,
            EnumOptions::default(),
        )
        .unwrap();
        assert_eq!(result.remaining, 0, "repaired to conformance");
        assert_eq!(result.applied.len(), 2, "one repair per focus");
        // the repaired graph genuinely conforms.
        assert!(
            witness_violations(&result.graph, &result.graph, &parsed.schema)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn unsatisfiable_when_no_reusable_value_exists() {
        // datatype integer, but the graph has no integer literal to reuse.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            "
        );
        let (sol, _, _) = repair(&ttl);
        assert!(
            sol.is_none(),
            "no integer to reuse ⇒ enumeration finds nothing"
        );
    }
}
