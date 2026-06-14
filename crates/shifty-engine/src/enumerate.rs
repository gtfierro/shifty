//! The enumeration reference driver (`docs/07-repair-drivers.md` §4) and the
//! `candidates` helper (`docs/06-repair.md` §3.3).
//!
//! This is the simplest driver: it walks the [`RepairTree`]'s finite choices —
//! `Any` branches, `Repeat` counts, and per-hole candidate terms drawn from the
//! data graph (reuse) or minted fresh — and returns the first plan whose `ΔG`
//! passes the [`gate`]. It is primarily a correctness check on the IR and the
//! gate; it cannot invent novel literals (those holes are filled only by reuse).
//!
//! Like every driver, it decides nothing the library doesn't expose: it only
//! composes [`instantiate`], [`candidates`], and [`gate`].

use crate::gate::{RepairOutcome, gate};
use crate::path::term_of;
use crate::validate::NonStratifiable;
use crate::value::value_type_holds;
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

/// Search for the first sound, progress-making repair of `tree` against
/// `(data, schema)`. `Ok(None)` means none was found within the budget.
pub fn enumerate_repair(
    tree: &RepairTree,
    data: &Graph,
    schema: &Schema,
    opts: EnumOptions,
) -> Result<Option<RepairSolution>, NonStratifiable> {
    let mut choices = HashMap::new();
    index_choices(tree, &mut choices);
    let mut budget = opts.budget;
    solve(tree, data, schema, &choices, Plan::default(), &mut budget, opts)
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

fn solve(
    tree: &RepairTree,
    data: &Graph,
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
                    if let Some(sol) = solve(tree, data, schema, choices, p, budget, opts)? {
                        return Ok(Some(sol));
                    }
                }
                Ok(None)
            }
            Some(Choice::Repeat(count)) => {
                let mut p = plan;
                p.count.insert(node, *count);
                solve(tree, data, schema, choices, p, budget, opts)
            }
            None => Ok(None),
        };
    }

    if let Some((hole, constraint)) = inst.open_holes.first().cloned() {
        for cand in candidates(&constraint, data, opts.max_candidates) {
            let mut p = plan.clone();
            p.binding.insert(hole, cand);
            if let Some(sol) = solve(tree, data, schema, choices, p, budget, opts)? {
                return Ok(Some(sol));
            }
        }
        return Ok(None);
    }

    // Fully resolved: gate it.
    *budget -= 1;
    let outcome = gate(data, schema, &inst.delta)?;
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
        HoleConstraint::ConformsTo(_) => with_fresh(graph_nodes(data)),
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
        let ws = witness_violations(&loaded.graph, &parsed.schema).unwrap();
        assert_eq!(ws.len(), 1);
        let tree = synthesize(&parsed.schema.arena, &ws[0]);
        let sol = enumerate_repair(&tree, &loaded.graph, &parsed.schema, EnumOptions::default())
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
        assert!(sol.is_none(), "no integer to reuse ⇒ enumeration finds nothing");
    }
}
