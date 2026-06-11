//! Stratification analysis over the polarity-aware dependency graph (Layer 4).
//!
//! We condense strongly-connected components and order them by dependency depth
//! (dependees first) — each SCC is one stratum for the fixpoint engine. A
//! recursive SCC is **stratifiable** iff it is *sign-balanced*: no cycle has
//! net-negative (odd) polarity. This is the correct test for our IR, where the
//! `∃≤0 π.¬φ` encoding of `∀` produces paired negative edges that compose to
//! positive (`docs/03-recursion-semantics.md`). A non-stratifiable SCC is a
//! recursion through genuine negation (e.g. `S := ¬∃p.S`) and is reported.

use crate::deps::{dependency_edges, DepEdge};
use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};
use shifty_algebra::{ShapeArena, ShapeId};
use std::collections::{HashMap, HashSet};

/// One stratum: a set of shapes evaluated together as a fixpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stratum {
    pub shapes: Vec<ShapeId>,
    /// True if the SCC has more than one member or a self-loop.
    pub recursive: bool,
    /// True unless this is a recursion through net negation.
    pub stratifiable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stratification {
    /// Strata in evaluation order (dependees first).
    pub strata: Vec<Stratum>,
    /// False iff any recursive SCC is sign-unbalanced.
    pub stratifiable: bool,
}

impl Stratification {
    pub fn shape_count(&self) -> usize {
        self.strata.iter().map(|s| s.shapes.len()).sum()
    }

    pub fn recursive(&self) -> impl Iterator<Item = &Stratum> {
        self.strata.iter().filter(|s| s.recursive)
    }
}

/// Analyze the recursion structure of an arena.
pub fn analyze(arena: &ShapeArena) -> Stratification {
    let edges = dependency_edges(arena);

    let mut graph = DiGraph::<ShapeId, ()>::new();
    let mut node_of: HashMap<ShapeId, NodeIndex> = HashMap::new();
    for i in 0..arena.len() {
        let id = ShapeId(i as u32);
        node_of.insert(id, graph.add_node(id));
    }
    for e in &edges {
        graph.add_edge(node_of[&e.from], node_of[&e.to], ());
    }

    let sccs = tarjan_scc(&graph);

    // scc index per shape
    let mut scc_of: HashMap<ShapeId, usize> = HashMap::new();
    for (i, scc) in sccs.iter().enumerate() {
        for n in scc {
            scc_of.insert(graph[*n], i);
        }
    }

    let self_loops: HashSet<ShapeId> =
        edges.iter().filter(|e| e.from == e.to).map(|e| e.from).collect();

    // depth of each SCC = longest dependency chain to a dependee (dependees = 0)
    let depths = scc_depths(&sccs, &edges, &scc_of);

    let mut indexed: Vec<(usize, Stratum)> = sccs
        .iter()
        .enumerate()
        .map(|(i, scc)| {
            let mut shapes: Vec<ShapeId> = scc.iter().map(|n| graph[*n]).collect();
            shapes.sort();
            let recursive = shapes.len() > 1 || shapes.iter().any(|s| self_loops.contains(s));
            let members: HashSet<ShapeId> = shapes.iter().copied().collect();
            let stratifiable = !recursive || sign_balanced(&members, &edges);
            (depths[i], Stratum { shapes, recursive, stratifiable })
        })
        .collect();

    indexed.sort_by_key(|(d, s)| (*d, s.shapes.first().copied()));
    let strata: Vec<Stratum> = indexed.into_iter().map(|(_, s)| s).collect();
    let stratifiable = strata.iter().all(|s| s.stratifiable);

    Stratification { strata, stratifiable }
}

/// Longest dependency-chain depth of each SCC (an SCC with no out-edges to other
/// SCCs has depth 0; a dependant is one deeper than its deepest dependee).
fn scc_depths(
    sccs: &[Vec<NodeIndex>],
    edges: &[DepEdge],
    scc_of: &HashMap<ShapeId, usize>,
) -> Vec<usize> {
    let mut succ: Vec<HashSet<usize>> = vec![HashSet::new(); sccs.len()];
    for e in edges {
        let (a, b) = (scc_of[&e.from], scc_of[&e.to]);
        if a != b {
            succ[a].insert(b);
        }
    }
    let mut memo = vec![None; sccs.len()];
    for i in 0..sccs.len() {
        depth_of(i, &succ, &mut memo);
    }
    memo.into_iter().map(|d| d.unwrap()).collect()
}

fn depth_of(i: usize, succ: &[HashSet<usize>], memo: &mut [Option<usize>]) -> usize {
    if let Some(d) = memo[i] {
        return d;
    }
    memo[i] = Some(0); // guard (condensation is a DAG, but be safe)
    let d = succ[i]
        .iter()
        .map(|&j| 1 + depth_of(j, succ, memo))
        .max()
        .unwrap_or(0);
    memo[i] = Some(d);
    d
}

/// Is the signed subgraph induced by `members` balanced? Assign each node a sign
/// potential by BFS (`σ(b) = σ(a)·polarity` along every internal edge); a
/// conflict means an odd-polarity cycle exists.
fn sign_balanced(members: &HashSet<ShapeId>, edges: &[DepEdge]) -> bool {
    let mut adj: HashMap<ShapeId, Vec<(ShapeId, i8)>> = HashMap::new();
    for e in edges {
        if members.contains(&e.from) && members.contains(&e.to) {
            let s = e.polarity.sign();
            adj.entry(e.from).or_default().push((e.to, s));
            adj.entry(e.to).or_default().push((e.from, s));
        }
    }

    let mut sigma: HashMap<ShapeId, i8> = HashMap::new();
    for &start in members {
        if sigma.contains_key(&start) {
            continue;
        }
        sigma.insert(start, 1);
        let mut stack = vec![start];
        while let Some(a) = stack.pop() {
            let sa = sigma[&a];
            if let Some(neighbours) = adj.get(&a) {
                for &(b, s) in neighbours {
                    let want = sa * s;
                    match sigma.get(&b) {
                        Some(&sb) if sb != want => return false,
                        Some(_) => {}
                        None => {
                            sigma.insert(b, want);
                            stack.push(b);
                        }
                    }
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_algebra::{NamedNode, Path, Shape};

    fn pred() -> NamedNode {
        NamedNode::new("http://ex/p").unwrap()
    }

    #[test]
    fn positive_recursion_is_stratifiable() {
        // S := ∀p.S  ==  ∃≤0 p.¬S  (sh:node self-reference)
        let mut arena = ShapeArena::new();
        let s = arena.reserve();
        let ns = arena.insert(Shape::Not(s));
        arena.set(
            s,
            Shape::Count { path: Path::Pred(pred()), min: None, max: Some(0), qualifier: ns },
        );

        let strat = analyze(&arena);
        assert!(strat.stratifiable, "∀-recursion must be stratifiable");
        // s and ns form one recursive, balanced SCC
        let rec: Vec<_> = strat.recursive().collect();
        assert_eq!(rec.len(), 1);
        assert!(rec[0].shapes.contains(&s) && rec[0].shapes.contains(&ns));
    }

    #[test]
    fn negation_recursion_is_not_stratifiable() {
        // S := ¬∃p.S  ==  ¬(∃≥1 p.S)
        let mut arena = ShapeArena::new();
        let s = arena.reserve();
        let exists = arena.insert(Shape::Count {
            path: Path::Pred(pred()),
            min: Some(1),
            max: None,
            qualifier: s,
        });
        arena.set(s, Shape::Not(exists));

        let strat = analyze(&arena);
        assert!(!strat.stratifiable, "¬∃-recursion must be flagged");
    }

    #[test]
    fn acyclic_schema_has_no_recursion() {
        let mut arena = ShapeArena::new();
        let a = arena.insert(Shape::TestKind(shifty_algebra::NodeKindSet::IRI));
        let _b = arena.insert(Shape::Not(a));
        let strat = analyze(&arena);
        assert!(strat.stratifiable);
        assert_eq!(strat.recursive().count(), 0);
    }
}
