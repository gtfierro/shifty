use oxigraph::model::{
    BlankNode, Graph, NamedNode, Subject, SubjectRef, Term, TermRef, Triple,
};
use petgraph::algo::is_isomorphic_matching;
use petgraph::graph::{DiGraph, NodeIndex};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

/// Converts an `oxigraph::model::Graph` to a `petgraph::graph::DiGraph`.
///
/// Each unique subject and object in the oxigraph graph becomes a node in the petgraph graph.
/// Each triple becomes a directed edge from the subject node to the object node, with the
/// predicate as the edge weight.
pub fn oxigraph_to_petgraph(ox_graph: &Graph) -> DiGraph<Term, NamedNode> {
    let mut pg_graph = DiGraph::<Term, NamedNode>::new();
    let mut node_map = HashMap::<Term, NodeIndex>::new();

    for triple_ref in ox_graph.iter() {
        let subject_term = Term::from(triple_ref.subject.into_owned());
        let object_term = triple_ref.object.into_owned();
        let predicate = triple_ref.predicate.into_owned();

        let s_node = *node_map
            .entry(subject_term.clone())
            .or_insert_with(|| pg_graph.add_node(subject_term));
        let o_node = *node_map
            .entry(object_term.clone())
            .or_insert_with(|| pg_graph.add_node(object_term));

        pg_graph.add_edge(s_node, o_node, predicate);
    }

    pg_graph
}

/// Checks if two `oxigraph::model::Graph`s are isomorphic.
///
/// This is done by converting both graphs to `petgraph` directed graphs and then
/// using `petgraph::algo::is_isomorphic_matching` to check for isomorphism.
pub fn are_isomorphic(g1: &Graph, g2: &Graph) -> bool {
    let pg1 = oxigraph_to_petgraph(g1);
    let pg2 = oxigraph_to_petgraph(g2);

    is_isomorphic_matching(&pg1, &pg2, |n1, n2| n1 == n2, |e1, e2| e1 == e2)
}

/// Contains the results of a graph diff operation.
pub struct GraphDiff {
    /// Triples that are in both graphs.
    pub in_both: Graph,
    /// Triples that are only in the first graph.
    pub in_first: Graph,
    /// Triples that are only in the second graph.
    pub in_second: Graph,
}

impl GraphDiff {
    pub fn dump(&self) {
        println!("unique to first");
        for triple in self.in_first.iter() {
            println!("{:?}", triple);
        }
        println!("unique to second");
        for triple in self.in_second.iter() {
            println!("{:?}", triple);
        }
    }
}

/// Computes the difference between two graphs, considering blank node isomorphism.
///
/// This function canonicalizes blank nodes in both graphs before comparing them.
/// It returns a `GraphDiff` struct containing three graphs:
/// - `in_both`: triples present in both graphs.
/// - `in_first`: triples present only in the first graph.
/// - `in_second`: triples present only in the second graph.
///
/// Note: The canonicalization algorithm is based on iterative hashing and may not correctly
/// handle all cases of graph symmetry. For most common graphs, it should be reliable.
pub fn graph_diff(g1: &Graph, g2: &Graph) -> GraphDiff {
    let cg1 = to_canonical_graph(g1);
    let cg2 = to_canonical_graph(g2);

    let triples1: HashSet<_> = cg1.iter().map(|t| t.into_owned()).collect();
    let triples2: HashSet<_> = cg2.iter().map(|t| t.into_owned()).collect();

    let mut in_both = Graph::new();
    for t in triples1.intersection(&triples2) {
        in_both.insert(t.as_ref());
    }

    let mut in_first = Graph::new();
    for t in triples1.difference(&triples2) {
        in_first.insert(t.as_ref());
    }

    let mut in_second = Graph::new();
    for t in triples2.difference(&triples1) {
        in_second.insert(t.as_ref());
    }

    GraphDiff {
        in_both,
        in_first,
        in_second,
    }
}

/// Creates a canonical version of a graph by replacing blank node identifiers
/// with deterministic, content-based identifiers.
///
/// This allows for meaningful comparison of graphs that contain blank nodes.
pub fn to_canonical_graph(graph: &Graph) -> Graph {
    let canonicalizer = TripleCanonicalizer::new(graph);
    let bnode_labels = canonicalizer.get_bnode_labels();

    if bnode_labels.is_empty() {
        return graph.clone();
    }

    let mut canonical_graph = Graph::new();
    for t in graph.iter() {
        let subject = match t.subject {
            SubjectRef::BlankNode(bn) => {
                Subject::from(BlankNode::new_unchecked(bnode_labels.get(&bn.into_owned()).unwrap()))
            }
            _ => t.subject.into_owned(),
        };
        let object = match t.object {
            TermRef::BlankNode(bn) => {
                Term::from(BlankNode::new_unchecked(bnode_labels.get(&bn.into_owned()).unwrap()))
            }
            _ => t.object.into_owned(),
        };
        canonical_graph.insert(Triple::new(subject, t.predicate.into_owned(), object).as_ref());
    }
    canonical_graph
}

struct TripleCanonicalizer<'a> {
    graph: &'a Graph,
    bnodes: Vec<BlankNode>,
    // Cache for neighbors to avoid re-querying the graph
    bnode_neighbors: HashMap<BlankNode, Vec<(bool, NamedNode, Term)>>, // bool is is_outgoing
}

impl<'a> TripleCanonicalizer<'a> {
    fn new(graph: &'a Graph) -> Self {
        let subjects: Vec<BlankNode> = graph.iter().filter_map(|t| match t.subject {
            SubjectRef::BlankNode(bn) => Some(bn.into_owned()),
            _ => None,
        }).collect();
        let objects: Vec<BlankNode> = graph.iter().filter_map(|t| match t.object {
            TermRef::BlankNode(bn) => Some(bn.into_owned()),
            _ => None,
        }).collect();
        let bnode_set: HashSet<BlankNode> = subjects.into_iter().chain(objects).collect();

        let bnodes: Vec<BlankNode> = bnode_set.iter().cloned().collect();

        let mut bnode_neighbors = HashMap::new();
        for bn in &bnodes {
            let mut neighbors = Vec::new();
            for t in graph.triples_for_subject(bn.as_ref()) {
                neighbors.push((true, t.predicate.into_owned(), t.object.into_owned()));
            }
            for t in graph.triples_for_object(bn.as_ref()) {
                neighbors.push((
                    false,
                    t.predicate.into_owned(),
                    Term::from(t.subject.into_owned()),
                ));
            }
            bnode_neighbors.insert(bn.clone(), neighbors);
        }

        Self {
            graph,
            bnodes,
            bnode_neighbors,
        }
    }

    fn get_bnode_labels(&self) -> HashMap<BlankNode, String> {
        if self.bnodes.is_empty() {
            return HashMap::new();
        }

        // Initial coloring: all bnodes have the same color.
        let mut bnode_colors: HashMap<BlankNode, String> = self
            .bnodes
            .iter()
            .map(|bn| (bn.clone(), "initial".to_string()))
            .collect();

        // Iteratively refine colors.
        loop {
            let mut next_bnode_colors = HashMap::new();
            let mut changed = false;

            for bn in &self.bnodes {
                let new_color = self.calculate_bnode_color(bn, &bnode_colors);
                if new_color != *bnode_colors.get(bn).unwrap() {
                    changed = true;
                }
                next_bnode_colors.insert(bn.clone(), new_color);
            }

            bnode_colors = next_bnode_colors;
            if !changed {
                break;
            }
        }

        // Note: This implementation does not handle graphs with non-trivial automorphisms
        // (symmetries) that lead to non-discrete partitions of blank nodes after refinement.
        // A full canonicalization algorithm would require a backtracking search to break
        // these symmetries, which is significantly more complex. For many common graphs,
        // this refinement approach is sufficient.

        let mut bnode_labels = HashMap::new();
        for (bn, color_hash) in bnode_colors {
            bnode_labels.insert(bn, format!("cb{}", color_hash));
        }
        bnode_labels
    }

    fn calculate_bnode_color(
        &self,
        bn: &BlankNode,
        current_colors: &HashMap<BlankNode, String>,
    ) -> String {
        let mut color_components = Vec::new();

        // 1. Start with the bnode's current color.
        color_components.push(current_colors.get(bn).unwrap().clone());

        // 2. Add info about neighbors.
        if let Some(neighbors) = self.bnode_neighbors.get(bn) {
            let mut neighbor_signatures = Vec::new();
            for (is_outgoing, p, other_term) in neighbors {
                let other_color = self.get_term_color(other_term, current_colors);
                let direction = if *is_outgoing { "out" } else { "in" };
                neighbor_signatures.push(format!("{}:{}:{}", direction, p.as_str(), other_color));
            }
            // Sort to make the signature canonical
            neighbor_signatures.sort();
            color_components.extend(neighbor_signatures);
        }

        // Hash the components to get the new color.
        let mut hasher = Sha256::new();
        for component in color_components {
            hasher.update(component.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    fn get_term_color(&self, term: &Term, bnode_colors: &HashMap<BlankNode, String>) -> String {
        match term {
            Term::BlankNode(bn) => bnode_colors.get(bn).unwrap().clone(),
            _ => {
                // For non-bnodes, their "color" is a hash of their N-Triples representation.
                let mut hasher = Sha256::new();
                hasher.update(term.to_string().as_bytes());
                format!("{:x}", hasher.finalize())
            }
        }
    }
}
