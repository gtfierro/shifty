use oxigraph::model::{
    BlankNode, Graph, GraphNameRef, NamedNode, Quad, Subject, SubjectRef, Term, TermRef, Triple,
};
use crate::components::ToSubjectRef;
use oxigraph::store::{StorageError, Store};
use petgraph::algo::{is_isomorphic, is_isomorphic_matching, is_isomorphic_subgraph};
use petgraph::graph::{DiGraph, NodeIndex};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;

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

fn node_equality(n1: &Term, n2: &Term) -> bool {
    match (n1, n2) {
        (Term::BlankNode(_), Term::BlankNode(_)) => true,
        (Term::BlankNode(_), Term::NamedNode(_)) => n1 == n2,
        (Term::NamedNode(_), Term::BlankNode(_)) => n1 == n2,
        (n1, n2) => n1 == n2,
    }
}

/// Checks if two `oxigraph::model::Graph`s are isomorphic.
///
/// This is done by converting both graphs to `petgraph` directed graphs and then
/// using `petgraph::algo::is_isomorphic_matching` to check for isomorphism.
pub fn are_isomorphic(g1: &Graph, g2: &Graph) -> bool {
    let pg1 = oxigraph_to_petgraph(g1);
    let pg2 = oxigraph_to_petgraph(g2);

    //is_isomorphic_matching(&pg1, &pg2, |n1, n2| node_equality(n1, n2), |e1, e2| e1 == e2)
    //is_isomorphic_subgraph(&pg1, &pg2) && is_isomorphic_subgraph(&pg2, &pg1)
    is_isomorphic(&pg1, &pg2)
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
    let mut canonicalizer = TripleCanonicalizer::new(graph);
    let bnode_labels = canonicalizer.get_bnode_labels();

    if bnode_labels.is_empty() {
        return graph.clone();
    }

    let mut canonical_graph = Graph::new();
    for t in graph.iter() {
        let subject = match t.subject {
            SubjectRef::BlankNode(bn) => {
                let owned_bn = bn.into_owned();
                let label = bnode_labels.get(&owned_bn).unwrap();
                Subject::from(BlankNode::new_unchecked(format!("cb{}", label)))
            }
            _ => t.subject.into_owned(),
        };
        let object = match t.object {
            TermRef::BlankNode(bn) => {
                let owned_bn = bn.into_owned();
                let label = bnode_labels.get(&owned_bn).unwrap();
                Term::from(BlankNode::new_unchecked(format!("cb{}", label)))
            }
            _ => t.object.into_owned(),
        };
        canonical_graph.insert(Triple::new(subject, t.predicate.into_owned(), object).as_ref());
    }
    canonical_graph
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Partition {
    nodes: Vec<Term>,
    hash: String,
}

impl Ord for Partition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hash
            .cmp(&other.hash)
            .then_with(|| self.nodes.len().cmp(&other.nodes.len()))
    }
}

impl PartialOrd for Partition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct TripleCanonicalizer<'a> {
    graph: &'a Graph,
    hash_cache: HashMap<String, String>,
}

impl<'a> TripleCanonicalizer<'a> {
    fn new(graph: &'a Graph) -> Self {
        Self {
            graph,
            hash_cache: HashMap::new(),
        }
    }

    fn get_bnode_labels(&mut self) -> HashMap<BlankNode, String> {
        let bnodes: HashSet<BlankNode> = self
            .graph
            .iter()
            .flat_map(|t| {
                let mut terms = vec![];
                if let SubjectRef::BlankNode(bn) = t.subject {
                    terms.push(bn.into_owned());
                }
                if let TermRef::BlankNode(bn) = t.object {
                    terms.push(bn.into_owned());
                }
                terms
            })
            .collect();

        if bnodes.is_empty() {
            return HashMap::new();
        }

        let mut coloring = self.initial_color(&bnodes);
        coloring = self.refine(coloring.clone());

        if !self.is_discrete(&coloring) {
            coloring = self.traces(coloring);
        }

        let mut bnode_labels = HashMap::new();
        for partition in coloring {
            if let Term::BlankNode(bn) = &partition.nodes[0] {
                bnode_labels.insert(bn.clone(), partition.hash);
            }
        }
        bnode_labels
    }

    fn hash(&mut self, data: &str) -> String {
        if let Some(hash) = self.hash_cache.get(data) {
            return hash.clone();
        }
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        self.hash_cache.insert(data.to_string(), hash.clone());
        hash
    }

    fn initial_color(&mut self, bnodes: &HashSet<BlankNode>) -> Vec<Partition> {
        let mut partitions = Vec::new();
        let mut bnode_terms: Vec<Term> = bnodes.iter().cloned().map(Term::from).collect();
        bnode_terms.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        partitions.push(Partition {
            nodes: bnode_terms,
            hash: self.hash("initial"),
        });

        let mut non_bnodes = HashSet::new();
        for bn in bnodes {
            for t in self.graph.triples_for_subject(bn.as_ref()) {
                if !matches!(t.object, TermRef::BlankNode(_)) {
                    non_bnodes.insert(t.object.into_owned());
                }
            }
            for t in self.graph.triples_for_object(bn.as_ref()) {
                if !matches!(t.subject, SubjectRef::BlankNode(_)) {
                    non_bnodes.insert(t.subject.into_owned().into());
                }
            }
        }

        let mut sorted_non_bnodes: Vec<Term> = non_bnodes.into_iter().collect();
        sorted_non_bnodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));

        for term in sorted_non_bnodes {
            partitions.push(Partition {
                hash: self.hash(&term.to_string()),
                nodes: vec![term],
            });
        }

        partitions.sort();
        partitions
    }

    fn refine(&mut self, mut coloring: Vec<Partition>) -> Vec<Partition> {
        let mut worklist = coloring.clone();
        while let Some(w) = worklist.pop() {
            let mut next_coloring = Vec::new();
            let mut changed = false;
            for c in &coloring {
                if c.nodes.len() > 1 {
                    let new_partitions = self.distinguish(c, &w);
                    if new_partitions.len() > 1 {
                        changed = true;
                        for p in new_partitions {
                            if p.nodes.len() < c.nodes.len() {
                                worklist.push(p.clone());
                            }
                            next_coloring.push(p);
                        }
                    } else {
                        next_coloring.extend(new_partitions);
                    }
                } else {
                    next_coloring.push(c.clone());
                }
            }
            if changed {
                next_coloring.sort();
                coloring = next_coloring;
            }
        }
        coloring
    }

    fn distinguish(&mut self, c: &Partition, w: &Partition) -> Vec<Partition> {
        let mut new_hashes: BTreeMap<String, Vec<Term>> = BTreeMap::new();
        for n in &c.nodes {
            let mut signature_parts = vec![c.hash.clone()];
            for w_node in &w.nodes {
                // Case where n is a subject and w_node is an object
                if let Ok(subject_ref) = n.try_to_subject_ref() {
                    for t in self.graph.triples_for_subject(subject_ref) {
                        if t.object == w_node.as_ref() {
                            signature_parts.push(format!("out:{}", t.predicate));
                        }
                    }
                }

                // Case where w_node is a subject and n is an object
                if let Ok(w_subject_ref) = n.try_to_subject_ref() {
                    for t in self.graph.triples_for_subject(w_subject_ref) {
                        if t.object == n.as_ref() {
                            signature_parts.push(format!("in:{}", t.predicate));
                        }
                    }
                }
            }
            signature_parts.sort();
            let new_hash = self.hash(&signature_parts.join(""));
            new_hashes.entry(new_hash).or_default().push(n.clone());
        }

        new_hashes
            .into_iter()
            .map(|(hash, mut nodes)| {
                nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                Partition { nodes, hash }
            })
            .collect()
    }

    fn traces(&mut self, coloring: Vec<Partition>) -> Vec<Partition> {
        if self.is_discrete(&coloring) {
            return coloring;
        }

        let candidates = self.get_candidates(&coloring);
        let mut best_coloring: Option<Vec<Partition>> = None;

        for (candidate_node, p_index) in candidates {
            let mut coloring_copy = coloring.clone();
            let _new_partition = self.individuate(&mut coloring_copy, &candidate_node, p_index);

            let mut refined = self.refine(coloring_copy);
            if !self.is_discrete(&refined) {
                refined = self.traces(refined);
            }

            if let Some(best) = &best_coloring {
                if refined > *best {
                    best_coloring = Some(refined);
                }
            } else {
                best_coloring = Some(refined);
            }
        }
        best_coloring.unwrap()
    }

    fn individuate(
        &mut self,
        coloring: &mut Vec<Partition>,
        node: &Term,
        p_index: usize,
    ) -> Partition {
        let p = &mut coloring[p_index];
        p.nodes.retain(|n| n != node);

        let new_hash = self.hash(&format!("individual:{}", p.hash));
        let new_partition = Partition {
            nodes: vec![node.clone()],
            hash: new_hash,
        };
        coloring.push(new_partition.clone());
        coloring.sort();
        new_partition
    }

    fn get_candidates(&self, coloring: &[Partition]) -> Vec<(Term, usize)> {
        let mut candidates = Vec::new();
        for (i, p) in coloring.iter().enumerate() {
            if p.nodes.len() > 1 {
                // To ensure determinism, we select the first node from the sorted list.
                candidates.push((p.nodes[0].clone(), i));
                break; // Only need to break one symmetry at a time.
            }
        }
        candidates
    }

    fn is_discrete(&self, coloring: &[Partition]) -> bool {
        coloring.iter().all(|p| p.nodes.len() <= 1)
    }
}

/// Replaces all blank nodes in a given graph within the store with unique IRIs (Skolemization).
///
/// A `base_iri` is used to construct the new IRIs. For each blank node, a new IRI is generated
/// by appending its identifier to the `base_iri`. This process is often called Skolemization.
///
/// The replacement is done within a single transaction to ensure atomicity. The skolemization is
/// deterministic: the same blank node identifier will always be mapped to the same IRI for a given
/// base IRI.
///
/// # Arguments
///
/// * `store` - The `oxigraph::store::Store` containing the graph to modify.
/// * `graph_name` - The name of the graph to perform skolemization on.
/// * `base_iri` - A base IRI to use for generating new skolem IRIs. It should probably end with a `/` or `#`.
///
/// # Errors
///
/// Returns a `StorageError` if there are issues with the underlying store during the transaction.
pub fn skolemize(
    store: &Store,
    graph_name: GraphNameRef,
    base_iri: &str,
) -> Result<(), StorageError> {
    let mut bnodes_to_skolemize = HashMap::<BlankNode, NamedNode>::new();
    let mut quads_to_remove = Vec::<Quad>::new();
    let mut quads_to_add = Vec::<Quad>::new();

    let quads_in_graph: Vec<Quad> = store
        .quads_for_pattern(None, None, None, Some(graph_name))
        .collect::<Result<Vec<_>, _>>()?;

    for quad in &quads_in_graph {
        let mut has_bnode = false;

        if let Subject::BlankNode(_) = &quad.subject {
            has_bnode = true;
        }
        if let Term::BlankNode(_) = &quad.object {
            has_bnode = true;
        }

        if has_bnode {
            quads_to_remove.push(quad.clone());

            let new_subject = if let Subject::BlankNode(bn) = &quad.subject {
                let skolem_iri = bnodes_to_skolemize.entry(bn.clone()).or_insert_with(|| {
                    NamedNode::new_unchecked(format!("{}{}", base_iri, bn.as_str()))
                });
                Subject::from(skolem_iri.clone())
            } else {
                quad.subject.clone()
            };

            let new_object = if let Term::BlankNode(bn) = &quad.object {
                let skolem_iri = bnodes_to_skolemize.entry(bn.clone()).or_insert_with(|| {
                    NamedNode::new_unchecked(format!("{}{}", base_iri, bn.as_str()))
                });
                Term::from(skolem_iri.clone())
            } else {
                quad.object.clone()
            };

            quads_to_add.push(Quad::new(
                new_subject,
                quad.predicate.clone(),
                new_object,
                quad.graph_name.clone(),
            ));
        }
    }

    if quads_to_add.is_empty() {
        return Ok(()); // Nothing to do
    }

    store.transaction(|mut transaction| {
        for quad in &quads_to_remove {
            transaction.remove(quad.as_ref())?;
        }
        for quad in &quads_to_add {
            transaction.insert(quad.as_ref())?;
        }
        Ok(())
    })
}
