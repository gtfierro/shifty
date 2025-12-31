use crate::skolem::SKOLEM_MARKER;
use log::debug;
use oxigraph::model::{
    BlankNode, Graph, GraphNameRef, NamedNode, NamedOrBlankNode as Subject,
    NamedOrBlankNodeRef as SubjectRef, Quad, Term, TermRef, Triple,
};
use oxigraph::store::{StorageError, Store};
use petgraph::graph::{DiGraph, NodeIndex};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Converts an `oxigraph::model::Graph` to a `petgraph::graph::DiGraph`.
///
/// Each unique subject and object in the oxigraph graph becomes a node in the petgraph graph.
/// Each triple becomes a directed edge from the subject node to the object node, with the
/// predicate as the edge weight.
#[allow(dead_code)]
pub(crate) fn oxigraph_to_petgraph(ox_graph: &Graph) -> DiGraph<Term, NamedNode> {
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

/// Checks if two `oxigraph::model::Graph`s are isomorphic (blank-node aware).
///
/// We canonicalize both graphs using RDFC-1.0 and then compare them as
/// sets of triples. If the canonicalization is correct, simple set-equality
/// is sufficient to decide isomorphism for single-graph datasets.
pub fn are_isomorphic(g1: &Graph, g2: &Graph) -> bool {
    let cg1 = to_canonical_graph(g1);
    let cg2 = to_canonical_graph(g2);

    if cg1.len() != cg2.len() {
        debug!(
            "graphs not isomorphic: different triple counts ({} vs {})",
            cg1.len(),
            cg2.len()
        );
        log_diff(&cg1, &cg2);
        return false;
    }

    // Check set equality
    let all_in = cg1.iter().all(|t| cg2.contains(t));
    if !all_in {
        debug!("graphs not isomorphic after canonicalization (set mismatch)");
        log_diff(&cg1, &cg2);
        return false;
    }

    true
}

/// Logs a human-readable diff between two graphs (triples present in one but not the other).
fn log_diff(g1: &Graph, g2: &Graph) {
    debug!("-- only in first graph --");
    for t in g1.iter() {
        if !g2.contains(t) {
            debug!("only in first: {} {} {}", t.subject, t.predicate, t.object);
        }
    }
    debug!("-- only in second graph --");
    for t in g2.iter() {
        if !g1.contains(t) {
            debug!("only in second: {} {} {}", t.subject, t.predicate, t.object);
        }
    }
}

/// Creates a canonical version of a graph by replacing blank node identifiers
/// with deterministic, content-based identifiers according to RDFC-1.0.
///
/// This allows for meaningful comparison of graphs that contain blank nodes.
pub(crate) fn to_canonical_graph(graph: &Graph) -> Graph {
    let bnode_labels = rdfc10::canonicalize(graph);

    if bnode_labels.is_empty() {
        return graph.clone();
    }

    let mut canonical_graph = Graph::new();
    for t in graph.iter() {
        let subject = match t.subject {
            SubjectRef::BlankNode(bn) => {
                let bn = bn.into_owned();
                let label = bnode_labels
                    .get(&bn)
                    .unwrap_or_else(|| panic!("No canonical label for blank node {}", bn.as_str()));
                Subject::from(BlankNode::new_unchecked(label))
            }
            _ => t.subject.into_owned(),
        };
        let object = match t.object {
            TermRef::BlankNode(bn) => {
                let bn = bn.into_owned();
                let label = bnode_labels
                    .get(&bn)
                    .unwrap_or_else(|| panic!("No canonical label for blank node {}", bn.as_str()));
                Term::from(BlankNode::new_unchecked(label))
            }
            _ => t.object.into_owned(),
        };
        canonical_graph.insert(Triple::new(subject, t.predicate.into_owned(), object).as_ref());
    }
    canonical_graph
}

mod rdfc10 {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[derive(Clone, Debug)]
    pub(super) struct IdentifierIssuer {
        prefix: String,
        counter: u64,
        issued_identifiers: BTreeMap<String, String>,
    }

    impl IdentifierIssuer {
        fn new(prefix: &str) -> Self {
            Self {
                prefix: prefix.to_string(),
                counter: 0,
                issued_identifiers: BTreeMap::new(),
            }
        }

        fn issue(&mut self, existing_identifier: &str) -> &str {
            self.issued_identifiers
                .entry(existing_identifier.to_string())
                .or_insert_with(|| {
                    let new_id = format!("{}{}", self.prefix, self.counter);
                    self.counter += 1;
                    new_id
                })
        }

        fn get(&self, existing_identifier: &str) -> Option<&str> {
            self.issued_identifiers
                .get(existing_identifier)
                .map(|s| s.as_str())
        }

        fn has_identifier_for(&self, existing_identifier: &str) -> bool {
            self.issued_identifiers.contains_key(existing_identifier)
        }
    }

    fn hash(data: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn serialize_term_for_hashing(term: &Term) -> String {
        term.to_string()
    }

    fn serialize_subject_for_hashing(subject: &Subject) -> String {
        subject.to_string()
    }

    fn serialize_triple_for_hashing(
        triple: &Triple,
        reference_bn_id: &str,
        other_bn_char: &str,
    ) -> String {
        let s = match &triple.subject {
            Subject::BlankNode(bn) if bn.as_str() == reference_bn_id => "_:a".to_string(),
            Subject::BlankNode(_) => format!("_:{}", other_bn_char),
            _ => serialize_subject_for_hashing(&triple.subject),
        };
        let p = serialize_term_for_hashing(&Term::NamedNode(triple.predicate.clone()));
        let o = match &triple.object {
            Term::BlankNode(bn) if bn.as_str() == reference_bn_id => "_:a".to_string(),
            Term::BlankNode(_) => format!("_:{}", other_bn_char),
            _ => serialize_term_for_hashing(&triple.object),
        };
        format!("{} {} {} .", s, p, o)
    }

    fn hash_first_degree_triples(
        bnode_id: &str,
        bnode_to_triples: &HashMap<String, Vec<Triple>>,
    ) -> String {
        let mut nquads = Vec::new();
        if let Some(triples) = bnode_to_triples.get(bnode_id) {
            for triple in triples {
                nquads.push(serialize_triple_for_hashing(triple, bnode_id, "z"));
            }
        }
        nquads.sort();
        hash(&nquads.join("\n"))
    }

    fn hash_related_blank_node(
        related_bnode_id: &str,
        triple: &Triple,
        position: char,
        first_degree_hashes: &HashMap<String, String>,
        canonical_issuer: &IdentifierIssuer,
        path_issuer: &IdentifierIssuer,
    ) -> String {
        let mut input = format!("{}<{}", position, triple.predicate);
        input.push('>');

        if let Some(id) = canonical_issuer.get(related_bnode_id) {
            input.push_str("_:");
            input.push_str(id);
        } else if let Some(id) = path_issuer.get(related_bnode_id) {
            input.push_str("_:");
            input.push_str(id);
        } else {
            input.push_str(first_degree_hashes.get(related_bnode_id).unwrap());
        }
        hash(&input)
    }

    fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
        if items.len() > 8 {
            panic!("Too many permutations to calculate, aborting.");
        }
        if items.is_empty() {
            return vec![vec![]];
        }
        let first = &items[0];
        let rest = &items[1..];
        let perms_rest = permutations(rest);
        let mut all_perms = Vec::new();
        for p in perms_rest {
            for i in 0..=p.len() {
                let mut new_p = p.clone();
                new_p.insert(i, first.clone());
                all_perms.push(new_p);
            }
        }
        all_perms
    }

    #[allow(clippy::too_many_arguments)]
    fn hash_n_degree_triples(
        bnode_id: &str,
        bnode_to_triples: &HashMap<String, Vec<Triple>>,
        first_degree_hashes: &HashMap<String, String>,
        canonical_issuer: &IdentifierIssuer,
        mut path_issuer: IdentifierIssuer,
    ) -> (String, IdentifierIssuer) {
        let mut h_n = BTreeMap::<String, Vec<String>>::new();
        if let Some(triples) = bnode_to_triples.get(bnode_id) {
            for triple in triples {
                if let Subject::BlankNode(s_bn) = &triple.subject {
                    if s_bn.as_str() != bnode_id {
                        let related_hash = hash_related_blank_node(
                            s_bn.as_str(),
                            triple,
                            's',
                            first_degree_hashes,
                            canonical_issuer,
                            &path_issuer,
                        );
                        h_n.entry(related_hash)
                            .or_default()
                            .push(s_bn.as_str().to_string());
                    }
                }
                if let Term::BlankNode(o_bn) = &triple.object {
                    if o_bn.as_str() != bnode_id {
                        let related_hash = hash_related_blank_node(
                            o_bn.as_str(),
                            triple,
                            'o',
                            first_degree_hashes,
                            canonical_issuer,
                            &path_issuer,
                        );
                        h_n.entry(related_hash)
                            .or_default()
                            .push(o_bn.as_str().to_string());
                    }
                }
            }
        }

        let mut data_to_hash = String::new();
        for (related_hash, bnode_list) in h_n {
            data_to_hash.push_str(&related_hash);

            let mut chosen_path = String::new();
            let mut chosen_issuer = None;

            for p in permutations(&bnode_list) {
                let mut issuer_copy = path_issuer.clone();
                let mut path = String::new();
                let mut recursion_list = Vec::new();

                for related in &p {
                    if canonical_issuer.has_identifier_for(related) {
                        path.push_str("_:");
                        path.push_str(canonical_issuer.get(related).unwrap());
                    } else {
                        if !issuer_copy.has_identifier_for(related) {
                            recursion_list.push(related.clone());
                        }
                        path.push_str("_:");
                        path.push_str(issuer_copy.issue(related));
                    }
                }

                for related in recursion_list {
                    let (hash, new_issuer) = hash_n_degree_triples(
                        &related,
                        bnode_to_triples,
                        first_degree_hashes,
                        canonical_issuer,
                        issuer_copy,
                    );
                    issuer_copy = new_issuer;
                    path.push_str("_:");
                    path.push_str(issuer_copy.get(&related).unwrap());
                    path.push('<');
                    path.push_str(&hash);
                    path.push('>');
                }

                if chosen_issuer.is_none() || path < chosen_path {
                    chosen_path = path;
                    chosen_issuer = Some(issuer_copy);
                }
            }
            data_to_hash.push_str(&chosen_path);
            path_issuer = chosen_issuer.unwrap();
        }

        (hash(&data_to_hash), path_issuer)
    }

    pub(super) fn canonicalize(graph: &Graph) -> HashMap<BlankNode, String> {
        let mut bnode_to_triples = HashMap::<String, Vec<Triple>>::new();
        let mut bnodes = HashSet::<String>::new();

        for t_ref in graph.iter() {
            let triple = t_ref.into_owned();
            if let Subject::BlankNode(bn) = &triple.subject {
                let id = bn.as_str().to_string();
                bnodes.insert(id.clone());
                bnode_to_triples.entry(id).or_default().push(triple.clone());
            }
            if let Term::BlankNode(bn) = &triple.object {
                let id = bn.as_str().to_string();
                bnodes.insert(id.clone());
                bnode_to_triples.entry(id).or_default().push(triple.clone());
            }
        }

        if bnodes.is_empty() {
            return HashMap::new();
        }

        let mut first_degree_hashes = HashMap::<String, String>::new();
        let mut hash_to_bnodes = BTreeMap::<String, Vec<String>>::new();
        for id in &bnodes {
            let hash = hash_first_degree_triples(id, &bnode_to_triples);
            first_degree_hashes.insert(id.clone(), hash.clone());
            hash_to_bnodes.entry(hash).or_default().push(id.clone());
        }

        for bnode_list in hash_to_bnodes.values_mut() {
            bnode_list.sort();
        }

        let mut canonical_issuer = IdentifierIssuer::new("c14n");
        let mut hashes_to_process = Vec::new();
        for (hash, bnode_list) in &hash_to_bnodes {
            if bnode_list.len() == 1 {
                canonical_issuer.issue(&bnode_list[0]);
            } else {
                hashes_to_process.push(hash.clone());
            }
        }
        hashes_to_process.sort();

        for hash in hashes_to_process {
            if let Some(bnode_list) = hash_to_bnodes.get(&hash) {
                let mut hash_path_list = Vec::new();
                for bnode_id in bnode_list {
                    if canonical_issuer.has_identifier_for(bnode_id) {
                        continue;
                    }
                    let mut temporary_issuer = IdentifierIssuer::new("b");
                    temporary_issuer.issue(bnode_id);
                    let (n_degree_hash, final_issuer) = hash_n_degree_triples(
                        bnode_id,
                        &bnode_to_triples,
                        &first_degree_hashes,
                        &canonical_issuer,
                        temporary_issuer,
                    );
                    hash_path_list.push((n_degree_hash, final_issuer));
                }

                hash_path_list.sort_by(|(h1, _), (h2, _)| h1.cmp(h2));

                for (_, issuer) in hash_path_list {
                    let mut ids_to_issue: Vec<_> =
                        issuer.issued_identifiers.keys().cloned().collect();
                    ids_to_issue.sort_by(|a, b| issuer.get(a).unwrap().cmp(issuer.get(b).unwrap()));
                    for id in ids_to_issue {
                        if !canonical_issuer.has_identifier_for(&id) {
                            canonical_issuer.issue(&id);
                        }
                    }
                }
            }
        }

        let mut result = HashMap::new();
        for (id_str, canonical_id) in canonical_issuer.issued_identifiers {
            result.insert(BlankNode::new_unchecked(id_str), canonical_id);
        }
        result
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
pub(crate) fn skolemize(
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
                    debug!("skolemizing subject {}{}", base_iri, bn.as_str());
                    NamedNode::new_unchecked(format!("{}{}", base_iri, bn.as_str()))
                });
                Subject::from(skolem_iri.clone())
            } else {
                quad.subject.clone()
            };

            let new_object = if let Term::BlankNode(bn) = &quad.object {
                let skolem_iri = bnodes_to_skolemize.entry(bn.clone()).or_insert_with(|| {
                    debug!("skolemizing object {}{}", base_iri, bn.as_str());
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

    let mut transaction = store.start_transaction()?;
    for quad in &quads_to_remove {
        transaction.remove(quad.as_ref());
    }
    for quad in &quads_to_add {
        transaction.insert(quad.as_ref());
    }
    transaction.commit()?;
    Ok(())
}

/// Replaces skolem IRIs in a graph with blank nodes (Deskolemization).
///
/// This is the reverse operation of `skolemize`. It looks for IRIs that start with
/// a given `base_iri` and replaces them with blank nodes. The identifier for the
/// new blank node is taken from the part of the IRI that follows the `base_iri`.
///
/// # Arguments
///
/// * `graph` - The `oxigraph::model::Graph` to perform deskolemization on.
/// * `base_iri` - The base IRI that was used for generating skolem IRIs.
///
/// # Returns
///
/// A new `Graph` with skolem IRIs replaced by blank nodes.
pub fn deskolemize_graph(graph: &Graph, base_iri: &str) -> Graph {
    let mut skolem_iris_to_bnode = HashMap::<NamedNode, BlankNode>::new();
    let mut new_graph = Graph::new();

    fn bnode_suffix<'a>(iri: &'a str, base_iri: &str) -> Option<&'a str> {
        if !base_iri.is_empty() && iri.starts_with(base_iri) {
            return Some(&iri[base_iri.len()..]);
        }

        iri.find(SKOLEM_MARKER)
            .map(|idx| &iri[idx + SKOLEM_MARKER.len()..])
    }

    for triple in graph.iter() {
        let new_subject = if let SubjectRef::NamedNode(nn) = triple.subject {
            match bnode_suffix(nn.as_str(), base_iri) {
                Some(id) => {
                    let bnode = skolem_iris_to_bnode
                        .entry(nn.into_owned())
                        .or_insert_with(|| BlankNode::new_unchecked(id));
                    Subject::from(bnode.clone())
                }
                None => triple.subject.into_owned(),
            }
        } else {
            triple.subject.into_owned()
        };

        let new_object = if let TermRef::NamedNode(nn) = triple.object {
            match bnode_suffix(nn.as_str(), base_iri) {
                Some(id) => {
                    let bnode = skolem_iris_to_bnode
                        .entry(nn.into_owned())
                        .or_insert_with(|| BlankNode::new_unchecked(id));
                    Term::from(bnode.clone())
                }
                None => triple.object.into_owned(),
            }
        } else {
            triple.object.into_owned()
        };

        new_graph
            .insert(Triple::new(new_subject, triple.predicate.into_owned(), new_object).as_ref());
    }
    new_graph
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxigraph::model::vocab::rdf;
    use oxigraph::model::{BlankNode, NamedNode, NamedOrBlankNode as Subject, Term, Triple};

    fn iri(s: &str) -> NamedNode {
        NamedNode::new_unchecked(s)
    }

    #[test]
    fn are_isomorphic_simple_blank_nodes() {
        // g1: _:a ex:p _:b
        let mut g1 = Graph::new();
        let a = BlankNode::new_unchecked("a");
        let b = BlankNode::new_unchecked("b");
        let p = iri("http://example.org/p");
        g1.insert(Triple::new(Subject::from(a.clone()), p.clone(), Term::from(b.clone())).as_ref());

        // g2: _:x ex:p _:y
        let mut g2 = Graph::new();
        let x = BlankNode::new_unchecked("x");
        let y = BlankNode::new_unchecked("y");
        g2.insert(Triple::new(Subject::from(x), p, Term::from(y)).as_ref());

        assert!(are_isomorphic(&g1, &g2));
    }

    #[test]
    fn are_isomorphic_rdf_list_two_cells() {
        // g1: _:a rdf:first ex:p ; rdf:rest _:b .
        //     _:b rdf:first ex:q ; rdf:rest rdf:nil .
        let mut g1 = Graph::new();
        let a = BlankNode::new_unchecked("a");
        let b = BlankNode::new_unchecked("b");
        let p = iri("http://example.org/p");
        let q = iri("http://example.org/q");

        g1.insert(
            Triple::new(Subject::from(a.clone()), rdf::FIRST, Term::from(p.clone())).as_ref(),
        );
        g1.insert(Triple::new(Subject::from(a.clone()), rdf::REST, Term::from(b.clone())).as_ref());
        g1.insert(
            Triple::new(Subject::from(b.clone()), rdf::FIRST, Term::from(q.clone())).as_ref(),
        );
        g1.insert(Triple::new(Subject::from(b.clone()), rdf::REST, Term::from(rdf::NIL)).as_ref());

        // g2 with different blank node ids: _:x/_:y but same structure
        let mut g2 = Graph::new();
        let x = BlankNode::new_unchecked("x");
        let y = BlankNode::new_unchecked("y");
        g2.insert(Triple::new(Subject::from(x.clone()), rdf::FIRST, Term::from(p)).as_ref());
        g2.insert(Triple::new(Subject::from(x.clone()), rdf::REST, Term::from(y.clone())).as_ref());
        g2.insert(Triple::new(Subject::from(y.clone()), rdf::FIRST, Term::from(q)).as_ref());
        g2.insert(Triple::new(Subject::from(y.clone()), rdf::REST, Term::from(rdf::NIL)).as_ref());

        assert!(are_isomorphic(&g1, &g2));
    }

    #[test]
    fn not_isomorphic_different_predicate() {
        // g1: _:a ex:p _:b
        let mut g1 = Graph::new();
        let a = BlankNode::new_unchecked("a");
        let b = BlankNode::new_unchecked("b");
        let p = iri("http://example.org/p");
        g1.insert(Triple::new(Subject::from(a.clone()), p, Term::from(b.clone())).as_ref());

        // g2: _:x ex:q _:y (different predicate)
        let mut g2 = Graph::new();
        let x = BlankNode::new_unchecked("x");
        let y = BlankNode::new_unchecked("y");
        let q = iri("http://example.org/q");
        g2.insert(Triple::new(Subject::from(x), q, Term::from(y)).as_ref());

        assert!(!are_isomorphic(&g1, &g2));
    }

    #[test]
    fn not_isomorphic_different_list_length() {
        // g1: list of length 2
        let mut g1 = Graph::new();
        let a1 = BlankNode::new_unchecked("a1");
        let a2 = BlankNode::new_unchecked("a2");
        let p = iri("http://example.org/p");
        let q = iri("http://example.org/q");
        g1.insert(Triple::new(Subject::from(a1.clone()), rdf::FIRST, Term::from(p)).as_ref());
        g1.insert(
            Triple::new(Subject::from(a1.clone()), rdf::REST, Term::from(a2.clone())).as_ref(),
        );
        g1.insert(Triple::new(Subject::from(a2.clone()), rdf::FIRST, Term::from(q)).as_ref());
        g1.insert(Triple::new(Subject::from(a2.clone()), rdf::REST, Term::from(rdf::NIL)).as_ref());

        // g2: list of length 1
        let mut g2 = Graph::new();
        let x = BlankNode::new_unchecked("x");
        let p2 = iri("http://example.org/p");
        g2.insert(Triple::new(Subject::from(x.clone()), rdf::FIRST, Term::from(p2)).as_ref());
        g2.insert(Triple::new(Subject::from(x.clone()), rdf::REST, Term::from(rdf::NIL)).as_ref());

        assert!(!are_isomorphic(&g1, &g2));
    }
}
