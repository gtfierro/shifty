use oxigraph::model::{Term, NamedNode, BlankNode, Literal};
use oxigraph::model::Graph;
use crate::types::ID;

pub struct IDLookupTable {
    id_map: std::collections::HashMap<Term, ID>,
    id_to_term: std::collections::HashMap<ID, Term>,
    next_id: ID,
}

impl IDLookupTable {
    pub fn new() -> Self {
        IDLookupTable {
            id_map: std::collections::HashMap::new(),
            next_id: 0,
        }
    }

    // Returns an ID for the given term, creating a new ID if it doesn't exist
    pub fn get_or_create_id(&mut self, term: Term) -> ID {
        if let Some(&id) = self.id_map.get(&term) {
            id
        } else {
            let id = self.next_id;
            self.id_map.insert(term.clone(), id);
            self.id_to_term.insert(id, term);
            self.next_id += 1;
            id
        }
    }

    // Returns the term associated with the given ID
    pub fn get_term(&self, id: ID) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}

pub struct ValidationContext {
    id_lookup: IDLookupTable,
    shape_graph: Graph,
    data_graph: Graph,
}

pub struct Context {
    focus_node: Term,
    path: Option<Term>,
    value_nodes: Option<Vec<Term>>,
    source_shape: Option<ID>,
}

impl Context {
    pub fn new(focus_node: Term, path: Option<Term>, value_nodes: Option<Vec<Term>>) -> Self {
        Context {
            focus_node,
            path,
            value_nodes,
            source_shape: None,
        }
    }

    pub fn set_source_shape(&mut self, shape: ID) {
        self.source_shape = Some(shape);
    }

    pub fn add_value_nodes(&mut self, value_nodes: &[Term]) {
        if let Some(existing) = &mut self.value_nodes {
            existing.extend(value_nodes.iter().cloned());
        } else {
            self.value_nodes = Some(value_nodes.to_vec());
        }
    }
}

impl ValidationContext {
    /// Returns an ID for the given term, creating a new one if necessary.
    pub fn get_or_create_id(&mut self, term: Term) -> ID {
        self.id_lookup.get_or_create_id(term)
    }
}
