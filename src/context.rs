use oxigraph::model::{Term, TermRef};
use crate::named_nodes::{RDF, SHACL};
use oxigraph::model::Graph;
use crate::components::ToSubjectRef;
use std::cell::RefCell;
use crate::types::ID;
use std::collections::HashSet;

pub struct IDLookupTable {
    id_map: std::collections::HashMap<Term, ID>,
    id_to_term: std::collections::HashMap<ID, Term>,
    next_id: ID,
}

impl IDLookupTable {
    pub fn new() -> Self {
        IDLookupTable {
            id_map: std::collections::HashMap::new(),
            id_to_term: std::collections::HashMap::new(),
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
    id_lookup: RefCell<IDLookupTable>,
    shape_graph: Graph,
    data_graph: Graph,
}

impl ValidationContext {
    pub fn new(shape_graph: Graph, data_graph: Graph) -> Self {
        ValidationContext {
            id_lookup: RefCell::new(IDLookupTable::new()),
            shape_graph,
            data_graph,
        }
    }

    fn get_node_shapes(&self) -> Vec<ID> {
        // here are all the ways to get a node shape:
        // - <shape> rdf:type sh:NodeShape
        // - ? sh:node <shape>
        // - ? sh:qualifiedValueShape <shape>
        // - ? sh:not <shape>
        // - ? sh:or (list of <shape>)
        // - ? sh:and (list of <shape>)
        // - ? sh:xone (list of <shape>)
        let rdf = RDF::new();
        let shacl = SHACL::new();

        // parse these out of the shape graph and return a vector of IDs
        let mut node_shapes = HashSet::new();
        // <shape> rdf:type sh:NodeShape
        while let Some(shape) = self.shape_graph.subject_for_predicate_object(rdf.type_, shacl.node_shape) {
            node_shapes.insert(self.get_or_create_id(shape.into()));
        }

        // ? sh:node <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.node) {
            node_shapes.insert(self.get_or_create_id(triple.object.into()));
        }

        // ? sh:qualifiedValueShape <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.qualified_value_shape) {
            node_shapes.insert(self.get_or_create_id(triple.object.into()));
        }

        // ? sh:not <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.not) {
            node_shapes.insert(self.get_or_create_id(triple.object.into()));
        }

        // ? sh:or (list of <shape>)


        return node_shapes.into_iter().collect();
    }

    pub fn parse(&mut self) {
        // parses the shape graph to get all of the shapes and components defined within


    }

    pub fn parse_rdf_list(&self, list: TermRef) -> Vec<TermRef> {
        let mut items: Vec<TermRef> = Vec::new();
        let rdf = RDF::new();
        let mut current: TermRef = list;
        while let Some(first) = self.shape_graph.object_for_subject_predicate(current.to_subject_ref(), rdf.first) {
            items.push(first);
            if let Some(rest) = self.shape_graph.object_for_subject_predicate(current.to_subject_ref(), rdf.rest) {
                current = rest.into();
            } else {
                break;
            }
        }
        items
    }

    pub fn shape_graph(&self) -> &Graph {
        &self.shape_graph
    }

    pub fn data_graph(&self) -> &Graph {
        &self.data_graph
    }
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
    pub fn get_or_create_id(&self, term: Term) -> ID {
        self.id_lookup.borrow_mut().get_or_create_id(term)
    }
}
