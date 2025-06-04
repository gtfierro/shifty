use oxigraph::model::{Term, TermRef, SubjectRef, Quad};
use std::hash::Hash;
use crate::named_nodes::{RDF, SHACL};
use crate::shape::NodeShape; // Removed PropertyShape, Shape
use oxigraph::model::Graph;
use crate::components::{ToSubjectRef, parse_components, Component}; // Added Component
use std::cell::RefCell;
use crate::types::{ID, Target, ComponentID};
use std::collections::{HashSet, HashMap};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::error::Error;
use oxigraph::io::{RdfParser, RdfFormat};

pub struct IDLookupTable<IdType: Copy + Eq + Hash> {
    id_map: std::collections::HashMap<Term, IdType>,
    id_to_term: std::collections::HashMap<IdType, Term>,
    next_id: u64, // Internal counter remains u64
}

impl<IdType: Copy + Eq + Hash + From<u64>> IDLookupTable<IdType> {
    pub fn new() -> Self {
        IDLookupTable {
            id_map: std::collections::HashMap::new(),
            id_to_term: std::collections::HashMap::new(),
            next_id: 0,
        }
    }

    // Returns an ID for the given term, creating a new ID if it doesn't exist
    pub fn get_or_create_id(&mut self, term: Term) -> IdType {
        if let Some(&id) = self.id_map.get(&term) {
            id
        } else {
            let id_val = self.next_id;
            let id: IdType = id_val.into(); // Convert u64 to IdType
            self.id_map.insert(term.clone(), id);
            self.id_to_term.insert(id, term);
            self.next_id += 1;
            id
        }
    }

    // Returns the term associated with the given ID
    pub fn get_term(&self, id: IdType) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}

pub struct ValidationContext {
    node_id_lookup: RefCell<IDLookupTable<ID>>,
    component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    shape_graph: Graph,
    data_graph: Graph,
    node_shapes: HashMap<ID, NodeShape>,
    components: HashMap<ComponentID, Component>,
}

impl ValidationContext {
    pub fn new(shape_graph: Graph, data_graph: Graph) -> Self {
        ValidationContext {
            node_id_lookup: RefCell::new(IDLookupTable::<ID>::new()),
            component_id_lookup: RefCell::new(IDLookupTable::<ComponentID>::new()),
            shape_graph,
            data_graph,
            node_shapes: HashMap::new(),
            components: HashMap::new(),
        }
    }

    fn load_graph_from_path_internal(file_path: &str) -> Result<Graph, Box<dyn Error>> {
        let path = Path::new(file_path);
        let file = File::open(path)
            .map_err(|e| format!("Failed to open file '{}': {}", file_path, e))?;
        let reader = BufReader::new(file);
        let format = RdfFormat::from_file_path(path)
            .map_err(|e| format!("Failed to determine RDF format for '{}': {}", file_path, e))?;

        let mut graph = Graph::new();
        let quad_iter = RdfParser::from_format(format).for_reader(reader);

        for quad_result in quad_iter {
            let quad = quad_result
                .map_err(|e| format!("RDF parsing error in '{}': {}", file_path, e))?;
            // Insert the subject, predicate, object from the Quad into the Graph.
            // This effectively merges all named graphs and the default graph into one.
            graph.insert((quad.subject.as_ref(), quad.predicate.as_ref(), quad.object.as_ref()).into());
        }
        Ok(graph)
    }

    pub fn from_files(shape_graph_path: &str, data_graph_path: &str) -> Result<Self, Box<dyn Error>> {
        let shape_graph = Self::load_graph_from_path_internal(shape_graph_path)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error loading shape graph from '{}': {}", shape_graph_path, e))))?;
        let data_graph = Self::load_graph_from_path_internal(data_graph_path)
            .map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error loading data graph from '{}': {}", data_graph_path, e))))?;
        Ok(Self::new(shape_graph, data_graph))
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
            node_shapes.insert(self.get_or_create_node_id(shape.into()));
        }

        // ? sh:node <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.node) {
            node_shapes.insert(self.get_or_create_node_id(triple.object.into()));
        }

        // ? sh:qualifiedValueShape <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.qualified_value_shape) {
            node_shapes.insert(self.get_or_create_node_id(triple.object.into()));
        }

        // ? sh:not <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.not) {
            node_shapes.insert(self.get_or_create_node_id(triple.object.into()));
        }

        // ? sh:or (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.or_) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(self.get_or_create_node_id(item.into()));
            }
        }

        // ? sh:and (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.and_) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(self.get_or_create_node_id(item.into()));
            }
        }

        // ? sh:xone (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.xone) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(self.get_or_create_node_id(item.into()));
            }
        }

        return node_shapes.into_iter().collect();
    }

    pub fn parse(&mut self) {
        // parses the shape graph to get all of the shapes and components defined within


    }

    pub fn parse_node_shape(&mut self, shape: TermRef) -> ID {
        // Parses a shape from the shape graph and returns its ID.
        // Adds the shape to the node_shapes map.
        let id = self.get_or_create_node_id(shape.into());

        let subject: SubjectRef = shape.to_subject_ref();

        // get the targets
        let targets: Vec<Target> = self.shape_graph
            .triples_for_subject(subject)
            .filter_map(|triple| Target::from_predicate_object(triple.predicate, triple.object))
            .collect();

        // get constraint components
        let constraints = parse_components(shape, self);
        let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
        for (component_id, component) in constraints {
            // add the component to our context.components map
            self.components.insert(component_id, component);
        }

        let node_shape = NodeShape::new(
            id,
            targets,
            vec![], // Property shapes will be added later
            component_ids,
        );
        self.node_shapes.insert(id, node_shape);
        id
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
    /// Returns an ID for the given term, creating a new one if necessary for a NodeShape.
    pub fn get_or_create_node_id(&self, term: Term) -> ID {
        self.node_id_lookup.borrow_mut().get_or_create_id(term)
    }

    /// Returns a ComponentID for the given term, creating a new one if necessary for a Component.
    pub fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup.borrow_mut().get_or_create_id(term)
    }
}
