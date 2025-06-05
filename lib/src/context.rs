use crate::components::{parse_components, Component, ToSubjectRef}; // Added Component
use crate::named_nodes::{RDF, SHACL};
use crate::shape::{NodeShape, PropertyShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, Target, ID};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::Graph;
use oxigraph::model::{SubjectRef, Term, TermRef, TripleRef};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::Path;

// Renamed from clean. Filters to alphanumeric characters.
pub(crate) fn sanitize_graphviz_string(input: &str) -> String {
    input.chars().filter(|c| c.is_alphanumeric()).collect()
}

// Formats a Term for display in a Graphviz label.
pub(crate) fn format_term_for_label(term: &Term) -> String {
    match term {
        Term::NamedNode(nn) => {
            let iri_str = nn.as_str();
            if let Some(hash_idx) = iri_str.rfind('#') {
                iri_str[hash_idx + 1..].to_string()
            } else if let Some(slash_idx) = iri_str.rfind('/') {
                if slash_idx == iri_str.len() - 1 && iri_str.len() > 1 {
                    // Handles cases like http://example.com/ns/
                    // Take the segment before the last slash if it ends with a slash
                    let without_trailing_slash = &iri_str[..slash_idx];
                    if let Some(prev_slash_idx) = without_trailing_slash.rfind('/') {
                        without_trailing_slash[prev_slash_idx + 1..].to_string()
                    } else {
                        without_trailing_slash.to_string() // Fallback for http://example.com/
                    }
                } else {
                    iri_str[slash_idx + 1..].to_string()
                }
            } else {
                iri_str.to_string() // Fallback if no # or /
            }
        }
        Term::BlankNode(_bn) => "bnode".to_string(),
        Term::Literal(lit) => lit.value().to_string().replace('"', "\\\""), // Escape quotes for DOT language
        Term::Triple(_t) => "rdf_triple".to_string(), // Handle Triple case
    }
}

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

    pub fn get(&self, term: &Term) -> Option<IdType> {
        self.id_map.get(term).map(|id| id.to_owned())
    }

    // Returns the term associated with the given ID
    pub fn get_term(&self, id: IdType) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}

pub struct ValidationContext {
    nodeshape_id_lookup: RefCell<IDLookupTable<ID>>,
    propshape_id_lookup: RefCell<IDLookupTable<PropShapeID>>,
    component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    shape_graph: Graph,
    data_graph: Graph,
    node_shapes: HashMap<ID, NodeShape>,
    prop_shapes: HashMap<PropShapeID, PropertyShape>,
    components: HashMap<ComponentID, Component>,
}

impl ValidationContext {
    pub fn new(shape_graph: Graph, data_graph: Graph) -> Self {
        ValidationContext {
            nodeshape_id_lookup: RefCell::new(IDLookupTable::<ID>::new()),
            propshape_id_lookup: RefCell::new(IDLookupTable::<PropShapeID>::new()),
            component_id_lookup: RefCell::new(IDLookupTable::<ComponentID>::new()),
            shape_graph,
            data_graph,
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            components: HashMap::new(),
        }
    }

    pub fn dump(&self) {
        // print all of the shapes
        for shape in self.node_shapes.values() {
            println!("{:?}", shape);
        }
    }

    pub fn graphviz(&self) {
        // print all node shapes
        println!("digraph {{");
        for shape in self.node_shapes.values() {
            let name = self
                .nodeshape_id_lookup
                .borrow()
                .id_to_term
                .get(shape.identifier())
                .unwrap()
                .clone();
            // 'name' here is the Term identifier of the NodeShape
            let name_label = format_term_for_label(&name);
            println!(
                "n{} [label=\"NodeShape\\n{}\"];",
                shape.identifier().0,
                name_label
            );
            for comp in shape.constraints() {
                println!("    n{} -> c{};", shape.identifier().0, comp.0);
            }
        }
        for pshape in self.prop_shapes.values() {
            let name = self
                .propshape_id_lookup
                .borrow()
                .id_to_term
                .get(pshape.identifier())
                .unwrap()
                .clone();
            // The 'name' variable (PropertyShape's own identifier, which is 'pshape_identifier_term' above) 
            // is not used for the label. We use the path term for the label as it's generally more informative.
            
            let path_term = pshape.path_term(); // Get the Term of the path
            let path_label = format_term_for_label(path_term); // Format it
            println!(
                "    p{} [label=\"PropertyShape\\nPath: {}\"];",
                pshape.identifier().0,
                path_label
            );
            for comp in pshape.constraints() {
                println!("    p{} -> c{};", pshape.identifier().0, comp.0);
            }
        }
        for (ident, comp) in self.components.iter() {
            comp.to_graphviz_string(*ident, self)
                .lines()
                .for_each(|line| println!("    {}", line));
        }
        println!("}}")
    }

    fn load_graph_from_path_internal(file_path: &str) -> Result<Graph, Box<dyn Error>> {
        let path = Path::new(file_path);
        let file =
            File::open(path).map_err(|e| format!("Failed to open file '{}': {}", file_path, e))?;
        let reader = BufReader::new(file);

        let mut graph = Graph::new();
        let quad_iter = RdfParser::from_format(RdfFormat::Turtle).for_reader(reader);

        for quad_result in quad_iter {
            let quad =
                quad_result.map_err(|e| format!("RDF parsing error in '{}': {}", file_path, e))?;
            // Insert the subject, predicate, object from the Quad into the Graph.
            // This effectively merges all named graphs and the default graph into one.
            graph.insert(TripleRef::new(
                quad.subject.as_ref(),
                quad.predicate.as_ref(),
                quad.object.as_ref(),
            ));
        }
        Ok(graph)
    }

    pub fn from_files(
        shape_graph_path: &str,
        data_graph_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let shape_graph = Self::load_graph_from_path_internal(shape_graph_path).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Error loading shape graph from '{}': {}",
                    shape_graph_path, e
                ),
            ))
        })?;
        let data_graph = Self::load_graph_from_path_internal(data_graph_path).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error loading data graph from '{}': {}", data_graph_path, e),
            ))
        })?;
        let mut ctx = Self::new(shape_graph, data_graph);
        ctx.parse();
        Ok(ctx)
    }

    fn get_property_shapes(&self) -> Vec<Term> {
        let rdf = RDF::new();
        let sh = SHACL::new();
        // - ? sh:property <pshape>
        // - <pshape> a sh:PropertyShape
        let mut prop_shapes = HashSet::new();
        for pshape in self
            .shape_graph
            .subjects_for_predicate_object(rdf.type_, sh.property_shape)
        {
            prop_shapes.insert(pshape.into());
        }

        for triple in self.shape_graph.triples_for_predicate(sh.property) {
            prop_shapes.insert(triple.object.into());
        }

        return prop_shapes.into_iter().collect();
    }

    fn get_node_shapes(&self) -> Vec<Term> {
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
        for shape in self
            .shape_graph
            .subjects_for_predicate_object(rdf.type_, shacl.node_shape)
        {
            node_shapes.insert(shape.into());
        }

        // ? sh:node <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.node) {
            node_shapes.insert(triple.object.into());
        }

        // ? sh:qualifiedValueShape <shape>
        for triple in self
            .shape_graph
            .triples_for_predicate(shacl.qualified_value_shape)
        {
            node_shapes.insert(triple.object.into());
        }

        // ? sh:not <shape>
        for triple in self.shape_graph.triples_for_predicate(shacl.not) {
            node_shapes.insert(triple.object.into());
        }

        // ? sh:or (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.or_) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(item.into());
            }
        }

        // ? sh:and (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.and_) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(item.into());
            }
        }

        // ? sh:xone (list of <shape>)
        for triple in self.shape_graph.triples_for_predicate(shacl.xone) {
            let list = triple.object.into();
            for item in self.parse_rdf_list(list) {
                node_shapes.insert(item.into());
            }
        }

        return node_shapes.into_iter().collect();
    }

    pub fn parse(&mut self) {
        // parses the shape graph to get all of the shapes and components defined within
        let shapes = self.get_node_shapes();
        for shape in shapes {
            self.parse_node_shape(shape.as_ref());
        }

        let pshapes = self.get_property_shapes();
        for pshape in pshapes {
            self.parse_property_shape(pshape.as_ref());
        }
    }

    pub fn parse_node_shape(&mut self, shape: TermRef) -> ID {
        // Parses a shape from the shape graph and returns its ID.
        // Adds the shape to the node_shapes map.
        let id = self.get_or_create_node_id(shape.into());
        let sh = SHACL::new();

        let subject: SubjectRef = shape.to_subject_ref();

        // get the targets
        let targets: Vec<Target> = self
            .shape_graph
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

        let property_shapes: Vec<PropShapeID> = self
            .shape_graph
            .objects_for_subject_predicate(subject, sh.property)
            .filter_map(|o| self.propshape_id_lookup.borrow().get(&o.into_owned()))
            .collect();

        let node_shape = NodeShape::new(id, targets, component_ids);
        self.node_shapes.insert(id, node_shape);
        id
    }

    pub fn parse_property_shape(&mut self, pshape: TermRef) -> PropShapeID {
        let id = self.get_or_create_prop_id(pshape.into());
        let shacl = SHACL::new();
        let subject: SubjectRef = pshape.to_subject_ref();
        let path_head: TermRef = self
            .shape_graph
            .object_for_subject_predicate(subject, shacl.path)
            .unwrap();
        let path = PShapePath::Simple(path_head.into());
        // get constraint components
        let constraints = parse_components(pshape, self);
        let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
        for (component_id, component) in constraints {
            // add the component to our context.components map
            self.components.insert(component_id, component);
        }
        let prop_shape = PropertyShape::new(id, path, component_ids);
        self.prop_shapes.insert(id, prop_shape);
        id
    }

    pub fn parse_rdf_list(&self, list: TermRef) -> Vec<TermRef> {
        let mut items: Vec<TermRef> = Vec::new();
        let rdf = RDF::new();
        let mut current: TermRef = list;
        while let Some(first) = self
            .shape_graph
            .object_for_subject_predicate(current.to_subject_ref(), rdf.first)
        {
            items.push(first);
            if let Some(rest) = self
                .shape_graph
                .object_for_subject_predicate(current.to_subject_ref(), rdf.rest)
            {
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

    /// Returns an ID for the given term, creating a new one if necessary for a NodeShape.
    pub fn get_or_create_node_id(&self, term: Term) -> ID {
        self.nodeshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    pub fn get_or_create_prop_id(&self, term: Term) -> PropShapeID {
        self.propshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    /// Returns a ComponentID for the given term, creating a new one if necessary for a Component.
    pub fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup.borrow_mut().get_or_create_id(term)
    }

    // Getter methods for ID lookup tables
    pub fn nodeshape_id_lookup(&self) -> &RefCell<IDLookupTable<ID>> {
        &self.nodeshape_id_lookup
    }

    pub fn propshape_id_lookup(&self) -> &RefCell<IDLookupTable<PropShapeID>> {
        &self.propshape_id_lookup
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
