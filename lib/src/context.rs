use crate::components::{parse_components, Component, ToSubjectRef}; // Added Component
use crate::named_nodes::{RDF, SHACL};
use crate::shape::{NodeShape, PropertyShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, Target, ID};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{SubjectRef, Term, TermRef}; // Removed TripleRef
use oxigraph::store::Store; // Added Store
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
    shape_graph: Store,
    data_graph: Store,
    node_shapes: HashMap<ID, NodeShape>,
    prop_shapes: HashMap<PropShapeID, PropertyShape>,
    components: HashMap<ComponentID, Component>,
}

impl ValidationContext {
    pub fn new(shape_graph: Store, data_graph: Store) -> Self {
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
                "{} [label=\"NodeShape\\n{}\"];",
                shape.identifier().to_graphviz_id(),
                name_label
            );
            for comp in shape.constraints() {
                println!("    {} -> {};", shape.identifier().to_graphviz_id(), comp.to_graphviz_id());
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
                "    {} [label=\"PropertyShape\\nPath: {}\"];",
                pshape.identifier().to_graphviz_id(),
                path_label
            );
            for comp in pshape.constraints() {
                println!("    {} -> {};", pshape.identifier().to_graphviz_id(), comp.to_graphviz_id());
            }
        }
        for (ident, comp) in self.components.iter() {
            comp.to_graphviz_string(*ident, self)
                .lines()
                .for_each(|line| println!("    {}", line));
        }
        println!("}}")
    }

    fn load_graph_from_path_internal(file_path: &str) -> Result<Store, Box<dyn Error>> {
        let path = Path::new(file_path);
        let file =
            File::open(path).map_err(|e| format!("Failed to open file '{}': {}", file_path, e))?;
        let reader = BufReader::new(file);

        let store = Store::new().map_err(|e| Box::new(e) as Box<dyn Error>)?;
        let parser = RdfParser::from_format(RdfFormat::Turtle); // Assuming Turtle, adjust if needed

        // Store::load_from_reader consumes the reader and parser.
        // We need to ensure all quads go into the default graph for Store,
        // or handle graph names if the RDF format supports them and it's desired.
        // For simplicity, this example loads into the default graph.
        // RdfParser can be configured with .with_default_graph() or .without_named_graphs()
        // if specific graph handling is needed before passing to load_from_reader.
        // The load_from_reader method itself doesn't offer direct control over which graph
        // triples from a triple-based format go into, beyond parser configuration.
        // For quad-based formats, it respects the graph name in the quad.

        // To mimic the old behavior of merging all into one graph (effectively the default graph of the Store):
        // We can iterate and insert manually if RdfParser is configured to strip graph names or if it's a triples format.
        for quad_result in parser.for_reader(reader) {
            let quad = quad_result.map_err(|e| format!("RDF parsing error in '{}': {}", file_path, e))?;
            // Insert into the store. If quad has a graph name, it will be used.
            // If it's from a triples format and parser is not set to a default graph, it goes to store's default graph.
            store.insert(&quad).map_err(|e| Box::new(e) as Box<dyn Error>)?;
        }
        Ok(store)
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
        let mut prop_shapes = HashSet::new();

        // - <pshape> a sh:PropertyShape
        for quad_res in self.shape_graph.quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(sh.property_shape),
            None,
        ) {
            if let Ok(quad) = quad_res {
                prop_shapes.insert(quad.subject.into()); // quad.subject is Subject, .into() converts to Term
            }
        }

        // - ? sh:property <pshape>
        for quad_res in self
            .shape_graph
            .quads_for_pattern(None, Some(sh.property), None, None)
        {
            if let Ok(quad) = quad_res {
                prop_shapes.insert(quad.object); // quad.object is Term
            }
        }

        prop_shapes.into_iter().collect()
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
        for quad_res in self.shape_graph.quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(shacl.node_shape),
            None,
        ) {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.subject.into());
            }
        }

        // ? sh:node <shape>
        for quad_res in self
            .shape_graph
            .quads_for_pattern(None, Some(shacl.node), None, None)
        {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // ? sh:qualifiedValueShape <shape>
        for quad_res in self.shape_graph.quads_for_pattern(
            None,
            Some(shacl.qualified_value_shape),
            None,
            None,
        ) {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // ? sh:not <shape>
        for quad_res in self
            .shape_graph
            .quads_for_pattern(None, Some(shacl.not), None, None)
        {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // Helper to process lists for logical constraints
        let mut process_list_constraint = |predicate_ref| {
            for quad_res in self
                .shape_graph
                .quads_for_pattern(None, Some(predicate_ref), None, None)
            {
                if let Ok(quad) = quad_res {
                    let list_head_term = quad.object; // This is Term
                    for item_term in self.parse_rdf_list(list_head_term) {
                        node_shapes.insert(item_term);
                    }
                }
            }
        };

        // ? sh:or (list of <shape>)
        process_list_constraint(shacl.or_);

        // ? sh:and (list of <shape>)
        process_list_constraint(shacl.and_);

        // ? sh:xone (list of <shape>)
        process_list_constraint(shacl.xone);

        node_shapes.into_iter().collect()
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
            .quads_for_pattern(Some(subject), None, None, None)
            .filter_map(Result::ok)
            .filter_map(|quad| {
                Target::from_predicate_object(quad.predicate.as_ref(), quad.object.as_ref())
            })
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
            .quads_for_pattern(Some(subject), Some(sh.property), None, None)
            .filter_map(Result::ok)
            .filter_map(|quad| self.propshape_id_lookup.borrow().get(&quad.object))
            .collect();

        let node_shape = NodeShape::new(id, targets, component_ids);
        self.node_shapes.insert(id, node_shape);
        id
    }

    pub fn parse_property_shape(&mut self, pshape: TermRef) -> PropShapeID {
        let id = self.get_or_create_prop_id(pshape.into_owned());
        let shacl = SHACL::new();
        let subject: SubjectRef = pshape.to_subject_ref();
        let path_head_term: Term = self
            .shape_graph
            .quads_for_pattern(Some(subject), Some(shacl.path), None, None)
            .filter_map(Result::ok)
            .map(|quad| quad.object)
            .next()
            .unwrap(); // Assuming path is always present and valid
        let path = PShapePath::Simple(path_head_term);
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

    // Parses an RDF list starting from list_head_term (owned Term) and returns a Vec of owned Terms.
    pub fn parse_rdf_list(&self, list_head_term: Term) -> Vec<Term> {
        let mut items: Vec<Term> = Vec::new();
        let rdf = RDF::new();
        let mut current_term = list_head_term;
        let nil_term: Term = rdf.nil.into_owned().into(); // Convert NamedNodeRef to Term

        while current_term != nil_term {
            let subject_ref = match current_term.as_ref() {
                TermRef::NamedNode(nn) => SubjectRef::NamedNode(nn),
                TermRef::BlankNode(bn) => SubjectRef::BlankNode(bn),
                _ => return items, // Or handle error: list node not an IRI/BlankNode
            };

            let first_val_opt: Option<Term> = self
                .shape_graph
                .quads_for_pattern(Some(subject_ref), Some(rdf.first), None, None)
                .filter_map(Result::ok)
                .map(|q| q.object)
                .next();

            if let Some(val) = first_val_opt {
                items.push(val);
            } else {
                break; // Malformed list (no rdf:first)
            }

            let rest_node_opt: Option<Term> = self
                .shape_graph
                .quads_for_pattern(Some(subject_ref), Some(rdf.rest), None, None)
                .filter_map(Result::ok)
                .map(|q| q.object)
                .next();

            if let Some(rest_term) = rest_node_opt {
                current_term = rest_term;
            } else {
                break; // Malformed list (no rdf:rest)
            }
        }
        items
    }

    pub fn shape_graph(&self) -> &Store {
        &self.shape_graph
    }

    pub fn data_graph(&self) -> &Store {
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
