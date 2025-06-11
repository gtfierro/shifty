use crate::components::Component;
use crate::optimize::Optimizer;
use crate::parser;
use crate::report::ValidationReportBuilder;
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, TermID, ID};
use log::{debug, error, info};
use ontoenv::api::OntoEnv;
use ontoenv::config::Config;
use ontoenv::ontology::OntologyLocation;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphNameRef, NamedNode, Term};
use oxigraph::store::Store;
use papaya::HashMap as FastMap;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use xxhash_rust::xxh3::xxh3_64;

const SHAPE_GRAPH_IRI: &str = "urn:shape_graph";
const DATA_GRAPH_IRI: &str = "urn:data_graph";

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
        Term::Triple(_t) => "rdf_triple".to_string(),                       // Handle Triple case
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
    pub(crate) nodeshape_id_lookup: RefCell<IDLookupTable<ID>>,
    pub(crate) propshape_id_lookup: RefCell<IDLookupTable<PropShapeID>>,
    pub(crate) component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    pub(crate) store: Store,
    pub(crate) shape_graph_iri: NamedNode,
    pub(crate) data_graph_iri: NamedNode,
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    pub(crate) components: HashMap<ComponentID, Component>,
    term_to_hash: FastMap<TermID, Term>,
    env: OntoEnv,
}

impl ValidationContext {
    pub fn new(
        store: Store,
        env: OntoEnv,
        shape_graph_iri: NamedNode,
        data_graph_iri: NamedNode,
    ) -> Self {
        ValidationContext {
            nodeshape_id_lookup: RefCell::new(IDLookupTable::<ID>::new()),
            propshape_id_lookup: RefCell::new(IDLookupTable::<PropShapeID>::new()),
            component_id_lookup: RefCell::new(IDLookupTable::<ComponentID>::new()),
            store,
            shape_graph_iri,
            data_graph_iri,
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            components: HashMap::new(),
            term_to_hash: FastMap::default(),
            env,
        }
    }

    // compute the hash and store hash -> term if it's not there yet
    pub fn term_to_hash(&mut self, term: Term) -> TermID {
        let map = self.term_to_hash.pin();
        let hash = TermID(xxh3_64(term.to_string().as_bytes()));
        if !map.contains_key(&hash) {
            // If the hash is not already in the map, insert it
            map.insert(hash, term.clone());
        }
        hash
    }

    pub fn hash_to_term(&self, hash: TermID) -> Option<Term> {
        // Searches for a term with the given hash
        let map = self.term_to_hash.pin();
        map.get(&hash).cloned()
    }

    pub fn validate(&self) -> ValidationReportBuilder {
        let mut b = ValidationReportBuilder::new();
        info!("Validating NodeShapes and PropertyShapes in the context");
        for node_shape in self.node_shapes.values() {
            // Validate each NodeShape
            if let Err(e) = node_shape.validate(self, &mut b) {
                error!(
                    "Error validating NodeShape {}: {}",
                    node_shape.identifier(),
                    e
                );
            }
        }
        b
    }

    pub fn dump(&self) {
        // print all of the shapes
        for shape in self.node_shapes.values() {
            debug!("{:?}", shape);
        }
    }

    pub fn graphviz(&self) -> Result<String, String> {
        let mut dot_string = String::new();
        // print all node shapes
        dot_string.push_str("digraph {\n");
        for shape in self.node_shapes.values() {
            let name = self
                .nodeshape_id_lookup
                .borrow()
                .id_to_term
                .get(shape.identifier())
                .ok_or_else(|| format!("Missing term for nodeshape ID: {:?}", shape.identifier()))?
                .clone();
            // 'name' here is the Term identifier of the NodeShape
            let name_label = format_term_for_label(&name);
            dot_string.push_str(&format!(
                "  {} [label=\"NodeShape\\n{}\"];\n",
                shape.identifier().to_graphviz_id(),
                name_label
            ));
            for comp in shape.constraints() {
                dot_string.push_str(&format!(
                    "    {} -> {};\n",
                    shape.identifier().to_graphviz_id(),
                    comp.to_graphviz_id()
                ));
            }
        }
        for pshape in self.prop_shapes.values() {
            let _name = self
                .propshape_id_lookup
                .borrow()
                .id_to_term
                .get(pshape.identifier())
                .ok_or_else(|| format!("Missing term for propshape ID: {:?}", pshape.identifier()))?
                .clone();
            // The 'name' variable (PropertyShape's own identifier, which is 'pshape_identifier_term' above)
            // is not used for the label. We use the path term for the label as it's generally more informative.

            let path_label = pshape.sparql_path(); // Get the Term of the path
            dot_string.push_str(&format!(
                "  {} [label=\"PropertyShape\\nPath: {}\"];\n",
                pshape.identifier().to_graphviz_id(),
                path_label
            ));
            for comp in pshape.constraints() {
                dot_string.push_str(&format!(
                    "    {} -> {};\n",
                    pshape.identifier().to_graphviz_id(),
                    comp.to_graphviz_id()
                ));
            }
        }
        for (ident, comp) in self.components.iter() {
            comp.to_graphviz_string(*ident, self)
                .lines()
                .for_each(|line| dot_string.push_str(&format!("    {}\n", line)));
        }
        dot_string.push_str("}\n");
        Ok(dot_string)
    }

    // Loads triples from a file into the specified named graph of the given store.
    fn load_graph_into_store(
        store: &Store,
        file_path: &str,
        target_graph_name: GraphNameRef,
    ) -> Result<(), Box<dyn Error>> {
        let path = Path::new(file_path);
        let file =
            File::open(path).map_err(|e| format!("Failed to open file '{}': {}", file_path, e))?;
        let reader = BufReader::new(file);

        // Assuming Turtle format for simplicity, adjust RdfFormat::Turtle if other formats are expected.
        // This parser will direct all triples from the input file into the `target_graph_name`.
        let parser = RdfParser::from_format(RdfFormat::Turtle) // TODO: Make format configurable or detect
            .with_default_graph(target_graph_name.into_owned()); // Load into the specified graph

        store
            .bulk_loader()
            .load_from_reader(parser, reader)
            .map_err(|e| Box::new(e) as Box<dyn Error>)?;
        Ok(())
    }

    pub fn from_files(
        shape_graph_path: &str,
        data_graph_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let store = Store::new().map_err(|e| Box::new(e) as Box<dyn Error>)?;
        let locations: Option<Vec<PathBuf>> = None;
        let mut env = OntoEnv::init(
            Config::new_with_default_matches(
                PathBuf::from("."), // root
                locations,          // no locations
                true,               // require ontology names
                false,              // strict parsing
                true,               // not offline
                true,               // in-memory
            )
            .unwrap(),
            false,
        )
        .expect("Failed to create OntoEnv with default configuration");

        let shape_graph_location = OntologyLocation::from_str(shape_graph_path)?;
        info!("Added shape graph: {}", shape_graph_location);
        let shape_id = env.add(shape_graph_location, true)?;
        let shape_graph_iri = env.get_ontology(&shape_id).unwrap().name().clone();
        let data_graph_location = OntologyLocation::from_str(data_graph_path)?;
        info!("Added data graph: {}", data_graph_location);
        let data_id = env.add(data_graph_location, true)?;
        let data_graph_iri = env.get_ontology(&data_id).unwrap().name().clone();

        let store = env.io().store().clone();
        //Self::load_graph_into_store(
        //    &store,
        //    shape_graph_path,
        //    GraphNameRef::NamedNode(shape_graph_iri.as_ref()),
        //)
        //.map_err(|e| {
        //    Box::new(std::io::Error::new(
        //        std::io::ErrorKind::Other,
        //        format!(
        //            "Error loading shape graph from '{}' into <{}>: {}",
        //            shape_graph_path, SHAPE_GRAPH_IRI, e
        //        ),
        //    ))
        //})?;

        //Self::load_graph_into_store(
        //    &store,
        //    data_graph_path,
        //    GraphNameRef::NamedNode(data_graph_iri.as_ref()),
        //)
        //.map_err(|e| {
        //    Box::new(std::io::Error::new(
        //        std::io::ErrorKind::Other,
        //        format!(
        //            "Error loading data graph from '{}' into <{}>: {}",
        //            data_graph_path, DATA_GRAPH_IRI, e
        //        ),
        //    ))
        //})?;

        info!(
            "Optimizing store with shape graph <{}> and data graph <{}>",
            shape_graph_iri, data_graph_iri
        );
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error optimizing store: {}", e),
            ))
        })?;

        let mut ctx = Self::new(store, env, shape_graph_iri, data_graph_iri);
        info!(
            "Parsing shapes from graph <{}> into context",
            ctx.shape_graph_iri_ref()
        );
        parser::run_parser(&mut ctx).map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error parsing shapes: {}", e),
            ))
        })?;
        info!("Optimizing shape graph");
        let mut o = Optimizer::new(ctx);
        o.optimize()?;
        info!("Finished parsing shapes and optimizing context");
        Ok(o.finish())
    }

    // Parses an RDF list starting from list_head_term (owned Term) and returns a Vec of owned Terms.
    pub fn parse_rdf_list(&self, list_head_term: Term) -> Vec<Term> {
        parser::parse_rdf_list(self, list_head_term)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn env(&self) -> &OntoEnv {
        &self.env
    }

    pub fn shape_graph_iri_ref(&self) -> GraphNameRef {
        GraphNameRef::NamedNode(self.shape_graph_iri.as_ref())
    }

    pub fn data_graph_iri_ref(&self) -> GraphNameRef {
        GraphNameRef::NamedNode(self.data_graph_iri.as_ref())
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

    pub fn get_component_by_id(&self, id: &ComponentID) -> Option<&Component> {
        // Returns a reference to the component by its ID
        self.components.get(id)
    }

    pub fn get_prop_shape_by_id(&self, id: &PropShapeID) -> Option<&PropertyShape> {
        // Returns a reference to the PropertyShape by its ID
        self.prop_shapes.get(id)
    }

    pub fn get_node_shape_by_id(&self, id: &ID) -> Option<&NodeShape> {
        // Returns a reference to the NodeShape by its ID
        self.node_shapes.get(id)
    }

    pub fn node_shapes(&self) -> &HashMap<ID, NodeShape> {
        &self.node_shapes
    }

    pub fn get_trace_item_label_and_type(&self, item: &TraceItem) -> (String, String) {
        match item {
            TraceItem::NodeShape(id) => {
                let label = self.nodeshape_id_lookup.borrow().get_term(*id).map_or_else(
                    || format!("Unknown NodeShape ID: {:?}", id),
                    |term| format_term_for_label(term),
                );
                (label, "NodeShape".to_string())
            }
            TraceItem::PropertyShape(id) => {
                let label = self.get_prop_shape_by_id(id).map_or_else(
                    || format!("Unknown PropertyShape ID: {:?}", id),
                    |ps| ps.sparql_path(),
                );
                // If you prefer the PropertyShape's own identifier term as label:
                // let label = self.propshape_id_lookup.borrow().get_term(*id)
                //     .map_or_else(|| format!("Unknown PropertyShape ID: {:?}", id), |term| format_term_for_label(term));
                (label, "PropertyShape".to_string())
            }
            TraceItem::Component(id) => {
                let label = self.get_component_by_id(id).map_or_else(
                    || format!("Unknown Component ID: {:?}", id),
                    |comp| comp.label(),
                );
                (label, "Component".to_string())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TraceItem {
    NodeShape(ID),
    PropertyShape(PropShapeID),
    Component(ComponentID),
}

impl TraceItem {
    pub fn to_string(&self) -> String {
        match self {
            TraceItem::NodeShape(id) => format!("NodeShape({})", id.to_graphviz_id()),
            TraceItem::PropertyShape(id) => format!("PropertyShape({})", id.to_graphviz_id()),
            TraceItem::Component(id) => format!("Component({})", id.to_graphviz_id()),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Context {
    focus_node: Term,
    path: Option<PShapePath>,
    value_nodes: Option<Vec<Term>>,
    source_shape: ID,
    execution_trace: Vec<TraceItem>,
}

impl Context {
    pub fn new(
        focus_node: Term,
        path: Option<PShapePath>,
        value_nodes: Option<Vec<Term>>,
        source_shape: ID,
    ) -> Self {
        Context {
            focus_node,
            path,
            value_nodes,
            source_shape,
            execution_trace: Vec::new(),
        }
    }

    pub fn add_value_nodes(&mut self, value_nodes: &[Term]) {
        if let Some(existing) = &mut self.value_nodes {
            existing.extend(value_nodes.iter().cloned());
        } else {
            self.value_nodes = Some(value_nodes.to_vec());
        }
    }

    pub fn focus_node(&self) -> &Term {
        &self.focus_node
    }

    pub fn path(&self) -> Option<&PShapePath> {
        self.path.as_ref()
    }

    pub fn value_nodes(&self) -> Option<&Vec<Term>> {
        self.value_nodes.as_ref()
    }

    pub fn source_shape(&self) -> ID {
        self.source_shape
    }

    pub fn record_node_shape_visit(&mut self, shape_id: ID) {
        self.execution_trace.push(TraceItem::NodeShape(shape_id));
    }

    pub fn record_property_shape_visit(&mut self, shape_id: PropShapeID) {
        self.execution_trace
            .push(TraceItem::PropertyShape(shape_id));
    }

    pub fn record_component_visit(&mut self, component_id: ComponentID) {
        self.execution_trace
            .push(TraceItem::Component(component_id));
    }

    pub fn execution_trace(&self) -> &Vec<TraceItem> {
        &self.execution_trace
    }
}
