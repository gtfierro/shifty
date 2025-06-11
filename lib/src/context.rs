use crate::components::{parse_components, Component, ToSubjectRef}; // Added Component
use crate::named_nodes::{OWL, RDFS, RDF, SHACL};
use crate::report::ValidationReportBuilder;
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, Severity, TermID, ID};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphName, GraphNameRef, NamedNode, QuadRef, SubjectRef, Term, TermRef}; // Removed TripleRef, Added NamedNode, GraphName, GraphNameRef
use oxigraph::store::Store; // Added Store
use papaya::HashMap as FastMap;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::Path;
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
    store: Store,
    shape_graph_iri: NamedNode,
    data_graph_iri: NamedNode,
    node_shapes: HashMap<ID, NodeShape>,
    prop_shapes: HashMap<PropShapeID, PropertyShape>,
    components: HashMap<ComponentID, Component>,
    term_to_hash: FastMap<TermID, Term>,
}

impl ValidationContext {
    pub fn new(store: Store, shape_graph_iri: NamedNode, data_graph_iri: NamedNode) -> Self {
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
        for node_shape in self.node_shapes.values() {
            // Validate each NodeShape
            if let Err(e) = node_shape.validate(self, &mut b) {
                eprintln!(
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
            println!("{:?}", shape);
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
                .ok_or_else(|| {
                    format!("Missing term for propshape ID: {:?}", pshape.identifier())
                })?
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

        let shape_graph_named_node = NamedNode::new(SHAPE_GRAPH_IRI)
            .map_err(|e| format!("Invalid shape graph IRI: {}", e))?;
        let data_graph_named_node =
            NamedNode::new(DATA_GRAPH_IRI).map_err(|e| format!("Invalid data graph IRI: {}", e))?;

        Self::load_graph_into_store(
            &store,
            shape_graph_path,
            shape_graph_named_node.as_ref().into(),
        )
        .map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Error loading shape graph from '{}' into <{}>: {}",
                    shape_graph_path, SHAPE_GRAPH_IRI, e
                ),
            ))
        })?;

        Self::load_graph_into_store(
            &store,
            data_graph_path,
            data_graph_named_node.as_ref().into(),
        )
        .map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Error loading data graph from '{}' into <{}>: {}",
                    data_graph_path, DATA_GRAPH_IRI, e
                ),
            ))
        })?;

        store.optimize().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error optimizing store: {}", e),
            ))
        })?;

        let mut ctx = Self::new(store, shape_graph_named_node, data_graph_named_node);
        ctx.parse().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error parsing shapes: {}", e),
            ))
        })?;
        Ok(ctx)
    }

    fn get_property_shapes(&self) -> Vec<Term> {
        let rdf = RDF::new();
        let sh = SHACL::new();
        let mut prop_shapes = HashSet::new();
        let shape_graph_name_ref = GraphNameRef::NamedNode(self.shape_graph_iri.as_ref());

        // - <pshape> a sh:PropertyShape
        for quad_res in self.store.quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(sh.property_shape.into()),
            Some(shape_graph_name_ref),
        ) {
            if let Ok(quad) = quad_res {
                prop_shapes.insert(quad.subject.into()); // quad.subject is Subject, .into() converts to Term
            }
        }

        // - ? sh:property <pshape>
        for quad_res in self.store.quads_for_pattern(
            None,
            Some(sh.property),
            None,
            Some(shape_graph_name_ref),
        ) {
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
        let shape_graph_name_ref = GraphNameRef::NamedNode(self.shape_graph_iri.as_ref());

        // parse these out of the shape graph and return a vector of IDs
        let mut node_shapes = HashSet::new();

        // <shape> rdf:type sh:NodeShape
        for quad_res in self.store.quads_for_pattern(
            None,
            Some(rdf.type_),
            Some(shacl.node_shape.into()),
            Some(shape_graph_name_ref),
        ) {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.subject.into());
            }
        }

        // ? sh:node <shape>
        for quad_res in self.store.quads_for_pattern(
            None,
            Some(shacl.node),
            None,
            Some(shape_graph_name_ref),
        ) {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // ? sh:qualifiedValueShape <shape>
        for quad_res in self.store.quads_for_pattern(
            None,
            Some(shacl.qualified_value_shape),
            None,
            Some(shape_graph_name_ref),
        ) {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // ? sh:not <shape>
        for quad_res in
            self.store
                .quads_for_pattern(None, Some(shacl.not), None, Some(shape_graph_name_ref))
        {
            if let Ok(quad) = quad_res {
                node_shapes.insert(quad.object);
            }
        }

        // Helper to process lists for logical constraints
        let mut process_list_constraint = |predicate_ref| {
            for quad_res in self.store.quads_for_pattern(
                None,
                Some(predicate_ref),
                None,
                Some(shape_graph_name_ref),
            ) {
                if let Ok(quad) = quad_res {
                    let list_head_term = quad.object; // This is Term
                                                      // parse_rdf_list will also use shape_graph_name_ref internally
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

    pub fn parse(&mut self) -> Result<(), String> {
        // parses the shape graph to get all of the shapes and components defined within
        let shapes = self.get_node_shapes();
        for shape in shapes {
            self.parse_node_shape(shape.as_ref())?;
        }

        let pshapes = self.get_property_shapes();
        for pshape in pshapes {
            self.parse_property_shape(pshape.as_ref())?;
        }
        Ok(())
    }

    pub fn parse_node_shape(&mut self, shape: TermRef) -> Result<ID, String> {
        // Parses a shape from the shape graph and returns its ID.
        // Adds the shape to the node_shapes map.
        let id = self.get_or_create_node_id(shape.into());
        let sh = SHACL::new();

        let subject: SubjectRef = shape.to_subject_ref();
        let shape_graph_name = GraphName::NamedNode(self.shape_graph_iri.clone());

        // get the targets
        let mut targets: Vec<crate::types::Target> = self
            .store
            .quads_for_pattern(Some(subject), None, None, Some(shape_graph_name.as_ref()))
            .filter_map(Result::ok)
            .filter_map(|quad| {
                crate::types::Target::from_predicate_object(
                    quad.predicate.as_ref(),
                    quad.object.as_ref(),
                )
            })
            .collect();

        // check for implicit classes. If 'shape' is also a class (rdfs:Class or owl:Class)
        // then add a Target::Class for it.
        // use store.contains(quad) to check
        let rdf = RDF::new();
        let rdfs = RDFS::new();
        let owl = OWL::new();
        let is_rdfs_class = self
            .store
            .contains(QuadRef::new(
                subject,
                rdf.type_,
                rdfs.class,
                shape_graph_name.as_ref(),
            ))
            .map_err(|e| e.to_string())?;
        let is_owl_class = self
            .store
            .contains(QuadRef::new(
                subject,
                rdf.type_,
                owl.class,
                shape_graph_name.as_ref(),
            ))
            .map_err(|e| e.to_string())?;
        if is_rdfs_class || is_owl_class {
            targets.push(crate::types::Target::Class(subject.into()));
        }

        // get constraint components
        // parse_components will internally use context.store() and context.shape_graph_iri_ref()
        let constraints = parse_components(shape, self);
        let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
        for (component_id, component) in constraints {
            // add the component to our context.components map
            self.components.insert(component_id, component);
        }

        let _property_shapes: Vec<PropShapeID> = self // This seems to be about sh:property linking to PropertyShapes.
            .store // It was collected but not used in NodeShape::new.
            .quads_for_pattern(
                Some(subject),
                Some(sh.property),
                None,
                Some(shape_graph_name.as_ref()),
            )
            .filter_map(Result::ok)
            .filter_map(|quad| self.propshape_id_lookup.borrow().get(&quad.object))
            .collect();
        // TODO: property_shapes are collected but not used in NodeShape::new. This might be an existing oversight or for future use.

        let severity_term_opt = self
            .store
            .quads_for_pattern(
                Some(subject),
                Some(sh.severity),
                None,
                Some(shape_graph_name.as_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next();

        let severity = severity_term_opt.as_ref().and_then(Severity::from_term);

        let node_shape = NodeShape::new(id, targets, component_ids, severity);
        self.node_shapes.insert(id, node_shape);
        Ok(id)
    }

    pub fn parse_property_shape(&mut self, pshape: TermRef) -> Result<PropShapeID, String> {
        let id = self.get_or_create_prop_id(pshape.into_owned());
        let shacl = SHACL::new();
        let subject: SubjectRef = pshape.to_subject_ref();
        let ps_shape_graph_name = GraphName::NamedNode(self.shape_graph_iri.clone());

        let path_object_term: Term = self
            .store
            .quads_for_pattern(
                Some(subject),
                Some(shacl.path),
                None,
                Some(ps_shape_graph_name.as_ref()),
            )
            .filter_map(Result::ok)
            .map(|quad| quad.object)
            .next()
            .ok_or_else(|| format!("Property shape {:?} must have a sh:path", pshape))?;

        let path = self.parse_shacl_path_recursive(path_object_term.as_ref())?;

        // get constraint components
        // parse_components will internally use context.store() and context.shape_graph_iri_ref()
        let constraints = parse_components(pshape, self);
        let component_ids: Vec<ComponentID> = constraints.keys().cloned().collect();
        for (component_id, component) in constraints {
            // add the component to our context.components map
            self.components.insert(component_id, component);
        }

        let severity_term_opt = self
            .store
            .quads_for_pattern(
                Some(subject),
                Some(shacl.severity),
                None,
                Some(ps_shape_graph_name.as_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next();

        let severity = severity_term_opt.as_ref().and_then(Severity::from_term);

        let prop_shape = PropertyShape::new(id, path, component_ids, severity);
        self.prop_shapes.insert(id, prop_shape);
        Ok(id)
    }

    // Helper function to recursively parse SHACL paths
    fn parse_shacl_path_recursive(&self, path_term_ref: TermRef) -> Result<PShapePath, String> {
        let shacl = SHACL::new();
        let _rdf = RDF::new();
        let shape_graph_name_ref = self.shape_graph_iri_ref();

        // Check for sh:inversePath
        if let Some(inverse_path_obj) = self
            .store
            .quads_for_pattern(
                Some(path_term_ref.to_subject_ref()),
                Some(shacl.inverse_path),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next()
        {
            let inner_path = self.parse_shacl_path_recursive(inverse_path_obj.as_ref())?;
            return Ok(PShapePath::Inverse(Box::new(inner_path)));
        }

        // Check for sh:alternativePath (RDF list)
        if let Some(alt_list_head) = self
            .store
            .quads_for_pattern(
                Some(path_term_ref.to_subject_ref()),
                Some(shacl.alternative_path),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next()
        {
            let alt_paths_terms = self.parse_rdf_list(alt_list_head);
            let alt_paths: Result<Vec<PShapePath>, String> = alt_paths_terms
                .iter()
                .map(|term| self.parse_shacl_path_recursive(term.as_ref()))
                .collect();
            return Ok(PShapePath::Alternative(alt_paths?));
        }

        // Check for sh:zeroOrMorePath
        if let Some(zom_path_obj) = self
            .store
            .quads_for_pattern(
                Some(path_term_ref.to_subject_ref()),
                Some(shacl.zero_or_more_path),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next()
        {
            let inner_path = self.parse_shacl_path_recursive(zom_path_obj.as_ref())?;
            return Ok(PShapePath::ZeroOrMore(Box::new(inner_path)));
        }

        // Check for sh:oneOrMorePath
        if let Some(oom_path_obj) = self
            .store
            .quads_for_pattern(
                Some(path_term_ref.to_subject_ref()),
                Some(shacl.one_or_more_path),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next()
        {
            let inner_path = self.parse_shacl_path_recursive(oom_path_obj.as_ref())?;
            return Ok(PShapePath::OneOrMore(Box::new(inner_path)));
        }

        // Check for sh:zeroOrOnePath
        if let Some(zoo_path_obj) = self
            .store
            .quads_for_pattern(
                Some(path_term_ref.to_subject_ref()),
                Some(shacl.zero_or_one_path),
                None,
                Some(shape_graph_name_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .next()
        {
            let inner_path = self.parse_shacl_path_recursive(zoo_path_obj.as_ref())?;
            return Ok(PShapePath::ZeroOrOne(Box::new(inner_path)));
        }

        let seq_paths_terms = self.parse_rdf_list(path_term_ref.into_owned());
        if !seq_paths_terms.is_empty() {
            let seq_paths: Result<Vec<PShapePath>, String> = seq_paths_terms
                .iter()
                .map(|term| self.parse_shacl_path_recursive(term.as_ref()))
                .collect();
            return Ok(PShapePath::Sequence(seq_paths?));
        }

        // If it's not a complex path node, it must be a simple path (an IRI)
        match path_term_ref {
            TermRef::NamedNode(_) => Ok(PShapePath::Simple(path_term_ref.into_owned())),
            _ => Err(format!("Expected an IRI for a simple path or a blank node for a complex path, found: {:?}", path_term_ref)),
        }
    }

    // Parses an RDF list starting from list_head_term (owned Term) and returns a Vec of owned Terms.
    pub fn parse_rdf_list(&self, list_head_term: Term) -> Vec<Term> {
        let mut items: Vec<Term> = Vec::new();
        let rdf = RDF::new();
        let mut current_term = list_head_term;
        let nil_term: Term = rdf.nil.into_owned().into(); // Convert NamedNodeRef to Term
        let shape_graph_name_ref = GraphNameRef::NamedNode(self.shape_graph_iri.as_ref());

        while current_term != nil_term {
            let subject_ref = match current_term.as_ref() {
                TermRef::NamedNode(nn) => SubjectRef::NamedNode(nn),
                TermRef::BlankNode(bn) => SubjectRef::BlankNode(bn),
                _ => return items, // Or handle error: list node not an IRI/BlankNode
            };

            let first_val_opt: Option<Term> = self
                .store
                .quads_for_pattern(
                    Some(subject_ref),
                    Some(rdf.first),
                    None,
                    Some(shape_graph_name_ref),
                )
                .filter_map(Result::ok)
                .map(|q| q.object)
                .next();

            if let Some(val) = first_val_opt {
                items.push(val);
            } else {
                break; // Malformed list (no rdf:first)
            }

            let rest_node_opt: Option<Term> = self
                .store
                .quads_for_pattern(
                    Some(subject_ref),
                    Some(rdf.rest),
                    None,
                    Some(shape_graph_name_ref),
                )
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

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn shape_graph_iri_ref(&self) -> GraphNameRef {
        GraphNameRef::NamedNode(self.shape_graph_iri.as_ref())
    }

    pub fn data_graph_iri_ref(&self) -> GraphNameRef {
        GraphNameRef::NamedNode(self.data_graph_iri.as_ref())
    }

    /// Returns an ID for the given term, creating a new one if necessary for a NodeShape.
    pub fn get_or_create_node_id(&self, term: Term) -> ID {
        self.nodeshape_id_lookup
            .borrow_mut()
            .get_or_create_id(term)
    }

    pub fn get_or_create_prop_id(&self, term: Term) -> PropShapeID {
        self.propshape_id_lookup
            .borrow_mut()
            .get_or_create_id(term)
    }

    /// Returns a ComponentID for the given term, creating a new one if necessary for a Component.
    pub fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup
            .borrow_mut()
            .get_or_create_id(term)
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
                let label = self
                    .nodeshape_id_lookup
                    .borrow()
                    .get_term(*id)
                    .map_or_else(
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
                let label = self
                    .get_component_by_id(id)
                    .map_or_else(
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

#[derive(Debug, Clone)]
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
        self.execution_trace.push(TraceItem::Component(component_id));
    }

    pub fn execution_trace(&self) -> &Vec<TraceItem> {
        &self.execution_trace
    }
}
