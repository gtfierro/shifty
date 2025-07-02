use crate::canonicalization;
use crate::components::Component;
use crate::optimize::Optimizer;
use crate::parser;
use crate::report::ValidationReportBuilder;
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, TraceItem, ID};
use log::{error, info};
use ontoenv::api::OntoEnv;
use ontoenv::ontology::OntologyLocation;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphNameRef, NamedNode, Term};
use oxigraph::store::Store;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::Path;

/// Filters a string to keep only alphanumeric characters, for use in Graphviz identifiers.
pub(crate) fn sanitize_graphviz_string(input: &str) -> String {
    input.chars().filter(|c| c.is_alphanumeric()).collect()
}

/// Formats a `Term` for display in a Graphviz label, typically by extracting the local name of an IRI.
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
        Term::BlankNode(_) => "BlankNode".to_string(),
        Term::Literal(lit) => lit.value().to_string().replace('"', "\\\""), // Escape quotes for DOT language
        Term::Triple(_t) => "rdf_triple".to_string(),                       // Handle Triple case
    }
}

/// A lookup table to map `Term`s to unique, compact integer-based IDs.
///
/// This is used to efficiently reference shapes and components using simple integer IDs
/// instead of complex `Term` objects.
pub(crate) struct IDLookupTable<IdType: Copy + Eq + Hash> {
    id_map: std::collections::HashMap<Term, IdType>,
    id_to_term: std::collections::HashMap<IdType, Term>,
    next_id: u64, // Internal counter remains u64
}

impl<IdType: Copy + Eq + Hash + From<u64>> IDLookupTable<IdType> {
    /// Creates a new, empty `IDLookupTable`.
    pub(crate) fn new() -> Self {
        IDLookupTable {
            id_map: std::collections::HashMap::new(),
            id_to_term: std::collections::HashMap::new(),
            next_id: 0,
        }
    }

    /// Returns an ID for the given term, creating a new ID if it doesn't exist.
    pub(crate) fn get_or_create_id(&mut self, term: Term) -> IdType {
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

    /// Looks up the ID for a given term.
    pub(crate) fn get(&self, term: &Term) -> Option<IdType> {
        self.id_map.get(term).map(|id| id.to_owned())
    }

    /// Returns the term associated with the given ID.
    pub(crate) fn get_term(&self, id: IdType) -> Option<&Term> {
        self.id_to_term.get(&id)
    }
}

/// The central struct for managing the validation process.
///
/// It holds the shapes and data graphs, parsed shapes, and other
/// contextual information needed for validation. This provides an
/// advanced API for users who need more control than the simple `Validator` facade.
pub struct ValidationContext {
    /// Lookup table for node shape `Term`s to `ID`s.
    pub(crate) nodeshape_id_lookup: RefCell<IDLookupTable<ID>>,
    /// Lookup table for property shape `Term`s to `PropShapeID`s.
    pub(crate) propshape_id_lookup: RefCell<IDLookupTable<PropShapeID>>,
    /// Lookup table for component `Term`s to `ComponentID`s.
    pub(crate) component_id_lookup: RefCell<IDLookupTable<ComponentID>>,
    /// The Oxigraph `Store` containing both shapes and data graphs.
    pub(crate) store: Store,
    /// The `NamedNode` identifying the shapes graph.
    pub(crate) shape_graph_iri: NamedNode,
    /// The `NamedNode` identifying the data graph.
    pub(crate) data_graph_iri: NamedNode,
    /// A map from `ID` to the parsed `NodeShape`.
    pub(crate) node_shapes: HashMap<ID, NodeShape>,
    /// A map from `PropShapeID` to the parsed `PropertyShape`.
    pub(crate) prop_shapes: HashMap<PropShapeID, PropertyShape>,
    /// A map from `ComponentID` to the parsed `Component`.
    pub(crate) components: HashMap<ComponentID, Component>,
    env: OntoEnv,
    /// A collection of all execution traces generated during validation.
    pub(crate) execution_traces: RefCell<Vec<Vec<TraceItem>>>,
}

impl ValidationContext {
    /// Creates a new `ValidationContext` with the given store and graph IRIs.
    pub(crate) fn new(
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
            env,
            execution_traces: RefCell::new(Vec::new()),
        }
    }

    /// Validates the loaded data graph against the loaded shapes graph.
    pub fn validate(&self) -> ValidationReportBuilder {
        let mut b = ValidationReportBuilder::new();
        info!("Validating NodeShapes and PropertyShapes in the context");
        for node_shape in self.node_shapes.values() {
            // Validate each NodeShape
            if let Err(e) = node_shape.process_targets(self, &mut b) {
                error!(
                    "Error validating NodeShape {}: {}",
                    node_shape.identifier(),
                    e
                );
            }
        }

        for prop_shape in self.prop_shapes.values() {
            if !prop_shape.targets.is_empty() {
                if let Err(e) = prop_shape.process_targets(self, &mut b) {
                    error!(
                        "Error validating PropertyShape {}: {}",
                        prop_shape.identifier(),
                        e
                    );
                }
            }
        }
        b
    }

    /// Generates a Graphviz DOT string representation of the shapes.
    pub(crate) fn graphviz(&self) -> Result<String, String> {
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

    /// Generates a Graphviz DOT string representation of the shapes, with nodes colored by execution frequency.
    pub(crate) fn graphviz_heatmap(
        &self,
        include_all_nodes: bool,
    ) -> Result<String, String> {
        let mut frequencies: HashMap<TraceItem, usize> = HashMap::new();
        for trace in self.execution_traces.borrow().iter() {
            for item in trace.iter() {
                *frequencies.entry(item.clone()).or_insert(0) += 1;
            }
        }

        let total_freq = frequencies.values().sum::<usize>();
        let max_freq = frequencies.values().max().copied().unwrap_or(1) as f32;

        let get_color = |count: usize| -> String {
            if count == 0 {
                return "#FFFFFF".to_string(); // white for not visited
            }
            let ratio = count as f32 / max_freq;
            // from white (255,255,255) to dark red (139,0,0)
            let r = (255.0 - (255.0 - 139.0) * ratio) as u8;
            let g = (255.0 - (255.0 - 0.0) * ratio) as u8;
            let b = (255.0 - (255.0 - 0.0) * ratio) as u8;
            format!("#{:02X}{:02X}{:02X}", r, g, b)
        };

        let mut dot_string = String::new();
        dot_string.push_str("digraph {\n");
        dot_string.push_str("    node [style=filled];\n");

        // Node Shapes
        for shape in self.node_shapes.values() {
            let trace_item = TraceItem::NodeShape(*shape.identifier());
            let count = frequencies.get(&trace_item).copied().unwrap_or(0);

            if !include_all_nodes && count == 0 {
                continue;
            }

            let color = get_color(count);
            let relative_freq = if total_freq > 0 {
                (count as f32 / total_freq as f32) * 100.0
            } else {
                0.0
            };

            let name = self
                .nodeshape_id_lookup
                .borrow()
                .id_to_term
                .get(shape.identifier())
                .ok_or_else(|| format!("Missing term for nodeshape ID: {:?}", shape.identifier()))?
                .clone();
            let name_label = format_term_for_label(&name);
            dot_string.push_str(&format!(
                "  {} [label=\"NodeShape\\n{}\\n({:.2}%) ({}/{})\", fillcolor=\"{}\"];\n",
                shape.identifier().to_graphviz_id(),
                name_label,
                relative_freq,
                count,
                total_freq,
                color
            ));
            for comp_id in shape.constraints() {
                let comp_trace_item = TraceItem::Component(*comp_id);
                let comp_count = frequencies.get(&comp_trace_item).copied().unwrap_or(0);

                if include_all_nodes || comp_count > 0 {
                    dot_string.push_str(&format!(
                        "    {} -> {};\n",
                        shape.identifier().to_graphviz_id(),
                        comp_id.to_graphviz_id()
                    ));
                }
            }
        }

        // Property Shapes
        for pshape in self.prop_shapes.values() {
            let trace_item = TraceItem::PropertyShape(*pshape.identifier());
            let count = frequencies.get(&trace_item).copied().unwrap_or(0);

            if !include_all_nodes && count == 0 {
                continue;
            }

            let color = get_color(count);
            let relative_freq = if total_freq > 0 {
                (count as f32 / total_freq as f32) * 100.0
            } else {
                0.0
            };

            let _name = self
                .propshape_id_lookup
                .borrow()
                .id_to_term
                .get(pshape.identifier())
                .ok_or_else(|| {
                    format!("Missing term for propshape ID: {:?}", pshape.identifier())
                })?
                .clone();

            let path_label = pshape.sparql_path();
            dot_string.push_str(&format!(
                "  {} [label=\"PropertyShape\\nPath: {}\\n({:.2}%) ({}/{})\", fillcolor=\"{}\"];\n",
                pshape.identifier().to_graphviz_id(),
                path_label,
                relative_freq,
                count,
                total_freq,
                color
            ));
            for comp_id in pshape.constraints() {
                let comp_trace_item = TraceItem::Component(*comp_id);
                let comp_count = frequencies.get(&comp_trace_item).copied().unwrap_or(0);

                if include_all_nodes || comp_count > 0 {
                    dot_string.push_str(&format!(
                        "    {} -> {};\n",
                        pshape.identifier().to_graphviz_id(),
                        comp_id.to_graphviz_id()
                    ));
                }
            }
        }

        // Components
        for (ident, comp) in self.components.iter() {
            let trace_item = TraceItem::Component(*ident);
            let count = frequencies.get(&trace_item).copied().unwrap_or(0);

            if !include_all_nodes && count == 0 {
                continue;
            }

            let color = get_color(count);
            let relative_freq = if total_freq > 0 {
                (count as f32 / total_freq as f32) * 100.0
            } else {
                0.0
            };

            let comp_str = comp.to_graphviz_string(*ident, self);
            for line in comp_str.lines() {
                let mut modified_line = line.to_string();
                if let Some(start_pos) = modified_line.find('[') {
                    if let Some(end_pos) = modified_line.rfind(']') {
                        let color_attr = format!("fillcolor=\"{}\", ", color);
                        modified_line.insert_str(start_pos + 1, &color_attr);

                        // Augment label in the now-modified line
                        if let Some(label_start) = modified_line.find("label=\"") {
                            // The end of attributes is now at a new position
                            let new_end_pos = end_pos + color_attr.len();
                            if let Some(label_end) = modified_line[..new_end_pos].rfind('"') {
                                if label_end > label_start {
                                    let freq_text = format!("\\n({:.2}%) ({}/{})", relative_freq, count, total_freq);
                                    modified_line.insert_str(label_end, &freq_text);
                                }
                            }
                        }
                    }
                }
                dot_string.push_str(&format!("    {}\n", modified_line.trim()));
            }
        }

        dot_string.push_str("}\n");
        Ok(dot_string)
    }

    /// Creates a new `ValidationContext` by loading shapes and data from files.
    pub fn from_files(
        shape_graph_path: &str,
        data_graph_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let mut env = OntoEnv::new_in_memory_online_with_search()?;

        let shape_graph_location = OntologyLocation::from_str(shape_graph_path)?;
        info!("Added shape graph: {}", shape_graph_location);
        let shape_id = env.add(shape_graph_location, false)?;
        let shape_graph_iri = env.get_ontology(&shape_id).unwrap().name().clone();

        let data_graph_location = OntologyLocation::from_str(data_graph_path)?;
        info!("Added data graph: {}", data_graph_location);
        let data_id = env.add(data_graph_location, false)?;
        let data_graph_iri = env.get_ontology(&data_id).unwrap().name().clone();

        let store = env.io().store().clone();

        let shape_graph_base_iri =
            format!("{}/.well-known/skolem/", shape_graph_iri.as_str().trim_end_matches('/'));
        info!(
            "Skolemizing shape graph <{}> with base IRI <{}>",
            shape_graph_iri, shape_graph_base_iri
        );
        //canonicalization::skolemize(
        //    &store,
        //    GraphNameRef::NamedNode(shape_graph_iri.as_ref()),
        //    &shape_graph_base_iri,
        //)?;

        let data_graph_base_iri =
            format!("{}/.well-known/skolem/", data_graph_iri.as_str().trim_end_matches('/'));
        info!(
            "Skolemizing data graph <{}> with base IRI <{}>",
            data_graph_iri, data_graph_base_iri
        );
        //canonicalization::skolemize(
        //    &store,
        //    GraphNameRef::NamedNode(data_graph_iri.as_ref()),
        //    &data_graph_base_iri,
        //)?;

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

    /// Parses an RDF list from the shapes graph, starting from the given head term.
    pub(crate) fn parse_rdf_list(&self, list_head_term: Term) -> Vec<Term> {
        parser::parse_rdf_list(self, list_head_term)
    }

    /// Returns a reference to the underlying `Store`.
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// Returns a reference to the `OntoEnv`.
    pub(crate) fn env(&self) -> &OntoEnv {
        &self.env
    }

    /// Returns the shapes graph IRI as a `GraphNameRef`.
    pub(crate) fn shape_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.shape_graph_iri.as_ref())
    }

    /// Returns the data graph IRI as a `GraphNameRef`.
    pub(crate) fn data_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.data_graph_iri.as_ref())
    }

    /// Returns an ID for the given term, creating a new one if necessary for a NodeShape.
    pub(crate) fn get_or_create_node_id(&self, term: Term) -> ID {
        self.nodeshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    /// Returns an ID for the given term, creating a new one if necessary for a PropertyShape.
    pub(crate) fn get_or_create_prop_id(&self, term: Term) -> PropShapeID {
        self.propshape_id_lookup.borrow_mut().get_or_create_id(term)
    }

    /// Returns a ComponentID for the given term, creating a new one if necessary for a Component.
    pub(crate) fn get_or_create_component_id(&self, term: Term) -> ComponentID {
        self.component_id_lookup.borrow_mut().get_or_create_id(term)
    }

    /// Creates a new execution trace and returns its index.
    pub(crate) fn new_trace(&self) -> usize {
        let mut traces = self.execution_traces.borrow_mut();
        traces.push(Vec::new());
        traces.len() - 1
    }

    /// Returns a reference to the node shape ID lookup table.
    pub(crate) fn nodeshape_id_lookup(&self) -> &RefCell<IDLookupTable<ID>> {
        &self.nodeshape_id_lookup
    }

    /// Returns a reference to the property shape ID lookup table.
    pub(crate) fn propshape_id_lookup(&self) -> &RefCell<IDLookupTable<PropShapeID>> {
        &self.propshape_id_lookup
    }

    /// Retrieves a component by its `ComponentID`.
    pub(crate) fn get_component_by_id(&self, id: &ComponentID) -> Option<&Component> {
        // Returns a reference to the component by its ID
        self.components.get(id)
    }

    /// Retrieves a property shape by its `PropShapeID`.
    pub(crate) fn get_prop_shape_by_id(&self, id: &PropShapeID) -> Option<&PropertyShape> {
        // Returns a reference to the PropertyShape by its ID
        self.prop_shapes.get(id)
    }

    /// Retrieves a node shape by its `ID`.
    pub(crate) fn get_node_shape_by_id(&self, id: &ID) -> Option<&NodeShape> {
        // Returns a reference to the NodeShape by its ID
        self.node_shapes.get(id)
    }

    /// Gets a human-readable label and type string for a `TraceItem`.
    pub(crate) fn get_trace_item_label_and_type(&self, item: &TraceItem) -> (String, String) {
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

/// Identifies the source shape that initiated a validation context, either a node or property shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SourceShape {
    /// The source was a `NodeShape`.
    NodeShape(ID),
    /// The source was a `PropertyShape`.
    PropertyShape(PropShapeID),
}

impl fmt::Display for SourceShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceShape::NodeShape(id) => write!(f, "{}", id),
            SourceShape::PropertyShape(id) => write!(f, "{}", id),
        }
    }
}

impl SourceShape {
    /// Returns the `PropShapeID` if the source is a property shape.
    pub(crate) fn as_prop_id(&self) -> Option<&PropShapeID> {
        match self {
            SourceShape::PropertyShape(id) => Some(id),
            _ => None,
        }
    }
    /// Returns the `ID` if the source is a node shape.
    pub(crate) fn as_node_id(&self) -> Option<&ID> {
        match self {
            SourceShape::NodeShape(id) => Some(id),
            _ => None,
        }
    }
    /// Retrieves the original `Term` of the source shape from the context.
    pub(crate) fn get_term(&self, ctx: &ValidationContext) -> Option<Term> {
        match self {
            SourceShape::NodeShape(id) => ctx
                .nodeshape_id_lookup()
                .borrow()
                .get_term(*id)
                .map(|t| t.clone()),
            SourceShape::PropertyShape(id) => ctx
                .propshape_id_lookup()
                .borrow()
                .get_term(*id)
                .map(|t| t.clone()),
        }
    }
}

/// Represents the state of a validation process at a specific point.
///
/// It contains the focus node, the path taken to reach the current value nodes,
/// and the value nodes themselves.
#[derive(Debug, Clone)]
pub(crate) struct Context {
    focus_node: Term,
    /// The property path that led to the current value nodes.
    pub(crate) result_path: Option<PShapePath>,
    value_nodes: Option<Vec<Term>>,
    value: Option<Term>, // something that violated a component
    source_shape: SourceShape,
    trace_index: usize,
    pub source_constraint: Option<Term>,
}

impl PartialEq for Context {
    fn eq(&self, other: &Self) -> bool {
        self.focus_node == other.focus_node
            && self.result_path == other.result_path
            && self.value_nodes == other.value_nodes
            && self.value == other.value
            && self.source_shape == other.source_shape
            && self.source_constraint == other.source_constraint
    }
}

impl Eq for Context {}

impl Hash for Context {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.focus_node.hash(state);
        self.result_path.hash(state);
        self.value_nodes.hash(state);
        self.value.hash(state);
        self.source_shape.hash(state);
        self.source_constraint.hash(state);
    }
}

impl Context {
    /// Creates a new `Context` for a validation process.
    pub(crate) fn new(
        focus_node: Term,
        result_path: Option<PShapePath>,
        value_nodes: Option<Vec<Term>>,
        source_shape: SourceShape,
        trace_index: usize,
    ) -> Self {
        Context {
            focus_node,
            result_path,
            value_nodes,
            source_shape,
            value: None,
            trace_index,
            source_constraint: None,
        }
    }

    /// Sets the specific value that violated a constraint.
    pub(crate) fn with_value(&mut self, value: Term) {
        self.value = Some(value);
    }

    /// Sets the result path for the context.
    pub(crate) fn with_result_path(&mut self, result_path: Term) {
        // In our implementation, we use a Simple path containing the given term.
        self.result_path = Some(crate::types::Path::Simple(result_path));
    }

    /// Returns the focus node for the current validation.
    pub(crate) fn focus_node(&self) -> &Term {
        &self.focus_node
    }

    /// Returns the property path that led to the current value nodes.
    pub(crate) fn result_path(&self) -> Option<&PShapePath> {
        self.result_path.as_ref()
    }

    /// Returns the set of value nodes being validated.
    pub(crate) fn value_nodes(&self) -> Option<&Vec<Term>> {
        self.value_nodes.as_ref()
    }

    /// Returns the source shape that initiated this validation context.
    pub(crate) fn source_shape(&self) -> SourceShape {
        self.source_shape.clone()
    }

    /// Returns the index of the execution trace associated with this context.
    pub(crate) fn trace_index(&self) -> usize {
        self.trace_index
    }

    /// Sets the index of the execution trace for this context.
    pub(crate) fn set_trace_index(&mut self, index: usize) {
        self.trace_index = index;
    }
}
