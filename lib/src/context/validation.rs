use super::graphviz::format_term_for_label;
use super::model::ShapesModel;
use crate::runtime::{build_component_from_descriptor, Component};
use crate::types::{ComponentID, Path as PShapePath, PropShapeID, TraceItem, ID};
use oxigraph::model::{GraphNameRef, NamedNode, NamedNodeRef, Term};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::rc::Rc;

pub struct ValidationContext {
    pub(crate) model: Rc<ShapesModel>,
    pub(crate) data_graph_iri: NamedNode,
    data_graph_skolem_base: String,
    shape_graph_skolem_base: String,
    pub(crate) execution_traces: RefCell<Vec<Vec<TraceItem>>>,
    pub(crate) components: HashMap<ComponentID, Component>,
}

impl ValidationContext {
    pub(crate) fn new(model: Rc<ShapesModel>, data_graph_iri: NamedNode) -> Self {
        let data_graph_skolem_base = format!(
            "{}/.well-known/skolem/",
            data_graph_iri.as_str().trim_end_matches('/')
        );
        let shape_graph_skolem_base = format!(
            "{}/.well-known/skolem/",
            model.shape_graph_iri.as_str().trim_end_matches('/')
        );
        let components = model
            .component_descriptors
            .iter()
            .map(|(id, descriptor)| (*id, build_component_from_descriptor(descriptor)))
            .collect();

        Self {
            model,
            data_graph_iri,
            data_graph_skolem_base,
            shape_graph_skolem_base,
            execution_traces: RefCell::new(Vec::new()),
            components,
        }
    }

    pub(crate) fn data_graph_iri_ref(&self) -> GraphNameRef<'_> {
        GraphNameRef::NamedNode(self.data_graph_iri.as_ref())
    }

    pub(crate) fn new_trace(&self) -> usize {
        let mut traces = self.execution_traces.borrow_mut();
        traces.push(Vec::new());
        traces.len() - 1
    }

    pub(crate) fn get_component(&self, id: &ComponentID) -> Option<&Component> {
        self.components.get(id)
    }

    pub(crate) fn is_data_skolem_iri(&self, node: NamedNodeRef<'_>) -> bool {
        node.as_str().starts_with(&self.data_graph_skolem_base)
    }

    pub(crate) fn is_shape_skolem_iri(&self, node: NamedNodeRef<'_>) -> bool {
        node.as_str().starts_with(&self.shape_graph_skolem_base)
    }

    pub(crate) fn get_trace_item_label_and_type(&self, item: &TraceItem) -> (String, String) {
        match item {
            TraceItem::NodeShape(id) => {
                let label = self
                    .model
                    .nodeshape_id_lookup
                    .borrow()
                    .get_term(*id)
                    .map_or_else(
                        || format!("Unknown NodeShape ID: {:?}", id),
                        format_term_for_label,
                    );
                (label, "NodeShape".to_string())
            }
            TraceItem::PropertyShape(id) => {
                let label = self.model.get_prop_shape_by_id(id).map_or_else(
                    || format!("Unknown PropertyShape ID: {:?}", id),
                    |ps| ps.sparql_path(),
                );
                (label, "PropertyShape".to_string())
            }
            TraceItem::Component(id) => {
                let label = self
                    .model
                    .get_component_descriptor(id)
                    .map(|descriptor| build_component_from_descriptor(descriptor).label())
                    .unwrap_or_else(|| format!("Unknown Component ID: {:?}", id));
                (label, "Component".to_string())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SourceShape {
    NodeShape(ID),
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
    pub(crate) fn as_prop_id(&self) -> Option<&PropShapeID> {
        match self {
            SourceShape::PropertyShape(id) => Some(id),
            _ => None,
        }
    }

    pub(crate) fn as_node_id(&self) -> Option<&ID> {
        match self {
            SourceShape::NodeShape(id) => Some(id),
            _ => None,
        }
    }

    pub(crate) fn get_term(&self, ctx: &ValidationContext) -> Option<Term> {
        match self {
            SourceShape::NodeShape(id) => ctx
                .model
                .nodeshape_id_lookup()
                .borrow()
                .get_term(*id)
                .cloned(),
            SourceShape::PropertyShape(id) => ctx
                .model
                .propshape_id_lookup()
                .borrow()
                .get_term(*id)
                .cloned(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Context {
    focus_node: Term,
    pub(crate) result_path: Option<PShapePath>,
    value_nodes: Option<Vec<Term>>,
    value: Option<Term>,
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
    pub(crate) fn new(
        focus_node: Term,
        result_path: Option<PShapePath>,
        value_nodes: Option<Vec<Term>>,
        source_shape: SourceShape,
        trace_index: usize,
    ) -> Self {
        Self {
            focus_node,
            result_path,
            value_nodes,
            source_shape,
            value: None,
            trace_index,
            source_constraint: None,
        }
    }

    pub(crate) fn with_value(&mut self, value: Term) {
        self.value = Some(value);
    }

    pub(crate) fn with_result_path(&mut self, result_path: Term) {
        self.result_path = Some(crate::types::Path::Simple(result_path));
    }

    pub(crate) fn set_focus_node(&mut self, focus_node: Term) {
        self.focus_node = focus_node;
    }

    pub(crate) fn focus_node(&self) -> &Term {
        &self.focus_node
    }

    pub(crate) fn result_path(&self) -> Option<&PShapePath> {
        self.result_path.as_ref()
    }

    pub(crate) fn value_nodes(&self) -> Option<&Vec<Term>> {
        self.value_nodes.as_ref()
    }

    pub(crate) fn value_nodes_mut(&mut self) -> Option<&mut Vec<Term>> {
        self.value_nodes.as_mut()
    }

    pub(crate) fn source_shape(&self) -> SourceShape {
        self.source_shape.clone()
    }

    pub(crate) fn trace_index(&self) -> usize {
        self.trace_index
    }

    pub(crate) fn set_trace_index(&mut self, index: usize) {
        self.trace_index = index;
    }
}
