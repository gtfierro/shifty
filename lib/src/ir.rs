//! Immutable, store-free representation of a parsed shapes graph.
//!
//! The current engine stores parsed shapes inside `ShapesModel`, which still
//! carries the Oxigraph store and other runtime conveniences. `ShapeIR`
//! distills just the semantic content needed for planning and execution so
//! backends can consume a lightweight, serializable structure.

use crate::context::model::{FeatureToggles, ShapesModel};
use crate::model::{
    ComponentDescriptor, ComponentTemplateDefinition, NodeShape, PropertyShape, Rule,
    ShapeTemplateDefinition,
};
use crate::types::{ComponentID, Path, PropShapeID, RuleID, Severity, Target, ID};
use oxigraph::model::{NamedNode, Term};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct NodeShapeIR {
    pub id: ID,
    pub targets: Vec<Target>,
    pub constraints: Vec<ComponentID>,
    pub severity: Severity,
    pub deactivated: bool,
}

impl NodeShapeIR {
    fn from_node_shape(shape: &NodeShape) -> Self {
        Self {
            id: *shape.identifier(),
            targets: shape.targets.clone(),
            constraints: shape.constraints().to_vec(),
            severity: shape.severity().clone(),
            deactivated: shape.is_deactivated(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PropertyShapeIR {
    pub id: PropShapeID,
    pub targets: Vec<Target>,
    pub path: Path,
    pub path_term: Term,
    pub constraints: Vec<ComponentID>,
    pub severity: Severity,
    pub deactivated: bool,
}

impl PropertyShapeIR {
    fn from_property_shape(shape: &PropertyShape) -> Self {
        Self {
            id: *shape.identifier(),
            targets: shape.targets.clone(),
            path: shape.path().clone(),
            path_term: shape.path_term().clone(),
            constraints: shape.constraints().to_vec(),
            severity: shape.severity().clone(),
            deactivated: shape.is_deactivated(),
        }
    }
}

/// Store-free snapshot of a shapes graph plus constraint/rule metadata.
#[derive(Debug, Clone)]
pub struct ShapeIR {
    pub shape_graph: NamedNode,
    pub data_graph: Option<NamedNode>,
    pub node_shapes: Vec<NodeShapeIR>,
    pub property_shapes: Vec<PropertyShapeIR>,
    pub components: HashMap<ComponentID, ComponentDescriptor>,
    pub component_templates: HashMap<NamedNode, ComponentTemplateDefinition>,
    pub shape_templates: HashMap<NamedNode, ShapeTemplateDefinition>,
    pub rules: HashMap<RuleID, Rule>,
    pub node_shape_rules: HashMap<ID, Vec<RuleID>>,
    pub prop_shape_rules: HashMap<PropShapeID, Vec<RuleID>>,
    pub features: FeatureToggles,
}

impl ShapeIR {
    /// Materialize an IR snapshot from the existing `ShapesModel`. The optional
    /// data graph IRI is supplied by callers that own the validation context.
    pub fn from_model(model: &ShapesModel, data_graph: Option<NamedNode>) -> Self {
        let node_shapes = model
            .node_shapes
            .values()
            .map(NodeShapeIR::from_node_shape)
            .collect();
        let property_shapes = model
            .prop_shapes
            .values()
            .map(PropertyShapeIR::from_property_shape)
            .collect();

        Self {
            shape_graph: model.shape_graph_iri.clone(),
            data_graph,
            node_shapes,
            property_shapes,
            components: model.component_descriptors.clone(),
            component_templates: model.component_templates.clone(),
            shape_templates: model.shape_templates.clone(),
            rules: model.rules.clone(),
            node_shape_rules: model.node_shape_rules.clone(),
            prop_shape_rules: model.prop_shape_rules.clone(),
            features: model.features.clone(),
        }
    }
}
