use crate::context::model::ShapesModel;
use crate::shape::{NodeShape, PropertyShape};
use oxigraph::model::NamedNode;
use shacl_ir::{ComponentDescriptor, FeatureToggles, NodeShapeIR, PropertyShapeIR, ShapeIR};

fn node_ir(shape: &NodeShape) -> NodeShapeIR {
    NodeShapeIR {
        id: *shape.identifier(),
        targets: shape.targets.clone(),
        constraints: shape.constraints().to_vec(),
        severity: shape.severity().clone(),
        deactivated: shape.is_deactivated(),
    }
}

fn prop_ir(shape: &PropertyShape) -> PropertyShapeIR {
    PropertyShapeIR {
        id: *shape.identifier(),
        targets: shape.targets.clone(),
        path: shape.path().clone(),
        path_term: shape.path_term().clone(),
        constraints: shape.constraints().to_vec(),
        severity: shape.severity().clone(),
        deactivated: shape.is_deactivated(),
    }
}

pub(crate) fn build_shape_ir(model: &ShapesModel, data_graph: Option<NamedNode>) -> ShapeIR {
    let node_shapes = model.node_shapes.values().map(node_ir).collect();
    let property_shapes = model.prop_shapes.values().map(prop_ir).collect();

    ShapeIR {
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

// Re-export IR types for downstream callers.
pub use shacl_ir::{ComponentDescriptor as IRComponentDescriptor, ShapeIR as IRShapeIR};
