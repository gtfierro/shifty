use crate::context::model::ShapesModel;
use crate::shape::{NodeShape, PropertyShape};
use oxigraph::model::{GraphName, NamedNode, Quad};
use shacl_ir::{NodeShapeIR, PropertyShapeIR, ShapeIR};
use std::collections::HashMap;

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

pub(crate) fn build_shape_ir(
    model: &ShapesModel,
    data_graph: Option<NamedNode>,
    shape_graphs: &[NamedNode],
) -> Result<ShapeIR, String> {
    let node_shapes = model.node_shapes.values().map(node_ir).collect();
    let property_shapes = model.prop_shapes.values().map(prop_ir).collect();

    let node_shape_terms: HashMap<_, _> = {
        let lookup = model.nodeshape_id_lookup.read().unwrap();
        model
            .node_shapes
            .keys()
            .filter_map(|id| lookup.get_term(*id).cloned().map(|term| (*id, term)))
            .collect()
    };

    let property_shape_terms: HashMap<_, _> = {
        let lookup = model.propshape_id_lookup.read().unwrap();
        model
            .prop_shapes
            .keys()
            .filter_map(|id| lookup.get_term(*id).cloned().map(|term| (*id, term)))
            .collect()
    };

    let mut shape_quads: Vec<Quad> = Vec::new();
    for graph in shape_graphs {
        let graph_name = GraphName::NamedNode(graph.clone());
        let mut graph_quads = model
            .store
            .quads_for_pattern(None, None, None, Some(graph_name.as_ref()))
            .map(|res| res.map_err(|e| e.to_string()))
            .collect::<Result<Vec<Quad>, _>>()?;
        shape_quads.append(&mut graph_quads);
    }

    Ok(ShapeIR {
        shape_graph: model.shape_graph_iri.clone(),
        data_graph,
        node_shapes,
        property_shapes,
        components: model.component_descriptors.clone(),
        component_templates: model.component_templates.clone(),
        shape_templates: model.shape_templates.clone(),
        shape_template_cache: model.shape_template_cache.clone(),
        node_shape_terms,
        property_shape_terms,
        shape_quads,
        rules: model.rules.clone(),
        node_shape_rules: model.node_shape_rules.clone(),
        prop_shape_rules: model.prop_shape_rules.clone(),
        features: model.features.clone(),
    })
}

// Re-export IR types for downstream callers.
pub use shacl_ir::{ComponentDescriptor as IRComponentDescriptor, ShapeIR as IRShapeIR};
