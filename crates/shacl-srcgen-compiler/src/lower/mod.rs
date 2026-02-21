use crate::ir::{
    FallbackAnnotation, ShapeKind, SrcGenComponent, SrcGenIR, SrcGenMeta, SrcGenShape,
};
use oxigraph::model::Term;
use shifty::shacl_ir::{ComponentDescriptor, ShapeIR};

const FALLBACK_REASON: &str =
    "phase-0 srcgen compiler delegates validation to shared runtime fallback";

pub fn lower_shape_ir(shape_ir: &ShapeIR) -> Result<SrcGenIR, String> {
    let mut shape_rows: Vec<(String, ShapeKind)> = Vec::new();

    for term in shape_ir.node_shape_terms.values() {
        if let Term::NamedNode(node) = term {
            shape_rows.push((node.as_str().to_string(), ShapeKind::Node));
        }
    }
    for term in shape_ir.property_shape_terms.values() {
        if let Term::NamedNode(node) = term {
            shape_rows.push((node.as_str().to_string(), ShapeKind::Property));
        }
    }

    shape_rows.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| shape_kind_key(a.1).cmp(&shape_kind_key(b.1)))
    });
    shape_rows.dedup_by(|a, b| a.0 == b.0 && shape_kind_key(a.1) == shape_kind_key(b.1));

    let shapes = shape_rows
        .into_iter()
        .enumerate()
        .map(|(idx, (iri, kind))| SrcGenShape {
            id: (idx + 1) as u64,
            iri,
            kind,
        })
        .collect();

    let mut component_rows: Vec<(u64, String)> = shape_ir
        .components
        .iter()
        .map(|(id, component)| {
            (
                id.0,
                component_constraint_component_iri(component).to_string(),
            )
        })
        .collect();
    component_rows.sort_by(|a, b| a.0.cmp(&b.0));

    let components = component_rows
        .iter()
        .map(|(id, iri)| SrcGenComponent {
            id: *id,
            iri: iri.clone(),
            fallback_only: true,
        })
        .collect();

    let fallback_annotations = component_rows
        .into_iter()
        .map(|(component_id, _)| FallbackAnnotation {
            component_id,
            reason: FALLBACK_REASON.to_string(),
        })
        .collect();

    let data_graph_iri = shape_ir
        .data_graph
        .as_ref()
        .map(|iri| iri.as_str().to_string())
        .unwrap_or_default();

    Ok(SrcGenIR {
        meta: SrcGenMeta {
            compiler_track: "srcgen".to_string(),
            schema_version: 1,
            shape_graph_iri: shape_ir.shape_graph.as_str().to_string(),
            data_graph_iri,
        },
        shapes,
        components,
        fallback_annotations,
    })
}

fn shape_kind_key(kind: ShapeKind) -> u8 {
    match kind {
        ShapeKind::Node => 0,
        ShapeKind::Property => 1,
    }
}

fn component_constraint_component_iri(component: &ComponentDescriptor) -> &str {
    match component {
        ComponentDescriptor::Node { .. } => "http://www.w3.org/ns/shacl#NodeConstraintComponent",
        ComponentDescriptor::Property { .. } => "http://www.w3.org/ns/shacl#PropertyShapeComponent",
        ComponentDescriptor::QualifiedValueShape { min_count, .. } => {
            if min_count.is_some() {
                "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
            } else {
                "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent"
            }
        }
        ComponentDescriptor::Class { .. } => "http://www.w3.org/ns/shacl#ClassConstraintComponent",
        ComponentDescriptor::Datatype { .. } => {
            "http://www.w3.org/ns/shacl#DatatypeConstraintComponent"
        }
        ComponentDescriptor::NodeKind { .. } => {
            "http://www.w3.org/ns/shacl#NodeKindConstraintComponent"
        }
        ComponentDescriptor::MinCount { .. } => {
            "http://www.w3.org/ns/shacl#MinCountConstraintComponent"
        }
        ComponentDescriptor::MaxCount { .. } => {
            "http://www.w3.org/ns/shacl#MaxCountConstraintComponent"
        }
        ComponentDescriptor::MinExclusive { .. } => {
            "http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent"
        }
        ComponentDescriptor::MinInclusive { .. } => {
            "http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent"
        }
        ComponentDescriptor::MaxExclusive { .. } => {
            "http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent"
        }
        ComponentDescriptor::MaxInclusive { .. } => {
            "http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent"
        }
        ComponentDescriptor::MinLength { .. } => {
            "http://www.w3.org/ns/shacl#MinLengthConstraintComponent"
        }
        ComponentDescriptor::MaxLength { .. } => {
            "http://www.w3.org/ns/shacl#MaxLengthConstraintComponent"
        }
        ComponentDescriptor::Pattern { .. } => {
            "http://www.w3.org/ns/shacl#PatternConstraintComponent"
        }
        ComponentDescriptor::LanguageIn { .. } => {
            "http://www.w3.org/ns/shacl#LanguageInConstraintComponent"
        }
        ComponentDescriptor::UniqueLang { .. } => {
            "http://www.w3.org/ns/shacl#UniqueLangConstraintComponent"
        }
        ComponentDescriptor::Equals { .. } => {
            "http://www.w3.org/ns/shacl#EqualsConstraintComponent"
        }
        ComponentDescriptor::Disjoint { .. } => {
            "http://www.w3.org/ns/shacl#DisjointConstraintComponent"
        }
        ComponentDescriptor::LessThan { .. } => {
            "http://www.w3.org/ns/shacl#LessThanConstraintComponent"
        }
        ComponentDescriptor::LessThanOrEquals { .. } => {
            "http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent"
        }
        ComponentDescriptor::Not { .. } => "http://www.w3.org/ns/shacl#NotConstraintComponent",
        ComponentDescriptor::And { .. } => "http://www.w3.org/ns/shacl#AndConstraintComponent",
        ComponentDescriptor::Or { .. } => "http://www.w3.org/ns/shacl#OrConstraintComponent",
        ComponentDescriptor::Xone { .. } => "http://www.w3.org/ns/shacl#XoneConstraintComponent",
        ComponentDescriptor::Closed { .. } => {
            "http://www.w3.org/ns/shacl#ClosedConstraintComponent"
        }
        ComponentDescriptor::HasValue { .. } => {
            "http://www.w3.org/ns/shacl#HasValueConstraintComponent"
        }
        ComponentDescriptor::In { .. } => "http://www.w3.org/ns/shacl#InConstraintComponent",
        ComponentDescriptor::Sparql { .. } => {
            "http://www.w3.org/ns/shacl#SPARQLConstraintComponent"
        }
        ComponentDescriptor::Custom { definition, .. } => definition.iri.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty::shacl_ir::{
        ComponentDescriptor, FeatureToggles, NodeShapeIR, PropShapeID, PropertyShapeIR, Severity,
        ShapeIR, ID,
    };
    use std::collections::HashMap;

    #[test]
    fn lower_is_deterministic_with_unsorted_maps() {
        let mut components = HashMap::new();
        components.insert(
            shifty::shacl_ir::ComponentID(9),
            ComponentDescriptor::MaxCount { max_count: 2 },
        );
        components.insert(
            shifty::shacl_ir::ComponentID(3),
            ComponentDescriptor::MinCount { min_count: 1 },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(8),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:z-node",
            )),
        );
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:a-node",
            )),
        );

        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:b-prop",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: Vec::new(),
                constraints: Vec::new(),
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(1),
                targets: Vec::new(),
                path: shifty::shacl_ir::Path::Simple(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:p"),
                )),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:p")),
                constraints: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let first = lower_shape_ir(&shape_ir).unwrap();
        let second = lower_shape_ir(&shape_ir).unwrap();

        assert_eq!(
            first.to_json_pretty().unwrap(),
            second.to_json_pretty().unwrap()
        );
        assert_eq!(first.shapes[0].iri, "urn:shape:a-node");
        assert_eq!(first.components[0].id, 3);
    }
}
