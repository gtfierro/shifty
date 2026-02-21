use oxigraph::model::Term;
use shacl_srcgen_compiler::{generate_modules_from_shape_ir_with_backend, SrcGenBackend};
use shifty::shacl_ir::{
    ComponentDescriptor, ComponentID, FeatureToggles, NodeShapeIR, Path, PropShapeID,
    PropertyShapeIR, Severity, ShapeIR, ID,
};
use std::collections::HashMap;

fn sample_shape_ir() -> ShapeIR {
    let mut components = HashMap::new();
    components.insert(
        ComponentID(5),
        ComponentDescriptor::Class {
            class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Class")),
        },
    );
    components.insert(
        ComponentID(2),
        ComponentDescriptor::MinCount { min_count: 1 },
    );

    let mut node_shape_terms = HashMap::new();
    node_shape_terms.insert(
        ID(1),
        Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
            "urn:shape:NodeShapeA",
        )),
    );

    let mut property_shape_terms = HashMap::new();
    property_shape_terms.insert(
        PropShapeID(1),
        Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
            "urn:shape:PropertyShapeA",
        )),
    );

    ShapeIR {
        shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:graph:shapes"),
        data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:graph:data")),
        node_shapes: vec![NodeShapeIR {
            id: ID(1),
            targets: Vec::new(),
            constraints: vec![ComponentID(5)],
            property_shapes: vec![PropShapeID(1)],
            severity: Severity::Violation,
            deactivated: false,
        }],
        property_shapes: vec![PropertyShapeIR {
            id: PropShapeID(1),
            targets: Vec::new(),
            path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:hasValue",
            ))),
            path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:hasValue")),
            constraints: vec![ComponentID(2)],
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
    }
}

#[test]
fn generated_output_is_deterministic() {
    let ir = sample_shape_ir();
    let first = generate_modules_from_shape_ir_with_backend(&ir, SrcGenBackend::Specialized)
        .expect("first codegen run should succeed");
    let second = generate_modules_from_shape_ir_with_backend(&ir, SrcGenBackend::Specialized)
        .expect("second codegen run should succeed");

    assert_eq!(first.root, second.root);
    assert_eq!(first.files, second.files);
}

#[test]
fn module_layout_snapshot() {
    let ir = sample_shape_ir();
    let generated = generate_modules_from_shape_ir_with_backend(&ir, SrcGenBackend::Tables)
        .expect("tables codegen should succeed");

    let module_names: Vec<_> = generated
        .files
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        module_names,
        vec![
            "prelude.rs",
            "paths.rs",
            "targets.rs",
            "validators_property.rs",
            "validators_node.rs",
            "inference.rs",
            "report.rs",
            "run.rs",
        ]
    );
    assert!(
        generated.root.contains("include!(\"run.rs\");"),
        "root module must include run.rs"
    );
}

#[test]
fn prelude_snapshot_contains_expected_constants() {
    let ir = sample_shape_ir();
    let generated = generate_modules_from_shape_ir_with_backend(&ir, SrcGenBackend::Tables)
        .expect("tables codegen should succeed");
    let prelude = generated
        .files
        .iter()
        .find(|(name, _)| name == "prelude.rs")
        .map(|(_, content)| content.as_str())
        .expect("prelude module must exist");

    assert!(prelude.contains("pub const COMPILER_TRACK: &str = \"srcgen\";"));
    assert!(prelude.contains("pub const BACKEND_MODE: &str = \"tables\";"));
    assert!(prelude.contains("pub fn shape_id_for_iri(iri: &str) -> Option<u64>"));
    assert!(prelude.contains("pub struct RuntimeMetricsSnapshot"));
    assert!(prelude.contains("pub fn runtime_metrics_snapshot() -> RuntimeMetricsSnapshot"));
    assert!(prelude.contains("pub fn record_fast_path_hit()"));
    assert!(prelude.contains("pub fn record_fallback_dispatch()"));
    assert!(prelude.contains("pub fn record_component_violation(component_id: u64)"));
    assert!(prelude.contains("\"urn:shape:NodeShapeA\""));
    assert!(prelude.contains("\"urn:shape:PropertyShapeA\""));
    assert!(prelude.contains("Some(1"));
    assert!(prelude.contains("Some(2"));
}

#[test]
fn validators_use_shape_data_union_queries() {
    let ir = sample_shape_ir();
    let generated = generate_modules_from_shape_ir_with_backend(&ir, SrcGenBackend::Specialized)
        .expect("specialized codegen should succeed");

    let validators_property = generated
        .files
        .iter()
        .find(|(name, _)| name == "validators_property.rs")
        .map(|(_, content)| content.as_str())
        .expect("validators_property module must exist");
    assert!(validators_property.contains("fn validation_graphs("));
    assert!(validators_property.contains("NamedNode::new(SHAPE_GRAPH)"));
    assert!(validators_property.contains("for graph in validation_graphs(data_graph)?"));
    assert!(validators_property.contains("values.dedup();"));

    let validators_node = generated
        .files
        .iter()
        .find(|(name, _)| name == "validators_node.rs")
        .map(|(_, content)| content.as_str())
        .expect("validators_node module must exist");
    assert!(validators_node.contains("for graph in validation_graphs(data_graph)?"));
}
