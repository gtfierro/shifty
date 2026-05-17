use oxrdf::{Literal, NamedNode, Quad, Term};
use shifty_shacl_core::{
    analyze_program, lower_to_program, parse_quads,
    source::{RefreshMode, ShapeSource, SourceLoadOptions, load_with_ontoenv},
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/fixtures")
        .join(name)
}

#[test]
fn lowers_af_targets_into_advanced_target_algebra() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_target_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);

    assert!(!program.shapes.is_empty());
    assert!(program.targets.iter().any(|target| {
        matches!(
            target.expr,
            shifty_shacl_core::algebra::TargetExpr::Advanced { .. }
        )
    }));
    assert!(program.features.iter().any(|feature| matches!(
        feature,
        shifty_shacl_core::algebra::FeatureUse::AdvancedTargets
    )));
}

#[test]
fn direct_quad_parsing_preserves_custom_component_constraints() {
    let shape = NamedNode::new("urn:shape").unwrap();
    let property = NamedNode::new("urn:property").unwrap();
    let path = NamedNode::new("urn:path").unwrap();
    let activate = NamedNode::new("http://example.org/activate").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_property,
            Term::NamedNode(property.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property.clone(),
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            property,
            activate,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    assert_eq!(program.shapes.len(), 2);
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::GenericPredicate { predicate, .. }
            if predicate.as_str() == "http://example.org/activate"
        )
    }));
}

#[test]
fn analysis_reports_recursive_components() {
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let a = NamedNode::new("urn:a").unwrap();
    let b = NamedNode::new("urn:b").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            a.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            a,
            sh_node.clone(),
            Term::NamedNode(b.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            b,
            sh_node,
            Term::NamedNode(NamedNode::new("urn:a").unwrap()),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let analysis = analyze_program(&program);

    assert!(
        analysis
            .dependency_components
            .iter()
            .any(|component| component.recursive)
    );
}
