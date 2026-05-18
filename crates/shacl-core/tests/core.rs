use oxrdf::{Literal, NamedNode, Quad, Term};
use shifty_shacl_core::{
    analyze_program, lower_to_program, parse_quads, render_shape_program_dot,
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
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent { predicate, .. }
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

#[test]
fn lowers_property_paths_and_qualified_shapes() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let sh_property_shape = NamedNode::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_alt = NamedNode::new("http://www.w3.org/ns/shacl#alternativePath").unwrap();
    let sh_qvs = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap();
    let sh_qmin = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedMinCount").unwrap();
    let sh_qdisjoint =
        NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint").unwrap();
    let prop = NamedNode::new("urn:prop").unwrap();
    let thumb = NamedNode::new("urn:thumb").unwrap();
    let p1 = NamedNode::new("urn:p1").unwrap();
    let p2 = NamedNode::new("urn:p2").unwrap();
    let alt = oxrdf::BlankNode::new("a1").unwrap();
    let list1 = oxrdf::BlankNode::new("l1").unwrap();
    let list2 = oxrdf::BlankNode::new("l2").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            prop.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_property_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            thumb.clone(),
            rdf_type,
            Term::NamedNode(sh_property_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_path,
            Term::BlankNode(alt.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            alt.clone(),
            sh_alt,
            Term::BlankNode(list1.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1.clone(),
            rdf_first.clone(),
            Term::NamedNode(p1),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1,
            rdf_rest.clone(),
            Term::BlankNode(list2.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2.clone(),
            rdf_first,
            Term::NamedNode(p2),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2,
            rdf_rest,
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qvs,
            Term::NamedNode(thumb),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qmin,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            prop.clone(),
            sh_qdisjoint,
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let prop_shape = program
        .shapes
        .iter()
        .find(|shape| shape.source == Term::NamedNode(prop.clone()))
        .unwrap();

    assert!(matches!(
        prop_shape.path,
        Some(shifty_shacl_core::algebra::PropertyPath::Alternative(ref inner)) if inner.len() == 2
    ));
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::QualifiedValueShape {
                shape: Some(_),
                min_count: Some(1),
                disjoint: Some(true),
                ..
            }
        )
    }));
}

#[test]
fn rule_conditions_create_owner_dependencies() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let cond = NamedNode::new("urn:cond").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            cond.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type,
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::NamedNode(cond.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from("CONSTRUCT { } WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let program = lower_to_program(&doc);
    let owner_id = program.shape_index[&Term::NamedNode(owner).to_string()];
    let cond_id = program.shape_index[&Term::NamedNode(cond).to_string()];
    assert!(
        program.dependencies.iter().any(|edge| edge.from == owner_id
            && edge.to == cond_id
            && edge.kind == "rule_condition")
    );
}

#[test]
fn parse_separates_metadata_from_constraints() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdfs_label = NamedNode::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_class = NamedNode::new("http://www.w3.org/ns/shacl#targetClass").unwrap();
    let ex_activate = NamedNode::new("http://example.org/activate").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();
    let target_class = NamedNode::new("urn:Target").unwrap();

    let syntax = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            rdfs_label.clone(),
            Term::Literal(Literal::from("A shape label")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape.clone(),
            sh_target_class,
            Term::NamedNode(target_class),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            ex_activate.clone(),
            Term::Literal(Literal::from(true)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    let shape = syntax
        .shapes
        .iter()
        .find(|shape| !shape.targets.is_empty())
        .unwrap();
    assert!(
        shape
            .extras
            .iter()
            .any(|extra| extra.predicate == rdfs_label)
    );
    assert!(
        shape
            .constraints
            .iter()
            .any(|constraint| constraint.predicate == ex_activate)
    );
}

#[test]
fn core_feature_is_always_present() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();

    let doc = parse_quads(vec![Quad::new(
        shape,
        rdf_type,
        Term::NamedNode(sh_node_shape),
        oxrdf::GraphName::DefaultGraph,
    )]);
    let program = lower_to_program(&doc);

    assert!(
        program
            .features
            .iter()
            .any(|feature| matches!(feature, shifty_shacl_core::algebra::FeatureUse::Core))
    );
}

#[test]
fn discovers_inline_shapes_from_logical_lists_and_rule_conditions() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let rdf_first = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap();
    let rdf_rest = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap();
    let rdf_nil = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_or = NamedNode::new("http://www.w3.org/ns/shacl#or").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_path = NamedNode::new("http://www.w3.org/ns/shacl#path").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let sh_rule = NamedNode::new("http://www.w3.org/ns/shacl#rule").unwrap();
    let sh_sparql_rule = NamedNode::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap();
    let sh_condition = NamedNode::new("http://www.w3.org/ns/shacl#condition").unwrap();
    let sh_construct = NamedNode::new("http://www.w3.org/ns/shacl#construct").unwrap();
    let owner = NamedNode::new("urn:owner").unwrap();
    let path = NamedNode::new("urn:path").unwrap();
    let list1 = oxrdf::BlankNode::new("ll1").unwrap();
    let list2 = oxrdf::BlankNode::new("ll2").unwrap();
    let inline_node = oxrdf::BlankNode::new("inline-node").unwrap();
    let inline_prop = oxrdf::BlankNode::new("inline-prop").unwrap();
    let inline_cond = oxrdf::BlankNode::new("inline-cond").unwrap();
    let inline_cond_prop = oxrdf::BlankNode::new("inline-cond-prop").unwrap();
    let rule = NamedNode::new("urn:rule").unwrap();

    let syntax = parse_quads(vec![
        Quad::new(
            owner.clone(),
            rdf_type.clone(),
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_or,
            Term::BlankNode(list1.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1.clone(),
            rdf_first.clone(),
            Term::BlankNode(inline_node.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list1,
            rdf_rest.clone(),
            Term::BlankNode(list2.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2.clone(),
            rdf_first,
            Term::NamedNode(owner.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            list2,
            rdf_rest,
            Term::NamedNode(rdf_nil),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_node.clone(),
            sh_property.clone(),
            Term::BlankNode(inline_prop.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_prop.clone(),
            sh_path.clone(),
            Term::NamedNode(path.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_prop.clone(),
            sh_min_count.clone(),
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            owner.clone(),
            sh_rule,
            Term::NamedNode(rule.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            rdf_type,
            Term::NamedNode(sh_sparql_rule),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule.clone(),
            sh_condition,
            Term::BlankNode(inline_cond.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            rule,
            sh_construct,
            Term::Literal(Literal::from("CONSTRUCT { } WHERE { }")),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond.clone(),
            sh_property,
            Term::BlankNode(inline_cond_prop.clone()),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond_prop.clone(),
            sh_path,
            Term::NamedNode(path),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            inline_cond_prop,
            sh_min_count,
            Term::Literal(Literal::from(1)),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);

    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_node.clone()))
    );
    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_prop.clone()))
    );
    assert!(
        syntax
            .shapes
            .iter()
            .any(|shape| shape.subject == Term::BlankNode(inline_cond.clone()))
    );
}

#[test]
fn renders_graphviz_for_shape_program() {
    let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap();
    let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape").unwrap();
    let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode").unwrap();
    let shape = NamedNode::new("urn:shape").unwrap();
    let focus = NamedNode::new("urn:focus").unwrap();

    let doc = parse_quads(vec![
        Quad::new(
            shape.clone(),
            rdf_type,
            Term::NamedNode(sh_node_shape),
            oxrdf::GraphName::DefaultGraph,
        ),
        Quad::new(
            shape,
            sh_target_node,
            Term::NamedNode(focus),
            oxrdf::GraphName::DefaultGraph,
        ),
    ]);
    let program = lower_to_program(&doc);
    let dot = render_shape_program_dot(&program);

    assert!(dot.contains("digraph shacl_core"));
    assert!(dot.contains("shape_1"));
    assert!(dot.contains("target_1"));
}

#[test]
fn parses_constraint_component_definitions() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");

    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    assert_eq!(syntax.constraint_components.len(), 1);
    let component = &syntax.constraint_components[0];
    assert!(component.subject.to_string().contains("LengthComponent"));
    assert_eq!(component.parameters.len(), 2);
    assert_eq!(component.validators.len(), 1);
    assert!(component.validators[0].select.is_some());
}

#[test]
fn lowers_custom_component_attachments() {
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(fixture_path("af_default_shapes.ttl"))],
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

    assert_eq!(program.constraint_components.len(), 1);
    assert!(program.constraints.iter().any(|constraint| {
        matches!(
            &constraint.expr,
            shifty_shacl_core::algebra::ConstraintExpr::CustomComponent {
                predicate,
                component: Some(_),
                ..
            } if predicate.as_str() == "http://example.org/activate"
        )
    }));
}
